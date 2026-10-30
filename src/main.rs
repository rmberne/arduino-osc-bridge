//! `arduino-osc-bridge`: bridges an Arduino hardware controller (USB serial)
//! to Reaper DAW using OSC over UDP.
//!
//! Architecture:
//! - [`arduino_manager`] owns the serial connection, auto-detects the
//!   device, reconnects transparently, and parses single-character ASCII
//!   commands off the wire.
//! - [`osc_manager`] turns those commands into OSC messages sent to Reaper,
//!   and separately listens for OSC feedback coming back: `/tempo` updates
//!   are logged, and `/play` / `/stop` are used as the source of truth for
//!   transport state, emitted as domain-level [`osc_manager::TransportState`]
//!   events. `osc_manager` has no notion of hardware or wire protocols.
//! - [`arduino_manager::run_state_bridge`] translates those `TransportState`
//!   events into the single-byte wire protocol the Arduino sketch expects
//!   (`b'1'`/`b'0'`) and feeds them into the serial downlink.
//! - `main` wires everything together with `mpsc` channels (commands up,
//!   transport state events down, translated to serial bytes) and drives
//!   graceful shutdown via `Ctrl+C`.
//!
//! Command-line flags can disable parts of the program: `--no-arduino`
//! skips the Arduino manager (and the tasks that only exist to serve it),
//! `--no-web-server` skips the HTTP web server.

mod arduino_manager;
mod osc_manager;
mod web_server;

use std::net::SocketAddr;

use tokio::sync::{mpsc, watch};
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

/// Capacity of the channel carrying parsed Arduino commands to the OSC
/// outbound task. Generous enough to absorb bursts of button presses.
const COMMAND_CHANNEL_CAPACITY: usize = 64;

/// Capacity of the channel carrying hardware-specific serial bytes
/// (`b'1'`/`b'0'`) from the state bridge down to the Arduino.
const SERIAL_CHANNEL_CAPACITY: usize = 32;

/// Capacity of the channel carrying domain-level `TransportState` events
/// from the OSC inbound task to the Arduino state bridge.
const TRANSPORT_STATE_CHANNEL_CAPACITY: usize = 32;

const USAGE: &str = "Usage: arduino-osc-bridge [OPTIONS]

Options:
      --no-arduino     Do not start the Arduino serial manager
      --no-web-server  Do not start the HTTP web server
  -h, --help           Print this help and exit";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Options {
    arduino: bool,
    web_server: bool,
}

impl Options {
    fn parse<I>(args: I) -> Result<Self, String>
    where
        I: IntoIterator<Item = String>,
    {
        let mut options = Options {
            arduino: true,
            web_server: true,
        };
        for arg in args {
            match arg.as_str() {
                "--no-arduino" => options.arduino = false,
                "--no-web-server" => options.web_server = false,
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                other => return Err(format!("unknown argument '{other}'")),
            }
        }
        Ok(options)
    }
}

#[tokio::main]
async fn main() {
    let options = match Options::parse(std::env::args().skip(1)) {
        Ok(options) => options,
        Err(e) => {
            eprintln!("error: {e}\n\n{USAGE}");
            std::process::exit(2);
        }
    };

    init_tracing();

    info!("Starting arduino-osc-bridge");

    let reaper_addr: SocketAddr = osc_manager::REAPER_ADDR
        .parse()
        .expect("REAPER_ADDR must be a valid socket address");
    let feedback_addr: SocketAddr = osc_manager::FEEDBACK_ADDR
        .parse()
        .expect("FEEDBACK_ADDR must be a valid socket address");

    let (command_tx, command_rx) = mpsc::channel::<char>(COMMAND_CHANNEL_CAPACITY);
    let (serial_tx, serial_rx) = mpsc::channel::<u8>(SERIAL_CHANNEL_CAPACITY);
    let (state_tx, state_rx) =
        mpsc::channel::<osc_manager::TransportState>(TRANSPORT_STATE_CHANNEL_CAPACITY);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let play_state = osc_manager::new_play_state();

    let arduino_tasks = if options.arduino {
        let arduino_task = tokio::spawn(arduino_manager::run(
            command_tx,
            serial_rx,
            shutdown_rx.clone(),
        ));

        let state_bridge_task =
            tokio::spawn(arduino_manager::run_state_bridge(state_rx, serial_tx));

        let outbound_task = tokio::spawn(osc_manager::run_outbound(
            command_rx,
            reaper_addr,
            shutdown_rx.clone(),
        ));

        Some((arduino_task, state_bridge_task, outbound_task))
    } else {
        info!("Arduino manager disabled");
        // Nothing consumes transport state without the Arduino, so drain it
        // to keep the inbound task from blocking on a full channel.
        let mut state_rx = state_rx;
        tokio::spawn(async move { while state_rx.recv().await.is_some() {} });
        None
    };

    let inbound_task = tokio::spawn(osc_manager::run_inbound(
        feedback_addr,
        play_state,
        state_tx,
        shutdown_rx.clone(),
    ));

    if options.web_server {
        tokio::spawn(web_server::run_web_server(shutdown_rx.clone()));
    } else {
        info!("Web server disabled");
    }

    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("Ctrl+C received, shutting down gracefully"),
        Err(e) => error!(error = %e, "Failed to listen for Ctrl+C, shutting down anyway"),
    }

    if shutdown_tx.send(true).is_err() {
        error!("Failed to broadcast shutdown signal, no active receivers");
    }

    if let Some((arduino_task, state_bridge_task, outbound_task)) = arduino_tasks {
        let (arduino_result, state_bridge_result, outbound_result) =
            tokio::join!(arduino_task, state_bridge_task, outbound_task);

        if let Err(e) = arduino_result {
            error!(error = %e, "Arduino manager task panicked");
        }
        if let Err(e) = state_bridge_result {
            error!(error = %e, "Arduino state bridge task panicked");
        }
        match outbound_result {
            Ok(Err(e)) => error!(error = %e, "OSC outbound task exited with error"),
            Err(e) => error!(error = %e, "OSC outbound task panicked"),
            Ok(Ok(())) => {}
        }
    }

    match inbound_task.await {
        Ok(Err(e)) => error!(error = %e, "OSC inbound task exited with error"),
        Err(e) => error!(error = %e, "OSC inbound task panicked"),
        Ok(Ok(())) => {}
    }

    info!("arduino-osc-bridge stopped");
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Options, String> {
        Options::parse(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn defaults_enable_everything() {
        assert_eq!(
            parse(&[]),
            Ok(Options {
                arduino: true,
                web_server: true,
            })
        );
    }

    #[test]
    fn no_arduino_disables_arduino_only() {
        assert_eq!(
            parse(&["--no-arduino"]),
            Ok(Options {
                arduino: false,
                web_server: true,
            })
        );
    }

    #[test]
    fn no_web_server_disables_web_server_only() {
        assert_eq!(
            parse(&["--no-web-server"]),
            Ok(Options {
                arduino: true,
                web_server: false,
            })
        );
    }

    #[test]
    fn flags_can_be_combined() {
        assert_eq!(
            parse(&["--no-arduino", "--no-web-server"]),
            Ok(Options {
                arduino: false,
                web_server: false,
            })
        );
    }

    #[test]
    fn unknown_argument_is_an_error() {
        assert_eq!(
            parse(&["--bogus"]),
            Err("unknown argument '--bogus'".to_string())
        );
    }
}
