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

mod arduino_manager;
mod osc_manager;

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

#[tokio::main]
async fn main() {
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

    let arduino_task = tokio::spawn(arduino_manager::run(
        command_tx,
        serial_rx,
        shutdown_rx.clone(),
    ));

    let state_bridge_task = tokio::spawn(arduino_manager::run_state_bridge(state_rx, serial_tx));

    let outbound_task = tokio::spawn(osc_manager::run_outbound(
        command_rx,
        reaper_addr,
        shutdown_rx.clone(),
    ));

    let inbound_task = tokio::spawn(osc_manager::run_inbound(
        feedback_addr,
        play_state,
        state_tx,
        shutdown_rx.clone(),
    ));

    match tokio::signal::ctrl_c().await {
        Ok(()) => info!("Ctrl+C received, shutting down gracefully"),
        Err(e) => error!(error = %e, "Failed to listen for Ctrl+C, shutting down anyway"),
    }

    if shutdown_tx.send(true).is_err() {
        error!("Failed to broadcast shutdown signal, no active receivers");
    }

    let (arduino_result, state_bridge_result, outbound_result, inbound_result) = tokio::join!(
        arduino_task,
        state_bridge_task,
        outbound_task,
        inbound_task
    );

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
    match inbound_result {
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
