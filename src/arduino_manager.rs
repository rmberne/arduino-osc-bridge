//! Serial communication with the Arduino controller.
//!
//! This module is responsible for:
//! - Auto-detecting the Arduino's USB serial device path on macOS/Linux.
//! - Opening the port asynchronously via `tokio-serial`.
//! - Toggling DTR/RTS on connect so boards that reset on a fresh serial
//!   connection (e.g. most Arduino Uno/Leonardo clones) come up cleanly.
//! - Reading newline-delimited single-character ASCII commands and
//!   forwarding them to the rest of the application over an mpsc channel.
//! - Automatically reconnecting (polling every [`RECONNECT_INTERVAL`]) if the
//!   device disappears, is unplugged, or the board reboots.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, ReadHalf, WriteHalf};
use tokio::sync::{mpsc, watch, Mutex};
use tokio_serial::{SerialPort, SerialPortBuilderExt, SerialStream};
use tracing::{debug, error, info, warn};

use crate::osc_manager::TransportState;

/// Baud rate expected by the Arduino sketch.
pub const BAUD_RATE: u32 = 115_200;

/// How long to wait between (re)connection attempts.
pub const RECONNECT_INTERVAL: Duration = Duration::from_secs(3);

/// How long to hold the DTR/RTS lines low while resetting the board.
const RESET_PULSE: Duration = Duration::from_millis(100);

/// Known device file name prefixes for Arduino-compatible USB serial adapters
/// on macOS (`cu.usbmodem*`, `cu.usbserial*`) and Linux (`ttyACM*`, `ttyUSB*`).
const DEVICE_PREFIXES: &[&str] = &["cu.usbmodem", "cu.usbserial", "ttyACM", "ttyUSB"];

/// Returns `true` if `file_name` matches one of the known Arduino serial
/// device naming conventions.
pub fn matches_arduino_prefix(file_name: &str) -> bool {
    DEVICE_PREFIXES
        .iter()
        .any(|prefix| file_name.starts_with(prefix))
}

/// Scans `/dev` for the first device path that looks like an Arduino.
pub fn find_arduino_port() -> Option<String> {
    find_arduino_port_in(Path::new("/dev"))
}

/// Scans `dir` for the first device path that looks like an Arduino.
///
/// Split out from [`find_arduino_port`] so tests can point it at a temporary
/// fixture directory instead of the real `/dev`.
pub fn find_arduino_port_in(dir: &Path) -> Option<String> {
    let entries = fs::read_dir(dir).ok()?;

    let mut candidates: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .map(matches_arduino_prefix)
                .unwrap_or(false)
        })
        .collect();

    candidates.sort();
    candidates
        .into_iter()
        .next()
        .map(|p| p.to_string_lossy().into_owned())
}

/// Parses a single line of serial input into one of the supported
/// single-character ASCII commands, ignoring surrounding whitespace.
///
/// Returns `None` if the trimmed line isn't exactly one character, since the
/// protocol only defines single-character command lines.
pub fn parse_command_line(line: &str) -> Option<char> {
    let trimmed = line.trim();
    let mut chars = trimmed.chars();
    let first = chars.next()?;
    if chars.next().is_none() {
        Some(first)
    } else {
        None
    }
}

/// Opens the serial port at `path` for asynchronous I/O.
fn open_port(path: &str) -> tokio_serial::Result<SerialStream> {
    tokio_serial::new(path, BAUD_RATE)
        .timeout(Duration::from_millis(100))
        .open_native_async()
}

/// Pulses DTR and RTS low then high, which forces many Arduino boards to
/// perform a clean reset when a serial connection is (re)established.
async fn toggle_reset_lines(stream: &mut SerialStream) -> io::Result<()> {
    stream
        .write_data_terminal_ready(false)
        .map_err(to_io_error)?;
    stream.write_request_to_send(false).map_err(to_io_error)?;
    tokio::time::sleep(RESET_PULSE).await;
    stream
        .write_data_terminal_ready(true)
        .map_err(to_io_error)?;
    stream.write_request_to_send(true).map_err(to_io_error)?;
    Ok(())
}

fn to_io_error(err: tokio_serial::Error) -> io::Error {
    io::Error::other(err)
}

/// Runs the Arduino connection supervisor until `shutdown` signals true.
///
/// This continuously tries to locate and connect to an Arduino device,
/// streams parsed commands into `tx`, and transparently reconnects on any
/// error (unplugged cable, board reset, read error, etc). Whenever a
/// connection is established, the serial port is split into a reader (fed
/// into the command parser) and a writer (fed by `serial_rx`, carrying play
/// state bytes down to the Arduino).
pub async fn run(
    tx: mpsc::Sender<char>,
    serial_rx: mpsc::Receiver<u8>,
    mut shutdown: watch::Receiver<bool>,
) {
    // Shared across reconnects: each successful connection spawns a fresh
    // downlink task that locks this to drain bytes meant for the Arduino,
    // and releases the lock (via task abort) when the connection drops so
    // the next connection's downlink task can pick up where it left off.
    let serial_rx = Arc::new(Mutex::new(serial_rx));

    loop {
        if *shutdown.borrow() {
            break;
        }

        let Some(port_path) = find_arduino_port() else {
            warn!(
                retry_in = ?RECONNECT_INTERVAL,
                "No Arduino device found on any known serial path"
            );
            if wait_or_shutdown(&mut shutdown, RECONNECT_INTERVAL).await {
                break;
            }
            continue;
        };

        info!(port = %port_path, "Attempting to connect to Arduino");

        match open_port(&port_path) {
            Ok(mut stream) => {
                info!(port = %port_path, baud = BAUD_RATE, "Serial port opened");

                if let Err(e) = toggle_reset_lines(&mut stream).await {
                    warn!(error = %e, "Failed to toggle DTR/RTS lines, continuing anyway");
                }

                let (reader, writer) = tokio::io::split(stream);
                let downlink_handle = tokio::spawn(downlink_loop(writer, Arc::clone(&serial_rx)));

                let read_result = read_loop(reader, &tx, &mut shutdown).await;
                downlink_handle.abort();

                match read_result {
                    Ok(()) => info!("Serial read loop ended"),
                    Err(e) => warn!(port = %port_path, error = %e, "Lost connection to Arduino"),
                }
            }
            Err(e) => {
                error!(port = %port_path, error = %e, "Failed to open serial port");
            }
        }

        if *shutdown.borrow() {
            break;
        }

        info!(retry_in = ?RECONNECT_INTERVAL, "Reconnecting to Arduino");
        if wait_or_shutdown(&mut shutdown, RECONNECT_INTERVAL).await {
            break;
        }
    }

    info!("Arduino manager shut down");
}

/// Bridges domain-level [`TransportState`] events (as produced by
/// `osc_manager::run_inbound`) onto the hardware-specific single-byte wire
/// protocol this Arduino sketch expects: `b'1'` for playing, `b'0'` for
/// stopped. This is the only place in the codebase that knows about that
/// mapping; `osc_manager` stays hardware-agnostic.
pub async fn run_state_bridge(
    mut state_rx: mpsc::Receiver<TransportState>,
    serial_tx: mpsc::Sender<u8>,
) {
    while let Some(state) = state_rx.recv().await {
        let byte = match state {
            TransportState::Playing => b'1',
            TransportState::Stopped => b'0',
        };

        if serial_tx.send(byte).await.is_err() {
            warn!("Serial downlink channel closed, dropping transport state update");
            break;
        }
    }

    debug!("Transport state channel closed, stopping state bridge task");
}

/// Drains bytes meant for the Arduino (play/stop state sync) from
/// `serial_rx` and writes each one directly to `writer`, flushing after
/// every write so the Arduino sees it promptly.
async fn downlink_loop(
    mut writer: WriteHalf<SerialStream>,
    serial_rx: Arc<Mutex<mpsc::Receiver<u8>>>,
) {
    loop {
        let byte = {
            let mut rx = serial_rx.lock().await;
            rx.recv().await
        };

        let Some(byte) = byte else {
            debug!("Serial downlink channel closed, stopping downlink task");
            break;
        };

        if let Err(e) = writer.write_all(&[byte]).await {
            warn!(error = %e, "Failed to write byte to Arduino");
            break;
        }
        if let Err(e) = writer.flush().await {
            warn!(error = %e, "Failed to flush serial writer");
            break;
        }
    }
}

/// Reads newline-delimited commands from `reader` and forwards parsed
/// commands to `tx` until an I/O error occurs or shutdown is requested.
async fn read_loop(
    reader: ReadHalf<SerialStream>,
    tx: &mpsc::Sender<char>,
    shutdown: &mut watch::Receiver<bool>,
) -> io::Result<()> {
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        tokio::select! {
            result = reader.read_line(&mut line) => {
                let bytes_read = result?;
                if bytes_read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "serial port closed by peer",
                    ));
                }

                debug!(line = %line.trim(), "Received raw line from Arduino");

                match parse_command_line(&line) {
                    Some(command) => {
                        debug!(command = %command, "Parsed command from Arduino");
                        if tx.send(command).await.is_err() {
                            warn!("Command channel closed, dropping parsed command");
                        }
                    }
                    None => {
                        if !line.trim().is_empty() {
                            warn!(line = %line.trim(), "Ignoring malformed serial line");
                        }
                    }
                }
            }
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return Ok(());
                }
            }
        }
    }
}

/// Sleeps for `duration` unless a shutdown is signalled first.
///
/// Returns `true` if shutdown was requested during the wait.
async fn wait_or_shutdown(shutdown: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(duration) => false,
        result = shutdown.changed() => result.is_err() || *shutdown.borrow(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;

    #[test]
    fn matches_known_macos_prefixes() {
        assert!(matches_arduino_prefix("cu.usbmodem14101"));
        assert!(matches_arduino_prefix("cu.usbserial-A50285BI"));
    }

    #[test]
    fn matches_known_linux_prefixes() {
        assert!(matches_arduino_prefix("ttyACM0"));
        assert!(matches_arduino_prefix("ttyUSB1"));
    }

    #[test]
    fn rejects_unrelated_device_names() {
        assert!(!matches_arduino_prefix("tty.Bluetooth-Incoming-Port"));
        assert!(!matches_arduino_prefix("disk0"));
        assert!(!matches_arduino_prefix("null"));
        assert!(!matches_arduino_prefix(""));
    }

    #[test]
    fn finds_first_matching_device_in_directory() {
        let dir = tempdir();
        File::create(dir.join("null")).unwrap();
        File::create(dir.join("cu.usbmodem14101")).unwrap();
        File::create(dir.join("ttyACM0")).unwrap();

        let found = find_arduino_port_in(&dir).unwrap();
        assert!(found.ends_with("cu.usbmodem14101") || found.ends_with("ttyACM0"));

        cleanup(dir);
    }

    #[test]
    fn returns_none_when_no_device_present() {
        let dir = tempdir();
        File::create(dir.join("null")).unwrap();
        File::create(dir.join("disk0")).unwrap();

        assert!(find_arduino_port_in(&dir).is_none());

        cleanup(dir);
    }

    #[test]
    fn parses_single_character_commands() {
        assert_eq!(parse_command_line("s\n"), Some('s'));
        assert_eq!(parse_command_line("m\r\n"), Some('m'));
        assert_eq!(parse_command_line("  n  "), Some('n'));
        assert_eq!(parse_command_line("p"), Some('p'));
    }

    #[test]
    fn rejects_multi_character_or_empty_lines() {
        assert_eq!(parse_command_line("hello\n"), None);
        assert_eq!(parse_command_line("\n"), None);
        assert_eq!(parse_command_line(""), None);
        assert_eq!(parse_command_line("ss"), None);
    }

    #[tokio::test]
    async fn state_bridge_translates_transport_state_to_wire_bytes() {
        let (state_tx, state_rx) = mpsc::channel::<TransportState>(4);
        let (serial_tx, mut serial_rx) = mpsc::channel::<u8>(4);

        tokio::spawn(run_state_bridge(state_rx, serial_tx));

        state_tx.send(TransportState::Playing).await.unwrap();
        assert_eq!(serial_rx.recv().await, Some(b'1'));

        state_tx.send(TransportState::Stopped).await.unwrap();
        assert_eq!(serial_rx.recv().await, Some(b'0'));

        drop(state_tx);
        assert_eq!(serial_rx.recv().await, None);
    }

    fn tempdir() -> PathBuf {
        let mut dir = std::env::temp_dir();
        let unique = format!(
            "arduino-osc-bridge-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        dir.push(unique);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn cleanup(dir: PathBuf) {
        let _ = fs::remove_dir_all(dir);
    }
}
