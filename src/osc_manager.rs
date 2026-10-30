use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use rosc::{OscMessage, OscPacket, OscType};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, watch};
use tracing::{debug, error, info, warn};

pub const REAPER_ADDR: &str = "127.0.0.1:8000";
pub const FEEDBACK_ADDR: &str = "127.0.0.1:9000";

const ACTION_PLAY_STOP: &str = "/action/40044";
const ACTION_TOGGLE_MUTE: &str = "/action/6";
const ACTION_NEXT_TAB: &str = "/action/40861";
const ACTION_PREV_TAB: &str = "/action/40862";
const ADDR_TEMPO: &str = "/tempo";
const ADDR_PLAY: &str = "/play";
const ADDR_STOP: &str = "/stop";

pub type PlayState = Arc<AtomicBool>;

pub fn new_play_state() -> PlayState {
    Arc::new(AtomicBool::new(false))
}

/// Domain-level transport event derived from Reaper's `/play` and `/stop`
/// feedback. This is intentionally hardware-agnostic: `osc_manager` knows
/// nothing about serial bytes, Arduinos, or any other downstream protocol.
/// It's up to consumers (e.g. [`crate::arduino_manager`]) to translate this
/// into whatever wire format their hardware expects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportState {
    Playing,
    Stopped,
}

/// Maps a single-character Arduino command to the OSC address(es) and
/// arguments that should be sent to Reaper.
///
/// Transport state (play/stop) is no longer tracked optimistically here:
/// `'s'` always triggers Reaper's play/stop toggle action and the actual
/// transport state is learned back from Reaper's `/play` and `/stop`
/// feedback (see [`run_inbound`]).
pub fn command_to_osc(command: char) -> Vec<(&'static str, Vec<OscType>)> {
    match command {
        's' => vec![(ACTION_PLAY_STOP, vec![OscType::Float(1.0)])],
        'm' => vec![(ACTION_TOGGLE_MUTE, vec![OscType::Float(1.0)])],
        'n' => vec![(ACTION_NEXT_TAB, vec![OscType::Float(1.0)])],
        'p' => vec![(ACTION_PREV_TAB, vec![OscType::Float(1.0)])],
        _ => vec![],
    }
}

pub fn build_osc_packets(command: char) -> Vec<OscPacket> {
    command_to_osc(command)
        .into_iter()
        .map(|(addr, args)| {
            OscPacket::Message(OscMessage {
                addr: addr.to_string(),
                args,
            })
        })
        .collect()
}

pub fn extract_tempo(msg: &OscMessage) -> Option<f32> {
    if msg.addr != ADDR_TEMPO {
        return None;
    }
    msg.args.first().cloned().and_then(OscType::float)
}

pub fn beat_interval_ms(bpm: f32) -> f32 {
    60_000.0 / bpm
}

/// Returns `true` if `msg` carries a float argument greater than `0.0`,
/// which is how Reaper signals a "triggered" boolean-style OSC message.
fn is_positive_trigger(msg: &OscMessage) -> bool {
    msg.args
        .first()
        .cloned()
        .and_then(OscType::float)
        .map(|value| value > 0.0)
        .unwrap_or(false)
}

pub async fn run_outbound(
    mut rx: mpsc::Receiver<char>,
    target_addr: SocketAddr,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    info!(target = %target_addr, "OSC outbound socket ready");

    loop {
        tokio::select! {
            maybe_command = rx.recv() => {
                let Some(command) = maybe_command else {
                    info!("Command channel closed, stopping OSC outbound task");
                    break;
                };
                send_command(&socket, target_addr, command).await;
            }
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    info!("Shutdown signal received, stopping OSC outbound task");
                    break;
                }
            }
        }
    }

    Ok(())
}

async fn send_command(socket: &UdpSocket, target_addr: SocketAddr, command: char) {
    let packets = build_osc_packets(command);
    if packets.is_empty() {
        warn!(command = %command, "Received unmapped Arduino command, ignoring");
        return;
    }

    for packet in packets {
        match rosc::encoder::encode(&packet) {
            Ok(buf) => match socket.send_to(&buf, target_addr).await {
                Ok(_) => {
                    debug!(command = %command, target = %target_addr, "Forwarded command to Reaper")
                }
                Err(e) => error!(error = %e, "Failed to send OSC packet to Reaper"),
            },
            Err(e) => error!(error = ?e, "Failed to encode OSC packet"),
        }
    }
}

/// Listens for OSC feedback from Reaper and reacts to it:
/// - `/tempo` is logged (BPM + beat interval).
/// - `/play` (with a positive float arg) marks the transport as playing and
///   emits [`TransportState::Playing`] on `state_tx`.
/// - `/stop` (with a positive float arg) marks the transport as stopped and
///   emits [`TransportState::Stopped`] on `state_tx`.
///
/// `state_tx` carries domain-level events only; this module has no notion
/// of how (or whether) those events get relayed to any hardware.
pub async fn run_inbound(
    listen_addr: SocketAddr,
    play_state: PlayState,
    state_tx: mpsc::Sender<TransportState>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let socket = UdpSocket::bind(listen_addr).await?;
    info!(addr = %listen_addr, "Listening for OSC feedback from Reaper");

    let mut buf = [0u8; 1024];
    loop {
        tokio::select! {
            result = socket.recv_from(&mut buf) => {
                let (size, src) = result?;
                debug!(bytes = size, from = %src, "Received UDP packet");
                match rosc::decoder::decode_udp(&buf[..size]) {
                    Ok((_, packet)) => handle_packet(packet, &play_state, &state_tx).await,
                    Err(e) => warn!(error = ?e, "Failed to decode OSC packet from Reaper"),
                }
            }
            result = shutdown.changed() => {
                if result.is_err() || *shutdown.borrow() {
                    info!("Shutdown signal received, stopping OSC inbound task");
                    break;
                }
            }
        }
    }

    Ok(())
}

fn handle_packet<'a>(
    packet: OscPacket,
    play_state: &'a PlayState,
    state_tx: &'a mpsc::Sender<TransportState>,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
    Box::pin(async move {
        match packet {
            OscPacket::Message(msg) => handle_message(msg, play_state, state_tx).await,
            OscPacket::Bundle(bundle) => {
                for nested in bundle.content {
                    handle_packet(nested, play_state, state_tx).await;
                }
            }
        }
    })
}

async fn handle_message(
    msg: OscMessage,
    play_state: &PlayState,
    state_tx: &mpsc::Sender<TransportState>,
) {
    match msg.addr.as_str() {
        ADDR_TEMPO => match extract_tempo(&msg) {
            Some(bpm) => {
                let interval = beat_interval_ms(bpm);
                info!(
                    bpm,
                    beat_interval_ms = interval,
                    "Received tempo update from Reaper"
                );
            }
            None => warn!(addr = %msg.addr, "Received /tempo message without a float argument"),
        },
        ADDR_PLAY if is_positive_trigger(&msg) => {
            play_state.store(true, Ordering::Relaxed);
            send_transport_state(state_tx, TransportState::Playing).await;
        }
        ADDR_STOP if is_positive_trigger(&msg) => {
            play_state.store(false, Ordering::Relaxed);
            send_transport_state(state_tx, TransportState::Stopped).await;
        }
        _ => debug!(addr = %msg.addr, args = ?msg.args, "Received unhandled OSC message"),
    }
}

async fn send_transport_state(state_tx: &mpsc::Sender<TransportState>, state: TransportState) {
    if state_tx.send(state).await.is_err() {
        warn!(?state, "Transport state channel closed, dropping event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rosc::{OscMessage, OscPacket, OscType};

    #[test]
    fn maps_play_stop_command_with_float_arg() {
        let messages = command_to_osc('s');
        assert_eq!(
            messages,
            vec![(ACTION_PLAY_STOP, vec![OscType::Float(1.0)])]
        );
    }

    #[test]
    fn maps_mute_command_with_float_arg() {
        let messages = command_to_osc('m');
        assert_eq!(
            messages,
            vec![(ACTION_TOGGLE_MUTE, vec![OscType::Float(1.0)])]
        );
    }

    #[test]
    fn maps_next_tab_command_with_float_arg() {
        let messages = command_to_osc('n');
        assert_eq!(
            messages,
            vec![(ACTION_NEXT_TAB, vec![OscType::Float(1.0)])]
        );
    }

    #[test]
    fn maps_previous_tab_command_with_float_arg() {
        let messages = command_to_osc('p');
        assert_eq!(
            messages,
            vec![(ACTION_PREV_TAB, vec![OscType::Float(1.0)])]
        );
    }

    #[test]
    fn rejects_unmapped_commands() {
        assert!(command_to_osc('z').is_empty());
        assert!(command_to_osc('1').is_empty());
        assert!(command_to_osc(' ').is_empty());
    }

    #[test]
    fn builds_one_packet_per_mapped_command() {
        let packets = build_osc_packets('s');
        assert_eq!(packets.len(), 1);
        match &packets[0] {
            OscPacket::Message(msg) => {
                assert_eq!(msg.addr, ACTION_PLAY_STOP);
                assert_eq!(msg.args, vec![OscType::Float(1.0)]);
            }
            _ => panic!("expected an OSC message packet"),
        }
    }

    #[test]
    fn builds_no_packets_for_unmapped_commands() {
        assert!(build_osc_packets('z').is_empty());
    }

    #[test]
    fn decodes_tempo_message_from_mock_bytes() {
        let packet = OscPacket::Message(OscMessage {
            addr: "/tempo".to_string(),
            args: vec![OscType::Float(128.0)],
        });
        let bytes = rosc::encoder::encode(&packet).expect("encoding should succeed");

        let (_, decoded) = rosc::decoder::decode_udp(&bytes).expect("decoding should succeed");
        match decoded {
            OscPacket::Message(msg) => {
                let bpm = extract_tempo(&msg).expect("bpm should be present");
                assert!((bpm - 128.0).abs() < f32::EPSILON);
                assert!((beat_interval_ms(bpm) - 468.75).abs() < 0.01);
            }
            _ => panic!("expected an OSC message packet"),
        }
    }

    #[test]
    fn extract_tempo_ignores_other_addresses() {
        let msg = OscMessage {
            addr: "/play".to_string(),
            args: vec![OscType::Float(1.0)],
        };
        assert_eq!(extract_tempo(&msg), None);
    }

    #[test]
    fn extract_tempo_handles_missing_argument() {
        let msg = OscMessage {
            addr: "/tempo".to_string(),
            args: vec![],
        };
        assert_eq!(extract_tempo(&msg), None);
    }

    #[test]
    fn extract_tempo_handles_wrong_argument_type() {
        let msg = OscMessage {
            addr: "/tempo".to_string(),
            args: vec![OscType::String("not-a-float".to_string())],
        };
        assert_eq!(extract_tempo(&msg), None);
    }

    #[test]
    fn beat_interval_matches_known_values() {
        assert!((beat_interval_ms(120.0) - 500.0).abs() < f32::EPSILON);
        assert!((beat_interval_ms(60.0) - 1000.0).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn play_feedback_sets_play_state_and_emits_playing_event() {
        let play_state = new_play_state();
        let (state_tx, mut state_rx) = mpsc::channel::<TransportState>(4);

        let msg = OscMessage {
            addr: "/play".to_string(),
            args: vec![OscType::Float(1.0)],
        };
        handle_message(msg, &play_state, &state_tx).await;

        assert!(play_state.load(Ordering::Relaxed));
        assert_eq!(state_rx.recv().await, Some(TransportState::Playing));
    }

    #[tokio::test]
    async fn stop_feedback_clears_play_state_and_emits_stopped_event() {
        let play_state = new_play_state();
        play_state.store(true, Ordering::Relaxed);
        let (state_tx, mut state_rx) = mpsc::channel::<TransportState>(4);

        let msg = OscMessage {
            addr: "/stop".to_string(),
            args: vec![OscType::Float(1.0)],
        };
        handle_message(msg, &play_state, &state_tx).await;

        assert!(!play_state.load(Ordering::Relaxed));
        assert_eq!(state_rx.recv().await, Some(TransportState::Stopped));
    }

    #[tokio::test]
    async fn play_feedback_with_non_positive_value_is_ignored() {
        let play_state = new_play_state();
        let (state_tx, mut state_rx) = mpsc::channel::<TransportState>(4);

        let msg = OscMessage {
            addr: "/play".to_string(),
            args: vec![OscType::Float(0.0)],
        };
        handle_message(msg, &play_state, &state_tx).await;

        assert!(!play_state.load(Ordering::Relaxed));
        drop(state_tx);
        assert_eq!(state_rx.recv().await, None);
    }
}
