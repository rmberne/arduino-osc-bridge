# Arduino OSC Bridge (`arduino-osc-bridge`)

A lightweight, asynchronous Rust daemon that bridges an external Arduino hardware controller with **REAPER DAW** using
**Open Sound Control (OSC)** over UDP.

It provides zero-configuration USB auto-detection, reliable hardware reconnection, and bidirectional transport state
synchronization so status LEDs stay completely in sync with REAPER.

---

## Features

- **Bidirectional Transport Sync:** Tracks actual REAPER transport state (`/play` and `/stop` feedback) rather than
  relying on optimistic local state. Works seamlessly when playback is toggled via laptop keyboard or UI clicks.
- **Project Display Updates:** Shares REAPER project name (`/project/name` or `/title`), tempo (`/tempo/raw`) and
  transport (`/play` / `/stop`) feedback with the web display in real time.
- **Dynamic Serial Auto-Detection:** Automatically scans `/dev` for Arduino devices (`cu.usbmodem*`, `cu.usbserial*`,
  `ttyACM*`, `ttyUSB*`) with continuous background polling (3s interval) to recover gracefully from unplugged cables or
  board resets.
- **DTR/RTS Hardware Pulse:** Automatically pulses reset lines on serial connection to initialize Arduino boards
  cleanly.
- **Decoupled Architecture:** Clean Separation of Concerns (SoC)—the OSC manager deals strictly with DAW domain events
  (`TransportState`), while the Arduino manager handles hardware wire protocols (`b'1'`, `b'0'`).
- **Low Latency & Async Core:** Built on `tokio`, `rosc`, and `tokio-serial` for non-blocking I/O and minimal overhead.

---

## Control Mapping

### Hardware to REAPER (Outbound UDP `127.0.0.1:8000`)

| Arduino Command | REAPER Action / OSC Address | Description                        | Float Arg |
|:---------------:|:----------------------------|:-----------------------------------|:---------:|
|     **`s`**     | `/action/40044`             | Transport: Play/stop toggle        |   `1.0`   |
|     **`m`**     | `/action/6`                 | Track: Mute/unmute selected tracks |   `1.0`   |
|     **`n`**     | `/action/40861`             | Project tabs: Next project tab     |   `1.0`   |
|     **`p`**     | `/action/40862`             | Project tabs: Previous project tab |   `1.0`   |

### REAPER to Hardware (Inbound UDP `127.0.0.1:9000` & Serial Downlink)

 | Incoming OSC Address | Payload / Type | Internal Event                             | Arduino Serial Downlink |
|:---------------------|:---------------|:-------------------------------------------|:-----------------------:|
| `/play`              | Float > `0.0`  | `TransportState::Playing`                  |       Byte `b'1'`       |
| `/stop`              | Float > `0.0`  | `TransportState::Stopped`                  |       Byte `b'0'`       |
| `/tempo/raw`         | Positive Float (BPM) | Updates displayed BPM & logs beat interval (ms) |            —            |
| `/tempo`             | Normalized Float | Ignored; REAPER's `/tempo` feedback is not BPM |            —            |
| `/project/name`      | String         | Updates displayed project name             |            —            |
| `/title`             | String         | Updates displayed project name             |            —            |

---

## REAPER Setup

1. Open **REAPER Preferences** (`Cmd + ,` on macOS / `Ctrl + P` on Windows).
2. Navigate to **Control/OSC/Web** and click **Add** (or **Edit** an existing entry).
3. Configure the following settings:
    - **Control surface mode:** `OSC (Open Sound Control)`
    - **Pattern config:** `Default`
    - **Mode:** `Configure device IP+port`
    - **Listen port:** `8000` *(Where REAPER listens for commands sent by this daemon)*
    - **Device IP:** `127.0.0.1`
    - **Device port:** `9000` *(Where REAPER sends feedback to this daemon)*
4. Check **Allow binding messages to actions**.
5. Click **OK** and **Apply**.

---

## Building and Running

### Prerequisites

- Rust toolchain (`stable` channel)
- REAPER DAW running locally

### Running in Development

```zsh
RUST_LOG=info cargo run
```

---

## Building for Production

```zsh
cargo build --release
./target/release/arduino-osc-bridge
```

### Command-line options

| Flag              | Effect                                                                                                                                                  |
|:------------------|:--------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--no-arduino`    | Do not start the Arduino serial manager (no serial connection, no hardware commands forwarded to REAPER). REAPER feedback is still received and logged. |
| `--no-web-server` | Do not start the HTTP web server on port `3000`.                                                                                                        |
| `-h`, `--help`    | Print usage and exit.                                                                                                                                   |

Example: `cargo run -- --no-arduino` or `./target/release/arduino-osc-bridge --no-web-server`.

## Unit testing

Run the test suite to verify command mappings, OSC packet encoding/decoding, and domain state event dispatch:

```zsh
cargo test
```
