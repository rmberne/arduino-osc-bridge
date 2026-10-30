// --- PIN MAP ---
constexpr uint8_t GREEN_PIN = 2;
constexpr uint8_t RED_PIN = 3;
constexpr uint8_t YELLOW_PIN = 4;
constexpr uint8_t BLUE_PIN = 5;

constexpr uint8_t GREEN_LED = 8;
constexpr uint8_t RED_LED = 9;
constexpr uint8_t YELLOW_LED = 10;
constexpr uint8_t BLUE_LED = 11;

constexpr uint8_t btnPins[] = { GREEN_PIN, RED_PIN, YELLOW_PIN, BLUE_PIN };
constexpr uint8_t ledPins[] = { GREEN_LED, RED_LED, YELLOW_LED, BLUE_LED };

bool isPlaying = false;
unsigned long lastDebounceTime[4] = {0, 0, 0, 0};
constexpr unsigned long DEBOUNCE_DELAY_MS = 250;

void updateLEDs() {
  digitalWrite(GREEN_LED, HIGH); // Green (Play/Stop toggle button) always lit

  if (isPlaying) {
    digitalWrite(RED_LED, HIGH);   // Mute active
    digitalWrite(YELLOW_LED, LOW);  // Next Tab inactive
    digitalWrite(BLUE_LED, LOW);    // Prev Tab inactive
  } else {
    digitalWrite(RED_LED, LOW);
    digitalWrite(YELLOW_LED, HIGH);
    digitalWrite(BLUE_LED, HIGH);
  }
}

void setup() {
  Serial.begin(115200);

  for (int i = 0; i < 4; i++) {
    pinMode(btnPins[i], INPUT_PULLUP);
    pinMode(ledPins[i], OUTPUT);
  }
  updateLEDs();
}

void loop() {
  // 1. Process all available inbound serial bytes from Rust daemon
  while (Serial.available() > 0) {
    char state = Serial.read();
    if (state == '1') {
      isPlaying = true;
      updateLEDs();
    } else if (state == '0') {
      isPlaying = false;
      updateLEDs();
    }
  }

  // 2. Non-blocking button handling
  const char cmds[] = {'s', 'm', 'n', 'p'};
  unsigned long now = millis();

  for (int i = 0; i < 4; i++) {
    if (digitalRead(btnPins[i]) == LOW) {
      if (now - lastDebounceTime[i] < DEBOUNCE_DELAY_MS) {
        continue;
      }

      // Contextual button guards
      if ((btnPins[i] == YELLOW_PIN || btnPins[i] == BLUE_PIN) && isPlaying) {
        continue; // Prevent project tab switching while playing
      } 
      if (btnPins[i] == RED_PIN && !isPlaying) {
        continue; // Prevent mute action when playback is stopped
      }

      Serial.println(cmds[i]);
      lastDebounceTime[i] = now;
    }
  }
}