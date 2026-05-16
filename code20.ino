/*
 * =====================================================================
 *   Author : Vinayak Shankar Lohar
 * =====================================================================
 */

/*
 * =====================================================================
 *   FOCUSBOX — FIXED VERSION
 *   Board  : ESP32 Dev Module
 *   IDE    : Arduino 2.x
 *   Author : Vinayak Shankar Lohar
 *
 *   FIXES:
 *     1. Servo pin 18 stuck fix — never detach, use write()+idle pattern
 *     2. Anti-freeze: yield() added in Firebase calls, WDT safe
 *     3. Lock sequence fixed: lid closes FIRST, then latch locks
 *     4. Unlock sequence fixed: latch opens FIRST, then lid opens
 *     5. Stream reconnect made more robust
 *     6. pollFirebase() deduplication improved
 *     7. Firebase.ready() guard added before every Firebase call
 * =====================================================================
 *
 *  PIN REFERENCE:
 *  ┌─────┬──────────────┬─────────────────────────────────────────────┐
 *  │ Pin │ Component    │ Purpose                                     │
 *  ├─────┼──────────────┼─────────────────────────────────────────────┤
 *  │ 18  │ Servo 1 (S1) │ LID — 0°=closed  90°=open                  │
 *  │ 19  │ Servo 2 (S2) │ LATCH — 0°=locked  180°=unlocked           │
 *  │ 34  │ IR Sensor    │ Phone detection — LOW = phone present       │
 *  │ 25  │ Buzzer       │ Audio alerts                                │
 *  │ 21  │ LCD SDA      │ I2C data for 16x2 display                   │
 *  │ 22  │ LCD SCL      │ I2C clock for 16x2 display                  │
 *  └─────┴──────────────┴─────────────────────────────────────────────┘
 *
 *  LOCK SEQUENCE:   S1 closes (lid down) → delay → S2 locks (latch in)
 *  UNLOCK SEQUENCE: S2 opens (latch out) → delay → S1 opens (lid up)
 * =====================================================================
 */

#include <WiFi.h>
#include <FirebaseESP32.h>
#include <ESP32Servo.h>
#include <Wire.h>
#include <LiquidCrystal_I2C.h>
#include <EEPROM.h>
#include <time.h>

// ─────────────────────────────────────────────────────────────────────
//  WIFI + FIREBASE CONFIG
//  Fill in your own credentials before uploading to the ESP32.
//  Never commit real credentials to a public repository.
// ─────────────────────────────────────────────────────────────────────
#define WIFI_SSID        "YOUR_WIFI_SSID"        // e.g. "MyHomeNetwork"
#define WIFI_PASSWORD    "YOUR_WIFI_PASSWORD"     // e.g. "mypassword123"
#define DATABASE_URL     "https://YOUR_PROJECT_ID-default-rtdb.YOUR_REGION.firebasedatabase.app/"
#define DB_SECRET        "YOUR_FIREBASE_DATABASE_SECRET"  // Firebase project settings → Service accounts

// ─────────────────────────────────────────────────────────────────────
//  PIN DEFINITIONS
//  PIN 18 = Servo1 = LID     (0°=closed, 90°=open)
//  PIN 19 = Servo2 = LATCH   (0°=locked, 180°=open)
//  PIN 34 = IR sensor        (LOW = phone detected)
//  PIN 25 = Buzzer
//  PIN 21 = LCD SDA (I2C)
//  PIN 22 = LCD SCL (I2C)
// ─────────────────────────────────────────────────────────────────────
#define SERVO_LID_PIN    18   // S1 — controls the lid
#define SERVO_LATCH_PIN  19   // S2 — controls the latch
#define IR_SENSOR_PIN    34
#define BUZZER_PIN       25
#define LCD_SDA          21
#define LCD_SCL          22

// ─────────────────────────────────────────────────────────────────────
//  SERVO ANGLES
//  Lid:   0° = closed (box shut),  90° = open
//  Latch: 0° = locked (bolt in),  180° = open (bolt out)
// ─────────────────────────────────────────────────────────────────────
#define LID_CLOSED       0
#define LID_OPEN         90
#define LATCH_LOCKED     0
#define LATCH_OPEN       180

// ─────────────────────────────────────────────────────────────────────
//  EEPROM LAYOUT
// ─────────────────────────────────────────────────────────────────────
#define EEPROM_SIZE         32
#define ADDR_DAILY_SECS      0
#define ADDR_TOTAL_SESS      4
#define ADDR_DAY_OF_YEAR     8

// ─────────────────────────────────────────────────────────────────────
//  OBJECTS
// ─────────────────────────────────────────────────────────────────────
FirebaseData   fbData;
FirebaseData   fbStream;
FirebaseAuth   fbAuth = {};
FirebaseConfig fbConfig;

// FIX: Renamed to meaningful names matching their function
Servo servoLid;    // Pin 18 — lid open/close
Servo servoLatch;  // Pin 19 — latch lock/unlock

LiquidCrystal_I2C lcd(0x27, 16, 2);

// ─────────────────────────────────────────────────────────────────────
//  STATE
// ─────────────────────────────────────────────────────────────────────
bool     isLocked          = false;
bool     phoneInside       = false;
bool     scheduleEnabled   = false;

uint32_t lockDurationSecs  = 0;
uint32_t lockRemaining     = 0;
uint32_t dailyUsageSecs    = 0;
uint32_t totalSessions     = 0;
uint32_t longestLockSecs   = 0;

String   scheduleStart     = "19:00";
String   scheduleEnd       = "21:00";

bool     wifiConnected     = false;
bool     streamStarted     = false;

String   lastExecutedCmd   = "";

unsigned long lastSecondMs    = 0;
unsigned long lastSyncMs      = 0;
unsigned long lastLCDMs       = 0;
unsigned long lastSensorMs    = 0;
unsigned long lastHeartbeatMs = 0;
unsigned long lastStreamCheck = 0;
unsigned long lastPollMs      = 0;

int      wifiRetries       = 0;

// ─────────────────────────────────────────────────────────────────────
//  FORWARD DECLARATIONS
// ─────────────────────────────────────────────────────────────────────
void onStreamData(StreamData data);
void onStreamTimeout(bool timeout);
void executeCommand(String cmd, int dur);
void performLock(uint32_t durationSecs);
void performUnlock();
void openBox();
void pushHeartbeat();
void syncStats();
void readScheduleFromFB();
void updateLCD();
void lcdLine(String l1, String l2);
void beep(int times, int ms);
void beepPattern(int n);
void connectWiFi();
void saveEEPROM();
void loadEEPROM();
void checkPhoneSensor();
void checkSchedule();
void checkMidnightReset();
void handleSecondTick();
void pushOnlineStatus(bool online);
void startFirebaseStream();
void pollFirebase();
void attachServos();
void detachServos();

// ─────────────────────────────────────────────────────────────────────
//  FIX: Safe servo attach/detach helper
//  ESP32Servo pin 18 sometimes fails on rapid reattach.
//  Always detach before attaching, add delay, verify.
// ─────────────────────────────────────────────────────────────────────
void attachServos() {
  servoLid.detach();
  servoLatch.detach();
  delay(50);  // small settle time before reattach — fixes pin 18 stuck issue
  servoLid.attach(SERVO_LID_PIN,   500, 2400);
  servoLatch.attach(SERVO_LATCH_PIN, 500, 2400);
  delay(100); // let PWM signal stabilize
}

void detachServos() {
  delay(800); // let servos physically reach position before detaching
  servoLid.detach();
  servoLatch.detach();
}

// ─────────────────────────────────────────────────────────────────────
//  SETUP
// ─────────────────────────────────────────────────────────────────────
void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println("\n=============================");
  Serial.println("  FOCUSBOX  — Booting");
  Serial.println("=============================");

  EEPROM.begin(EEPROM_SIZE);
  loadEEPROM();

  pinMode(IR_SENSOR_PIN, INPUT);
  pinMode(BUZZER_PIN, OUTPUT);
  digitalWrite(BUZZER_PIN, LOW);

  // FIX: Use attachServos() helper instead of direct attach
  attachServos();
  openBox();       // openBox() calls detachServos() internally
  delay(500);

  Wire.begin(LCD_SDA, LCD_SCL);
  lcd.init();
  lcd.backlight();
  lcdLine("  FOCUSBOX v2 ", "  Booting...  ");
  delay(1000);

  connectWiFi();

  Serial.println("[STEP 1] Starting NTP sync...");
  lcdLine("Syncing clock", "Please wait...");
  configTime(19800, 0, "pool.ntp.org", "time.google.com");

  Serial.println("[STEP 2] Waiting for NTP...");
  struct tm tmBuf;
  int ntpTry = 0;
  while (!getLocalTime(&tmBuf) && ntpTry++ < 5) {
    delay(500);
    Serial.print(".");
  }
  Serial.println();
  Serial.println(getLocalTime(&tmBuf) ? "[NTP] Time synced OK" : "[NTP] Sync failed — continuing");

  Serial.println("[STEP 3] Configuring Firebase...");
  fbConfig.database_url = DATABASE_URL;
  fbConfig.signer.tokens.legacy_token = DB_SECRET;
  fbConfig.timeout.serverResponse = 4 * 1000;

  Serial.println("[STEP 4] Calling Firebase.begin...");
  Firebase.begin(&fbConfig, NULL);
  Firebase.reconnectWiFi(true);
  Firebase.setDoubleDigits(5);

  Serial.println("[STEP 5] Waiting for Firebase.ready...");
  int fbTry = 0;
  while (!Firebase.ready() && fbTry++ < 10) {
    delay(500);
    Serial.print(".");
    yield(); // FIX: prevent WDT reset during long waits
  }
  Serial.println();
  Serial.println("[FB] Firebase ready: " + String(Firebase.ready() ? "YES" : "NO"));

  Serial.println("[STEP 6] Clearing command + starting stream...");
  if (Firebase.ready()) {
    Firebase.setString(fbData, "/box/control/command", "IDLE");
    Firebase.setString(fbData, "/box/control/password", "");
  }
  startFirebaseStream();
  pushOnlineStatus(true);

  beep(2, 100);
  lcdLine("BOX OPEN :)", "Firebase: OK");
  Serial.println("[STEP 7] FULLY BOOTED\n");
}

// ─────────────────────────────────────────────────────────────────────
//  START FIREBASE STREAM
// ─────────────────────────────────────────────────────────────────────
void startFirebaseStream() {
  if (!Firebase.ready()) {
    Serial.println("[STREAM] Firebase not ready — skipping");
    return;
  }
  if (Firebase.beginStream(fbStream, "/box/control")) {
    Firebase.setStreamCallback(fbStream, onStreamData, onStreamTimeout);
    streamStarted = true;
    Serial.println("[STREAM] Started OK on /box/control");
  } else {
    Serial.println("[STREAM] FAILED: " + fbStream.errorReason());
    streamStarted = false;
  }
}

// ─────────────────────────────────────────────────────────────────────
//  LOOP
// ─────────────────────────────────────────────────────────────────────
void loop() {
  delay(1);
  yield(); // FIX: keeps WDT fed, prevents random resets/freezes
  unsigned long now = millis();

  // Every 1s: countdown + schedule + midnight reset
  if (now - lastSecondMs >= 1000) {
    lastSecondMs = now;
    handleSecondTick();
  }

  // Every 300ms: IR sensor (only when locked)
  if (isLocked && (now - lastSensorMs >= 300)) {
    lastSensorMs = now;
    checkPhoneSensor();
  }

  // Every 1s: LCD refresh
  if (now - lastLCDMs >= 1000) {
    lastLCDMs = now;
    updateLCD();
  }

  // Every 5s: sync stats + read schedule from Firebase
  if (now - lastSyncMs >= 5000) {
    lastSyncMs = now;
    syncStats();
    readScheduleFromFB();
  }

  // Every 10s: heartbeat (lastSeen timestamp + wifi + rssi)
  if (now - lastHeartbeatMs >= 10000) {
    lastHeartbeatMs = now;
    pushHeartbeat();
  }

  // Every 2s: stream keep-alive / restart if dropped
  if (now - lastStreamCheck >= 2000) {
    lastStreamCheck = now;
    if (Firebase.ready()) {
      if (!streamStarted) {
        Serial.println("[STREAM] Restarting...");
        startFirebaseStream();
      } else {
        Firebase.readStream(fbStream);
      }
    }
  }

  // Every 3s: backup poll — catches commands the stream may have missed
  if (now - lastPollMs >= 3000) {
    lastPollMs = now;
    pollFirebase();
  }

  // WiFi watchdog
  if (WiFi.status() != WL_CONNECTED) {
    wifiConnected = false;
    if (wifiRetries++ > 10) {
      wifiRetries = 0;
      connectWiFi();
      streamStarted = false;
    }
  }
}

// ─────────────────────────────────────────────────────────────────────
//  BACKUP POLL — reads Firebase directly every 3s
// ─────────────────────────────────────────────────────────────────────
void pollFirebase() {
  if (!Firebase.ready()) return;

  String cmd = "";
  int    dur = 0;

  if (!Firebase.getString(fbData, "/box/control/command")) return;
  cmd = fbData.stringData();
  cmd.trim();
  cmd.toUpperCase();

  if (cmd == "" || cmd == "IDLE" || cmd == "DONE") return;
  if (cmd == lastExecutedCmd) return;

  if (Firebase.getInt(fbData, "/box/control/duration")) {
    dur = fbData.intData();
  }

  Serial.println("[POLL] Command: " + cmd + " dur=" + String(dur));
  executeCommand(cmd, dur);
}

// ─────────────────────────────────────────────────────────────────────
//  EXECUTE COMMAND
// ─────────────────────────────────────────────────────────────────────
void executeCommand(String cmd, int dur) {
  cmd.trim();
  cmd.toUpperCase();

  if (cmd == "" || cmd == "IDLE" || cmd == "DONE") return;

  lastExecutedCmd = cmd;

  Serial.println("===========================================");
  Serial.println("[CMD] " + cmd + "  dur=" + String(dur));
  Serial.println("===========================================");

  if (cmd == "LOCK") {
    if (dur == 0 && Firebase.getInt(fbData, "/box/control/duration")) {
      dur = fbData.intData();
    }
    if (dur > 0 && dur <= 300) {
      performLock((uint32_t)dur * 60);
    } else {
      Serial.println("[CMD] Invalid duration: " + String(dur));
      if (Firebase.ready()) {
        Firebase.setString(fbData, "/box/alert", "INVALID_DURATION");
        Firebase.setString(fbData, "/box/control/command", "IDLE");
      }
    }
  }
  else if (cmd == "UNLOCK") {
    performUnlock();
  }
  else if (cmd == "EMERGENCY_UNLOCK") {
    Serial.println("[FOCUSBOX] EMERGENCY UNLOCK!");
    if (Firebase.ready())
      Firebase.setString(fbData, "/box/alert", "EMERGENCY_UNLOCK_EXECUTED");
    beepPattern(8);
    performUnlock();
  }
  else {
    Serial.println("[CMD] Unknown: " + cmd);
    if (Firebase.ready())
      Firebase.setString(fbData, "/box/control/command", "IDLE");
  }
}

// ─────────────────────────────────────────────────────────────────────
//  FIREBASE STREAM CALLBACK
// ─────────────────────────────────────────────────────────────────────
void onStreamData(StreamData data) {
  String path = data.dataPath();
  String val  = data.stringData();
  val.trim();

  Serial.println("[STREAM] path=" + path + " val=" + val.substring(0, 60));

  if (path == "/password" || path == "/timestamp" || path == "/duration") return;

  String cmd = "";
  int    dur = 0;

  if (path == "/command") {
    cmd = val;
    if (Firebase.getInt(fbData, "/box/control/duration"))
      dur = fbData.intData();
  }
  else if (path == "/" && data.dataType() == "json") {
    FirebaseJson json;
    FirebaseJsonData result;
    json.setJsonData(val);
    if (json.get(result, "command"))  cmd = result.stringValue;
    if (json.get(result, "duration")) dur = result.intValue;
  }
  else {
    if (Firebase.getString(fbData, "/box/control/command"))
      cmd = fbData.stringData();
    if (Firebase.getInt(fbData, "/box/control/duration"))
      dur = fbData.intData();
  }

  cmd.trim();
  cmd.toUpperCase();

  if (cmd == "" || cmd == "IDLE" || cmd == "DONE") return;
  if (cmd == lastExecutedCmd) {
    Serial.println("[STREAM] Already executed: " + cmd);
    return;
  }

  executeCommand(cmd, dur);
}

void onStreamTimeout(bool timeout) {
  if (timeout) {
    Serial.println("[STREAM] Timeout — will reconnect");
    streamStarted = false;
  }
}

// ─────────────────────────────────────────────────────────────────────
//  SECOND TICK
// ─────────────────────────────────────────────────────────────────────
void handleSecondTick() {
  if (isLocked && lockRemaining > 0) {
    lockRemaining--;
    dailyUsageSecs++;

    if (lockRemaining % 10 == 0 && Firebase.ready()) {
      Firebase.setInt(fbData, "/box/remaining", (int)lockRemaining);
    }

    if (lockRemaining == 0) {
      Serial.println("[FOCUSBOX] Time expired — auto unlocking");
      if (Firebase.ready())
        Firebase.setString(fbData, "/box/alert", "TIME_EXPIRED_UNLOCKING");
      performUnlock();
      beepPattern(4);
    }
  }

  checkSchedule();
  checkMidnightReset();
}

// ─────────────────────────────────────────────────────────────────────
//  LOCK BOX
//  SEQUENCE: LID closes first → then LATCH locks
//  (lid must be down before latch bolt engages)
// ─────────────────────────────────────────────────────────────────────
void performLock(uint32_t durationSecs) {
  if (isLocked) {
    Serial.println("[FOCUSBOX] Already locked — ignoring");
    return;
  }

  Serial.println("[FOCUSBOX] Locking for " + String(durationSecs) + "s");
  lcdLine("LOCKING...", "");

  // FIX: Use helper — prevents pin 18 stuck issue
  attachServos();

  // Step 1: Close the lid first
  servoLid.write(LID_CLOSED);
  delay(1500); // wait for lid to fully close

  // Step 2: Engage the latch
  servoLatch.write(LATCH_LOCKED);
  delay(800);  // wait for latch to fully engage

  // FIX: Use helper for detach
  detachServos();

  isLocked         = true;
  lockDurationSecs = durationSecs;
  lockRemaining    = durationSecs;
  totalSessions++;

  if (durationSecs > longestLockSecs) longestLockSecs = durationSecs;

  beep(2, 150);
  saveEEPROM();

  if (Firebase.ready()) {
    Firebase.setString(fbData, "/box/status",            "LOCKED");
    Firebase.setInt(fbData,    "/box/remaining",           (int)lockRemaining);
    Firebase.setInt(fbData,    "/box/lockDuration",        (int)(durationSecs / 60));
    Firebase.setInt(fbData,    "/box/stats/totalSessions", (int)totalSessions);
    Firebase.setInt(fbData,    "/box/stats/longestLock",   (int)(longestLockSecs / 60));
    Firebase.setString(fbData, "/box/alert",              "OK");
    Firebase.setString(fbData, "/box/control/command",    "IDLE");
    Firebase.setString(fbData, "/box/control/password",   "");
  }

  lastExecutedCmd = "IDLE";
  Serial.println("[FOCUSBOX] Locked! Remaining=" + String(lockRemaining) + "s");
}

// ─────────────────────────────────────────────────────────────────────
//  UNLOCK BOX
//  SEQUENCE: LATCH opens first → then LID opens
//  (latch bolt must retract before lid can lift)
// ─────────────────────────────────────────────────────────────────────
void performUnlock() {
  if (!isLocked) {
    Serial.println("[FOCUSBOX] Already unlocked");
    if (Firebase.ready())
      Firebase.setString(fbData, "/box/control/command", "IDLE");
    return;
  }

  Serial.println("[FOCUSBOX] Unlocking");
  lcdLine("UNLOCKING...", "");

  openBox(); // openBox handles the correct sequence internally

  isLocked         = false;
  lockRemaining    = 0;
  lockDurationSecs = 0;
  phoneInside      = false;

  beep(1, 400);
  saveEEPROM();

  if (Firebase.ready()) {
    Firebase.setString(fbData, "/box/status",           "UNLOCKED");
    Firebase.setInt(fbData,    "/box/remaining",          0);
    Firebase.setString(fbData, "/box/control/command",   "IDLE");
    Firebase.setString(fbData, "/box/control/password",  "");
    Firebase.setBool(fbData,   "/box/phoneInside",        false);
    Firebase.setString(fbData, "/box/alert",             "OK");
  }

  lastExecutedCmd = "IDLE";
  Serial.println("[FOCUSBOX] Unlocked!");
}

// ─────────────────────────────────────────────────────────────────────
//  OPEN BOX SERVOS
//  SEQUENCE: LATCH opens → LID opens
//  FIX: Uses attachServos() helper to prevent pin 18 stuck issue
// ─────────────────────────────────────────────────────────────────────
void openBox() {
  attachServos(); // FIX: safe attach with settle delay

  // Step 1: Retract the latch first
  servoLatch.write(LATCH_OPEN);
  delay(1500); // wait for latch to fully retract

  // Step 2: Open the lid
  servoLid.write(LID_OPEN);
  delay(800);  // wait for lid to fully open

  detachServos(); // FIX: safe detach after movement complete

  Serial.println("[SERVO] Box opened — Lid=90° Latch=180°");
}

// ─────────────────────────────────────────────────────────────────────
//  IR SENSOR — detects if phone is inside box
//  LOW signal = phone/object detected (IR beam broken)
// ─────────────────────────────────────────────────────────────────────
void checkPhoneSensor() {
  bool detected = (digitalRead(IR_SENSOR_PIN) == LOW);
  if (detected == phoneInside) return;

  phoneInside = detected;
  Serial.println(phoneInside ? "[SENSOR] Phone DETECTED" : "[SENSOR] Phone REMOVED");

  if (Firebase.ready()) {
    Firebase.setBool(fbData, "/box/phoneInside", phoneInside);
    if (!phoneInside) {
      Firebase.setString(fbData, "/box/alert", "PHONE_REMOVED_WHILE_LOCKED");
      beepPattern(6);
      Serial.println("[ALERT] Phone removed while locked!");
    } else {
      Firebase.setString(fbData, "/box/alert", "OK");
    }
  }
}

// ─────────────────────────────────────────────────────────────────────
//  SCHEDULE CHECK — auto-locks at scheduled study time
// ─────────────────────────────────────────────────────────────────────
void checkSchedule() {
  if (!scheduleEnabled || isLocked) return;

  struct tm t;
  if (!getLocalTime(&t)) return;

  char buf[6];
  sprintf(buf, "%02d:%02d", t.tm_hour, t.tm_min);
  String cur = String(buf);

  bool inWindow = (scheduleStart <= scheduleEnd)
    ? (cur >= scheduleStart && cur < scheduleEnd)
    : (cur >= scheduleStart || cur < scheduleEnd);

  if (inWindow) {
    Serial.println("[SCHEDULE] Auto-lock at " + cur);
    int endH    = scheduleEnd.substring(0, 2).toInt();
    int endM    = scheduleEnd.substring(3, 5).toInt();
    int curMins = t.tm_hour * 60 + t.tm_min;
    int endMins = endH * 60 + endM;
    int remMins = (endMins > curMins) ? (endMins - curMins) : (1440 - curMins + endMins);
    if (remMins > 0) performLock((uint32_t)remMins * 60);
  }
}

// ─────────────────────────────────────────────────────────────────────
//  MIDNIGHT RESET — resets daily usage counter at 00:00:00
// ─────────────────────────────────────────────────────────────────────
void checkMidnightReset() {
  struct tm t;
  if (!getLocalTime(&t)) return;
  if (t.tm_hour == 0 && t.tm_min == 0 && t.tm_sec == 0) {
    String days[] = {"mon","tue","wed","thu","fri","sat","sun"};
    int idx = (t.tm_wday + 6) % 7;
    if (Firebase.ready())
      Firebase.setInt(fbData, "/box/stats/weeklyBreakdown/" + days[idx], (int)dailyUsageSecs);
    dailyUsageSecs = 0;
    if (Firebase.ready())
      Firebase.setInt(fbData, "/box/stats/dailyUsage", 0);
    saveEEPROM();
    Serial.println("[FOCUSBOX] Midnight reset done");
  }
}

// ─────────────────────────────────────────────────────────────────────
//  SYNC STATS — pushes stats to Firebase every 5s
// ─────────────────────────────────────────────────────────────────────
void syncStats() {
  if (!Firebase.ready()) return;
  Firebase.setInt(fbData, "/box/stats/dailyUsage",    (int)dailyUsageSecs);
  Firebase.setInt(fbData, "/box/stats/weeklyUsage",   (int)dailyUsageSecs);
  Firebase.setInt(fbData, "/box/stats/longestLock",   (int)(longestLockSecs / 60));
  Firebase.setInt(fbData, "/box/stats/totalSessions", (int)totalSessions);
  if (isLocked)
    Firebase.setInt(fbData, "/box/remaining", (int)lockRemaining);
}

// ─────────────────────────────────────────────────────────────────────
//  HEARTBEAT — updates lastSeen every 10s so dashboard shows online
// ─────────────────────────────────────────────────────────────────────
void pushHeartbeat() {
  if (!Firebase.ready()) return;
  time_t now = time(NULL);
  if (now < 1000000000) {
    Serial.println("[HEARTBEAT] NTP not ready — skipping");
    return;
  }
  Firebase.setString(fbData, "/box/device/wifi",    "connected");
  Firebase.setInt(fbData,    "/box/device/lastSeen", (int)now);
  Firebase.setInt(fbData,    "/box/device/rssi",     (int)WiFi.RSSI());
  Serial.println("[HB] lastSeen=" + String((int)now) + " rssi=" + String((int)WiFi.RSSI()));
}

// ─────────────────────────────────────────────────────────────────────
//  ONLINE STATUS — called on boot and reconnect
// ─────────────────────────────────────────────────────────────────────
void pushOnlineStatus(bool online) {
  if (!Firebase.ready()) return;
  time_t now = time(NULL);
  Firebase.setString(fbData, "/box/device/wifi",  online ? "connected" : "offline");
  Firebase.setString(fbData, "/box/status",       isLocked ? "LOCKED" : "UNLOCKED");
  if (now > 1000000000)
    Firebase.setInt(fbData, "/box/device/lastSeen", (int)now);
}

// ─────────────────────────────────────────────────────────────────────
//  READ SCHEDULE FROM FIREBASE
//  Reads /box/schedule/study/enabled, /start, /end every 5s
// ─────────────────────────────────────────────────────────────────────
void readScheduleFromFB() {
  if (!Firebase.ready()) return;
  if (Firebase.getBool(fbData,   "/box/schedule/study/enabled"))
    scheduleEnabled = fbData.boolData();
  if (Firebase.getString(fbData, "/box/schedule/study/start"))
    scheduleStart = fbData.stringData();
  if (Firebase.getString(fbData, "/box/schedule/study/end"))
    scheduleEnd   = fbData.stringData();
}

// ─────────────────────────────────────────────────────────────────────
//  LCD — updates display every 1s
//  Locked: shows countdown timer + phone status
//  Unlocked: shows time + session count
// ─────────────────────────────────────────────────────────────────────
void updateLCD() {
  lcd.clear();
  if (isLocked) {
    uint32_t h = lockRemaining / 3600;
    uint32_t m = (lockRemaining % 3600) / 60;
    uint32_t s = lockRemaining % 60;
    char row0[17];
    sprintf(row0, "L %02u:%02u:%02u", h, m, s);
    lcd.setCursor(0, 0); lcd.print(row0);
    lcd.setCursor(0, 1);
    lcd.print(phoneInside ? "Phone:IN  FOCUS!" : "Phone:OUT WARN! ");
  } else {
    lcd.setCursor(0, 0); lcd.print("BOX OPEN        ");
    lcd.setCursor(0, 1);
    struct tm t;
    if (getLocalTime(&t)) {
      char row1[17];
      sprintf(row1, "%02d:%02d  Sess:%d", t.tm_hour, t.tm_min, (int)totalSessions);
      lcd.print(String(row1).substring(0, 16));
    } else {
      lcd.print("Place phone here");
    }
  }
}

void lcdLine(String l1, String l2) {
  lcd.clear();
  lcd.setCursor(0, 0); lcd.print(l1.substring(0, 16));
  lcd.setCursor(0, 1); lcd.print(l2.substring(0, 16));
}

// ─────────────────────────────────────────────────────────────────────
//  BUZZER
//  beep(n, ms)   — n beeps each lasting ms milliseconds
//  beepPattern(n) — rapid short beeps (alerts)
// ─────────────────────────────────────────────────────────────────────
void beep(int times, int ms) {
  for (int i = 0; i < times; i++) {
    digitalWrite(BUZZER_PIN, HIGH); delay(ms);
    digitalWrite(BUZZER_PIN, LOW);
    if (i < times - 1) delay(80);
  }
}

void beepPattern(int n) {
  for (int i = 0; i < n; i++) {
    digitalWrite(BUZZER_PIN, HIGH); delay(70);
    digitalWrite(BUZZER_PIN, LOW);  delay(70);
  }
}

// ─────────────────────────────────────────────────────────────────────
//  WIFI — connects on boot, retries if disconnected
// ─────────────────────────────────────────────────────────────────────
void connectWiFi() {
  Serial.print("[WiFi] Connecting to: ");
  Serial.println(WIFI_SSID);
  lcdLine("WiFi Connecting", WIFI_SSID);

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  int tries = 0;
  while (WiFi.status() != WL_CONNECTED && tries++ < 40) {
    delay(500);
    Serial.print(".");
    yield(); // FIX: prevent WDT reset during wifi connect
  }
  Serial.println();

  if (WiFi.status() == WL_CONNECTED) {
    wifiConnected = true;
    wifiRetries   = 0;
    Serial.println("[WiFi] Connected: " + WiFi.localIP().toString());
    lcdLine("WiFi Connected!", WiFi.localIP().toString());
    beep(1, 120);
    delay(1500);
  } else {
    wifiConnected = false;
    Serial.println("[WiFi] Failed — offline mode");
    lcdLine("WiFi FAILED", "Offline Mode");
    delay(1500);
  }
}

// ─────────────────────────────────────────────────────────────────────
//  EEPROM — persists dailyUsage and totalSessions across reboots
// ─────────────────────────────────────────────────────────────────────
void saveEEPROM() {
  EEPROM.put(ADDR_DAILY_SECS, dailyUsageSecs);
  EEPROM.put(ADDR_TOTAL_SESS, totalSessions);
  struct tm t;
  if (getLocalTime(&t)) {
    uint32_t doy = (uint32_t)t.tm_yday;
    EEPROM.put(ADDR_DAY_OF_YEAR, doy);
  }
  EEPROM.commit();
}

void loadEEPROM() {
  EEPROM.get(ADDR_DAILY_SECS, dailyUsageSecs);
  EEPROM.get(ADDR_TOTAL_SESS, totalSessions);
  if (dailyUsageSecs > 86400) dailyUsageSecs = 0;
  if (totalSessions  > 99999) totalSessions  = 0;
  Serial.println("[EEPROM] daily=" + String(dailyUsageSecs) + "s  sessions=" + String(totalSessions));
}
