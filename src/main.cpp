#include <Arduino.h>
#include <ArduinoOTA.h>
#include <ArduinoJson.h>
#include <PubSubClient.h>
#include <WiFi.h>
#include <esp_bt.h>
#include <esp_bt_main.h>
#include <esp_gap_ble_api.h>
#include <esp_wifi.h>
#include <esp32-hal-bt.h>
#include <nvs_flash.h>

#include "secrets.h"

namespace {
constexpr char DEVICE_NAME[] = "esp32-smart-light-mqtt";
constexpr char FRIENDLY_NAME[] = "Bed Room Light";
constexpr uint8_t GROUP_ID = 1;
constexpr uint32_t TX_DURATION_MS = 500;
constexpr uint16_t MIN_MIREDS = 167;
constexpr uint16_t MAX_MIREDS = 333;
constexpr bool MQTT_RETAIN_STATE = true;

constexpr char AVAILABILITY_TOPIC[] = "lampsmart/bed_room_light/availability";
constexpr char STATE_TOPIC[] = "lampsmart/bed_room_light/state";
constexpr char COMMAND_TOPIC[] = "lampsmart/bed_room_light/set";
constexpr char PAIR_TOPIC[] = "lampsmart/bed_room_light/pair/set";
constexpr char UNPAIR_TOPIC[] = "lampsmart/bed_room_light/unpair/set";
constexpr char DISCOVERY_LIGHT_TOPIC[] = "homeassistant/light/esp32-smart-light-mqtt/bed_room_light/config";
constexpr char DISCOVERY_PAIR_TOPIC[] = "homeassistant/button/esp32-smart-light-mqtt/bed_room_light_pair/config";
constexpr char DISCOVERY_UNPAIR_TOPIC[] = "homeassistant/button/esp32-smart-light-mqtt/bed_room_light_unpair/config";

constexpr uint8_t CMD_PAIR = 0x28;
constexpr uint8_t CMD_UNPAIR = 0x45;
constexpr uint8_t CMD_TURN_ON = 0x10;
constexpr uint8_t CMD_TURN_OFF = 0x11;
constexpr uint8_t CMD_DIM = 0x21;

const char PACKET_BASE[32] = {
    0x1F, 0x02, 0x01, 0x01, 0x1B, 0x03, 0x71, 0x0F,
    0x55, 0xAA, 0x98, 0x43, static_cast<char>(0xAF), 0x0B, 0x46, 0x46,
    0x46, 0x00, 0x00, 0x00, 0x00, 0x00, static_cast<char>(0x83), 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};

const uint16_t CRC_TABLE[256] = {
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7,
    0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6,
    0x9339, 0x8318, 0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485,
    0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76D7, 0x66F6, 0x5695, 0x46B4,
    0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC,
    0x48C4, 0x58E5, 0x6886, 0x78A7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xC9CC, 0xD9ED, 0xE98E, 0xF9AF, 0x8948, 0x9969, 0xA90A, 0xB92B,
    0x5AF5, 0x4AD4, 0x7AB7, 0x6A96, 0x1A71, 0x0A50, 0x3A33, 0x2A12,
    0xDBFD, 0xCBDC, 0xFBBF, 0xEB9E, 0x9B79, 0x8B58, 0xBB3B, 0xAB1A,
    0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
    0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49,
    0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51, 0x0E70,
    0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78,
    0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F,
    0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E,
    0x02B1, 0x1290, 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xB5EA, 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D,
    0x34E2, 0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E, 0xC71D, 0xD73C,
    0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xD94C, 0xC96D, 0xF90E, 0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
    0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A,
    0x4A75, 0x5A54, 0x6A37, 0x7A16, 0x0AF1, 0x1AD0, 0x2AB3, 0x3A92,
    0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9,
    0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1,
    0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBFBA, 0x8FD9, 0x9FF8,
    0x6E17, 0x7E36, 0x4E55, 0x5E74, 0x2E93, 0x3EB2, 0x0ED1, 0x1EF0};

WiFiClient wifiClient;
PubSubClient mqttClient(wifiClient);
WiFiServer logServer(23);
WiFiClient logClient;

bool bleReady = false;
bool gapCallbackRegistered = false;
volatile bool advRawConfigured = false;
volatile bool advStarted = false;
volatile bool advStopped = true;

esp_ble_adv_params_t advertisingParams = {
    .adv_int_min = 0x20,
    .adv_int_max = 0x20,
    .adv_type = ADV_TYPE_NONCONN_IND,
    .own_addr_type = BLE_ADDR_TYPE_PUBLIC,
    .peer_addr = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
    .peer_addr_type = BLE_ADDR_TYPE_PUBLIC,
    .channel_map = ADV_CHNL_ALL,
    .adv_filter_policy = ADV_FILTER_ALLOW_SCAN_ANY_CON_ANY,
};

struct LightState {
  bool isOn = false;
  uint8_t brightness = 255;
  uint16_t colorTemp = 250;
} lightState;

void logPrint(const char *message) {
  Serial.print(message);
  if (logClient && logClient.connected()) {
    logClient.print(message);
  }
}

void logPrintln(const char *message = "") {
  Serial.println(message);
  if (logClient && logClient.connected()) {
    logClient.println(message);
  }
}

void logPrintf(const char *format, ...) {
  char buffer[256];
  va_list args;
  va_start(args, format);
  vsnprintf(buffer, sizeof(buffer), format, args);
  va_end(args);
  logPrint(buffer);
}

void handleLogClient() {
  if (logServer.hasClient()) {
    WiFiClient incoming = logServer.available();
    if (!logClient || !logClient.connected()) {
      logClient = incoming;
      logClient.print("\r\nConnected to ESP32 log stream\r\n");
    } else {
      incoming.print("Log stream already in use\r\n");
      incoming.stop();
    }
  }

  if (logClient && !logClient.connected()) {
    logClient.stop();
  }
}

void setupOta() {
  ArduinoOTA.setHostname(OTA_HOSTNAME);
  ArduinoOTA.setPassword(OTA_PASSWORD);
  ArduinoOTA
      .onStart([]() {
        logPrintf("OTA start: %s\n", ArduinoOTA.getCommand() == U_FLASH ? "firmware" : "filesystem");
      })
      .onEnd([]() {
        logPrintln("OTA complete");
      })
      .onProgress([](unsigned int progress, unsigned int total) {
        static unsigned int lastPercent = 101;
        const unsigned int percent = (progress * 100U) / total;
        if (percent != lastPercent && percent % 10 == 0) {
          lastPercent = percent;
          logPrintf("OTA progress: %u%%\n", percent);
        }
      })
      .onError([](ota_error_t error) {
        logPrintf("OTA error[%u]\n", static_cast<unsigned int>(error));
      });
  ArduinoOTA.begin();
}

char *bitReverse(char *buffer) {
  static char reversed[25];
  for (int i = 0; i < 25; ++i) {
    char value = 0;
    for (int bit = 0; bit < 8; ++bit) {
      value += (((buffer[i] & 0xFF) >> (7 - bit)) & 0x01) << bit;
    }
    reversed[i] = value;
  }
  return reversed;
}

char *bleWhitening(char *buffer) {
  static char whitened[38];
  int state = 83;
  for (int i = 0; i < 38; ++i) {
    int currentState = state;
    char value = 0;
    for (int bit = 0; bit < 8; ++bit) {
      int stateByte = currentState & 0xFF;
      value |= ((((stateByte & 64) >> 6) << bit) ^ (buffer[i] & 0xFF)) & (1 << bit);
      int shifted = stateByte << 1;
      int carry = (shifted >> 7) & 1;
      int rotated = (shifted & -2) | carry;
      currentState = ((rotated ^ (carry << 4)) & 16) | (rotated & -17);
    }
    whitened[i] = value;
    state = currentState;
  }
  return whitened;
}

char *bleWhiteningForPacket(char *buffer) {
  char scratch[38] = {};
  for (int i = 0; i < 25; ++i) {
    scratch[i + 13] = buffer[i];
  }

  static char whitenedPacket[25];
  char *whitened = bleWhitening(scratch);
  for (int i = 0; i < 25; ++i) {
    whitenedPacket[i] = whitened[i + 13];
  }
  return whitenedPacket;
}

uint16_t crc16(const char *buffer, int len, int offset) {
  uint16_t crc = 0xFFFF;
  for (int i = 0; i < len; ++i) {
    crc = CRC_TABLE[((crc >> 8) ^ static_cast<uint8_t>(buffer[offset + i])) & 0xFF] ^ (crc << 8);
  }
  return crc;
}

char *buildPacket(uint8_t command, uint8_t control0, uint8_t control1, uint8_t arg1, uint8_t arg2, uint8_t groupId) {
  char messageBase[25];
  static char packet[32];

  for (int i = 0; i < 25; ++i) {
    messageBase[i] = PACKET_BASE[i + 6];
  }

  messageBase[11] = static_cast<char>(command);
  messageBase[12] = static_cast<char>(control0);
  messageBase[13] = static_cast<char>((control1 & 0xF0) | (groupId & 0x0F));
  messageBase[14] = static_cast<char>(arg1);
  messageBase[15] = static_cast<char>(arg2);
  messageBase[17] = static_cast<char>(esp_random() & 0xFF);

  const uint16_t crc = crc16(messageBase, 12, 11);
  messageBase[23] = static_cast<char>((crc >> 8) & 0xFF);
  messageBase[24] = static_cast<char>(crc & 0xFF);

  char *reversed = bitReverse(messageBase);
  char *whitened = bleWhiteningForPacket(reversed);

  for (int i = 0; i < 6; ++i) {
    packet[i] = PACKET_BASE[i];
  }
  for (int i = 0; i < 25; ++i) {
    packet[i + 6] = whitened[i];
  }
  packet[31] = PACKET_BASE[31];
  return packet;
}

void getHostDeviceIdentifier(uint8_t &first, uint8_t &second) {
  uint8_t mac[6] = {0};
  esp_wifi_get_mac(WIFI_IF_STA, mac);
  const uint16_t hostCrc = crc16(reinterpret_cast<const char *>(mac), 6, 0);
  first = (hostCrc >> 8) & 0xFF;
  second = hostCrc & 0xFF;
}

void waitForFlag(volatile bool &flag, bool expected, uint32_t timeoutMs) {
  const uint32_t started = millis();
  while (flag != expected && (millis() - started) < timeoutMs) {
    delay(5);
  }
}

void gapCallback(esp_gap_ble_cb_event_t event, esp_ble_gap_cb_param_t *param) {
  switch (event) {
    case ESP_GAP_BLE_ADV_DATA_RAW_SET_COMPLETE_EVT:
      advRawConfigured = true;
      break;
    case ESP_GAP_BLE_ADV_START_COMPLETE_EVT:
      advStarted = (param->adv_start_cmpl.status == ESP_BT_STATUS_SUCCESS);
      break;
    case ESP_GAP_BLE_ADV_STOP_COMPLETE_EVT:
      advStopped = (param->adv_stop_cmpl.status == ESP_BT_STATUS_SUCCESS);
      break;
    default:
      break;
  }
}

bool isOkOrInvalidState(esp_err_t err) {
  return err == ESP_OK || err == ESP_ERR_INVALID_STATE;
}

void initBleAdvertising() {
  if (bleReady) {
    return;
  }

  esp_err_t err = nvs_flash_init();
  if (err == ESP_ERR_NVS_NO_FREE_PAGES || err == ESP_ERR_NVS_NEW_VERSION_FOUND) {
    ESP_ERROR_CHECK(nvs_flash_erase());
    err = nvs_flash_init();
  }
  if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
    ESP_ERROR_CHECK(err);
  }

  if (!btStarted() && !btStart()) {
    logPrintln("BT controller failed to start");
    return;
  }

  esp_err_t btErr = esp_bt_controller_mem_release(ESP_BT_MODE_CLASSIC_BT);
  if (!isOkOrInvalidState(btErr)) {
    ESP_ERROR_CHECK(btErr);
  }
  logPrintf("BT controller status: %d\n", esp_bt_controller_get_status());

  btErr = esp_bluedroid_init();
  if (!isOkOrInvalidState(btErr)) {
    ESP_ERROR_CHECK(btErr);
  }
  btErr = esp_bluedroid_enable();
  if (!isOkOrInvalidState(btErr)) {
    ESP_ERROR_CHECK(btErr);
  }
  if (!gapCallbackRegistered) {
    btErr = esp_ble_gap_register_callback(gapCallback);
    if (!isOkOrInvalidState(btErr)) {
      ESP_ERROR_CHECK(btErr);
    }
    gapCallbackRegistered = true;
  }

  btErr = esp_ble_gap_set_device_name(DEVICE_NAME);
  if (!isOkOrInvalidState(btErr)) {
    ESP_ERROR_CHECK(btErr);
  }

  logPrintf("Bluedroid status: %d\n", esp_bluedroid_get_status());
  delay(500);

  bleReady = true;
}

void sendLampCommand(uint8_t command, uint8_t arg1, uint8_t arg2) {
  initBleAdvertising();

  if (!bleReady) {
    logPrintln("BLE advertising not ready, skipping command");
    return;
  }

  logPrintf("sendLampCommand: bt=%d bluedroid=%d\n", esp_bt_controller_get_status(), esp_bluedroid_get_status());

  uint8_t host0 = 0;
  uint8_t host1 = 0;
  getHostDeviceIdentifier(host0, host1);
  uint8_t *packet = reinterpret_cast<uint8_t *>(buildPacket(command, host0, host1, arg1, arg2, GROUP_ID));

  advRawConfigured = false;
  advStarted = false;
  advStopped = false;

  esp_err_t err = esp_ble_gap_stop_advertising();
  if (err != ESP_OK && err != ESP_ERR_INVALID_STATE) {
    logPrintf("pre-stop advertising failed: 0x%x\n", err);
  }
  delay(20);

  err = esp_ble_gap_config_adv_data_raw(&packet[1], 31);
  if (err != ESP_OK) {
    logPrintf("esp_ble_gap_config_adv_data_raw failed: 0x%x\n", err);
    return;
  }
  waitForFlag(advRawConfigured, true, 250);
  if (!advRawConfigured) {
    logPrintln("Timed out waiting for raw advertising config callback");
    return;
  }
  err = esp_ble_gap_start_advertising(&advertisingParams);
  if (err != ESP_OK) {
    logPrintf("esp_ble_gap_start_advertising failed: 0x%x\n", err);
    return;
  }
  waitForFlag(advStarted, true, 250);
  if (!advStarted) {
    logPrintln("Timed out waiting for advertising start callback");
    return;
  }
  delay(TX_DURATION_MS);
  err = esp_ble_gap_stop_advertising();
  if (err != ESP_OK) {
    logPrintf("esp_ble_gap_stop_advertising failed: 0x%x\n", err);
    return;
  }
  waitForFlag(advStopped, true, 250);
}

uint8_t computeColdChannel(uint8_t brightness, uint16_t colorTemp) {
  const float span = static_cast<float>(MAX_MIREDS - MIN_MIREDS);
  const float warmRatio = constrain((static_cast<float>(colorTemp) - MIN_MIREDS) / span, 0.0f, 1.0f);
  return static_cast<uint8_t>(roundf(brightness * (1.0f - warmRatio)));
}

uint8_t computeWarmChannel(uint8_t brightness, uint16_t colorTemp) {
  const float span = static_cast<float>(MAX_MIREDS - MIN_MIREDS);
  const float warmRatio = constrain((static_cast<float>(colorTemp) - MIN_MIREDS) / span, 0.0f, 1.0f);
  return static_cast<uint8_t>(roundf(brightness * warmRatio));
}

void publishAvailability(const char *value) {
  mqttClient.publish(AVAILABILITY_TOPIC, value, true);
}

void publishState() {
  JsonDocument doc;
  doc["state"] = lightState.isOn ? "ON" : "OFF";
  doc["brightness"] = lightState.brightness;
  doc["color_mode"] = "color_temp";
  doc["color_temp"] = lightState.colorTemp;

  char payload[192];
  const size_t len = serializeJson(doc, payload, sizeof(payload));
  mqttClient.publish(STATE_TOPIC, reinterpret_cast<const uint8_t *>(payload), len, MQTT_RETAIN_STATE);
}

void publishDiscovery() {
  JsonDocument lightConfig;
  lightConfig["name"] = FRIENDLY_NAME;
  lightConfig["uniq_id"] = "esp32_smart_light_mqtt_bed_room_light";
  lightConfig["schema"] = "json";
  lightConfig["cmd_t"] = COMMAND_TOPIC;
  lightConfig["stat_t"] = STATE_TOPIC;
  lightConfig["avty_t"] = AVAILABILITY_TOPIC;
  lightConfig["pl_avail"] = "online";
  lightConfig["pl_not_avail"] = "offline";
  lightConfig["brightness"] = true;
  lightConfig["brightness_scale"] = 255;
  lightConfig["supported_color_modes"].add("color_temp");
  lightConfig["min_mireds"] = MIN_MIREDS;
  lightConfig["max_mireds"] = MAX_MIREDS;
  lightConfig["device"]["ids"].add(DEVICE_NAME);
  lightConfig["device"]["name"] = DEVICE_NAME;
  lightConfig["device"]["mdl"] = "ESP32 LampSmart Pro MQTT";
  lightConfig["device"]["mf"] = "Custom";
  lightConfig["device"]["sw"] = "1.0.0";

  char lightPayload[768];
  const size_t lightLen = serializeJson(lightConfig, lightPayload, sizeof(lightPayload));
  mqttClient.publish(DISCOVERY_LIGHT_TOPIC, reinterpret_cast<const uint8_t *>(lightPayload), lightLen, true);

  JsonDocument pairConfig;
  pairConfig["name"] = "Bed Room Light Pair";
  pairConfig["uniq_id"] = "esp32_smart_light_mqtt_bed_room_light_pair";
  pairConfig["cmd_t"] = PAIR_TOPIC;
  pairConfig["pl_prs"] = "PAIR";
  pairConfig["avty_t"] = AVAILABILITY_TOPIC;
  pairConfig["device"]["ids"].add(DEVICE_NAME);

  char pairPayload[384];
  const size_t pairLen = serializeJson(pairConfig, pairPayload, sizeof(pairPayload));
  mqttClient.publish(DISCOVERY_PAIR_TOPIC, reinterpret_cast<const uint8_t *>(pairPayload), pairLen, true);

  JsonDocument unpairConfig;
  unpairConfig["name"] = "Bed Room Light Unpair";
  unpairConfig["uniq_id"] = "esp32_smart_light_mqtt_bed_room_light_unpair";
  unpairConfig["cmd_t"] = UNPAIR_TOPIC;
  unpairConfig["pl_prs"] = "UNPAIR";
  unpairConfig["avty_t"] = AVAILABILITY_TOPIC;
  unpairConfig["device"]["ids"].add(DEVICE_NAME);

  char unpairPayload[384];
  const size_t unpairLen = serializeJson(unpairConfig, unpairPayload, sizeof(unpairPayload));
  mqttClient.publish(DISCOVERY_UNPAIR_TOPIC, reinterpret_cast<const uint8_t *>(unpairPayload), unpairLen, true);
}

void applyLightState() {
  if (!lightState.isOn) {
    logPrintln("Sending LampSmart OFF command");
    sendLampCommand(CMD_TURN_OFF, 0, 0);
    publishState();
    return;
  }

  const uint8_t cold = computeColdChannel(lightState.brightness, lightState.colorTemp);
  const uint8_t warm = computeWarmChannel(lightState.brightness, lightState.colorTemp);

  logPrintf("Sending LampSmart ON command, cold=%u warm=%u\n", cold, warm);
  sendLampCommand(CMD_TURN_ON, 0, 0);
  delay(50);
  sendLampCommand(CMD_DIM, cold, warm);
  publishState();
}

void pairLamp() {
  logPrintln("Sending LampSmart pair command");
  uint8_t host0 = 0;
  uint8_t host1 = 0;
  getHostDeviceIdentifier(host0, host1);
  sendLampCommand(CMD_PAIR, host0, host1);
}

void unpairLamp() {
  logPrintln("Sending LampSmart unpair command");
  uint8_t host0 = 0;
  uint8_t host1 = 0;
  getHostDeviceIdentifier(host0, host1);
  sendLampCommand(CMD_UNPAIR, host0, host1);
}

void handleJsonCommand(const JsonDocument &doc) {
  if (doc["state"].is<const char *>()) {
    const char *state = doc["state"];
    if (strcmp(state, "ON") == 0) {
      lightState.isOn = true;
    } else if (strcmp(state, "OFF") == 0) {
      lightState.isOn = false;
    }
  }

  if (doc["brightness"].is<uint16_t>()) {
    lightState.brightness = static_cast<uint8_t>(constrain(doc["brightness"].as<uint16_t>(), 0, 255));
    lightState.isOn = lightState.brightness > 0;
  }

  if (doc["color_temp"].is<uint16_t>()) {
    lightState.colorTemp = static_cast<uint16_t>(constrain(doc["color_temp"].as<uint16_t>(), MIN_MIREDS, MAX_MIREDS));
  }

  applyLightState();
}

void mqttCallback(char *topic, byte *payload, unsigned int length) {
  char message[256] = {};
  const unsigned int copyLength = min(length, static_cast<unsigned int>(sizeof(message) - 1));
  memcpy(message, payload, copyLength);
  message[copyLength] = '\0';

  logPrintf("MQTT message on %s: %s\n", topic, message);

  if (strcmp(topic, COMMAND_TOPIC) == 0) {
    if (strcmp(message, "ON") == 0) {
      lightState.isOn = true;
      applyLightState();
      return;
    }

    if (strcmp(message, "OFF") == 0) {
      lightState.isOn = false;
      applyLightState();
      return;
    }

    JsonDocument doc;
    const auto error = deserializeJson(doc, message);
    if (!error) {
      handleJsonCommand(doc);
    } else {
      logPrintf("Ignoring invalid JSON payload: %s\n", error.c_str());
    }
    return;
  }

  if (strcmp(topic, PAIR_TOPIC) == 0) {
    pairLamp();
    return;
  }

  if (strcmp(topic, UNPAIR_TOPIC) == 0) {
    unpairLamp();
  }
}

void ensureWifi() {
  if (WiFi.status() == WL_CONNECTED) {
    return;
  }

  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASSWORD);

  logPrintf("Connecting to Wi-Fi SSID %s", WIFI_SSID);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    logPrint(".");
  }
  logPrintf("\nWi-Fi connected, IP: %s\n", WiFi.localIP().toString().c_str());
}

void ensureMqtt() {
  while (!mqttClient.connected()) {
    ensureWifi();
    logPrint("Connecting to MQTT broker...");

    if (mqttClient.connect(DEVICE_NAME, MQTT_USER, MQTT_PASSWORD, AVAILABILITY_TOPIC, 0, true, "offline")) {
      logPrintln("connected");
      publishAvailability("online");
      mqttClient.subscribe(COMMAND_TOPIC);
      mqttClient.subscribe(PAIR_TOPIC);
      mqttClient.subscribe(UNPAIR_TOPIC);
      publishDiscovery();
      publishState();
      return;
    }

    logPrintf("failed, rc=%d. Retrying in 5 seconds.\n", mqttClient.state());
    delay(5000);
  }
}
}  // namespace

void setup() {
  Serial.begin(115200);
  delay(1000);
  logPrintln();
  logPrintln("LampSmart Pro MQTT bridge starting");

  initBleAdvertising();

  mqttClient.setServer(MQTT_HOST, MQTT_PORT);
  mqttClient.setCallback(mqttCallback);

  ensureWifi();
  logServer.begin();
  logServer.setNoDelay(true);
  setupOta();
  logPrintf("OTA ready at %s.local or %s\n", OTA_HOSTNAME, WiFi.localIP().toString().c_str());
  logPrintln("Telnet log stream ready on port 23");
  ensureMqtt();
}

void loop() {
  ensureWifi();
  ensureMqtt();
  handleLogClient();
  ArduinoOTA.handle();
  mqttClient.loop();
}
