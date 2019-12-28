#include <RCSwitch.h>
#include <SPI.h> 
#include <Ethernet.h>

// Sensor RADIO
RCSwitch reciever = RCSwitch();

// 2326701c-43f7-4b78-ad44-f918e8e8bbb6 = CAVE
// 5ccbfff8-f217-41b9-9f11-65641a58f99e = EXTERIEUR
// e8170a8d-f059-49aa-bfac-56d6f4c499b6 = SALON

const String uuids[] = { "NO VALUE FOR 0", "5ccbfff8-f217-41b9-9f11-65641a58f99e", "2326701c-43f7-4b78-ad44-f918e8e8bbb6", "e8170a8d-f059-49aa-bfac-56d6f4c499b6" };
int last_message_ids[] = {-1, -1, -1, -1};

const char FOLDER[] = "geanges";

const bool DEBUG = false;

bool printed = false;

// ETHERNET

// Enter a MAC address for your controller below.
// Newer Ethernet shields have a MAC address printed on a sticker on the shield
byte mac[] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xFE, 0xED };

// if you don't want to use DNS (and reduce your sketch size)
// use the numeric IP instead of the name for the server:
IPAddress server(52, 47, 106, 177);  // numeric IP for Google (no DNS)
const char GET_REPORT_SERVER[] = "sensors.rattrapchair.org";
const char GET_REPORT_URI[] = "/report";
const char GET_PING_URI[] = "/ping";

// Set the static IP address to use if the DHCP fails to assign
IPAddress ip(192, 168, 8, 250);
IPAddress myDns(8, 8, 8, 8);

// Initialize the Ethernet client library
// with the IP address and port of the server
// that you want to connect to (port 80 is default for HTTP):
EthernetClient client;

// Variables to measure the speed
unsigned long beginMicros, endMicros;
unsigned long byteCount = 0;
bool printWebData = true;  // set to false for better speed measurement

unsigned long lastPingMillis = millis();
unsigned long lastReconnectMillis = millis();

int connectionFailureCount = 0;

void setup() {
  // Open serial communications and wait for port to open:
  Serial.begin(9600);
  while (!Serial) {
    ; // wait for serial port to connect. Needed for native USB port only
  }

  reciever.enableReceive(0);  // Receiver on interrupt 0 => that is pin #2

  // start the Ethernet connection:
  Serial.println(F("Initialize Ethernet with DHCP:"));
  if (Ethernet.begin(mac) == 0) {
    Serial.println(F("Failed to configure Ethernet using DHCP"));
    // Check for Ethernet hardware present
    if (Ethernet.hardwareStatus() == EthernetNoHardware) {
      Serial.println(F("Ethernet shield was not found.  Sorry, can't run without hardware. :("));
      while (true) {
        delay(1); // do nothing, no point running without Ethernet hardware
      }
    }
    if (Ethernet.linkStatus() == LinkOFF) {
      Serial.println(F("Ethernet cable is not connected."));
    }
    // try to congifure using IP address instead of DHCP:
    Ethernet.begin(mac, ip, myDns);
  } else {
    Serial.print(F("  DHCP assigned IP "));
    Serial.println(Ethernet.localIP());
  }
  // give the Ethernet shield a second to initialize:
  delay(1000);

  Serial.println(F("Sensor V2 Geange Gateway"));
  Serial.println();

  Serial.println(F("Performing ping"));
  performPing();
  
}

void loop() {
  if (connectionFailureCount > 5) {
    connectEthernet();
    connectionFailureCount = 0;
  }

  if (reciever.available()) {
  
/*    Serial.print("reciever.getReceivedValue()::");
    Serial.println(reciever.getReceivedValue());*/

    unsigned long num = reciever.getReceivedValue();


    reciever.resetAvailable();

    String message = longToMessage(num);
//    Serial.println( message );
    int sensorId = decodeSensorId(message);
    int messageId = decodeMessageId(message);
    float value = decodeValue(message);

    if (sensorId > 0 && sensorId <= 3 && last_message_ids[sensorId] != messageId) {
      sendData(uuids[sensorId], value);
      last_message_ids[sensorId] = messageId;
    } /* else {
      Serial.println("Ignoring message");
      Serial.print(sensorId);
      Serial.print(";");
      Serial.print(messageId);
      Serial.print(";");
      Serial.println(value);
    }*/
  }

  if ((millis() - lastPingMillis) >= 60000) {
    performPing();
    delay(1000);
    lastPingMillis = millis();
  }

  if ((millis() - lastReconnectMillis) >= 1200000) {
    connectEthernet();
    lastReconnectMillis = millis();
  }
}

void connectEthernet() {
  Serial.println(F("Initialize Ethernet with DHCP:"));
  if (Ethernet.begin(mac) == 0) {
    Serial.println(F("Failed to configure Ethernet using DHCP"));
    // Check for Ethernet hardware present
    if (Ethernet.hardwareStatus() == EthernetNoHardware) {
      Serial.println(F("Ethernet shield was not found.  Sorry, can't run without hardware. :("));
      while (true) {
        delay(1); // do nothing, no point running without Ethernet hardware
      }
    }
    if (Ethernet.linkStatus() == LinkOFF) {
      Serial.println(F("Ethernet cable is not connected."));
    }
    // try to congifure using IP address instead of DHCP:
    Ethernet.begin(mac, ip, myDns);
  } else {
    Serial.print(F("  DHCP assigned IP "));
    Serial.println(Ethernet.localIP());
  }
}

void performPing() {
  Serial.println(F("------------------------------------"));
  Serial.println(F("Performing ping"));
  Serial.print(F("Connecting to "));
  Serial.print(GET_REPORT_SERVER);
  Serial.println(F("..."));

  
  int httpFailureCount = 0;
  // if you get a connection, report back via serial:
  while (client.connect(GET_REPORT_SERVER, 80) != 1) {
    if (httpFailureCount >= 60) {
      client.flush();
      client.stop();
      connectionFailureCount++;
      return;
    }
    // if you didn't get a connection to the server:
    Serial.println(F("connection failed"));
    httpFailureCount++;
    delay(1000);
  }

  Serial.print(F("connected to "));
  Serial.println(client.remoteIP());
  // Make a HTTP request:
  client.print(F("GET "));
  client.print(GET_PING_URI);
  client.print(F("?f="));
  client.print(FOLDER);
  client.println(F(" HTTP/1.1"));
  client.print(F("Host: "));
  client.println(GET_REPORT_SERVER);
  client.println(F("Accept: */*"));
  client.println(F("User-Agent: ArduinoClient/GeangesV2"));
  client.println(F("Connection: close\r\n"));
  client.println();
  client.println();

  int connectLoop=0;
  while(client.connected()) {
    if (connectLoop > 3000) {
      Serial.println(F("TIMEOUT"));
      client.flush();
      client.stop();
      connectionFailureCount++;
      return;
    }
    delay(10);
    int len = client.available();
    while (client.available()) {
//      Serial.println(F("Read packet"));
      byte buff[80];
      if (len > 80) len = 80;
      client.read(buff, len);
      if (DEBUG) {
        Serial.write(buff, len);
      }
      len = client.available();
    }
    delay(1);
    connectLoop++;
  }
  if (!client.connected()) {
    endMicros = micros();
    Serial.println();
    Serial.println(F("disconnecting."));
    client.stop();
  }
  Serial.println();
  Serial.println(F("------------------------------------"));
  delay(1000);
}

void sendData(String sensorId, float value) {
  Serial.println(F("------------------------------------"));
  Serial.print(F("Uploading message sensorId="));
  Serial.print(sensorId);
  Serial.print(F(", value="));
  Serial.println(value);
  Serial.print(F("Connecting to "));
  Serial.print(GET_REPORT_SERVER);
  Serial.println("...");

  int httpFailureCount = 0;
  // if you get a connection, report back via serial:
  while (client.connect(GET_REPORT_SERVER, 80) != 1) {
    if (httpFailureCount >= 60) {
      client.flush();
      client.stop();
      connectionFailureCount++;
      return;
    }
    // if you didn't get a connection to the server:
    Serial.println(F("connection failed"));
    httpFailureCount++;
    delay(1000);
  }


  Serial.print(F("connected to "));
  Serial.println(client.remoteIP());
  // Make a HTTP request:
  client.print(F("GET "));
  client.print(GET_REPORT_URI);
  client.print(F("?f="));
  client.print(FOLDER);
  client.print(F("&s="));
  client.print(sensorId);
  client.print(F("&v="));
  client.print(value);
  client.println(F(" HTTP/1.1"));
  client.print(F("Host: "));
  client.println(GET_REPORT_SERVER);
  client.println(F("Accept: */*"));
  client.println(F("User-Agent: ArduinoClient/GeangesV2"));
  client.println(F("Connection: close\r\n"));
  client.println();
  client.println();

  int connectLoop=0;
  while(client.connected()) {
    if (connectLoop > 3000) {
      Serial.println(F("TIMEOUT"));
      client.flush();
      client.stop();
      connectionFailureCount++;
      return;
    }
    delay(10);
    int len = client.available();
    while (client.available()) {
//      Serial.println(F("Read packet"));
      byte buff[80];
      if (len > 80) len = 80;
      client.read(buff, len);
      if (DEBUG) {
        Serial.write(buff, len);
      }
      len = client.available();
    }
    delay(1);
    connectLoop++;
  }
  if (!client.connected()) {
    endMicros = micros();
    Serial.println();
    Serial.println(F("disconnecting."));
    client.stop();
  }
  Serial.println();
  Serial.println(F("------------------------------------"));
  delay(1000);
}

// Message structure
// 10101|01010101|10101010101
// |_SENS|_MSG|_FLOAT
// FLOAT is reprsented as follows
// 1|0101010101
// +/-|VALUE * 10
unsigned long encodeMessage(int sensorId, int messageId, float value) {
  String res = "";
//  Serial.println(res);
  res += encodeInt(sensorId, 5);
//  Serial.println(res);
  res += encodeInt(messageId, 8);

  if (value < 0) {
    res += "1";
  } else {
    res += "0";
  }

  res += encodeInt((int) (abs(value) * 10), 10);

  if (DEBUG) {
    Serial.print("[DEBUG] SensorId=");
    Serial.print(sensorId);
    Serial.print(", MessageId=");
    Serial.print(messageId);
    Serial.print(", Value=");
    Serial.print(value);
    Serial.print("->'");
    Serial.print(res);
    Serial.println("'");
  }

  return messageToLong(res);
}

unsigned long messageToLong(String binary) {
  unsigned long res = 0;
  for (int index = 0; index <= binary.length(); index++) {
    if (binary.charAt(index) == '1') {
      if (index == 0) {
        res = res + 1;
      } else if (index == 1) {
        res = res + 2;
      } else {
        res = res + pow(2, index) + 1;
      }
    }
  }
  if (DEBUG) {
    Serial.print("[DEBUG] messageToLong(");
    Serial.print(binary);
    Serial.print(")=");
    Serial.println(res);
  }
  return res;
}

String longToMessage(unsigned long value) {
  if (value > 16777215) {
    Serial.print("[ERROR] Value '");
    Serial.print(value);
    Serial.println("' is too large");
    return "111111111111111111111111";
  }
  String res = "000000000000000000000000";
  unsigned long allPowValues = 0;
  
  for (int index = 23; index >= 0; index--) {
    if ((value - allPowValues) >= base2Pow(index)) {
      res.setCharAt(index, '1');
      allPowValues+=base2Pow(index);
    }
  }
  if (DEBUG) {
    Serial.print("[DEBUG] longToMessage(");
    Serial.print(value);
    Serial.print(")=");
    Serial.println(res);
  }

  return res;
}

unsigned long base2Pow(int p) {
  if (p == 0) {
    return 1;
  }
  if (p == 1) {
    return 2;
  }
  return pow(2, p) + 1;
}

int decodeSensorId(String message) {
  return decodeInt(message.substring(0,5));
}

int decodeMessageId(String message) {
  return decodeInt(message.substring(5, 13));
}

float decodeValue(String message) {
  float tempValue = decodeInt(message.substring(14));
  if (message.charAt(13) == '1') {
    tempValue = -1 * tempValue;
  }
  float res = tempValue / 10;
  return res;
}

String encodeInt(int value, int byteLength) {
  String res = "";
  if (value < 0) {
    Serial.print("[ERROR] Unsigned int only, value is ");
    Serial.println(value);
    for (int index = 0; index < byteLength; index++) {
      res+="1";
    }
    return res;
  }
  if (byteLength == 4 && value > 15) {
    Serial.print("[ERROR] Value ");
    Serial.print(value);
    Serial.print(" is too large for ");
    Serial.print(byteLength);
    Serial.println(" representation");
    return "1111";
  } else if (byteLength == 5 && value > 31) {
    Serial.print("[ERROR] Value ");
    Serial.print(value);
    Serial.print(" is too large for ");
    Serial.print(byteLength);
    Serial.println(" representation");
    return "11111";
  } else if (byteLength == 6 && value > 63) {
    Serial.print("[ERROR] Value ");
    Serial.print(value);
    Serial.print(" is too large for ");
    Serial.print(byteLength);
    Serial.println(" representation");
    return "111111";
  } else if (byteLength == 7 && value > 127) {
    Serial.print("[ERROR] Value ");
    Serial.print(value);
    Serial.print(" is too large for ");
    Serial.print(byteLength);
    Serial.println(" representation");
    return "1111111";
  } else if (byteLength == 8 && value > 255) {
    Serial.print("[ERROR] Value ");
    Serial.print(value);
    Serial.print(" is too large for ");
    Serial.print(byteLength);
    Serial.println(" representation");
    return "11111111";
  } else if (byteLength == 9 && value > 511) {
    Serial.print("[ERROR] Value ");
    Serial.print(value);
    Serial.print(" is too large for ");
    Serial.print(byteLength);
    Serial.println(" representation");
    return "111111111";
  } else if (byteLength == 10 && value > 1023) {
    Serial.print("[ERROR] Value ");
    Serial.print(value);
    Serial.print(" is too large for ");
    Serial.print(byteLength);
    Serial.println(" representation");
    return "1111111111";
  }

  int bytes[byteLength];

  for (int index = 0; index < byteLength; index++) {
    bytes[index] = 0;
  }

  int currentValue = value;

  while (currentValue > 0) {
    if (currentValue >= 512) {
      bytes[9] = 1;
      currentValue = currentValue % 512;
    } else if (currentValue >= 256) {
      bytes[8] = 1;
      currentValue = currentValue % 256;
    } else if (currentValue >= 128) {
      bytes[7] = 1;
      currentValue = currentValue % 128;
    } else if (currentValue >= 64) {
      bytes[6] = 1;
      currentValue = currentValue % 64;
    } else if (currentValue >= 32) {
      bytes[5] = 1;
      currentValue = currentValue % 32;
    } else if (currentValue >= 16) {
      bytes[4] = 1;
      currentValue = currentValue % 16;
    } else if (currentValue >= 8) {
      bytes[3] = 1;
      currentValue = currentValue % 8;
    } else if (currentValue >= 4) {
      bytes[2] = 1;
      currentValue = currentValue % 4;
    } else if (currentValue >= 2) {
      bytes[1] = 1;
      currentValue = currentValue % 2;
      if (currentValue == 1) {
        bytes[0] = 1;
        currentValue = 0;
      }
    } else {
      if (currentValue == 1) {
        bytes[0] = 1;
      }
      currentValue = 0;
    }
  }
  for (int index = 0; index<byteLength;index++) {
    if (bytes[index] == 0) {
      res+="0";
    } else {
      res+="1";
    }
  }

  if (DEBUG) {
    Serial.print("[DEBUG] Encoded value of ");
    Serial.print(value);
    Serial.print(" with byte number ");
    Serial.print(byteLength);
    Serial.print(" is ");
    Serial.println(res);
  }
  return res;
}

int decodeInt(String binary) {
  int res = 0;
  for (int index = 0; index <= binary.length(); index++) {
    if (binary.charAt(index) == '1') {
//      Serial.print("2^");
//      Serial.print(index);
      if (index == 0) {
//        Serial.print("[");
//        Serial.print(res);
//        Serial.print("+1]");
        res = res + 1;
      } else if (index == 1) {
//        Serial.print("[");
//        Serial.print(res);
//        Serial.print("+2]");
        res = res + 2;
      } else {
//        Serial.print("[");
//        Serial.print(res);
//        Serial.print("+");
//        Serial.print(pow(2, index));
//        Serial.print("]");
        res = res + pow(2, index) + 1;
      }
//      Serial.print("+");
    }
  }
  if (DEBUG) {
    Serial.print("[DEBUG] decodeInt(");
    Serial.print(binary);
    Serial.print(")=");
    Serial.println(res);
  }
  return res;
}
