/*
  433 MHz RF Module Receiver Demonstration 1
  RF-Rcv-Demo-1.ino
  Demonstrates 433 MHz RF Receiver Module
  Use with Transmitter Demonstration 1

  DroneBot Workshop 2018
  https://dronebotworkshop.com
*/

// Include dependant SPI Library 
#include <RF24.h> 
#include <printf.h>
#include <SPI.h> 

// NRF24
RF24 radio(7, 8); // CE, CSN
const byte address[6] = "00001";

const bool DEBUG = false;

const int SENSOR_IDS[] = {16, 15, 14, 13, 12, 11};
const String SENSOR_UUIDS[] = {"eef014ca-4261-40a8-aecd-ec41e466e5d0", "c4944883-1151-4263-9e7b-965e285e212c", "5f7cee1f-2e74-48b4-a53a-9be4bbe0abec", "c6f07270-c345-4e6d-9098-2c758efcc13f", "9ad754a1-4ae1-4a73-9658-734e19747617", "f10e6f83-0b15-45d6-9af0-332753fd8b9b"};

unsigned long timer = 0;

void setup()
{
    // Initialize ASK Object
    // Setup Serial Monitor
    Serial.begin(9600);
    radio.begin();
    radio.openReadingPipe(0, address);
    radio.setPALevel(RF24_PA_MAX);
    radio.startListening();
  
    printf_begin();
    radio.printDetails();
}

void loop()
{
  if (radio.available()) {
      unsigned long num;
      radio.read(&num, sizeof(unsigned long));
//      Serial.print("radio.read()::");
//      Serial.println(num);
  
      String message = longToMessage(num);
      //Serial.println( message );
      int sensorId = decodeSensorId(message);
      int messageId = decodeMessageId(message);
      float value = decodeValue(message);

      for (int i = 0; i < sizeof(SENSOR_IDS); i++) {
        if (SENSOR_IDS[i]==sensorId) {
          Serial.print(F("MSG://"));
          Serial.print(SENSOR_UUIDS[i]);
          Serial.print(F(";C="));
          Serial.println(value);
          Serial.flush();
          delay(100);
        }
      }
  
    }

    if (timer == 0) {
      timer=millis();
    }
    if (millis()-timer > 60000) {
      Serial.println("MSG://RECIEVER;C=PING");
      timer=millis();
    }
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
    Serial.print(F("[DEBUG] SensorId="));
    Serial.print(sensorId);
    Serial.print(F(", MessageId="));
    Serial.print(messageId);
    Serial.print(F(", Value="));
    Serial.print(value);
    Serial.print(F("->'"));
    Serial.print(res);
    Serial.println(F("'"));
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
    Serial.print(F("[DEBUG] messageToLong("));
    Serial.print(binary);
    Serial.print(F(")="));
    Serial.println(res);
  }
  return res;
}

String longToMessage(unsigned long value) {
  if (value > 16777215) {
    Serial.print(F("[ERROR] Value '"));
    Serial.print(value);
    Serial.println(F("' is too large"));
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
    Serial.print(F("[DEBUG] longToMessage("));
    Serial.print(value);
    Serial.print(F(")="));
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
    Serial.print(F("[ERROR] Unsigned int only, value is "));
    Serial.println(value);
    for (int index = 0; index < byteLength; index++) {
      res+="1";
    }
    return res;
  }
  if (byteLength == 4 && value > 15) {
    Serial.print(F("[ERROR] Value "));
    Serial.print(value);
    Serial.print(F(" is too large for "));
    Serial.print(byteLength);
    Serial.println(F(" representation"));
    return "1111";
  } else if (byteLength == 5 && value > 31) {
    Serial.print(F("[ERROR] Value "));
    Serial.print(value);
    Serial.print(F(" is too large for "));
    Serial.print(byteLength);
    Serial.println(F(" representation"));
    return "11111";
  } else if (byteLength == 6 && value > 63) {
    Serial.print(F("[ERROR] Value "));
    Serial.print(value);
    Serial.print(F(" is too large for "));
    Serial.print(byteLength);
    Serial.println(F(" representation"));
    return "111111";
  } else if (byteLength == 7 && value > 127) {
    Serial.print(F("[ERROR] Value "));
    Serial.print(value);
    Serial.print(F(" is too large for "));
    Serial.print(byteLength);
    Serial.println(F(" representation"));
    return "1111111";
  } else if (byteLength == 8 && value > 255) {
    Serial.print(F("[ERROR] Value "));
    Serial.print(value);
    Serial.print(F(" is too large for "));
    Serial.print(byteLength);
    Serial.println(F(" representation"));
    return "11111111";
  } else if (byteLength == 9 && value > 511) {
    Serial.print(F("[ERROR] Value "));
    Serial.print(value);
    Serial.print(F(" is too large for "));
    Serial.print(byteLength);
    Serial.println(F(" representation"));
    return "111111111";
  } else if (byteLength == 10 && value > 1023) {
    Serial.print(F("[ERROR] Value "));
    Serial.print(value);
    Serial.print(F(" is too large for "));
    Serial.print(byteLength);
    Serial.println(F(" representation"));
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
    Serial.print(F("[DEBUG] Encoded value of "));
    Serial.print(value);
    Serial.print(F(" with byte number "));
    Serial.print(byteLength);
    Serial.print(F(" is "));
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
    Serial.print(F("[DEBUG] decodeInt("));
    Serial.print(binary);
    Serial.print(F(")="));
    Serial.println(res);
  }
  return res;
}
