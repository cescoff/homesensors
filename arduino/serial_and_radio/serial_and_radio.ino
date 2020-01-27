#include <RF24.h> 
#include <printf.h>
#include <SPI.h> 
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Wire.h> 
#include <SoftwareSerial.h>

const bool DEBUG = false;

const byte address[6] = "00001";
RF24 radio(7, 8); // CE, CSN

#define ONE_WIRE_BUS1 4
#define ONE_WIRE_BUS2 9
#define ONE_WIRE_BUS3 5
#define ONE_WIRE_BUS4 6

OneWire oneWire1(ONE_WIRE_BUS1);
OneWire oneWire2(ONE_WIRE_BUS2);
OneWire oneWire3(ONE_WIRE_BUS3);
OneWire oneWire4(ONE_WIRE_BUS4);
DallasTemperature bathRoomSensor(&oneWire1);
DallasTemperature heaterSensor(&oneWire2);
DallasTemperature heatingSensor(&oneWire3);
DallasTemperature heatingReturnSensor(&oneWire4);
float bathroomCelcius=0;
float heatingCelcius=0;
float heatingReturnCelcius=0;
float heaterCelcius=0;

float previousHeatingCelcius=0;
float previousHeaterCelcius=0;

/*
const char* BATHROOM_SENSOR_UUID = "eef014ca-4261-40a8-aecd-ec41e466e5d0";
const char* HEATER_SENSOR_UUID = "c4944883-1151-4263-9e7b-965e285e212c";
const char* HEATING_SENSOR_UUID = "5f7cee1f-2e74-48b4-a53a-9be4bbe0abec";
*/

const int BATHROOM_SENSOR_ID = 16;
const int HEATER_SENSOR_ID = 15;
const int HEATING_SENSOR_ID = 14;
const int HEATING_RETURN_SENSOR_ID = 13;
const int EXTERNAL_SENSOR_ID = 12;

const int HEAT_BURN_SENSOR_ID=11;

int messageId = 1;

unsigned long lastSerialValueMillis = 0;

String inputString = "";         // a String to hold incoming data
bool stringComplete = false;  // whether the string is complete

unsigned long serialMessage = 0;
bool updated = false;

void setup() {
  Serial.begin(115200);

    // Initialize ASK Object
  bathRoomSensor.begin();
  heaterSensor.begin();
  heatingSensor.begin();
  heatingReturnSensor.begin();

  radio.begin();
  radio.openWritingPipe(address);
  radio.setPALevel(RF24_PA_MAX);
  radio.stopListening();

  printf_begin();
  radio.printDetails();

  delay(5000);
}


void loop() {
      unsigned long message = 0;

      float heatBurn=1.0;
  
      bathRoomSensor.requestTemperatures(); 
      bathroomCelcius=bathRoomSensor.getTempCByIndex(0);    
      if (bathroomCelcius > -20) {
        message = encodeMessage(BATHROOM_SENSOR_ID, messageId, bathroomCelcius);
        Serial.print("Bathroom temperature is ");
        Serial.print(bathroomCelcius);
        Serial.println("C");
        if (radio.isChipConnected()) {
          if(radio.write(&message, sizeof(unsigned long))) {
            Serial.print("Message ");
            Serial.print(message);
            Serial.println(" sent");
          } else {
            Serial.println("Failed to sent message");
          }
        } else {
          Serial.println("Message will not be sent by NF24, because hardware is NOT connected");
        }
        delay(1000);
      } else {
        Serial.println("Bathroom sensor is not connected");
      }
      
      heaterSensor.requestTemperatures(); 
      heaterCelcius=heaterSensor.getTempCByIndex(0);
      if (heaterCelcius > -20) {
        Serial.print("Heater temperature is ");
        Serial.print(heaterCelcius);
        Serial.println("C");
        message = encodeMessage(HEATER_SENSOR_ID, messageId, heaterCelcius);
        if (radio.isChipConnected()) {
          if(radio.write(&message, sizeof(unsigned long))) {
            Serial.print("Message ");
            Serial.print(message);
            Serial.println(" sent");
          } else {
            Serial.println("Failed to sent message");
          }
        } else {
          Serial.println("Message will not be sent by NF24, because hardware is NOT connected");
        }
        delay(1000);
      } else {
        Serial.println("Heater sensor is not connected");
      }

      heatingSensor.requestTemperatures(); 
      heatingCelcius=heatingSensor.getTempCByIndex(0);    
      if (heatingCelcius > -20) {
        Serial.print("Heating temperature is ");
        Serial.print(heatingCelcius);
        Serial.println("C");
        message = encodeMessage(HEATING_SENSOR_ID, messageId, heatingCelcius);
        if (radio.isChipConnected()) {
          if(radio.write(&message, sizeof(unsigned long))) {
            Serial.print("Message ");
            Serial.print(message);
            Serial.println(" sent");
          } else {
            Serial.println("Failed to sent message");
          }
        } else {
          Serial.println("Message will not be sent by NF24, because hardware is NOT connected");
        }
        delay(1000);
      } else {
        Serial.println("Heating sensor is not connected");
      }
  
      heatingReturnSensor.requestTemperatures();
      heatingReturnCelcius=heatingReturnSensor.getTempCByIndex(0);
      if (heatingReturnCelcius > -20) {
        Serial.print("HeatingReturn temperature is ");
        Serial.print(heatingReturnCelcius);
        Serial.println("C");
        message = encodeMessage(HEATING_RETURN_SENSOR_ID, messageId, heatingReturnCelcius);
        if (radio.isChipConnected()) {
          if(radio.write(&message, sizeof(unsigned long))) {
            Serial.print("Message ");
            Serial.print(message);
            Serial.println(" sent");
          } else {
            Serial.println("Failed to sent message");
          }
        } else {
          Serial.println("Message will not be sent by NF24, because hardware is NOT connected");
        }
        delay(1000);
        if (previousHeatingCelcius > heatingCelcius) {
          heatBurn = 0.0;
        }
        if (previousHeaterCelcius > heaterCelcius) {
          heatBurn = 0.0;
        }
  
        message = encodeMessage(HEAT_BURN_SENSOR_ID, messageId, heatBurn);
        if (radio.isChipConnected()) {
          if(radio.write(&message, sizeof(unsigned long))) {
            Serial.print("Message ");
            Serial.print(message);
            Serial.println(" sent");
          } else {
            Serial.println("Failed to sent message");
          }
        } else {
          Serial.println("Message will not be sent by NF24, because hardware is NOT connected");
        }
        previousHeatingCelcius=heatingCelcius;
        previousHeaterCelcius=heaterCelcius;
        delay(1000);
      } else {
        Serial.println("HeatingReturn sensor is not connected");
      }

      messageId=(messageId + 1) % 255;
      
      Serial.print(F("Sent values bathroom="));
      Serial.print(bathroomCelcius);
      Serial.print(F(", heater="));
      Serial.print(heaterCelcius);
      Serial.print(F(", heating="));
      Serial.print(heatingCelcius);
      Serial.print(F(", heating-return="));
      Serial.println(heatingReturnCelcius);


      if ((millis() - lastSerialValueMillis) < 300000) {
        if (serialMessage != 0) {
          if (radio.isChipConnected()) {
            if(radio.write(&serialMessage, sizeof(unsigned long))) {
              Serial.print("Message ");
              Serial.print(serialMessage);
              Serial.println(" sent");
            } else {
              Serial.println("Failed to sent message");
            }
          } else {
            Serial.println("Message will not be sent by NF24, because hardware is NOT connected");
          }
          Serial.print(F("Sent external="));
          Serial.println(decodeValue(longToMessage(serialMessage)));
        }
      }
}

/*
  SerialEvent occurs whenever a new data comes in the hardware serial RX. This
  routine is run between each time loop() runs, so using delay inside loop can
  delay response. Multiple bytes of data may be available.
*/
void serialEvent() {
  while (Serial.available()) {
    // get the new byte:
    char inChar = (char)Serial.read();
    // add it to the inputString:
    inputString += inChar;
    // if the incoming character is a newline, set a flag so the main loop can
    // do something about it:
    if (inChar == '\n') {
        if (inputString.indexOf("MSG://") >= 0) {
          inputString = inputString.substring(6,inputString.length() - 1);
          Serial.print(F("Recieved oregon scientific sensor '"));
          Serial.print(inputString);
          Serial.println(F("'"));

          inputString = inputString.substring(inputString.indexOf("C=") + 2);

          Serial.print(F("Sensor float value is '"));
          Serial.print(inputString);
          Serial.println(F("'"));

          serialMessage = encodeMessage(EXTERNAL_SENSOR_ID, messageId, inputString.toFloat());
          
          //updated = true;
          lastSerialValueMillis = millis();
        }
        inputString="";
    }
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
