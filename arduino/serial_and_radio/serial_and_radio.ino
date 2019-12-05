#include <RH_ASK.h>
#include <SPI.h> 
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Wire.h> 
#include <SoftwareSerial.h>

#define ONE_WIRE_BUS1 5
#define ONE_WIRE_BUS2 6
#define ONE_WIRE_BUS3 7

// Create Amplitude Shift Keying Object
RH_ASK rf_driver;

OneWire oneWire1(ONE_WIRE_BUS1);
OneWire oneWire2(ONE_WIRE_BUS2);
OneWire oneWire3(ONE_WIRE_BUS3);
DallasTemperature bathRoomSensor(&oneWire1);
DallasTemperature heaterSensor(&oneWire2);
DallasTemperature heatingSensor(&oneWire3);
float bathroomCelcius=0;
float heatingCelcius=0;
float heaterCelcius=0;

const char* BATHROOM_SENSOR_UUID = "eef014ca-4261-40a8-aecd-ec41e466e5d0";
const char* HEATER_SENSOR_UUID = "c4944883-1151-4263-9e7b-965e285e212c";
const char* HEATING_SENSOR_UUID = "5f7cee1f-2e74-48b4-a53a-9be4bbe0abec";

unsigned long lastPingMillis = 0;

String inputString = "";         // a String to hold incoming data
bool stringComplete = false;  // whether the string is complete

char serialMessage[150];
bool updated = false;

void setup() {
  Serial.begin(115200);

    // Initialize ASK Object
  rf_driver.init();
  bathRoomSensor.begin();
  heaterSensor.begin();
  heatingSensor.begin();
}


void loop() {
  if (lastPingMillis == 0) {
    lastPingMillis = millis();
  }
  if ((millis() - lastPingMillis) >= 30000) {
    if (updated) {
          rf_driver.send((uint8_t *)serialMessage, strlen(serialMessage));
          rf_driver.waitPacketSent();
          updated = false;
    }

    char string[150];
    char str_temp[6];

    bathRoomSensor.requestTemperatures(); 
    bathroomCelcius=bathRoomSensor.getTempCByIndex(0);    

    heaterSensor.requestTemperatures(); 
    heaterCelcius=heaterSensor.getTempCByIndex(0);    

    heatingSensor.requestTemperatures(); 
    heatingCelcius=heatingSensor.getTempCByIndex(0);    
    
    dtostrf(bathroomCelcius, 4, 2, str_temp);

    sprintf(string, "%s;C=%s", BATHROOM_SENSOR_UUID, str_temp);
    Serial.println(string);
    rf_driver.send((uint8_t *)string, strlen(string));
    rf_driver.waitPacketSent();

    dtostrf(heaterCelcius, 4, 2, str_temp);

    sprintf(string, "%s;C=%s", HEATER_SENSOR_UUID, str_temp);
    Serial.println(string);
    rf_driver.send((uint8_t *)string, strlen(string));
    rf_driver.waitPacketSent();

    dtostrf(heatingCelcius, 4, 2, str_temp);

    sprintf(string, "%s;C=%s", HEATING_SENSOR_UUID, str_temp);
    Serial.println(string);
    rf_driver.send((uint8_t *)string, strlen(string));
    rf_driver.waitPacketSent();

    lastPingMillis = millis();
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
          Serial.print("Forwarding message to radio '");
          Serial.print(inputString);
          Serial.println("'");
          
          inputString.toCharArray(serialMessage,64);
          updated = true;
        }
        inputString="";
    }
  }
}
