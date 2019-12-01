#include <RH_ASK.h>
#include <SPI.h> 
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Wire.h> 
#include <SoftwareSerial.h>

#define ONE_WIRE_BUS 5

// Create Amplitude Shift Keying Object
RH_ASK rf_driver;

SoftwareSerial mySerial(2, 3); // RX, TX
OneWire oneWire(ONE_WIRE_BUS);
DallasTemperature sensors(&oneWire);
float celcius=0;

const char* LOCAL_SENSOR_UUID = "eef014ca-4261-40a8-aecd-ec41e466e5d0";

unsigned long lastPingMillis = 0;

String inputString = "";         // a String to hold incoming data
bool stringComplete = false;  // whether the string is complete

void setup() {
  Serial.begin(115200);

    // Initialize ASK Object
  rf_driver.init();
  sensors.begin();

  mySerial.begin(9600);
  mySerial.println("Hello, world?");
}


void loop() {
  if (lastPingMillis == 0) {
    lastPingMillis = millis();
  }
  if ((millis() - lastPingMillis) >= 60000) {
    sensors.requestTemperatures(); 
    celcius=sensors.getTempCByIndex(0);    
    char string[150];
    char str_temp[6];
    dtostrf(celcius, 4, 2, str_temp);

    sprintf(string, "%s;C=%s", LOCAL_SENSOR_UUID, str_temp);
    Serial.println(string);
    rf_driver.send((uint8_t *)string, strlen(string));
    rf_driver.waitPacketSent();
    lastPingMillis = millis();
  }
  
}

void readSoftwareSerial() {
  while (mySerial.available()) {
    // get the new byte:
    char inChar = (char)mySerial.read();
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
          
          char string[150];
          inputString.toCharArray(string,64);
          rf_driver.send((uint8_t *)string, strlen(string));
          rf_driver.waitPacketSent();
        }
        inputString="";
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
          Serial.print("Forwarding message to radio '");
          Serial.print(inputString);
          Serial.println("'");
          
          char string[150];
          inputString.toCharArray(string,64);
          rf_driver.send((uint8_t *)string, strlen(string));
          rf_driver.waitPacketSent();
        }
        inputString="";
    }
  }
}
