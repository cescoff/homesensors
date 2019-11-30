#include <RH_ASK.h>
#include <SPI.h> 
#include <OneWire.h>
#include <DallasTemperature.h>
#include <Wire.h> 

#define ONE_WIRE_BUS1 5
#define ONE_WIRE_BUS2 6


// Create Amplitude Shift Keying Object
RH_ASK rf_driver;

OneWire oneWire1(ONE_WIRE_BUS1);
OneWire oneWire2(ONE_WIRE_BUS2);
DallasTemperature heaterSensors(&oneWire1);
DallasTemperature heatingSensors(&oneWire2);
float heaterCelcius=0;
float heatingCelcius=0;


const char* HEATER_SENSOR_UUID = "c4944883-1151-4263-9e7b-965e285e212c";
const char* HEATING_SENSOR_UUID = "5f7cee1f-2e74-48b4-a53a-9be4bbe0abec";

unsigned long lastPingMillis = 0;

String inputString = "";         // a String to hold incoming data
bool stringComplete = false;  // whether the string is complete

void setup() {
  Serial.begin(115200);

    // Initialize ASK Object
  rf_driver.init();
  heaterSensors.begin();
  heatingSensors.begin();
  delay(2000);
}


void loop() {
/*  if (lastPingMillis == 0) {
    lastPingMillis = millis();
  }*/
  if ((millis() - lastPingMillis) >= 60000) {
    heaterSensors.requestTemperatures(); 
    heaterCelcius=heaterSensors.getTempCByIndex(0);    
    sendTemperature(heaterCelcius, HEATER_SENSOR_UUID);

    
    heatingSensors.requestTemperatures(); 
    heatingCelcius=heatingSensors.getTempCByIndex(0);    
    sendTemperature(heatingCelcius, HEATING_SENSOR_UUID);
    
    lastPingMillis = millis();
  }
}

void sendTemperature(float temp, const char* sensorId) {
  char str_temp[6];
  dtostrf(temp, 4, 2, str_temp);
  char string[96];
  Serial.print(sensorId);
  Serial.print("=");
  Serial.print(temp);
  Serial.println("C");
  sprintf(string, "FWD://%s;C=%s", sensorId, str_temp);
  Serial.println(string);
  rf_driver.send((uint8_t *)string, strlen(string));
  rf_driver.waitPacketSent();
}
