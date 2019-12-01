/*
  433 MHz RF Module Receiver Demonstration 1
  RF-Rcv-Demo-1.ino
  Demonstrates 433 MHz RF Receiver Module
  Use with Transmitter Demonstration 1

  DroneBot Workshop 2018
  https://dronebotworkshop.com
*/

// Include RadioHead Amplitude Shift Keying Library
#include <RH_ASK.h>
// Include dependant SPI Library 
#include <SPI.h> 

// Create Amplitude Shift Keying Object
RH_ASK rf_driver;

unsigned long timer = 0;

void setup()
{
    // Initialize ASK Object
    rf_driver.init();
    // Setup Serial Monitor
    Serial.begin(9600);
}

void loop()
{
    // Set buffer to size of expected message
    uint8_t buf[44];
    uint8_t buflen = sizeof(buf);
    // Check if received packet is correct size
    if (rf_driver.recv(buf, &buflen))
    {
      
      // Message received with valid checksum
      char string[150];
      sprintf(string, "%s", (char*)buf);

      Serial.print("DEBUG::'");
      Serial.print(string);
      Serial.println("'");
      
      String inputString=string;
      if (inputString.indexOf("FWD://") >= 0) {
          inputString = inputString.substring(6,inputString.length() - 1);
          Serial.print("FWD://");
          Serial.println(inputString);
      } else {
        Serial.print("Message ignored '");
        Serial.print(inputString);
        Serial.println("'");
      }

    }
    if (timer == 0) {
      timer=millis();
    }
    if (millis()-timer > 10000) {
      Serial.println("RECIEVER;C=PING");
      timer=millis();
    }
    delay(200);
}
