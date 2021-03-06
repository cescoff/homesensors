
const int INPUT_PIN = 2;

byte state = LOW;
unsigned long lastStateChange = 0;
unsigned long lastPulseWidth = 0;
// Initial value was 1024 but within radiohead it consumes too much memory
// Works fine with 845, but needed to reduce it to 692 because of some extra features added for temperature measures
const int MAX_BUFFER = 1224;
int bufferUpTo = 0;
char buffer[MAX_BUFFER];

const char* OREGON_S_SENSOR_UUID = "9ad754a1-4ae1-4a73-9658-734e19747617";

int preambleCount = 0;

void setup() {
  Serial.begin(115200);
  pinMode(INPUT_PIN, INPUT_PULLUP);
  attachInterrupt((INPUT_PIN), changed, CHANGE);

}

void changed() { }

void receivePulse(unsigned long lowPulseWidth, unsigned long highPulseWidth) {
  
  char last = bufferUpTo != 0 ? buffer[bufferUpTo - 1] : '0';
  if (highPulseWidth >= 320 && highPulseWidth < 1350) {
    if (highPulseWidth > 615) {
      buffer[bufferUpTo++] = '0';
      buffer[bufferUpTo++] = '1';
    }
    else {
      if (bufferUpTo != 0 && lowPulseWidth >= 350 && lowPulseWidth < 850) {
        buffer[bufferUpTo++] = last;
      } else if (lowPulseWidth >= 850 && lowPulseWidth < 1400) {
        buffer[bufferUpTo++] = '0';
      } else {
        if (bufferUpTo > 10) {
          //Serial.print("low-out ");
          //Serial.println(lowPulseWidth);
          printResult(buffer, bufferUpTo);
        }
        bufferUpTo = 0;
      }
    }
  } else {
    if (bufferUpTo > 10) {
      //Serial.print("high-out ");
      //Serial.println(highPulseWidth);
      printResult(buffer, bufferUpTo);
    }
    bufferUpTo = 0;
  }
  
  if (bufferUpTo >= MAX_BUFFER - 2) {
    bufferUpTo = 0;
  }
}

void printResult(char* buf, int bufferUpTo) {
  char string[75];
  char result[50];
  char checksum[5];
  buffer[bufferUpTo++] = 0;
  //Serial.println(bufferUpTo);
//  Serial.println(buf);
  
  char* remaining = buf;
  do {
    remaining = decodeBuffer(remaining, result, checksum);
    if (remaining != 0) {
      bool checksumOK = checksum[0] == result[13] && checksum[1] == result[12];
      if (checksumOK) {
        sprintf(string, "MSG://%s;C=%c%c%c.%c", OREGON_S_SENSOR_UUID, result[11] != '0' ? '-' : '+', result[10], result[9], result[8]);
        Serial.println(string);
      }
    }
  } while (remaining != 0 && remaining - buf < bufferUpTo);
}

char* decodeBuffer(char* buf, char* strResult, char* strChecksum) {
  unsigned char result[10];
  for (int i = 0; i < sizeof(result); i++) {
    result[i] = 0;
  }

  const char* SEARCH_PATTERN = "0101010110011001";

  char* p = strstr(buf, SEARCH_PATTERN);
  if (!p) {
    return 0;
  }

  //printf("found\n");
  p += strlen(SEARCH_PATTERN) + 1;
  char* start = p;
  for (int cur = 0; *p != 0 && *(p + 1) != 0 && (cur < sizeof(result)*8); p += 2, cur++) {
    int b = cur % 8;
    if (*p == '1') {
      result[cur / 8] |= 1 << (b < 4 ? b + 4 : b - 4);
    }
  }

  for (int i = 0; i < sizeof(result); i++) {
    sprintf(strResult+i*2, "%02X", result[i]);
  }

  const int expectedMessageSize = 6;
  int checksum = 0;
  for (int i = 0; i < expectedMessageSize; i++) {
    checksum += ((result[i] & 0xF0) >> 4) + (result[i] & 0x0F);
  }
  
  sprintf(strChecksum, "%02X", (char)checksum);

  return p;
}

void loop() {
  // put your main code here, to run repeatedly:
  byte oldState = state;
  state = digitalRead(INPUT_PIN);
  if (state == oldState) {
    return;
  }

  unsigned long pulseWidth = micros() - lastStateChange;
  lastStateChange = micros();

  if (state == LOW) {
    receivePulse(lastPulseWidth, pulseWidth);
  }
  
  lastPulseWidth = pulseWidth;

  if (pulseWidth >= 600 && pulseWidth < 1350) {
    preambleCount++;
  } else {
    preambleCount = 0;
  }
  
  if (preambleCount == 32) {
//    Serial.println(micros());
    preambleCount = 0;
  }
}
