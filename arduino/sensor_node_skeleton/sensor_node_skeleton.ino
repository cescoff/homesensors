
bool printed = false;

void setup() {
  // Open serial communications and wait for port to open:
  Serial.begin(9600);

  // send an intro:
  Serial.println("Sensor Node Skeleton");
  Serial.println();
}

void loop() {
  delay(10000);
  if (!printed) {
    String messages[3];

    messages[0] = encodeMessage(0, 5, 19.6);
    messages[1] = encodeMessage(18, 8, 99.6);
    messages[2] = encodeMessage(27, 3, -18.2);


    for (int i = 0; i < 3; i++) {
      int sensorId = decodeSensorId(messages[i]);
      int messageId = decodeMessageId(messages[i]);
      float value = decodeValue(messages[i]);

      Serial.print(messages[i]);
      Serial.print(" : ");
      Serial.print("SensorId=");
      Serial.print(sensorId);
      Serial.print(", MessageId=");
      Serial.print(messageId);
      Serial.print(", Value=");
      Serial.println(value);
    }

    printed = true;
  }
  
}


// Message structure
// 10101|01010101|10101010101
// |_SENS|_MSG|_FLOAT
// FLOAT is reprsented as follows
// 1|0101010101
// +/-|VALUE * 10
String encodeMessage(int sensorId, int messageId, float value) {
  String res = "";
  Serial.println(res);
  res += encodeInt(sensorId, 5);
  Serial.println(res);
  res += encodeInt(messageId, 8);

  if (value < 0) {
    res += "1";
  } else {
    res += "0";
  }

  res += encodeInt((int) (abs(value) * 10), 10);

  Serial.print("[DEBUG] SensorId=");
  Serial.print(sensorId);
  Serial.print(", MessageId=");
  Serial.print(messageId);
  Serial.print(", Value=");
  Serial.print(value);
  Serial.print("->'");
  Serial.print(res);
  Serial.println("'");

  return res;
}

int decodeSensorId(String message) {
  return decodeInt(message.substring(0,4));
}

int decodeMessageId(String message) {
  return decodeInt(message.substring(4, 13));
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
    Serial.print("CurrentValue=");
    Serial.println(currentValue);
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
  Serial.print("[DEBUG] Encoded value of ");
  Serial.print(value);
  Serial.print(" with byte number ");
  Serial.print(byteLength);
  Serial.print(" is ");
  Serial.println(res);
  return res;
}

int decodeInt(String binary) {
  int res = 0;
  for (int index = 0; index <= binary.length(); index++) {
    if (binary.charAt(index) == '1') {
      Serial.print("2^");
      Serial.print(index);
      if (index == 0) {
        Serial.print("[");
        Serial.print(res);
        Serial.print("+1]");
        res = res + 1;
      } else if (index == 1) {
        Serial.print("[");
        Serial.print(res);
        Serial.print("+2]");
        res = res + 2;
      } else {
        Serial.print("[");
        Serial.print(res);
        Serial.print("+");
        Serial.print(pow(2, index));
        Serial.print("]");
        res = res + pow(2, index) + 1;
      }
      Serial.print("+");
    }
  }
  Serial.println();
  Serial.print("[DEBUG] ");
  Serial.print(binary);
  Serial.print("->");
  Serial.println(res);
  return res;
}