import serial
import re
from datetime import datetime

pattern = re.compile("MSG://([a-z0-9\-]+);(C=[\+\-]+[0-9]+\.[0-9]+)");

ser = serial.Serial('/dev/ttyUSB0', 9600)
while 1: 
    if(ser.in_waiting >0):
        line = ser.readline()[:-2]
        match = pattern.match(line.decode('utf-8'))
        if match:
            rawMessage=match.group().replace("MSG://", "")
            rewrittenMessage=rawMessage.split(';')[0]+";"+datetime.now().strftime("%d/%m/%Y")+";"+datetime.now().strftime("%H:%M:%S")+";"+rawMessage.split(';')[1]
            print(rewrittenMessage)
            f=open("/home/pi/Sensors/external_temperatures.log", "a")
            f.write(rewrittenMessage + "\n")
            f.close()
        else:
            print("No match for line='" + line.decode('utf-8') + "'")
