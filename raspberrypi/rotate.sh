#!/bin/bash
DATE_VAR=`date +%Y-%m-%d`
echo "Date prefix : $DATE_VAR"
FILE_NAME=temperatures.log
mv /home/pi/Sensors/$FILE_NAME "/home/pi/Sensors/${DATE_VAR}_${FILE_NAME}"
