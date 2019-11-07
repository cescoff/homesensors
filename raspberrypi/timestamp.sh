#!/bin/bash

CURRENT_DATE=`date +%Y-%m-%dT%H:%M`

echo "TIMESTAMP=${CURRENT_DATE}" >> /home/pi/Sensors/external_temperatures.log
