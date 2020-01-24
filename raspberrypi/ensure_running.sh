#/bin/bash

PS=`ps -ef|grep usb_serial.py|grep python3`

if [ -z "$PS" ]; then
	echo "[WARN] Program is not running"
	echo "[WARN] Program is not running">>$HOME/Sensors/serial_state.log
	cd $HOME/Sensors
	sudo nohup python3 usb_serial.py&
else
	PID=`pgrep python3`
	echo "[INFO] Program is running with PID $PID"
	echo "[INFO] Program is running with PID $PID">>$HOME/Sensors/serial_state.log
fi

