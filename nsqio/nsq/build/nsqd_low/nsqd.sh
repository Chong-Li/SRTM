rm *.dat & rm write*
./nsqd --broadcast-address=192.168.1.11 --tcp-address=0.0.0.0:4170 --http-address=0.0.0.0:4171 --lookupd-tcp-address=127.0.0.1:4160 -tls-required=false -tls-min-version='ssl3.0' -daemon-priority=LOW
