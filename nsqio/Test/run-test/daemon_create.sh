cp /home/RTM/bin/nsqd /home/RTM/bin/nsqd2
taskset -c 7-15 $GOPATH/bin/nsqd -verbose=false  -max-rdy-count=20000000 -mem-queue-size=2000000 -data-path="temp"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=192.168.40.3 --tcp-address=192.168.40.3:5010 --http-address=192.168.40.3:5011 --lookupd-tcp-address=192.168.40.6:4160  -daemon-priority=HIGH &
chrt -r -p 95 $!

#taskset -c 7-15 $GOPATH/bin/nsqd2 -verbose=false -max-rdy-count=20000000 -mem-queue-size=2000000 -data-path="temp2"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=192.168.40.3 --tcp-address=192.168.40.3:5012 --http-address=192.168.40.3:5013 --lookupd-tcp-address=192.168.40.6:4160  -daemon-priority=LOW &
#chrt -r -p 94 $!

#taskset -c 14-15 $GOPATH/bin/nsqd -verbose=false -max-rdy-count=2000000 -mem-queue-size=2000000 -data-path="temp3"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=192.168.40.3 --tcp-address=192.168.40.3:5014 --http-address=192.168.40.3:5015 --lookupd-tcp-address=192.168.40.4:4160  -daemon-priority=HIGH &
#chrt -r -p 95 $!

#taskset -c 4-5 $GOPATH/bin/nsqd -verbose=false -max-rdy-count=2000000 -mem-queue-size=2000000 -data-path="temp4"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=192.168.40.3 --tcp-address=192.168.40.3:5016 --http-address=192.168.40.3:5017 --lookupd-tcp-address=192.168.40.4:4160  -daemon-priority=HIGH &
#chrt -r -p 95 $!

#taskset -c 6-7 $GOPATH/bin/nsqd -verbose=false -max-rdy-count=2000000 -mem-queue-size=2000000 -data-path="temp5"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=192.168.40.3 --tcp-address=192.168.40.3:5018 --http-address=192.168.40.3:5019 --lookupd-tcp-address=192.168.40.4:4160  -daemon-priority=HIGH &
#chrt -r -p 95 $!

#taskset -c 8-9 $GOPATH/bin/nsqd -verbose=false -max-rdy-count=2000000 -mem-queue-size=2000000 -data-path="temp6"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=192.168.40.3 --tcp-address=192.168.40.3:5020 --http-address=192.168.40.3:5021 --lookupd-tcp-address=192.168.40.4:4160  -daemon-priority=HIGH &
#chrt -r -p 95 $!



#taskset -c 9-10 $GOPATH/bin/nsqd -verbose=false -max-rdy-count=20000 -mem-queue-size=20000 -data-path="temp2"  -tls-required=false -tls-min-version='ssl3.0' --broadcast-address=192.168.40.3 --tcp-address=192.168.40.3:5012 --http-address=192.168.40.3:5013 --lookupd-tcp-address=172.16.28.164:4160  -daemon-priority=LOW &
#chrt -r -p 94 $!
