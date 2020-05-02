tc qdisc del dev em1 root
tc qdisc add dev em1 root handle 8001: prio bands 4
tc filter add dev em1 protocol ip parent 8001: prio 1 u32 match ip dport 47205 0xffff flowid 8001:1
tc filter add dev em1 protocol ip parent 8001: prio 2 u32 match u8 0 0 flowid 8001:2
