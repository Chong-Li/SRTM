#!/bin/bash
pkill subscriber
pkill publisher
pkill nsqd
pkill nsqlookupd
rm -rf htmp ltmp
mkdir htmp ltmp
