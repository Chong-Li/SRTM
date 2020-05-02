#!/usr/bin/python
import os

os.system("go build TCPServer.go")
os.system("go build TCPClient.go")

os.system("go build UDPServer.go")
os.system("go build UDPClient.go")

