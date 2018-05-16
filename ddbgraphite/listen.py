#!/usr/bin/env python
import time
import SocketServer
from ddbpy.client import Send
import os

DDB = (os.environ['DDBHOST'] if 'DDBHOST' in os.environ else '127.0.0.1', 5555)
BUCKET = 'metrics'
DEBUG = True


class TCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            data = self.request.recv(1024).strip()
            if data:
                if DEBUG:
                    print data.split()[0], data.split()[1]
                with Send(DDB) as send:
                    send.switch_streaming(BUCKET)
                    ts = int(time.time())
                    metric = data.split()[0]
                    value = data.split()[1]
                    send.send_payload(metric, ts, value)
        except Exception, e:
            print e


def main():
    HOST, PORT = os.environ['DDBGRAPHITE_INTERFACE'] if 'DDBGRAPHITE_INTERFACE' in os.environ else "0.0.0.0", 2003
    server = SocketServer.TCPServer((HOST, PORT), TCPHandler)
    server.serve_forever()
