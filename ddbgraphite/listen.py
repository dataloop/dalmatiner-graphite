#!/usr/bin/env python
import time
import threading
import SocketServer
from ddbpy.client import Send
import os
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count

DDB = (str(os.environ['DDBHOST']) if 'DDBHOST' in os.environ else '127.0.0.1', 5555)
BUCKET = str(os.environ['DDBBUCKET']) if 'DDBBUCKET' in os.environ else 'metrics'
DEBUG = os.environ['DEBUG'] in ['true','True'] if 'DEBUG' in os.environ else False

if DEBUG:
    print('GRAPHITE DDB: %s, BUCKET: %s, DEBUG: %s' % (DDB, BUCKET, DEBUG) )

pool = {}

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

def realign_metrics(metrics):

    # if metrics are array and contains different paths len then
    # realign it to make all paths len equal
    first_metric = metrics[0]
    first_metric_path = first_metric.split('.')

    if len(metrics) > 1:
        second_metric_path = metrics[1].split('.')

        if len( second_metric_path ) != len( first_metric_path ):            
            metric_prefix = '.'.join( first_metric_path[:-1] )
            _metrics = first_metric_path[-1:] + metrics[1:]
            return metric_prefix, _metrics 
    return False, metrics


# handle data in this formats:
#   local.random.diceroll 9.2 1537515876
#       will be stored in ddb as:
#           local.random.diceroll 9.2 1537515876
#   and
#   local.random.diceroll.1;2;3;4 9.2;1.1;2.5;4.3 1537515876
#       will be stored in ddb as
#           local.random.diceroll.1 9.2 1537515876
#           local.random.diceroll.2 1.1 1537515876
#           local.random.diceroll.3 2.5 1537515876
#           local.random.diceroll.4 4.3 1537515876

class TCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        try:
            data = self.request.recv(1024).strip()
            if data:
                if DEBUG:
                    print(BUCKET)
                    print()
                    print data.split()[0], data.split()[1]
                with Send(DDB) as send:
                    send.switch_streaming(BUCKET)

                    # extract data
                    # ts = int(time.time())
                    ts = int(data.split()[2])
                    metrics = data.split()[0].split(';')
                    values = data.split()[1].split(';')

                    if DEBUG:
                        print( metrics, ts, values )
                        print(values, type(values), len(values) )
                        print(metrics, type(metrics), len(metrics) )
                    
                    metric_prefix, metrics = realign_metrics( metrics )

                    def send_payload( payload ):
                        _metric, value = payload

                        if metric_prefix:
                            _metric = '%s.%s' % (metric_prefix, _metric)
                        if DEBUG:
                            print(_metric, ts, float(value))
                        send.send_payload( _metric, ts, float(value) )

                    p = pool.map( send_payload, zip(metrics, values) )
                    
        except Exception, e:
            print e


def main():
    global pool
    HOST = str(os.environ['DDBGRAPHITE_INTERFACE']) if 'DDBGRAPHITE_INTERFACE' in os.environ else "0.0.0.0"
    PORT = int(os.environ['DDBGRAPHITE_PORT']) if 'DDBGRAPHITE_PORT' in os.environ else 2003
    if DEBUG:
        print('ddbgraphite trying to start on ', HOST, PORT)

    pool = Pool(processes=cpu_count())
    server = ThreadedTCPServer((HOST, PORT), TCPHandler)

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = False
    server_thread.start()
    print("Server loop running in thread:", server_thread.name)

