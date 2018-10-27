#!/usr/bin/env python
import time
import threading
import SocketServer
from ddbpy.client import Send
import os
from multiprocessing.dummy import Pool
from multiprocessing import cpu_count
import logging

DDB = (str(os.environ['DDBHOST']) if 'DDBHOST' in os.environ else '127.0.0.1', 5555)
BUCKET = str(os.environ['DDBBUCKET']) if 'DDBBUCKET' in os.environ else 'metrics'
DEBUG = os.environ['DEBUG'] in ['true','True'] if 'DEBUG' in os.environ else False

logging.basicConfig(level=logging.DEBUG if DEBUG else logging.INFO,
                    format='%(name)s: %(message)s',
                    )

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

def processLine(send, data, logger):

    # extract data
    # ts = int(time.time())
    logger.debug('processLine()->"%s"', data)
    data = data.split()

    ts = int(data[2])
    metrics = data[0].split(';')
    values = data[1].split(';')

    logger.debug('metrics: "%s" ==%s, ts: "%s", values: "%s" ==%s', (metrics), len(metrics), ts, values, len(values))
        
    metric_prefix, metrics = realign_metrics( metrics )

    def send_payload( payload ):
        _metric, value = payload

        if metric_prefix:
            _metric = '%s.%s' % (metric_prefix, _metric)

        logger.debug('_metric: "%s", ts: "%s", value: "%s"', _metric, ts, float(value))
        send.send_payload( _metric, ts, float(value) )

    p = pool.map( send_payload, zip(metrics, values) )
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

        self.request.settimeout(15)
        try:
            logger = logging.getLogger('ddbgraphiteServer')
            cnt = 0
            buff = ''
            while True:

                store_to_buff_last = False

                data = self.request.recv(8192)
                logger.debug('------------ l: %s --------------------------', cnt)
                if not data: break
                
                logger.debug('recv()->"%s"', data)
                
                cnt +=1
                logger.debug('cnt->"%s"', cnt)

                # if end of string not contain end of line - use buffer
                if data[-1] != '\n' and data[-2:] != '\n\r':
                    store_to_buff_last = True
                    
                logger.debug('store_to_buff_last->"%s"', store_to_buff_last)
                logger.debug('buff->"%s"', buff)
                # join data with buff
                if len(buff):
                    data = buff + data
                    buff = ''

                data = data.split('\n')
                logger.debug('data->"%s"', data)

                if store_to_buff_last:
                    buff = data[-1]
                    logger.debug('buff->"%s"', buff)
                    data = data[:-1]
                    # if data length is one 
                    # and store_to_buff is true then wait for another

                if data[-1] == '':
                    data = data[:-1]

                if not len(data):
                    continue
                
                # self.logger.debug('recv()->"%s"', data)
                with Send(DDB) as send:
                    logger.debug('switch_streaming to: "%s"', BUCKET)
                    send.switch_streaming(BUCKET)

                    list( map(lambda _data: processLine(send, _data, logger), data)) 
                    # processLine( send, data)

                continue
                    
        except Exception, e:
            print e
            pass


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

