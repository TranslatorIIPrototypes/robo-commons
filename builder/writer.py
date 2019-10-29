#!/usr/bin/env python

import os
import sys
from time import sleep, strftime
from datetime import datetime
import logging
import json
import pickle

import pika

from greent.util import LoggingUtil
from greent.export import BufferedWriter
from builder.buildmain import setup
from greent.graph_components import KNode, KEdge
from builder.api import logging_config
from pika.exceptions import StreamLostError

logger = LoggingUtil.init_logging("builder.writer", level=logging.DEBUG)

greent_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..')
sys.path.insert(0, greent_path)
rosetta = setup(os.path.join(greent_path, 'greent', 'greent.conf'))

writer = BufferedWriter(rosetta)

def callback(ch, method, properties, body):
    # logger.info(f" [x] Received {body}")
    graph = pickle.loads(body)
    if isinstance(graph, str) and graph == 'flush':
        logger.debug('Flushing buffer...')
        writer.flush()
        return
    for node in graph['nodes']:
        # logger.debug(f'Writing node {node.id}')
        writer.write_node(node)
    for edge in graph['edges']:
        # logger.debug(f'Writing edge {edge.source_id}->{edge.target_id}')
        if 'force' in graph:
            writer.write_edge(edge, force_create= True)
        else:
            writer.write_edge(edge)
    # Found out that rabbitmq will reset connections for channels 
    # and sometimes it is the case that neo4j related things take time 
    # and the broker decides this client is no longer active and kills the connection
    # so to avoid dropping of any incoming data that might, have not been written we can
    # tell the queue that we've processed them if we still have the channel open
    # else we will just leave them on the queue.
    if ch.is_open:
        ch.basic_ack(method.delivery_tag)
    return
    
def setup_consumer(callback = callback):
    # Setup code same as our previous, creating the queue on the channel.
    # Not doing auto_ack incase the channel drops on us and we lose some data that 
    # the channel has picked up but not processed yet.
    logger.info(f' [*] Setting up consumer, creating new connection')
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        heartbeat= 1,
        host=os.environ['BROKER_HOST'],
        virtual_host='builder',
        credentials=pika.credentials.PlainCredentials(os.environ['BROKER_USER'], os.environ['BROKER_PASSWORD'])
    ))
    channel = connection.channel()
    channel.queue_declare(queue='neo4j')
    channel.basic_consume('neo4j', callback, auto_ack=False)
    return channel


def start_consuming(max_retries = 0):    
    # Consumer wrappper tries to connect to the broker for 
    # max_retries then exits. We don't want to loop over and over for ever
    try:
        channel = setup_consumer(callback= callback)
        logger.info(' [*] Waiting for messages.')
        channel.start_consuming()
        
    except StreamLostError as error:
        logger.info(f' [x] {error}')
        logger.info(f' [x] channel connection status: Open = {channel.is_open}')
        if channel.is_open:
            channel.close()
        if max_retries > 0:
            max_retries -= 1
            logger.info(f" [x] Retrying connection to {os.environ['BROKER_HOST']} : {max_retries} retries left")
            start_consuming(max_retries= max_retries)
start_consuming(10)
print('To exit press CTRL+C')