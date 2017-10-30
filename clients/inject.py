#!/usr/bin/env python

import pika
import time
import sys
import random

def send_message(q,node_id,message):
    queue = "%s%d" % (q,node_id)
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='',
            routing_key=queue,
            body=message)
    connection.close()

#node_id = int(sys.argv[1]) 
#destination = int(sys.argv[2])

while True:
    snet = int(random.random() * 60 + 1)
    saddr = 1
    dnet = int(random.random() * 60 + 1)
    daddr = 1

    if dnet != snet:
        send_message("q",snet,'{"snet": %d, "saddr": %d, "dnet": %d, "daddr": %d, "route": [], "message": {"time": %d, "body": "Hello"}}' % (snet, saddr, dnet, daddr, time.time()))
        time.sleep(5)
