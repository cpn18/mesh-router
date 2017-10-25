#!/usr/bin/env python

import pika
import time
import sys
import random

def send_message(node_id,message):
    queue = "q%d" % node_id
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='',
            routing_key=queue,
            body=message)
    connection.close()

node_id = int(sys.argv[1]) 
destination = int(sys.argv[2])

while True:
    node_id = int(random.random()*60 + 1)
    saddr = 0
    destination = int(random.random() * 60 + 1)
    daddr = 0
    send_message(node_id,'{"snet": %d, "saddr": %d, "dnet": %d, "daddr": %d, "route": [], "message": {"time": %d, "body": "Hello2"}}' % (node_id, saddr, destination, daddr, time.time()))
    time.sleep(5)
