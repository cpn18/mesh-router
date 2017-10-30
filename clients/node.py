#!/usr/bin/env python

import pika
import time
import sys
import random
import json

def send_message(q,node_id,message):
    queue = "%s%d" % (q,node_id)
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='',
            routing_key=queue,
            body=message)
    connection.close()

def get_message(net,node_id):
    queue = "n%d_%d" % (net,node_id)
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    messages = []
    method_frame, properties, body = channel.basic_get(queue)
    if method_frame:
        messages.append(body)
        #print "Received: %d %s" % (node_id,body) 
        channel.basic_ack(method_frame.delivery_tag)
    connection.close()
    return messages

snet = int(sys.argv[1]) 
saddr = int(sys.argv[2])

print "Network: %d, Node: %d" % (snet,saddr)

while True:
    for msg in get_message(snet,saddr):
        jmsg = json.loads(msg)
        print "%d_%d: %s" % (snet,saddr,jmsg)
        if jmsg['message']['body'] == "Hello":
            send_message("q",snet,'{"snet": %d, "saddr": %d, "dnet": %d, "daddr": %d, "route": [], "message": {"time": %d, "body": "Ack"}}' % (snet,saddr,jmsg['snet'],jmsg['saddr'],time.time()))
    time.sleep(1)
