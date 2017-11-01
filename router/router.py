#!/usr/bin/env python
"""
Reference design for mesh routing
"""

import threading
import time
import hashlib
import json
import sys
import importlib
import pika

MESH = importlib.import_module(sys.argv[1], package=None)

HASH_TABLE = []

DONE = False

def in_hash(self, msg):
    """
    Check and update the hash table
    """
    global HASH_TABLE
    expiration = 60
    key = hashlib.md5().update(msg).digest()
    now = time.time()
    if key in HASH_TABLE[self]:
        retval = HASH_TABLE[self][key] > 0
    else:
        HASH_TABLE[self][key] = now + expiration
        retval = False
    for key, value in HASH_TABLE[self].items():
        if value < now:
            del HASH_TABLE[self][key]
    return retval

def get_message(node_id):
    """
    Get a message from the queue
    """
    queue = "q%d" % node_id
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    messages = []
    method_frame, properties, body = channel.basic_get(queue)
    if method_frame:
        messages.append(body)
        channel.basic_ack(method_frame.delivery_tag)
    connection.close()
    return messages

def send_message(queue_prefix, node_id, message):
    """
    Send a Message
    """
    queue = "%s%d" % (queue_prefix, node_id)
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='',
                          routing_key=queue,
                          body=message)
    connection.close()

def get_destination_route(dnet, routes):
    """
    Lookup the desination route
    """
    if dnet in routes:
        dest_route = routes[dnet]
    else:
        dest_route = []
    return dest_route

def router(self, adjacent):
    """
    Top Level Router Thread
    """
    global DONE
    print("Started: %d" % self)
    routes = {}
    while not DONE:
        time.sleep(1)
        try:
            for msg in get_message(self):
                jmsg = json.loads(msg)
                if in_hash(self, json.dumps(jmsg['message'])):
                    continue

                jmsg['route'].append(self)

                # Learn shortest route
                if jmsg['snet'] in routes:
                    if len(jmsg['route']) < len(routes[jmsg['snet']]):
                        routes[jmsg['snet']] = jmsg['route'][::-1]
                else:
                    routes[jmsg['snet']] = jmsg['route'][::-1]


                # Route/Deliver the Message
                msg = json.dumps(jmsg)
                if jmsg['dnet'] == self:
                    # Deliver message
                    print("%d: Delivered: %s" % (self, msg))
                    send_message("n", jmsg['daddr'], msg)
                    continue

                dest_route = get_destination_route(jmsg['dnet'], routes)

                if len(dest_route) > 1:
                    # Route via learned route
                    print("%d: Routing: %s" % (self, msg))
                    send_message("q", dest_route[1], msg)
                    continue

                # Broadcast
                print("%d: Broadcasting: %s" % (self, msg))
                for each_node in adjacent:
                    send_message("q", each_node, msg)

        except IOError:
            break

################################################################

TLIST = []
HASH_TABLE.append({})
for node in MESH.route_table:
    t = threading.Thread(target=router,
                         args=(node, MESH.route_table[node]))
    HASH_TABLE.append({})
    TLIST.append(t)
    t.start()

while not DONE:
    try:
        time.sleep(120)
    except KeyboardInterrupt:
        DONE = True

for t in TLIST:
    t.join()
