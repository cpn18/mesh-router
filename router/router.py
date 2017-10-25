#!/usr/bin/env python

#import simple_mesh as mesh
import soccer_mesh as mesh
import pika
import threading
import time
import hashlib
import json

hash_table = []
expiration = 60

done = False

def in_hash(self,msg):
    global hash_table
    m = hashlib.md5()
    m.update(msg)
    key = m.digest()
    now = time.time()
    if key in hash_table[self]: 
        retval = hash_table[self][key] > 0
    else:
        hash_table[self][key] = now + expiration 
        retval = False
    for key, value in hash_table[self].items():
        if value < now:
            del hash_table[self][key]
    return retval

def get_message(node_id):
    queue = "q%d" % node_id
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

def send_message(node_id,message):
    queue = "q%d" % node_id
    connection = pika.BlockingConnection()
    channel = connection.channel()
    channel.queue_declare(queue=queue)
    channel.basic_publish(exchange='',
            routing_key=queue,
            body=message)
    connection.close()

def router(self, adjacent):
    global done
    routes = {}
    while not done:
        try:
            for msg in get_message(self):
                jmsg = json.loads(msg)
                if not in_hash(self,json.dumps(jmsg['message'])):
                    jmsg['route'].append(self)
                    msg = json.dumps(jmsg)
                    #print "%d: Sending %s" % (self,msg)

                    # Learn shortest route
                    if jmsg['snet'] in routes:
                        if len(jmsg['route']) < len(routes[jmsg['snet']]):
                            routes[jmsg['snet']] = jmsg['route'][::-1]
                    else:
                        routes[jmsg['snet']] = jmsg['route'][::-1]

                    if jmsg['dnet'] in routes: 
                        dest_route = routes[jmsg['dnet']]
                    else:
                        dest_route = []

                    print "%d %d" % (self, jmsg['dnet'])
                    print "%d %s" % (self, routes)
                    print "%d %s" % (self, dest_route)

                    if jmsg['dnet'] == self:
                        # Deliver message
                        print "%d: Delivered: %s" % (self,msg)
                        print routes 
                    elif len(dest_route) > 1:
                        # Route via learned route
                        print "%d: Routing: %s" % (self,msg)
                        send_message(dest_route[1],msg)
                    else:
                        # Broadcast
                        print "%d: Broadcasting: %s" % (self,msg)
                        for a in adjacent:
                            send_message(a,msg)
                else:
                    pass
                    #print "%d: %s" % (self, msg)
                    #print "%d: Dropped" % self
            else:
                time.sleep(1)
        except IOError:
            break

################################################################

tlist = []
hash_table.append({})
for node in mesh.route_table:
    t = threading.Thread(target=router,args=(node,mesh.route_table[node]))
    hash_table.append({})
    tlist.append(t)
    t.start()
    print node

#time.sleep(120)
#done = True
#for t in tlist:
#    t.join()
