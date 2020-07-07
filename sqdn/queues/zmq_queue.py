#!/bin/env/python
# -*- encoding: utf-8 -*-
"""

"""
from __future__ import print_function, division
import numpy as np
import datetime
import time
import ray

import time
import zmq

from ray_streaming.queues.inmemory_queue import InMemoryQueue



@ray.remote
class ZMQServer(object):

    def __init__(self, server, queue='zmq-queue', host='*', port=5555):
        # Verify args
        self.host = host
        self.port = port
        context = zmq.Context()
        self.socket = context.socket(zmq.REP)
        self.socket.bind("tcp://{}:{}".format(self.host, self.port))
        self.server = server

    def run(self):
        while True:
            #  Wait for next request from client
            pkg = self.socket.recv()
            #  Do some 'work'
            self.queue.push.remote(pkg)
            #  Send reply back to client
            self.socket.send(b"received")


def main():
    import zmq

    server = ZMQServer.remote()
    server.run.remote()
    time.sleep(3)

    context = zmq.Context()

    #  Socket to talk to server
    print("Connecting to hello world server…")
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")

    #  Do 10 requests, waiting each time for a response
    for request in range(10):
        print("Sending request %s …" % request)
        socket.send(b"Hello")

        #  Get the reply.
        message = socket.recv()
        print("Received reply %s [ %s ]" % (request, message))


if __name__ == "__main__":
    main()