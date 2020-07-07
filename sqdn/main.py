#!/bin/env/python
# -*- encoding: utf-8 -*-
"""

"""
from __future__ import print_function, division
import numpy as np
import time
import ray


class Queue(object):

    def __init__(self):
        pass

    def push(self):
        raise NotImplementedError

    def pull(self):
        raise NotImplementedError

    def len(self):
        raise NotImplementedError

    def can_pull(self):
        raise NotImplementedError

    def can_push(self):
        raise NotImplementedError


class InMemoryQueue(Queue):

    def __init__(self, n_data=np.inf):
        super().__init__()


class OnDiskQueue(Queue):

    def __init__(self, n_data=np.inf):
        super().__init__()


class DeltaQueue(Queue):

    def __init__(self, n_data=np.inf):
        super().__init__()


@ray.remote
class RemoteServer(object):

    def __init__(self, id_index: int):
        self.id_index = id_index
        self.queues = {}

    def add_queue(self):
        pass

    def del_queue(self):
        pass

    def pull(self, q):
        raise NotImplementedError

    def push(self, q):
        raise NotImplementedError

    def can_pull(self, q):
        raise NotImplementedError

    def can_push(self, q):
        raise NotImplementedError


class MasterServer(object):

    def __init__(self, n_servers=1):
        # TODO: Verify Args
        self._servers = [RemoteServer.remote(i) for i in range(n_servers)]

    def add_queue(self):
        pass

    def del_queue(self):
        pass

    def pull(self, q):
        raise NotImplementedError

    def push(self, q):
        raise NotImplementedError

    def _can_push(self, q, timeout=300, wait_time=1e-3):
        start_time = time.time()
        while time.time() - start_time <= timeout:
            if ray.get(self._servers[q].can_push.remote()) is True:
                break
            else:
                time.sleep(wait_time)


    def _can_pull(self, q, timeout=300, wait_time=1e-3):
        start_time = time.time()
        while time.time() - start_time <= timeout:
            if ray.get(self._servers[q].can_pull.remote()) is True:
                break
            else:
                time.sleep(wait_time)


