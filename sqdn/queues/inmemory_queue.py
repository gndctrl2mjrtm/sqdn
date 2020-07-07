#!/bin/env/python
# -*- encoding: utf-8 -*-
"""

"""
from __future__ import print_function, division
import numpy as np
import datetime
import time
import ray

logo = """
            (\.-./)
          .'   :   '.
     _.-'`     '     `'-._
  .-'   S   Q  :  D   N    '-.
,'_.._         .         _.._ ',
'`    `'-.     '     .-'`    ``'
          '.   :   .'
    ^       \     /
     \       \   /
      |       | |
      |       | ;
      \ '.___.' /
        '-....-'                         
"""


class Package(object):

    def __init__(self, val):
        self.val = val
        self.timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')


@ray.remote
class InMemoryQueue(object):

    def __init__(self, max_n=np.inf):
        if not max_n == np.inf:
            if not isinstance(max_n, int):
                raise TypeError("Invalid max_n arg type, must be int, not {}".format(
                    type(max_n).__name__))
            if max_n < 0:
                raise UserWarning("max_n must be positive")
        self._queue = []
        self.max_n = max_n

    def push(self, data, expand=True):
        if isinstance(data, list):
            if expand is True:
                for n in data:
                    self._queue.append(n)
            else:
                self._queue.append(data)
        else:
            self._queue.append(data)

    def pull(self, n_data: int = 1, rm: bool = True):
        data = self._queue[:n_data]
        if rm is True:
            self._queue = self._queue[n_data:]
        return data

    def len(self):
        return len(self._queue)

    def __len__(self):
        return self.len()

    def can_push(self, n_data=1):
        if self.len() + n_data <= self.max_n:
            return True
        else:
            return False

    def can_pull(self, n_data=1):
        if not isinstance(n_data, int):
            raise TypeError("Invalid n_data arg type, must be int, not {}".format(
                type(n_data).__name__))
        if n_data < 0:
            raise UserWarning("n_data must be positive")
        if self.len() >= n_data:
            return True
        else:
            return False


@ray.remote
class RemoteServer(object):

    def __init__(self):
        self.queues = {}

    def _verify_queue(self, queue, create=True):
        if not queue in self.queues.keys():
            if create is True:
                self.queues[queue] = InMemoryQueue.remote()
                return True
            else:
                return False
        else:
            return False

    def create_queue(self, queue):
        if not isinstance(queue, str):
            raise TypeError
        if not queue in self.queues.keys():
            self.queues[queue] = InMemoryQueue.remote()

    def can_pull(self, queue, n_data=1):
        self._verify_queue(queue)
        return ray.get(self.queues[queue].can_pull.remote(n_data))

    def can_push(self, queue, n_data=1):
        self._verify_queue(queue)
        return ray.get(self.queues[queue].can_push.remote(n_data))

    def pull(self, queue, n_data=1, create=True):
        self._verify_queue(queue, create=create)
        q = self.queues[queue]
        output = ray.get(q.pull.remote(n_data))
        return output

    def push(self, data, queue, expand=False):
        q = self.queues[queue]
        if isinstance(data, list):
            if expand is True:
                for n in data:
                    while ray.get(q.can_push.remote()) is False:
                        time.sleep(3e-5)
                    q.push.remote(n)
            else:
                q.push.remote(data)
        else:
            q.push.remote(data)


class PullResult(object):

    def __init__(self, res, status):
        self.res = res
        self.status = status

    def get(self):
        if isinstance(self.res, list):
            if len(self.res) == 1:
                return self.res[0]
        return self.res


@ray.remote
class CentralServer(object):

    def __init__(self, n_servers=1):
        if not isinstance(n_servers, int):
            raise TypeError
        if n_servers <= 0:
            raise UserWarning
        self.servers = {}
        for i in range(n_servers):
            self.servers['s{}'.format(i)] = RemoteServer.remote()
        self.queues = {'master': 's0'}
        self.servers['s0'].create_queue.remote('master')
        self._index = 0

    def _verify_queue(self, queue, create=True):
        if not isinstance(queue, str):
            raise TypeError(queue, type(queue).__name__)
        if not isinstance(create, bool):
            raise TypeError
        if not queue in list(self.queues.keys()):
            if create is True:
                server = self._select_server()
                self.servers[server].create_queue.remote(queue)
                self.queues[queue] = server
                return True
            else:
                print("Warning, queue not found: {}".format(queue))
                return False
        else:
            return True

    def _select_server(self):
        server_keys = list(self.servers.keys())
        selected = server_keys[self._index % len(server_keys)]
        self._index += 1
        return selected

    def _get_default(self):
        queue = list(self.queues.keys())[0]
        return queue

    def can_push(self, queue=None, n_data=1):
        if queue is None:
            queue = self._get_default()
        self._verify_queue(queue)
        return ray.get(self.servers[self.queues[queue]].can_push.remote(queue, n_data))

    def can_pull(self, queue=None, n_data=1):
        if queue is None:
            queue = self._get_default()
        self._verify_queue(queue)
        return ray.get(self.servers[self.queues[queue]].can_pull.remote(queue, n_data))

    def push(self, data, queue=None):
        if queue is None:
            queue = self._get_default()
        self._verify_queue(queue)
        self.servers[self.queues[queue]].push.remote(data, queue)

    def pull(self, queue=None):
        if queue is None:
            queue = self._get_default()
        if self._verify_queue(queue) is True:
            res = ray.get(self.servers[self.queues[queue]].pull.remote(queue))
            output = PullResult(res, True)
        else:
            output = PullResult(None, False)
        return output


class SQDN(object):

    def __init__(self):
        if not ray.is_initialized():
            ray.init()
        self.server = CentralServer.remote()

    def pull(self, queues):
        if isinstance(queues, list):
            for n in queues:
                if not isinstance(n, str):
                    raise TypeError
        elif isinstance(queues, str):
            queues = [queues]
        else:
            raise TypeError
        results = []
        for q in queues:
            while ray.get(self.server.can_pull.remote(q)) is False:
                time.sleep(3e-5)
            res = ray.get(self.server.pull.remote(q))
            results.append(res)
        if len(results) == 1:
            results = results[0]
        return results

    def push(self, data, queues):
        if isinstance(queues, list):
            for n in queues:
                if not isinstance(n, str):
                    raise TypeError(n)
        elif isinstance(queues, str):
            queues = [queues]
        else:
            raise TypeError
        results = []
        for q in queues:
            while ray.get(self.server.can_push.remote(q)) is False:
                time.sleep(3e-5)
            res = ray.get(self.server.push.remote(data, q))
            results.append(res)
        if len(results) == 1:
            results = results[0]
        return results

rs = RayServePkg()


@ray.remote
def task0():
    for i in range(100):
        rs.rsPush(i, ['q1', 'q2'])


@ray.remote
def task1():
    while True:
        res = rs.rsPull('q1')
        data = res.get()
        print('Recieved ', data)

@ray.remote
def task2():
    while True:
        res = rs.rsPull('q2')
        data = res.get()
        print('Recieved ', data)


def main():
    task0.remote()
    task1.remote()
    task2.remote()
    while True:
        time.sleep(10)


if __name__ == "__main__":
    main()
