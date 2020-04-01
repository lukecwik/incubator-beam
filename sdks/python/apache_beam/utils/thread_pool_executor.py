#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

from __future__ import absolute_import

import sys
import threading
import time
import weakref
from concurrent.futures import _base

try:  # Python3
  import queue
except Exception:  # Python2
  import Queue as queue  # type: ignore[no-redef]


_SHUTDOWN = 'SHUTDOWN'
_EXIT_THREAD = 'EXIT_THREAD'


class _WorkItem(object):
  def __init__(self, future, fn, args, kwargs):
    self._future = future
    self._fn = fn
    self._fn_args = args
    self._fn_kwargs = kwargs

  def run(self):
    if self._future.set_running_or_notify_cancel():
      # If the future wasn't cancelled, then attempt to execute it.
      try:
        self._future.set_result(self._fn(*self._fn_args, **self._fn_kwargs))
      except BaseException as exc:
        # Even though Python 2 futures library has #set_exection(),
        # the way it generates the traceback doesn't align with
        # the way in which Python 3 does it so we provide alternative
        # implementations that match our test expectations.
        if sys.version_info.major >= 3:
          self._future.set_exception(exc)
        else:
          e, tb = sys.exc_info()[1:]
          self._future.set_exception_info(e, tb)


class _Worker(threading.Thread):
  def __init__(
      self, work_queue):
    super(_Worker, self).__init__()
    self._work_queue = work_queue

  def run(self):
    while True:
      work_item = self._work_queue.get()
      # If we ever get the _SHUTDOWN or _EXIT_THREAD signals then let us
      # terminate.
      if work_item is _SHUTDOWN:
        self._work_queue.put(_SHUTDOWN)
        return
      elif work_item is _EXIT_THREAD:
        return

      # Otherwise run the work item we were assigned.
      work_item.run()


class UnboundedThreadPoolExecutor(_base.Executor):
  def __init__(self, permitted_thread_age_in_seconds=30):
    self._permitted_thread_age_in_seconds = permitted_thread_age_in_seconds
    self._work_queue = queue.Queue()
    self._workers = weakref.WeakSet()
    self._shutdown = False
    self._lock = threading.Lock()  # Guards access to _workers and _shutdown
    self._last_new_worker = time.clock()
    # Ensure at least one worker is watching the work queue
    worker = _Worker(self._work_queue)
    worker.daemon = True
    worker.start()
    self._workers.add(worker)

  def submit(self, fn, *args, **kwargs):
    """Attempts to submit the work item.

    A runtime error is raised if the pool has been shutdown.
    """
    with self._lock:
      if self._shutdown:
        raise RuntimeError(
            'Cannot schedule new tasks after thread pool '
            'has been shutdown.')

      # If there is a pending work item in the queue from the last time and we
      # haven't created a thread in some amount of time, increase the number
      # of workers.
      if not self._work_queue.empty():
        current_time = time.clock()
        if current_time - self._last_new_worker > 10:
          self._last_new_worker = current_time
          worker = _Worker(self._work_queue)
          worker.daemon = True
          worker.start()
          self._workers.add(worker)

    # Add the work item to the work queue
    future = _base.Future()
    work_item = _WorkItem(future, fn, args, kwargs)
    self._work_queue.put(work_item)
    return future

  def shutdown(self, wait=True):
    with self._lock:
      if self._shutdown:
        return
      self._shutdown = True

    self._work_queue.put(_SHUTDOWN)
    if wait:
      for worker in self._workers:
        worker.join()
