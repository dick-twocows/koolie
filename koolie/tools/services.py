import koolie.tools.common

import abc
import contextlib
import ctypes
import enum
import logging
import multiprocessing
import multiprocessing.connection
import os
import queue
import signal
import sys
import time
import threading
import traceback
import typing
import uuid


_logger = logging.getLogger(__name__)

_queue = multiprocessing.Queue()


@enum.unique
class Go(enum.IntEnum):
    START = enum.auto()
    STOP = enum.auto()
    STOP_ALL = enum.auto()


def sig_int(signum: int, frame):
    _queue.put((Go.STOP_ALL, None))


signal.signal(signal.SIGINT, sig_int)

_manager = multiprocessing.Manager()

_services = _manager.list()

_dispatch = _manager.Value(ctypes.c_bool, True)


def stop_all(item):
    _logger.debug('stop_all()')
    for service in _services:
        service.stop()
    _dispatch.value = False


def go():
    _logger.debug('go()')

    def unknown(item):
        _logger.warning('Unknown item [{}]'.format(item))

    dispatcher = {
        Go.STOP_ALL: stop_all
    }

    while _dispatch.value:
        item: tuple = _queue.get()
        _logger.debug(item)
        dispatcher.get(item[0], unknown)(item)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    _logger.info(koolie.tools.common.system_info())

    go()
