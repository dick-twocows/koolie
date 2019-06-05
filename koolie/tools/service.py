from pickle import _load

import koolie.tools.common

import contextlib
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


_sig_rlock = threading.RLock()

Handle = typing.Callable[[int], None]
Handles = typing.Mapping[str, Handle]
_sig_handles: typing.Mapping[int, Handles] = dict()


def add_sig_handles(name: str, handles: typing.Mapping[int, Handle]):
    assert isinstance(name, str)
    assert isinstance(handles, dict)

    for signum, handle in handles.items():
        add_sig_handle(signum, name, handle)


def add_sig_handle(signum: int, name: str, handle: Handle):
    _logger.debug('add_sig_handle() [{}] [{}] [{}]'.format(signum, name, handle))

    assert isinstance(signum, int)
    assert isinstance(name, str)
    assert isinstance(handle, typing.Callable)

    with _sig_rlock:
        try:
            handles: Handles = _sig_handles.get(signum)
            if handles is None:
                handles = dict()
                _sig_handles[signum] = handles
                _logger.debug('Register signal [{}].'.format(signum))
                signal.signal(signum, go_sig_handle)
            if name in handles:
                _logger.warning('Attempt to add duplicate handle [{}] for [{}]'.format(name, signum))
            else:
                handles[name] = handle
        except Exception as exception:
            _logger.warning('Failed to add signal handle [{}] for [{}].'.format(signum, name))


def remove_sig_handles(name: str):
    _logger.debug('remove_sig_handles() [{}]'.format(name))

    assert isinstance(name, str)

    with _sig_rlock:
        handles: Handles
        for handles in _sig_handles.values():
            handles.pop(name)


def go_sig_handle(signum: int, frame):
    _logger.debug('go_sig_handle [{}]'.format(signal.Signals(signum).name))

    with _sig_rlock:
        stack = traceback.extract_stack(frame)
        _logger.debug(stack)
        handles: Handles = _sig_handles.get(signum)
        if handles is None:
            _logger.warning('No handles defined for [{}].'.format(signum))
        else:
            for name, handle in handles.copy().items():  # Use copy() to avoid 'RuntimeError: dictionary changed size during iteration'
                try:
                    handle(signum)
                except Exception as exception:
                    _logger.warning('Exception [{}] calling handler [{}] for [{}].'.format(exception, name, signum))


class ServiceException(Exception):

    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class ServiceState(enum.Enum):
    EXCEPTION = -2
    CREATED = 0
    STARTED = 2
    STOPPED = 4


class Service(contextlib.AbstractContextManager):

    """Abstract service class which provides start() and stop() methods.
    Work is performed in the go() method which is started in its own thread."""

    NAME: str = 'service_name'

    def __init__(self, **kwargs) -> None:
        super().__init__()

        self._kwargs = kwargs

        self.__name = self.get_kv(self.NAME)
        assert self.__name is None or isinstance(self.__name, str)
        if self.__name is None:
            self.__name = str(uuid.uuid4())

        self._rlock = threading.RLock()

        self.__state: ServiceState = ServiceState.CREATED

        self.__pending_state: ServiceState = None

        self.signal_handlers: typing.Mapping[int, Handle] = {
            signal.SIGHUP: self._sig_hup,  # Reload configuration.
            # signal.SIGINFO: self._sig_info,  # Status, on BSD and OS X, via 'Ctrl+T'.
            signal.SIGINT: self._sig_int,  # Interrupt, via 'Ctrl+C'.
            signal.SIGTERM: self._sig_term,
            signal.SIGUSR1: self._sig_usr1,  # On Linux typically used to mimic SIGINFO, via 'kill -SIGUSR1 pid'
            signal.SIGUSR2: self._sig_usr2
        }

    # Signal handler methods.

    def _signal_handlers(self) -> typing.Mapping[int, Handle]:
        return self.signal_handlers

    def _sig_hup(self, signum):
        self.restart()

    def _sig_info(self, signum):
        _logger.info(self)

    def _sig_int(self, signum):
        self.stop()

    def _sig_term(self, signum):
        self.stop()

    def _sig_usr1(self, signum):
        _logger.info(self)

    def _sig_usr2(self, signum):
        _logger.info(self)

    # ConextManager methods i.e. with.

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def kv(self) -> typing.Mapping[str, object]:
        return self._kwargs

    def get_kv(self, k: str, v: object = None) -> object:
        return self._kwargs.get(k, v)

    def has_k(self, k: str) -> bool:
        return k in self._kwargs.keys()

    def name(self) -> str:
        return self.__name

    def state(self) -> ServiceState:
        """The state of the service."""
        return self.__state

    def _state(self, state: ServiceState, **kwargs):
        """Change the state to the given state."""
        _logger.debug('state() [{}] [{}] -> [{}]'.format(self.name(), self.state(), state))
        with self._rlock:
            self.__state = state

            clear_pending_state = kwargs.get('clear_pending_state', False)
            assert isinstance(clear_pending_state, bool)

            if clear_pending_state:
                self._pending_state(None)

    def pending_state(self) -> ServiceState:
        """The pending state of the service."""
        return self.__pending_state

    def _pending_state(self, pending_state: ServiceState):
        """Change the pending state to the given state."""
        _logger.debug('pending_state() [{}] [{}] -> [{}]'.format(self.name(), self.pending_state(), pending_state))
        with self._rlock:
            self.__pending_state = pending_state

    def start(self):
        """Start the service."""
        _logger.debug('start() [{}]'.format(self.name()))
        try:
            with self._rlock:
                if self.state() in {ServiceState.CREATED, ServiceState.STOPPED}:
                    self._pending_state(ServiceState.STARTED)
                    add_sig_handles(self.__name, self.signal_handlers)
                    self.on_start()
                    self._state(ServiceState.STARTED, clear_pending_state=True)
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
            self._state(ServiceState.EXCEPTION, clear_pending_state=True)

    def stop(self):
        """Stop the service."""
        _logger.debug('stop() [{}]'.format(self.name()))
        try:
            with self._rlock:
                if self.__state in {ServiceState.STARTED}:
                    self._pending_state(ServiceState.STOPPED)
                    self.on_stop()
                    self._state(ServiceState.STOPPED, clear_pending_state=True)
                    remove_sig_handles(self.name())
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
            self._state(ServiceState.EXCEPTION, clear_pending_state=True)

    def on_start(self):
        """Called by start() before the state is change to STARTED."""
        pass

    def on_stop(self):
        """Called by stop() before the state is change to STOPPED."""
        pass

    def __str__(self) -> str:
        return 'Name [{}] State [{}/{}] PID [{}]'.format(self.name(), self.state(), self.pending_state(), os.getpid())


def duration(service: Service, seconds: float):
    """Run the given service for the given duration."""
    _logger.debug('duration() [{}]'.format(seconds))

    assert isinstance(service, Service)
    assert isinstance(seconds, float)

    service.start()
    try:
        time.sleep(seconds)
    finally:
        service.stop()


def restart(self, service: Service):
    """Restart the given service."""
    _logger.debug('restart() [{}]'.format(service.name()))

    assert isinstance(service, Service)

    try:
        self.stop()
        self.start()
    except Exception as exception:
        koolie.tools.common.log_exception(exception, _logger)


class ServicePacket(object):

    def __init__(self) -> None:
        super().__init__()


class StatePacket(ServicePacket):

    def __init__(self, state : ServiceState) -> None:
        super().__init__()

        self.__state = state

    def state(self) -> ServiceState:
        return self.__state


class PendingStatePacket(ServicePacket):

    def __init__(self, pending_state: ServiceState) -> None:
        super().__init__()

        self.__pending_state = pending_state

    def pending_state(self) -> ServiceState:
        return self.__pending_state


class ProcessService(Service):

    def __init__(self, **kwargs) -> None:
        """

        :param kwargs:
            rlock
            queue
            pipe
            manager
        """
        super().__init__(**kwargs)

        self.__parent_connection: multiprocessing.connection.Connection = None
        self.__child_connection: multiprocessing.connection.Connection = None

        self.__process: multiprocessing.Process = None

    def on_start(self):
        super().on_start()

    def on_stop(self):
        super().on_stop()

    def on_start(self):
        _logger.debug('on_start()')
        self.start_process()

    def on_stop(self):
        _logger.debug('on_stop()')
        self.stop_or_terminate_process()

    def rlock(self) -> multiprocessing.RLock:
        return self.get_kv('rlock')

    def queue(self) -> multiprocessing.Queue:
        return self.get_kv('queue')

    def start_process(self):
        _logger.debug('start_process()')
        self.__process = multiprocessing.Process(group=None, target=self.process, **self.kv())
        self.__process.start()

    def stop_process(self):
        _logger.debug('stop_process()')

    def terminate_process(self):
        _logger.debug('terminate_process()')
        self.__process.terminate()

    def stop_or_terminate_process(self):
        _logger.debug('stop_or_terminate_process()')
        try:
            self.stop_process()
        except ServiceException as service_exception:
            koolie.tools.common.log_exception(service_exception, logger=_logger)
            self.terminate_process()

    def wait_for_recv(self, c: multiprocessing.connection.Connection, **kwargs) -> typing.Tuple[bool, ServicePacket]:
        _logger.debug('wait_for_recv()')

        timeout = kwargs.get('timeout')
        assert timeout is None or isinstance(timeout, float)
        _logger.debug('timeout = [{}]'.format(timeout))

        expected = kwargs.get('expected', None)
        assert expected is None or isinstance(expected, set)
        _logger.debug('expected = [{}]'.format(expected))

        while True:
            c.poll(timeout)
            received = c.recv()
            _logger.debug('received = [{}]'.format(received))

            if expected is None:
                return True, received

            if type(received) in expected:
                return True, received

            _logger.debug('Skipping received.')

    def process(self, **kwargs):
        _logger.debug('process() [{}]'.format(kwargs))
        pass


class SleepService(Service):

    SLEEP_INTERVAL = 'sleep_interval'
    SLEEP_INTERVAL_DEFAULT = 1

    WAKE_INTERVAL = 'wake_interval'
    WAKE_INTERVAL_DEFAULT = 10

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.__sleep_interval: int = self.get_kv(self.SLEEP_INTERVAL, self.SLEEP_INTERVAL_DEFAULT)
        self.__wake_interval: int = self.get_kv(self.WAKE_INTERVAL, self.WAKE_INTERVAL_DEFAULT)

        self.__wake_count: int = 0

    def sleep_interval(self) -> int:
        return self.__sleep_interval

    def wake_interval(self) -> int:
        return self.__wake_interval

    def go(self):
        """Alternate between calling wake() and sleep()."""
        sleep_interval = self.sleep_interval()
        wake_interval = 0  # Causes the wake() call to be made when we enter the while loop.
        while self.state() is ServiceState.STARTED and self.pending_state() is not ServiceState.STOPPED:
            try:
                wake_interval -= sleep_interval
                if wake_interval <= 0:
                    wake_interval = self.wake_interval()
                    self.wake()
                time.sleep(sleep_interval)
            except Exception as exception:
                _logger.warning('Exception in go() [{}].'.format(exception))

    def wake(self):
        """Called from go() after sleeping, override to do something every interval."""
        self.__wake_count += 1
        pass

    def __str__(self) -> str:
        return '{}\nSleep [{}] Wake [{}]'.format(super().__str__(), self.sleep_interval(), self.wake_interval())


class QueueService(Service):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self._queue: queue.Queue = None

        self._producer_thread: threading.Thread = None

    def go(self):
        _logger.debug('go()')
        self._queue = queue.Queue()
        self._producer_thread = threading.Thread(group=None, target=self.producer)
        self._producer_thread.start()
        while self.state() is ServiceState.STARTED and self.pending_state() is not ServiceState.STOPPED:
            try:
                item = self._queue.get(block=True, timeout=1)
                self.item(item)
                self._queue.task_done()
            except queue.Empty as empty:
                # _logger.debug('empty')
                pass
        _logger.debug('go() END')

    def producer(self):
        pass

    def item(self, item):
        pass


def test_queue_service():
    class Add(QueueService):

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)

        def producer(self):
            _logger.debug('producer()')
            i = 0
            while self.state() is ServiceState.STARTED and self.pending_state() is not ServiceState.STOPPED:
                i += 1
                _logger.debug('put() {}'.format(i))
                self._queue.put(i)
                time.sleep(1)

        def item(self, item):
            _logger.debug('item()')
            print(item)

    add = Add(wake_interval=1)
    add.start()
    time.sleep(10)
    add.stop()


def test_service():
    duration(Service(), 5.0)


def test_process_service():

    class Sleep(ProcessService):

        def __init__(self, **kwargs) -> None:
            super().__init__(**kwargs)

        def on_stop(self):
            value: multiprocessing.Value = self.kv().get('stop')
            with value.get_lock():
                value.value = True

        def process(self, **kwargs):
            _logger.debug('process')
            value: multiprocessing.Value = self.kv().get('stop')
            stop = True
            with value.get_lock():
                stop = value.value
            while not stop:
                _logger.debug('sleeping...')
                time.sleep(1)
                with value.get_lock():
                    stop = value.value

    duration(Sleep(stop=multiprocessing.Value('b', False)), 2.0)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    _logger.info(koolie.tools.common.system_info())

    # test_service()

    test_process_service()

    # class Hello(ProcessService):
    #
    #     def __init__(self, **kwargs) -> None:
    #         super().__init__(**kwargs)
    #
    #     def process(self, child_connection: multiprocessing.connection.Connection):
    #         child_connection.send(StatePacket(ServiceState.STARTED))
    #         while True:
    #             print('Hello')
    #             if child_connection.poll(timeout=1):
    #                 print(child_connection.recv())
    #                 break
    #         child_connection.send(StatePacket(ServiceState.STOPPED))
    #
    #
    # process_service = Hello()
    # print(process_service)
    # process_service.start()
    # print(process_service)
    # time.sleep(2)
    # process_service.stop()
    # print(process_service)

    # abstract_service: AbstractService = AbstractService()
    # abstract_service.start()
    # time.sleep(2)
    # abstract_service.stop()

    # sleep_service: SleepService = SleepService()
    # sleep_service.start()
    # time.sleep(2)
    # # sleep_service.stop()
    # _logger.info(sleep_service)
    # sleep_service.restart()
    # _logger.info(sleep_service)
