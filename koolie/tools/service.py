from pickle import _load

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

    """Abstract service class which provides start() and stop() methods."""

    NAME: str = 'service_name'

    def __init__(self, name: str = None) -> None:
        super().__init__()

        if name is None:
            self.__name = str(uuid.uuid4())
        else:
            self.__name = name

        self.__rlock = threading.RLock()

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

    @property
    def name(self) -> str:
        return self.__name

    @property
    def rlock(self) -> threading.RLock:
        return self.__rlock

    @property
    def state(self) -> ServiceState:
        """The state of the service."""
        return self.__state

    @state.setter
    def state(self, state: ServiceState):
        """Change the state to the given state."""
        self.__state = state

    @property
    def pending_state(self) -> ServiceState:
        """The pending state of the service."""
        return self.__pending_state

    @pending_state.setter
    def pending_state(self, pending_state: ServiceState):
        """Change the pending state to the given state."""
        self.__pending_state = pending_state

    def start(self):
        """Start the service."""
        _logger.debug('start() [{}]'.format(self.name))
        try:
            with self.rlock:
                if self.state in {ServiceState.CREATED, ServiceState.STOPPED}:
                    self.pending_state = ServiceState.STARTED
                    add_sig_handles(self.__name, self.signal_handlers)
                    self.on_start()
                    self.state = ServiceState.STARTED
                    self.pending_state = None
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
            self.state = ServiceState.EXCEPTION

    def stop(self):
        """Stop the service."""
        _logger.debug('stop() [{}]'.format(self.name))
        try:
            with self.rlock:
                if self.state in {ServiceState.STARTED}:
                    self.pending_state = ServiceState.STOPPED
                    self.on_stop()
                    self.state = ServiceState.STOPPED
                    self.pending_state = None
                    remove_sig_handles(self.name)
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
            self.state = ServiceState.EXCEPTION

    def on_start(self):
        """Called by start() before the state is change to STARTED."""
        pass

    def on_stop(self):
        """Called by stop() before the state is change to STOPPED."""
        pass

    def __str__(self) -> str:
        return 'Name [{}] State [{}/{}] PID [{}]'.format(self.name, self.state, self.pending_state, os.getpid())


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


class ProcessService(Service):

    def __init__(self, name: str = None) -> None:
        super().__init__(name)

        self.__process: multiprocessing.Process = None

    @property
    def process(self) -> multiprocessing.Process:
        return self.__process

    @process.setter
    def process(self, process: multiprocessing.Process):
        self.__process = process

    def on_start(self):
        _logger.debug('on_start()')
        self.start_process()

    def on_stop(self):
        _logger.debug('on_stop()')
        self.stop_or_terminate_process()

    @abc.abstractmethod
    def start_process(self):
        pass

    @abc.abstractmethod
    def stop_process(self):
        pass

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


class ValueProcessService(ProcessService):

    def __init__(self, name: str = None):
        super().__init__(name)

        self.__value = multiprocessing.Value(ctypes.c_bool, False)

    @property
    def _value(self) -> multiprocessing.Value:
        """Private getter for the underlying 'multiprocessing.Value'."""
        return self.__value

    @property
    def value(self) -> bool:
        with self._value.get_lock():
            return self._value.value

    @value.setter
    def value(self, value: bool):
        with self._value.get_lock():
            self._value.value = value

    def start_process(self):
        self.process = multiprocessing.Process(group=None, target=self.go)
        self.process.start()

    def stop_process(self):
        _logger.debug('stop_process()')

        self.value = True
        attempt = 0
        while self.process.exitcode is None and attempt < 3:
            attempt += 1
            _logger.debug('attempt = [{}]'.format(attempt))
            self.process.join(10)

        if self.process.exitcode is None or self.process.exitcode != 0:
            raise ServiceException()

    @abc.abstractmethod
    def go(self, value: multiprocessing.Value):
        pass


def test_service():
    duration(Service(), 5.0)


def test_process_service():

    class Sleep(ValueProcessService):

        def __init__(self, name: str = None) -> None:
            super().__init__(name)

        def go(self):
            _logger.debug('go()')
            try:
                while not self.value:
                    _logger.debug('sleeping...')
                    time.sleep(1)
                _logger.debug('finished')
            except Exception as exception:
                koolie.tools.common.log_exception(exception, logger=_logger)

    duration(
        Sleep(),
        5.0
    )


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    _logger.info(koolie.tools.common.system_info())

    # test_service()

    test_process_service()
