import contextlib
import enum
import logging
import os
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


def add_sig_handle(signum: int, name: str, handle: Handle):
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
    with _sig_rlock:
        handles: Handles
        for handles in _sig_handles.values():
            handles.pop(name)


def go_sig_handle(signum: int, frame):
    _logger.info('{}.'.format(signal.Signals(signum).name))
    with _sig_rlock:
        stack = traceback.extract_stack(frame)
        _logger.debug(stack)
        handles: Handles = _sig_handles.get(signum)
        if handles is None:
            _logger.debug('No handles defined for [{}].'.format(signum))
        else:
            for name, handle in handles.copy().items():  # Use copy() to avoid 'RuntimeError: dictionary changed size during iteration'
                try:
                    handle(signum)
                except Exception as exception:
                    _logger.warning('Exception [{}] calling handler [{}] for [{}].'.format(exception, name, signum))


class ServiceState(enum.Enum):
    EXCEPTION = -2
    CREATED = 0
    STARTED = 2
    STOPPED = 4


class AbstractService(contextlib.AbstractContextManager):

    """Abstract service class which provides start() and stop() methods.
    Work is performed in the go() method which is started in its own thread."""

    NAME: str = 'abstract_service_name'

    def __init__(self, **kwargs) -> None:
        super().__init__()

        self._kwargs = kwargs

        self.__name = self.get_kv(self.NAME)
        if self.__name is None:
            self.__name = str(uuid.uuid4())

        self.__rlock = threading.RLock()
        self.__go_thread: threading.Thread = None

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

    def get_kv(self, k: str, v: object = None) -> object:
        return self._kwargs.get(k, v)

    def has_k(self, k: str) -> bool:
        return k in self._kwargs.keys()

    def name(self) -> str:
        return self.__name

    def state(self) -> ServiceState:
        """The state of the service."""
        return self.__state

    def _state(self, state: ServiceState):
        """Change the state to the given state."""
        _logger.debug('Change [{}] state from [{}] to [{}].'.format(self.name(), self.state(), state))
        with self.__rlock:
            self.__state = state

    def pending_state(self) -> ServiceState:
        """The pending state of the service."""
        return self.__pending_state

    def _pending_state(self, pending_state: ServiceState):
        """Change the pending state to the given state."""
        _logger.debug('Change [{}] pending state [{}] -> [{}].'.format(self.name(), self.pending_state(), pending_state))
        with self.__rlock:
            self.__pending_state = pending_state

    def restart(self):
        """Restart the service."""
        _logger.info('Restart [{}].'.format(self.name()))
        try:
            with self.__rlock:
                self.stop()
                self.start()
        except Exception as exception:
            _logger.warning('Failed to start with exception [{}].'.format(exception))

    def start(self):
        """Start the service."""
        _logger.info('Start [{}].'.format(self.name()))
        try:
            with self.__rlock:
                if self.state() in {ServiceState.CREATED, ServiceState.STOPPED}:
                    _logger.debug('Adding signal handlers [{}].'.format(self.signal_handlers.keys()))
                    for signum, handle in self.signal_handlers.items():
                        add_sig_handle(signum, self.__name, handle)
                    self.before_start()
                    self._state(ServiceState.STARTED)
                    self._pending_state(None)
                    self.__go_thread = threading.Thread(group=None, target=self.go)
                    self.__go_thread.start()
        except Exception as exception:
            _logger.warning('Failed to start with exception [{}].'.format(exception))

    def before_start(self):
        """Called by start() before the state is change to STARTED."""
        pass

    def stop(self):
        """Stop the service."""
        _logger.info('Stop [{}].'.format(self.name()))
        try:
            with self.__rlock:
                if self.__state in {ServiceState.STARTED}:
                    self.__pending_state = ServiceState.STOPPED
                    while self.__go_thread.is_alive():
                        _logger.debug('Waiting for go thread to join.')
                        self.__go_thread.join(1)
                    self.before_stop()
                    self._state(ServiceState.STOPPED)
                    self._pending_state(None)
                    remove_sig_handles(self.name())
        except Exception as exception:
            _logger.warning('Failed to stop with exception [{}].'.format(exception))

    def before_stop(self):
        """Called by stop() before the state is change to STOPPED."""
        pass

    def go(self):
        _logger.debug('Go  [{}].'.format(self.name()))

    def __str__(self) -> str:
        return 'Name [{}] State [{}/{}] PID [{}]'.format(self.name(), self.state(), self.pending_state(), os.getpid())


class SleepService(AbstractService):

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


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    _logger.info('PID is [{}].'.format(os.getpid()))

    # with AbstractService():
    #     pass

    # abstract_service: AbstractService = AbstractService()
    # abstract_service.start()
    # time.sleep(2)
    # abstract_service.stop()

    sleep_service: SleepService = SleepService()
    sleep_service.start()
    time.sleep(2)
    # sleep_service.stop()
    _logger.info(sleep_service)
    sleep_service.restart()
    _logger.info(sleep_service)
