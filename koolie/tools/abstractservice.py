import abc
import contextlib
import enum
import logging
import signal
import sys
import threading
import time
import uuid
from typing import Optional, Callable, Any, Iterable, Mapping

_logger = logging.getLogger(__name__)

signalled_to_exit = False


def signal_to_exit(signum, frame):
    """Set the global variable to True, if this is not be referenced somewhere nothing will happen."""
    global signalled_to_exit
    signalled_to_exit = True


def signal_sigint(signum, frame):
    """Indirection method so we can log the source of the SIG (INT)."""
    _logger.info('SIGINT')
    signal_to_exit(signum, frame)


def signal_sigterm(signum, frame):
    """Indirection method so we can log the source of the SIG (TERM)."""
    _logger.info('SIGTERM')
    signal_to_exit(signum, frame)


signal.signal(signal.SIGINT, signal_sigint)
signal.signal(signal.SIGTERM, signal_sigterm)


class ServiceState(enum.Enum):
    CREATED = 2
    STARTING = 4
    STARTED = 8
    STOPPING = 32
    STOPPED = 64
    EXCEPTION = 1024


class ServiceGo(threading.Thread):

    def __init__(self, group: None = ..., target: Optional[Callable[..., Any]] = ..., name: Optional[str] = ...,
                 args: Iterable = ..., kwargs: Mapping[str, Any] = ..., *, daemon: Optional[bool] = ...) -> None:
        super().__init__(group, target, name, args, kwargs, daemon=daemon)

    def run(self) -> None:
        _logger.info('run start')
        time.sleep(2)
        _logger.info('run stop')


class AbstractService(contextlib.AbstractContextManager):

    SERVICE_NAME = 'service_name'

    def __init__(self, **kwargs) -> None:
        super().__init__()
        _logger.debug('kwargs [{}]'.format(kwargs))
        self.__kwargs = kwargs

        self.__lock = threading.Lock()

        self.__state: ServiceState = ServiceState.CREATED
        self.__exit = False

        self.__name = kwargs.get(self.SERVICE_NAME, str(uuid.uuid4()))

    def __enter__(self):
        self.start()
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        return super().__exit__(exc_type, exc_value, traceback)

    def name(self):
        return self.__name

    def state(self):
        return self.__state

    def _state(self, state: ServiceState):
        self.__state = state
        _logger.info(self.__str__())

    def signalled_to_exit(self):
        return self.__exit or signalled_to_exit

    def start(self):
        try:
            if self.__state in [ServiceState.CREATED, ServiceState.STOPPED]:
                _logger.info('Press Ctrl+C to exit.')
                self.starting()
                self._state(ServiceState.STARTED)
                self.__exit = False
                while not self.signalled_to_exit():
                    self.go()
            else:
                _logger.warning('Cannot start service [{}]'.format(self))
        except Exception as exception:
            self.exception(exception)

    def starting(self):
        self._state(ServiceState.STARTING)
        pass

    def stop(self):
        try:
            if self.__state in [ServiceState.STARTED]:
                self.__exit = True
                while self.__state in [ServiceState.STARTED]:
                    _logger.debug('Waiting for [{}].'.format(ServiceState.STOPPING))
                    time.sleep(1)
                self._state(ServiceState.STOPPED)
            else:
                _logger.warning('Cannot stop service [{}]'.format(self))
        except Exception as exception:
            self.exception(exception)

    def stopping(self):
        self._state(ServiceState.STOPPING)
        pass

    def exception(self, exception: Exception):
        self._state(ServiceState.EXCEPTION)
        _logger.warn('Exception [{}] [{}]'.format(exception, self.__str__()))

    @abc.abstractmethod
    def go(self):
        pass

    def __str__(self) -> str:
        return 'AbstractService [Name [{}] State [{}]]'.format(self.name(), self.state())


class SleepService(AbstractService):

    SLEEP_LATENCY = 'sleep_latency'
    SLEEP_LATENCY_DEFAULT = 1
    SLEEP_INTERVAL = 'sleep_service_interval'
    SLEEP_INTERVAL_DEFAULT = 10

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.__kwargs = kwargs
        self.__latency = self.__kwargs.get(self.SLEEP_LATENCY, self.SLEEP_LATENCY_DEFAULT)
        self.__interval = self.__kwargs.get(self.SLEEP_INTERVAL, self.SLEEP_INTERVAL_DEFAULT)
        _logger.info(self.__str__())

    def sleep_interval(self) -> int:
        return self.__kwargs.get(self.SLEEP_INTERVAL, self.SLEEP_INTERVAL_DEFAULT)

    def start(self):
        super().start()

    def stop(self):
        super().stop()

    def go(self):
        _logger.debug('go()')
        count = 0
        while count <= self.__interval and not self.signalled_to_exit():
            time.sleep(self.__latency)
            count = count + 1

    def __str__(self) -> str:
        return 'SleepService [Latency [{}] Interval [{}] [{}]]'.format(self.__latency, self.__interval, super().__str__())


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    service_go: ServiceGo = ServiceGo(None).start()

    time.sleep(5)

    # with SleepService() as sleep_service:
    #     pass
