import abc
import contextlib
import enum
import logging
import signal
import sys
import time
import uuid

_logger = logging.getLogger(__name__)

signalled_to_exit = False


def signal_to_exit(signum, frame):
    global signalled_to_exit
    signalled_to_exit = True


def signal_sigint(signum, frame):
    _logger.info('SIGINT')
    signal_to_exit(signum, frame)


def signal_sigterm(signum, frame):
    _logger.info('SIGTERM')
    signal_to_exit(signum, frame)


signal.signal(signal.SIGINT, signal_sigint)
signal.signal(signal.SIGTERM, signal_sigterm)


class ServiceState(enum.Enum):
    CREATED = 2
    STARTING = 4
    STARTED = 8
    STOPPING = 16
    STOPPED = 32
    EXCEPTION = 1024


class AbstractService(contextlib.AbstractContextManager):

    SERVICE_NAME = 'service_name'

    def __init__(self, **kwargs) -> None:
        super().__init__()
        _logger.debug('kwargs [{}]'.format(kwargs))
        self.__kwargs = kwargs
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
                print('Ctrl+C to exit.')
                self._state(ServiceState.STARTED)
                self.__exit = False
                while not self.signalled_to_exit():
                    self.go()
                self._state(ServiceState.STOPPED)
            else:
                _logger.warning('Cannot start service [{}]'.format(self))
        except Exception as exception:
            _logger.warn('Exception trying to start [{}] [{}]'.format(exception, self.__str__()))

    def starting(self):
        pass

    def stop(self):
        if self.__state in [ServiceState.STOPPED]:
            _logger.warning('Cannot stop service [{}]'.format(self))
        self.__exit = True

    def stopping(self):
        pass

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

    with SleepService() as sleep_service:
        pass
