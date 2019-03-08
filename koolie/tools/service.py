import abc
import logging
import signal
import time

_logger = logging.getLogger(__name__)

signalled_to_exit = False


def signal_to_exit(signum, frame):
    global signalled_to_exit
    signalled_to_exit = True
    _logger.info('Signalled to exit...')


signal.signal(signal.SIGINT, signal_to_exit)
signal.signal(signal.SIGTERM, signal_to_exit)


class Service(abc.ABC):

    CREATED = 2
    STARTED = 4
    STOPPED = 8

    def __init__(self, **kwargs) -> None:
        super().__init__()
        _logger.debug('kwargs [{}]'.format(kwargs))
        self.__kwargs = kwargs
        self.__state = Service.CREATED
        self.__exit = None

    def signalled_to_exit(self):
        return self.__exit or signalled_to_exit

    def state(self):
        return self.__state

    @abc.abstractmethod
    def start(self):
        self.__state = Service.STARTED
        self.__exit = False
        while not self.signalled_to_exit():
            self.go()
        self.__state = Service.STOPPED

    @abc.abstractmethod
    def stop(self):
        self.__exit = True

    @abc.abstractmethod
    def go(self):
        pass


class SleepService(Service):

    INTERVAL = 'sleep_service_interval'
    INTERVAL_DEFAULT = 10

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.__kwargs = kwargs
        self.__latency = 1
        self.__interval = self.__kwargs.get(SleepService.INTERVAL, SleepService.INTERVAL_DEFAULT)
        _logger.info('Latency [{}] Interval [{}]'.format(self.__latency, self.__interval))

    def sleep_interval(self) -> int:
        return self.__kwargs.get(SleepService.INTERVAL, SleepService.INTERVAL_DEFAULT)

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


if __name__ == '__main__':
    SleepService().start()
