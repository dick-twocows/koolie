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

    def __init__(self, **kwargs) -> None:
        super().__init__()
        _logger.debug('kwargs [{}]'.format(kwargs))
        self.__kwargs = kwargs
        self.__exit = None

    @abc.abstractmethod
    def start(self):
        self.__exit = False
        while not self.__exit and not signalled_to_exit:
            self.go()

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
        self.__interval = self.__kwargs.get(SleepService.INTERVAL, SleepService.INTERVAL_DEFAULT)
        _logger.info('Interval [{}]'.format(self.__interval))

    def start(self):
        super().start()

    def stop(self):
        super().stop()

    def go(self):
        _logger.debug('go()')
        time.sleep(self.__interval)


if __name__ == '__main__':
    SleepService().start()
