import abc
import logging
import signal

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

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass
