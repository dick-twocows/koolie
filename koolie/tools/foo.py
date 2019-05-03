import contextlib
import enum
import logging
import sys
import time
import threading


_logging = logging.getLogger(__name__)


class ServiceState(enum.Enum):
    EXCEPTION = -2
    CREATED = 0
    STARTED = 2
    STOPPED = 4


class AbstractService(contextlib.AbstractContextManager):

    def __init__(self) -> None:
        super().__init__()

        self.__rlock = threading.RLock()
        self.__go_thread: threading.Thread = None
        self.__exit: bool = False

        self.__state: ServiceState = None
        self.__pending_state: ServiceState = None
        self._state(ServiceState.CREATED)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def state(self) -> ServiceState:
        """The state of the service."""
        return self.__state

    def _state(self, state: ServiceState):
        """Change the state to the given state."""
        with self.__rlock:
            _logging.debug('_state([{}])'.format(state))
            self.__state = state

    def pending_state(self) -> ServiceState:
        """The pending state of the service."""
        return self.__pending_state

    def _pending_state(self, state: ServiceState):
        """Change the pending state to the given state."""
        with self.__rlock:
            _logging.debug('_pending_state([{}])'.format(state))
            self.__pending_state = state

    def exit(self) -> bool:
        return self.__exit

    def start(self):
        """Start the service."""
        try:
            with self.__rlock:
                if self.state() in {ServiceState.CREATED, ServiceState.STOPPED}:
                    self.before_start()
                    self._state(ServiceState.STARTED)
                    self.__go_thread = threading.Thread(group=None, target=self.go)
                    self.__go_thread.start()
        except Exception as exception:
            _logging.warning('Failed to start with exception [{}].'.format(exception))

    def before_start(self):
        """Called by start() before the state is change to STARTED."""
        pass

    def stop(self):
        """Stop the service."""
        try:
            with self.__rlock:
                if self.__state in {ServiceState.STARTED}:
                    self.__pending_state = ServiceState.STOPPED
                    self.__go_thread.join()
                    self.before_stop()
                    self._state(ServiceState.STOPPED)
        except Exception as exception:
            _logging.warning('Failed to stop with exception [{}].'.format(exception))

    def before_stop(self):
        """Called by stop() before the state is change to STOPPED."""
        pass

    def go(self):
        _logging.debug('go()')

    def __str__(self) -> str:
        return 'State [{}]'.format(self.__state)


class SleepService(AbstractService):

    SLEEP_INTERVAL = 'sleep_interval'
    SLEEP_INTERVAL_DEFAULT = 1

    def __init__(self) -> None:
        super().__init__()

        self.__interval: int = self.SLEEP_INTERVAL_DEFAULT

    def interval(self) -> int:
        return self.__interval

    def go(self):
        while self.state() is ServiceState.STARTED and self.pending_state() is not ServiceState.STOPPED:
            _logging.debug('go() interval.')
            time.sleep(self.interval())


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    with AbstractService():
        pass

    abstract_service: AbstractService = AbstractService()
    abstract_service.start()
    time.sleep(2)
    abstract_service.stop()

    sleep_service: SleepService = SleepService()
    sleep_service.start()
    time.sleep(2)
    sleep_service.stop()
