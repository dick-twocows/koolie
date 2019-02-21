import logging
import signal

_logger = logging.getLogger(__name__)


flag_to_exit = False


def set_flag_to_exit(signum, frame):
    comm = True

signal.signal()

signal.signal(signal.SIGINT, set_flag_to_exit)
signal.signal(signal.SIGTERM, set_flag_to_exit)
