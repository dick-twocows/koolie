import datetime
import logging
import time
import uuid
import yaml


_logger = logging.getLogger(__name__)


class Load(object):

    METADATA_ID = 'id'
    METADATA_STARTED = 'started'
    METADATA_STOPPED = 'stopped'

    def __init__(self) -> None:
        super().__init__()

        self.__metadata = dict()

    def start(self):
        self.__load_metadata[Load.METADATA_ID] = str(uuid.uuid4())
        self.__load_metadata[Load.METADATA_STARTED] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        self.__load_metadata['load_count'] = 0

    def stop(self):
        self.__load_metadata[Load.METADATA_STOPPED] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    def load_list(self, source: list):
        pass

    def load_file(self, *args):
        _logger.debug('load_file([{}])'.format(args))
        for name in args:
            _logger.debug('Name [{}]'.format(name))
            try:
                with open(file=name, mode='r') as file:
                    raw = file.read()
                self.load_list(yaml.load(raw.decode('utf-8')))
            except Exception as exception:
                _logger.warning('load_file() Failed to load file [{}] with exception [{}]'.format(name, exception))
