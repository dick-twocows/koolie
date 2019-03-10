import datetime
import logging
import sys
import time
import uuid
import yaml


_logger = logging.getLogger(__name__)

LOAD_POLICY_KEY = 'loadPolicy'

NAME_KEY = 'name'

TYPE_KEY = 'type'


NGINX_SERVER_TYPE = 'nginx/server'


LOAD_POLICY_UNIQUE = 'unique'

LOAD_POLICY_APPEND = 'append'


class Load(object):

    METADATA_ID = 'id'
    METADATA_STARTED = 'started'
    METADATA_STOPPED = 'stopped'
    METADATA_LOAD_COUNT = 'loadCount'

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

        self.__metadata = dict()

        self.__loaders = dict()
        self.__loaders[NGINX_SERVER_TYPE] = self.load_server

        self.__servers = dict()

    def loaders(self) -> dict:
        return self.__loaders

    def metadata(self) -> dict:
        return self.__metadata

    def load_success(self):
        self.metadata()[self.METADATA_LOAD_COUNT] = self.metadata()[self.METADATA_LOAD_COUNT] + 1

    def start(self):
        self.metadata()[Load.METADATA_ID] = str(uuid.uuid4())
        self.metadata()[Load.METADATA_STARTED] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        self.metadata()[Load.METADATA_LOAD_COUNT] = 0

    def stop(self):
        self.metadata()[Load.METADATA_STOPPED] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

    def load_dict(self, source: dict):
        _logger.debug('load_dict()')
        try:
            assert isinstance(source, dict)
            self.loaders()[source[TYPE_KEY]](source)
        except Exception as exception:
            _logger.warning('load_dict() Failed with exception [{}]'.format(exception))

    def load_list(self, items: list):
        _logger.debug('load_list([{}])'.format(list))
        try:
            assert isinstance(items, list)
            for item in items:
                self.load_dict(item)
        except Exception as exception:
            _logger.warning('load_list() Failed with exception [{}]'.format(exception))

    def load_file(self, *args):
        _logger.debug('load_file([{}])'.format(args))
        for name in args:
            _logger.debug('Name [{}]'.format(name))
            try:
                assert isinstance(name, str)
                with open(file=name, mode='r') as file:
                    raw = file.read()
                self.load_list(yaml.load(raw))
            except Exception as exception:
                _logger.warning('load_file() Failed to load file [{}] with exception [{}]'.format(name, exception))

    def load_server(self, source: dict):
        def load_unique():
            assert source[NAME_KEY] not in self.__servers.keys()
            # Add a new list containing the source.
            self.__servers[source[NAME_KEY]] = [source]

        def load_append():
            self.__servers.setdefault(source[NAME_KEY], []).append(source)

        try:
            assert isinstance(source, dict)
            {LOAD_POLICY_APPEND: load_append, LOAD_POLICY_UNIQUE: load_unique}[source[LOAD_POLICY_KEY]]()
            self.load_success()
        except Exception as exception:
            _logger.warning('load_server() Failed with exception [{}]'.format(exception))

    def __str__(self) -> str:
        return str(self.metadata())


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    load = Load()
    load.start()
    load.load_file('example_nginx_config.yaml')
    load.stop()
    print(load)
