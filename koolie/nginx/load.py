import datetime
import logging
import sys
import time
import typing
import uuid
import yaml

import koolie.nginx.config
import koolie.tools.common

_logger = logging.getLogger(__name__)

LOAD_POLICY_KEY = 'loadPolicy'

NAME_KEY = 'name'

TYPE_KEY = 'type'


NGINX_MAIN_TYPE = 'nginx/main'

NGINX_EVENTS_TYPE = 'nginx/events'

NGINX_HTTP_TYPE = 'nginx/http'

NGINX_SERVER_TYPE = 'nginx/server'

NGINX_LOCATION_TYPE = 'nginx/location'

NGINX_UPSTREAM_TYPE = 'nginx/upstream'


LOAD_METADATA_KEY = 'loadMetadata'

LOAD_POLICY_UNIQUE = 'unique'

LOAD_POLICY_APPEND = 'append'


class Load(object):

    METADATA_ID = 'id'
    METADATA_STARTED = 'started'
    METADATA_STOPPED = 'stopped'
    METADATA_LOAD_COUNT = 'loadCount'

    def __init__(self, config: koolie.nginx.config.Config) -> None:
        super().__init__()

        if config is None:
            self.__config = koolie.nginx.config.Config()
        else:
            self.__config = config

        self.__loaders = {
            NGINX_MAIN_TYPE: self.load_main,
            NGINX_EVENTS_TYPE: self.load_events,
            NGINX_HTTP_TYPE: self.load_http,
            NGINX_SERVER_TYPE: self.load_server,
            NGINX_LOCATION_TYPE: self.load_location,
            NGINX_UPSTREAM_TYPE: self.load_upstream
        }

    def config(self) -> list:
        return self.__config

    def metadata(self) -> dict:
        return self.config().load_metadata()

    def loaders(self) -> dict:
        return self.__loaders

    def config(self) -> koolie.nginx.config.Config:
        return self.__config

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

    def load_unique(self, item: koolie.nginx.config.NGINX, items: typing.Dict[str, typing.List]):
        _logger.debug('load_unique')
        assert isinstance(item, koolie.nginx.config.NGINX)
        assert item.fqn() not in items.keys()
        # Add a new list containing the item.
        items[item.fqn()] = [item]

    def load_append(self, item: koolie.nginx.config.NGINX, items: typing.Dict[str, typing.List]):
        if item.fqn() in items.keys():
            items[item.fqn()].append(item)
        else:
            items[item.fqn()] = [item]

    load_policy = {LOAD_POLICY_APPEND: load_append, LOAD_POLICY_UNIQUE: load_unique}

    def load_main(self, source: typing.Dict):
        _logger.debug('load_main()')
        try:
            assert isinstance(source, typing.Dict)
            self.config().add_item(koolie.nginx.config.Main(source))
            self.load_success()
        except Exception as exception:
            _logger.warning('load_main() Failed with exception [{}]'.format(exception))

    def load_events(self, source: dict):
        _logger.debug('load_events()')
        try:
            assert isinstance(source, dict)
            self.load_policy[source[LOAD_POLICY_KEY]](self, source, self.config().events())
            self.load_success()
        except Exception as exception:
            _logger.warning('load_events() Failed with exception [{}]'.format(exception))

    def load_http(self, source: dict):
        _logger.debug('load_http()')
        try:
            assert isinstance(source, dict)
            self.load_policy[source[LOAD_POLICY_KEY]](self, source, self.config().http())
            self.load_success()
        except Exception as exception:
            _logger.warning('load_http() Failed with exception [{}]'.format(exception))

    # Servers

    def load_server_unique(self, server: koolie.nginx.config.Server):
        _logger.debug('load_server_unique()')
        try:
            assert server.fqn() not in self.config().servers().keys()
            self.config().servers()[server.fqn()] = [server]
            self.load_success()
        except Exception as exception:
            _logger.warning('load_server() Failed with exception [{}]'.format(exception))

    server_load_policy = {koolie.nginx.config.LOAD_POLICY_UNIQUE: load_server_unique}

    def load_server(self, source: dict):
        _logger.debug('load_server()')
        try:
            assert isinstance(source, dict)
            server = koolie.nginx.config.Server(source)
            self.server_load_policy[server.load_policy()](self, server)
            self.load_success()
        except Exception as exception:
            _logger.warning('load_server() Failed with exception [{}]'.format(exception))

    # location

    def location_fqn(self, location: dict) -> str:
        assert isinstance(location, dict)
        return '{}_{}'.format(location[koolie.nginx.config.SERVER_KEY], koolie.nginx.config.NAME_KEY)

    def load_location_unique(self, location: dict):
        assert isinstance(location, dict)
        assert self.location_fqn(location) not in self.config().locations()
        # Add a new list containing the item.
        self.config().locations()[self.location_fqn(location)] = [location]

    def load_location_append(self, location: dict):
        self.config().locations().setdefault(self.location_fqn(location), []).append(location)

    def load_location(self, location: dict):
        _logger.debug('load_location()')
        try:
            assert isinstance(location, dict)
            {LOAD_POLICY_APPEND: self.load_location_append, LOAD_POLICY_UNIQUE: self.load_location_unique}[location[koolie.nginx.config.LOAD_POLICY]](location)
            self.load_success()
        except Exception as exception:
            _logger.warning('load_location() Exception [{}]'.format(koolie.tools.common.decode_exception(exception)))

    def load_upstream(self, source: dict):
        _logger.debug('load_upstream()')
        try:
            assert isinstance(source, dict)
            self.load_policy[source[LOAD_POLICY_KEY]](self, source, self.config().upstreams())
            self.load_success()
        except Exception as exception:
            _logger.warning('load_upstream() Failed with exception [{}]'.format(exception))

    def __str__(self) -> str:
        return str(self.metadata())


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    load = Load()
    load.start()
    load.load_file('example_nginx_config.yaml')
    load.stop()
    print(load)
