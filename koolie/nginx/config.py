import abc
import collections
import datetime
import logging
import os
import re
import subprocess
import time
import typing
import koolie.tools.common
import uuid
import yaml

_logger = logging.getLogger(__name__)


NGINX_KEY = 'nginx'

CONFIG_KEY = 'config'

NGINX_DIRECTORY_KEY = 'nginx_directory'

NGINX_SERVERS_DIRECTORY_KEY = 'nginx_servers_directory'

NGINX_UPSTREAMS_DIRECTORY_KEY = 'nginx_upstreams_directory'

NGINX_DIRECTORY_DEAFULT = '/tmp/nginx/'

SOURCE_KEY = 'source'

TAG_KEY = 'tag'

TYPE_KEY = 'type'

NAME_KEY = 'name'

LOAD_POLICY = 'loadPolicy'

LOAD_POLICY_UNIQUE = 'unique'

LOAD_POLICY_APPEND = 'append'

LOAD_POLICY_REMOVE = 'remove'

CORE_KEY = 'core'

SERVER_KEY = 'server'

SERVERS_KEY = 'servers'

LOCATIONS_KEY = 'locations'

UPSTREAMS_KEY = 'upstreams'

LOCATION_MATCH_MODIFIER = 'matchModifier'
LOCATION_LOCATION_MATCH = 'locationMatch'



NGINX_ROOT_TYPE = 'nginx/root'

NGINX_MAIN_TYPE = 'nginx/main'

NGINX_EVENTS_TYPE = 'nginx/events'

NGINX_HTTP_TYPE = 'nginx/http'

NGINX_SERVER_TYPE = 'nginx/server'
NGINX_SERVER_PREFIX_TYPE = 'nginx/server_prefix'
NGINX_SERVER_SUFFIX_TYPE = 'nginx/server_suffix'

NGINX_LOCATION_TYPE = 'nginx/location'
NGINX_LOCATION_PREFIX_TYPE = 'nginx/location_prefix'
NGINX_LOCATION_SUFFIX_TYPE = 'nginx/location_suffix'

NGINX_UPSTREAM_TYPE = 'nginx_upstream'
NGINX_UPSTREAM_PREFIX_TYPE = 'nginx_upstream_prefix'
NGINX_UPSTREAM_SUFFIX_TYPE = 'nginx_upstream_suffix'


NGINX_SERVER_PREFIX_FQN = '{}.{}'.format(NGINX_SERVER_TYPE, '_prefix')

NGINX_SERVER_SUFFIX_FQN = '{}.{}'.format(NGINX_SERVER_TYPE, '_suffix')

NGINX_UPSTREAM_PREFIX_FQN = '{}.{}'.format(NGINX_LOCATION_TYPE, '_prefix')

NGINX_UPSTREAM_SUFFIX_FQN = '{}.{}'.format(NGINX_LOCATION_TYPE, '_suffix')


LOAD_METADATA_KEY = 'load_metadata'

DUMP_METADATA_KEY = 'dump_metadata'


class Base(abc.ABC):

    TYPE_PATTERN = re.compile('[a-zA-Z0-9/_-]+')

    def __init__(self, data: dict = None) -> None:
        super().__init__()

        if data is None:
            self.__data = {}
        else:
            assert isinstance(data, dict)
            self.__data = data

    def data(self) -> dict:
        return self.__data

    def type(self) -> str:
        return self.data().get(TYPE_KEY, '')

    def name(self) -> str:
        return self.data().get(NAME_KEY, '')

    def load_policy(self) -> str:
        return self.data().get(LOAD_POLICY, LOAD_POLICY_UNIQUE)

    def fqn(self) -> str:
        """
        The FQN which by default is {type__name}
        :return: str
        """
        return '{}__{}'.format(self.type(), self.name())

    def source(self) -> str:
        return self.data().get(SOURCE_KEY, '')

    def tag(self) -> str:
        return self.data().self(TAG_KEY, '')

    def tokens(self) -> typing.Dict[str, str]:
        tokens = {}
        for k, v in self.data().items():
            tokens['{}__{}'.format(self.type(), k)] = v
        return tokens

    def __str__(self) -> str:
        return self.fqn()


class NGINX(Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)

    def config(self, substitute: typing.Dict[str, str] = None, self_in_substitute: bool = True) -> str:
        if substitute is None:
            return self.data().get(CONFIG_KEY, '')

        if self_in_substitute:
            return koolie.tools.common.substitute(self.config(), collections.ChainMap(substitute, self.tokens()))

        return koolie.tools.common.substitute(self.config(), substitute)


class Root(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Main(Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Events(Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class HTTP(Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Server(Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class ServerPrefix(Server):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class ServerSuffix(Server):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Location(Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)

    def server(self) -> str:
        return self.data()[SERVER_KEY]

    def fqn(self) -> str:
        return '{}__{}__{}'.format(self.type(), self.server(), self.name())

    def match_modifier(self) -> str:
        return self.data()[LOCATION_MATCH_MODIFIER]

    def location_match(self) -> str:
        return self.data()[LOCATION_LOCATION_MATCH]


class Affix(Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


DEFAULT_LOCATION_PREFIX = [
    Affix(
        {
            TYPE_KEY: NGINX_LOCATION_PREFIX_TYPE,
            NAME_KEY: '_default',
            CONFIG_KEY: 'location ${match_modifier} ${location_match} {{\n'
        }
    )
]

DEFAULT_LOCATION_SUFFIX = [
    Affix(
        {
            TYPE_KEY: NGINX_LOCATION_SUFFIX_TYPE,
            NAME_KEY: '_default',
            CONFIG_KEY: '}\n'
        }
    )
]


DEFAULT_UPSTREAM_PREFIX = [
    Affix(
        {
            TYPE_KEY: NGINX_UPSTREAM_PREFIX_TYPE,
            NAME_KEY: '_default',
            CONFIG_KEY: 'upstream ${nginx_upstream__name} {\n'
        }
    )
]

DEFAULT_UPSTREAM_SUFFIX = [
    Affix(
        {
            TYPE_KEY: NGINX_UPSTREAM_SUFFIX_TYPE,
            NAME_KEY: '_default',
            CONFIG_KEY: '}\n'
        }
    )
]

class LocationPrefix(Affix):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class LocationSuffix(Affix):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Upstream(Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Config(object):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

        self.__args = args

        self.__kwargs = kwargs

        self.__items: typing.Dict[str, typing.List] = {}

    def items(self) -> typing.Dict[str, typing.List]:
        return self.__items

    def add_unique_item(self, item: Base):
        _logger.debug('add_unique()')
        assert isinstance(item, Base)
        _logger.debug('add_unique() item=[{}]'.format(item))
        assert item.fqn() not in self.items().keys()
        # Add a new list containing the item.
        self.items()[item.fqn()] = [item]

    def append_item(self, item: Base):
        _logger.debug('append_item()')
        assert isinstance(item, Base)
        _logger.debug('append_item() item=[{}]'.format(item))
        if item.fqn() in self.items().keys():
            self.items()[item.fqn()].append(item)
        else:
            self.items()[item.fqn()] = [item]

    load_dispatcher: typing.Dict[str, typing.Callable] = {LOAD_POLICY_APPEND: append_item, LOAD_POLICY_UNIQUE: add_unique_item}

    def add_item(self, item: Base):
        assert isinstance(item, Base)
        self.load_dispatcher[item.load_policy()](self, item)

    item_creator: typing.Dict[str, typing.Callable[[typing.Dict], Base]] = {
        NGINX_ROOT_TYPE: Root,
        NGINX_MAIN_TYPE: Main,
        NGINX_EVENTS_TYPE: Events,
        NGINX_HTTP_TYPE: HTTP,
        NGINX_SERVER_TYPE: Server,
        NGINX_SERVER_PREFIX_TYPE: ServerPrefix,
        NGINX_SERVER_SUFFIX_TYPE: ServerSuffix,
        NGINX_LOCATION_TYPE: Location,
        NGINX_UPSTREAM_TYPE: Upstream
    }

    def load(self, *args: typing.List[typing.Union[str]]):
        _logger.debug('load()')
        for name in args:
            try:
                assert isinstance(name, str)
                _logger.debug('add_file() name=[{}]'.format(name))
                with open(file=name, mode='r') as file:
                    raw = file.read()
                items: typing.List[typing.Dict] = yaml.load(raw)
                for item in items:
                    try:
                        base: Base = Base(item)
                        self.add_item(self.item_creator[base.type()](base.data()))
                    except Exception as exception:
                        _logger.warning('load() Item exception [{}]'.format(koolie.tools.common.decode_exception(exception)))
            except Exception as exception:
                _logger.warning('load() File exception [{}]'.format(koolie.tools.common.decode_exception(exception)))

    # Dump

    def dump(self, **kwargs: typing.Dict[str, str]):
        _logger.debug('dump()')

        dump_tokens: typing.Dict[str, str] = {
            'config__nginx_directory': NGINX_DIRECTORY_DEAFULT
        }

        def nginx_directory() -> str:
            return dump_tokens.get('config__nginx_directory')

        def file_prefix() -> str:
            return '# Koolie\n\n'

        def write(directory: str, name: str, base: typing.List[Base], tokens: typing.Dict[str, str]):
            _logger.debug('write()')
            _logger.debug('tokens = [{}]'.format(tokens))
            koolie.tools.common.ensure_directory(directory)
            with open(file='{}{}'.format(directory, name), mode='a') as file:
                if file.tell() == 0:
                    file.write(file_prefix())
                for item in base:
                    file.write('# FQN [{}]\n'.format(item.fqn()))
                    file.write(koolie.tools.common.substitute(item.config(), **tokens))

        def dump_root(root: Root):
            _logger.debug('dump_root()')
            write(nginx_directory(), 'nginx.conf', [root])

        def dump_main(main: Main):
            _logger.debug('dump_main()')
            write(nginx_directory(), 'main.conf', [main])

        def dump_events(events: Events):
            _logger.debug('dump_events()')
            write(nginx_directory(), 'events.conf', [events])

        def dump_http(http: HTTP):
            _logger.debug('dump_http()')
            write(nginx_directory(), 'http.conf', [http])

        def dump_server(server: Server):
            _logger.debug('dump_server()')
            write('{}servers/'.format(nginx_directory()), server.name(), self.items()[NGINX_SERVER_PREFIX_FQN])
            write('{}servers/'.format(nginx_directory()), server.name(), [server])
            write('{}servers/'.format(nginx_directory()), server.name(), self.items()[NGINX_SERVER_SUFFIX_FQN])

        def dump_location(location: Location):
            _logger.debug('dump_location()')

        def dump_upstream(parts: typing.List[Upstream]):
            _logger.debug('dump_upstream()')

            tokens = collections.ChainMap(dump_tokens, parts[0].tokens())
            write('{}upstreams/'.format(nginx_directory()), parts[0].name(), self.items().get(NGINX_UPSTREAM_PREFIX_TYPE, DEFAULT_UPSTREAM_PREFIX), tokens)

            write('{}upstreams/'.format(nginx_directory()), parts[0].name(), self.items().get(NGINX_UPSTREAM_SUFFIX_TYPE, DEFAULT_UPSTREAM_SUFFIX), tokens)

            # tokens = collections.ChainMap(dump_tokens, upstream.data())
            # write('{}upstreams/'.format(nginx_directory()), upstream.name(), self.items().get('nginx/upstreams._prefix', DEFAULT_LOCATION_PREFIX), tokens)
            # write('{}upstreams/'.format(nginx_directory()), upstream.name(), [upstream])
            # write('{}upstreams/'.format(nginx_directory()), upstream.name(), self.items().get('nginx/upstreams._suffix', DEFAULT_LOCATION_SUFFIX))

        def dump_ignore(base: Base):
            _logger.debug('dump_ignore()')

        dump_dispatcher: typing.Dict[str, typing.Callable[['Config', Base, typing.Dict[str, str]], None]] = {
            # NGINX_ROOT_TYPE: dump_root,
            # NGINX_MAIN_TYPE: dump_main,
            # NGINX_EVENTS_TYPE: dump_events,
            # NGINX_HTTP_TYPE: dump_http,
            # NGINX_SERVER_TYPE: dump_server,
            # NGINX_LOCATION_TYPE: dump_location,
            NGINX_UPSTREAM_TYPE: dump_upstream
        }

        for parts in self.items().values():
            dump_dispatcher.get(parts[0].type(), dump_ignore)(parts)


        # _logger.debug('kwargs [{}]'.format('\n'.join(k for k in kwargs.keys())))
        # for fqn in self.items().keys():
        #     _logger.debug('dump() FQN [{}]'.format(fqn))
        #     dump_parts(self.items()[fqn])
            # for item in self.items()[fqn]:
            #     _logger.debug('Item [{}]'.format(item))
            #     try:
            #         # dump_dispatcher.get(item.type(), dump_ignore)(item)
            #         pass
            #     except Exception as exception:
            #         _logger.warning('dump() Exception [{}]'.format(koolie.tools.common.decode_exception(exception)))

    def args(self) -> list:
        return self.__args

    def kwargs(self) -> dict:
        return self.__kwargs

    def load_metadata(self) -> dict:
        return self.data()[LOAD_METADATA_KEY]

    def dump_metadata(self) -> dict:
        return self.data()[DUMP_METADATA_KEY]

    def nginx_directory(self) -> str:
        return self.__kwargs.get(NGINX_DIRECTORY_KEY, NGINX_DIRECTORY_DEAFULT)

    def __str__(self) -> str:
        return '{}'.format(len(self.items()))


def if_none(v: object, o: object) -> object:
    return koolie.tools.common.if_none(v, o)


class NGINXConfig(object):

    def __init__(self, **kwargs) -> None:
        super().__init__()

        self.__kwargs = kwargs

        self.__nginx_directory = if_none(self.get_from_kwargs('nginx_directory'), NGINX_DIRECTORY_DEAFULT)
        self.__nginx_servers_directory = if_none(self.get_from_kwargs('nginx_servers_directory'), '{}servers/'.format(self.__nginx_directory))
        self.__nginx_upstreams_directory = if_none(self.get_from_kwargs('nginx_upstreams_directory'), '{}upstreams/'.format(self.__nginx_directory))
        _logger.info('NGINX folders\nNGINX [{}]\nServers [{}]\nUpstreams [{}]'.format(self.__nginx_directory, self.__nginx_servers_directory, self.__nginx_upstreams_directory))

        self.__data = dict()
        self.reset()

        self.__loaded_count = 0

        self.__load_metadata = dict()

        self.__load_from_list = dict()
        self.__load_from_list[CORE_TYPE] = self.load_core
        self.__load_from_list[SERVER_TYPE] = self.load_server
        self.__load_from_list[LOCATION_TYPE] = self.load_location
        self.__load_from_list[UPSTREAM_TYPE] = self.load_upstream

        self.__dump_metadata = dict()

    def __str__(self) -> str:
        return str(self.__data)

    def get_from_kwargs(self, k, v: object = None):
        return koolie.tools.common.get_from_kwargs(k, v, **self.__kwargs)

    @property
    def data(self) -> dict:
        return self.__data

    @data.setter
    def data(self, data: dict):
        assert data is not None and isinstance(data, dict)
        self.__data = data

    def loaded_count(self) -> int:
        return self.__loaded_count

    def load_metadata(self):
        return self.__load_metadata

    def common_nginx_conf_comments(self):
        return '# Created by Koolie\n# [{}]\n# Metadata [{}]\n\n'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'), self.__load_metadata)

    def create_servers_folder(self):
        if not os.path.exists(self.__nginx_servers_directory):
            os.makedirs(name=self.__nginx_servers_directory, exist_ok=True)

    def write_conf(self, name, data):
        _logger.debug('write_conf()')
        try:
            with open(file=name, mode='w') as file:
                file.write(self.common_nginx_conf_comments())
                file.write(data)
        except Exception as exception:
            _logger.warning('Failed to write file [{}] [{}]'.format(name, exception))

    def get_tag(self, source: dict) -> str:
        assert source is not None and isinstance(source, dict)
        return source.get(TAG_KEY, '')

    def get_location_id(self, source) -> str:
        assert source is not None and isinstance(source, dict)
        return '({}){}/{}'.format(self.get_tag(source), source.get(SERVER_KEY), source.get(NAME_KEY))

    def core(self) -> dict:
        return self.data[CORE_KEY]

    def servers(self) -> list:
        return self.data[SERVERS_KEY]

    def get_server(self, name) -> dict:
        assert name is not None and isinstance(name, str)
        for server in self.servers():
            if server[NAME_KEY] == name:
                return server
        return None

    def server_exists(self, name) -> bool:
        return self.get_server(name) is not None

    def locations(self) -> list:
        return self.data[LOCATIONS_KEY]

    def get_location(self, server, name) -> dict:
        assert server is not None and isinstance(server, str)
        assert name is not None and isinstance(name, str)
        for location in self.locations():
            if location[SERVER_KEY] == server and location[NAME_KEY] == name:
                return location
        return None

    def location_exists(self, server, name) -> bool:
        return self.get_location(server, name) is not None

    # Upstream.

    def get_upstream_id(self, source) -> str:
        assert source is not None and isinstance(source, dict)
        return '({}){}/{}'.format(self.get_tag(source), source.get(SERVER_KEY), source.get(NAME_KEY))

    def get_upstream(self, name) -> dict:
        assert name is not None and isinstance(name, str)
        for upstream in self.upstreams():
            if upstream[NAME_KEY] == name:
                return upstream
        return None

    def upstream_exists(self, name) -> bool:
        return self.get_upstream(name) is not None

    def upstreams(self) -> dict:
        return self.data[UPSTREAMS_KEY]

    # Reset.

    def clear(self):
        _logger.debug('clear()')
        self.__data = dict()
        self.data[SERVERS_KEY] = list()
        self.data[LOCATIONS_KEY] = list()
        self.data[UPSTREAMS_KEY] = list()

    def reset(self):
        self.clear()

    def reset_upstreams_folder(self):
        _logger.debug('reset_upstreams_folder()')
        try:
            koolie.tools.common.clear_directory(self.__nginx_upstreams_directory)
            # if os.path.exists(self.__nginx_upstreams_directory):
            #     shutil.rmtree(self.__nginx_upstreams_directory)
            #     _logging.debug('Removed upstreams folder [{}]'.format(self.__nginx_upstreams_directory))
            # os.makedirs(self.__nginx_upstreams_directory)
            # _logging.debug('Created upstreams folder [{}]'.format(self.__nginx_upstreams_directory))
        except Exception as exception:
            _logger.warning('Failed to reset upstreams folder.\nException [{}]'.format(exception))

    def reset_servers_folder(self):
        _logger.debug('reset_servers_folder()')
        try:
            koolie.tools.common.clear_directory(self.__nginx_servers_directory)
            # if os.path.exists(self.__nginx_servers_directory):
            #     shutil.rmtree(self.__nginx_servers_directory)
            #     _logging.debug('Removed servers folder [{}]'.format(self.__nginx_servers_directory))
            # os.makedirs(self.__nginx_servers_directory)
            # _logging.debug('Created servers folder [{}]'.format(self.__nginx_servers_directory))
        except Exception as exception:
            _logger.warning('Failed to reset servers folder.\nException [{}]'.format(exception))

    # Load.

    def load_start(self):
        _logger.debug('load_start()')
        try:
            self.__load_metadata = dict()
            self.__load_metadata['id'] = str(uuid.uuid4())
            self.__load_metadata['started'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            self.__load_metadata['load_count'] = 0
            _logger.info('Load start [{}]'.format(self.__load_metadata))

            self.load_from_file(*self.get_from_kwargs('config_load_file', list()))
        except Exception as exception:
            _logger.warning('Failed to load [{}]'.format(exception))

    def load_stop(self):
        _logger.debug('load_stop()')
        try:
            self.__load_metadata['stopped'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            _logger.info('Load stop [{}]'.format(self.__load_metadata))
        except Exception as exception:
            _logger.warning('Failed to load [{}]'.format(exception))

    def load(self, source):
        _logger.debug('load()')
        try:
            if isinstance(source, bytes):
                return self.load_from_bytes(source)
            elif isinstance(source, list):
                return self.load_from_list(source)
            elif isinstance(source, str):
                return self.load_from_file(source)
            else:
                assert TypeError('Unknown source type [{}]'.format(type(source)))
        except Exception as exception:
            _logger.warning('Failed to load from file [{}]'.format(exception))

    def load_main(self, source: dict):
        _logger.debug('load_main()')
        assert isinstance(source, dict)
        self.data[MAIN_TYPE] = source

    def load_events(self, source: dict):
        _logger.debug('load_events()')
        assert isinstance(source, dict)
        self.data[EVENTS_TYPE] = source

    def load_http(self, source: dict):
        _logger.debug('load_http()')
        assert isinstance(source, dict)
        self.data[HTTP_TYPE] = source

    def load_core(self, source: dict):
        _logger.debug('load_core()')
        assert isinstance(source, dict)
        _logger.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))
        self.__data[CORE_KEY] = source

    def load_server(self, source: dict):
        _logger.debug('load_server()')
        assert isinstance(source, dict)
        _logger.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))
        if source[LOAD_POLICY] == LOAD_POLICY_UNIQUE:
            server = self.get_server(source[NAME_KEY])
            if server is None:
                self.servers().append(source)
                _logger.info('Created server [{}]'.format(source[NAME_KEY]))
            else:
                _logger.warning('Failed to load server [{}], already exists.\n{}'.format(source[NAME_KEY], server))
        elif source[LOAD_POLICY] == LOAD_POLICY_APPEND:
            server = self.get_server(source[NAME_KEY])
            if server is None:
                self.servers().append(source)
                _logger.info('Created server [{}]'.format(source[NAME_KEY]))
            else:
                server[CONFIG_KEY] = '{}\n# Appended [{}]\n{}'.format(server[CONFIG_KEY], self.get_tag(source), source[CONFIG_KEY])
                _logger.info('Appended server [{}]'.format(source[NAME_KEY]))
        else:
            _logger.warning('Failed to load server [{}] [{}], unknown load policy [{}]'.format(source[SERVER_KEY], source[NAME_KEY], source[LOAD_POLICY]))

    def load_comment_source(self, prefix, source, data):
        return '{}\n# koolie -> [{}]\n{}\n# koolie <- [{}]\n'.format(prefix, source, data, source)

    def load_location(self, source: dict):
        try:
            _logger.debug('load_location()')
            assert isinstance(source, dict)
            _logger.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))
            if source[LOAD_POLICY] == LOAD_POLICY_UNIQUE:
                location = self.get_location(source[SERVER_KEY], source[NAME_KEY])
                if location is None:
                    self.locations().append(source)
                    source[CONFIG_KEY] = self.load_comment_source('', self.get_location_id(source), source[CONFIG_KEY])
                    _logger.info('Created location [{}]'.format(self.get_location_id(source)))
                else:
                    _logger.warning('Failed to load unique location\n[{}]\nExisting location \n[{}]'.format(source, location))
            elif source[LOAD_POLICY] == LOAD_POLICY_APPEND:
                location = self.get_location(source[SERVER_KEY], source[NAME_KEY])
                if location is None:
                    self.locations().append(source)
                    source[CONFIG_KEY] = '# From [{}]\n{}'.format(self.get_location_id(source), source[CONFIG_KEY])
                else:
                    location[CONFIG_KEY] = '{}\n# -> [{}]\n{}\n# <- [{}]'.format(location[CONFIG_KEY], self.get_location_id(source), source[CONFIG_KEY], self.get_location_id(source))
                    _logger.debug('Appended location [{}]'.format(self.get_location_id(source)))
            else:
                _logger.warning('Failed to load location [{}], unknown load policy [{}]'.format(self.get_location_id(source), source[LOAD_POLICY]))
        except Exception as exception:
            _logger.warning('Failed to load location.\nConfig [{}]\nException [{}]'.format(source, exception))

    def load_upstream(self, source: dict):
        try:
            _logger.debug('load_upstream()')

            assert isinstance(source, dict)
            _logger.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))

            if source[LOAD_POLICY] == LOAD_POLICY_UNIQUE:
                upstream = self.get_upstream(source[NAME_KEY])
                if upstream is None:
                    self.upstreams().append(source)
                    source[CONFIG_KEY] = self.load_comment_source('', self.get_upstream_id(source), source[CONFIG_KEY])
                    _logger.info('Created upstream [{}]'.format(self.get_upstream_id(source)))
                else:
                    _logger.warning('Failed to load upstream [{}] already exists as [{}].'.format(self.get_upstream_id(source), self.get_upstream_id(upstream)))
            elif source[LOAD_POLICY] == LOAD_POLICY_APPEND:
                upstream = self.get_upstream(source[NAME_KEY])
                if upstream is None:
                    self.upstreams().append(source)
                    source[CONFIG_KEY] = self.load_comment_source('', self.get_upstream_id(source), source[CONFIG_KEY])
                    _logger.info('Created upstream [{}]'.format(self.get_upstream_id(source)))
                else:
                    upstream[CONFIG_KEY] = self.load_comment_source(upstream[CONFIG_KEY], self.get_upstream_id(source), source[CONFIG_KEY])
            else:
                _logger.warning('Failed to load upstream [{}], unknown load policy [{}]'.format(self.get_upstream_id(source), source[LOAD_POLICY]))

        except Exception as exception:
            _logger.warning('Failed to load upstream.\nSource {}\nException {}'.format(source, exception))

    def load_from_bytes(self, source: bytes):
        _logger.debug('load_from_bytes()')
        try:
            if source is None:
                _logger.info('Nothing to load')
                return

            assert isinstance(source, bytes)

            self.load(yaml.load(source.decode('utf-8')))

        except Exception as exception:
            _logger.warning('Failed to load from file [{}]'.format(exception))

    def load_from_list(self, source: list):
        _logger.debug('load_from_list()\nsource: [{}]'.format(source))

        try:
            if source is None:
                _logger.info('Nothing to load')
                return

            assert isinstance(source, list)

            _logger.debug('source length [{}]'.format(len(source)))

            for item in source:
                _logger.debug('item type [{}]'.format(type(item)))
                if isinstance(item, dict):
                    loader = self.__load_from_list.get(item[TYPE_KEY])
                    if loader is None:
                        _logger.warning('load_from_list() Unknown item [({}){}]'.format(self.get_tag(item), item[TYPE_KEY]))
                    else:
                        loader(item)
                        self.__load_metadata['load_count'] = self.__load_metadata['load_count'] + 1
                        _logger.debug('Returned from loader.')
                else:
                    _logger.warning('Failed to load item due to type [{}]'.format(type(item)))

        except Exception as exception:
            _logger.warning('Failed to load from list [{}:{}]'.format(type(exception), exception))

    def load_from_file(self, *args):
        _logger.debug('load_from_file(source=[{}])'.format(args))
        try:
            if args is None:
                _logger.info('Nothing to load.')
                return

            for name in args:
                try:
                    with open(file=name, mode='r') as file:
                        data = file.read()
                    l = yaml.load(koolie.tools.common.safe_substitute(data, **self.__kwargs))
                    for d in l:
                        d[SOURCE_KEY] = name
                    self.load(l)
                except Exception as exception:
                    _logger.warning('Failed to load file [{}] with exception [{}]'.format(name, exception))
        except Exception as exception:
            _logger.warning('Failed to load from file [{}]'.format(exception))

    # Dump.

    def dump_start(self):
        self.__dump_metadata.clear()
        self.__dump_metadata['id'] = str(uuid.uuid4())
        self.__dump_metadata['started'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        _logger.info('Dump start [{}]'.format(self.__dump_metadata))

    def dump_stop(self):
        self.__dump_metadata['stopped'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        _logger.info('Dump stopped [{}]'.format(self.__dump_metadata))

    def dump(self):
        _logger.debug('dump()')
        try:
            # if os.path.exists('{}nginx.conf'.format(self.__nginx_directory)):
            #     os.remove('{}nginx.conf'.format(self.__nginx_directory))

            self.reset_servers_folder()

            self.reset_upstreams_folder()

            # self.dump_core()

            self.dump_servers()

            self.dump_locations()

            self.dump_upstreams()

            _logger.info('Dump OK')
        except Exception as exception:
            _logger.warning('Failed to dump [{}] [{}]'.format(type(exception), exception))

    def dump_file_comment(self):
        return '# Created by Koolie\n# Load [{}]\n# Dump [{}]\n\n'.format(self.__load_metadata, self.__dump_metadata)

    def dump_core(self):
        file_name = '{}nginx.conf'.format(self.__nginx_directory)
        with open(file=file_name, mode='w') as file:
            file.write(self.dump_file_comment())
            file.write(self.core()[CONFIG_KEY])
            file.write(self.data[EVENTS_TYPE][CONFIG_KEY])
            file.write(self.data[HTTP_TYPE][CONFIG_KEY])

    def dump_upstream(self, upstream):
        _logger.debug('dump_upstream()')

        file_name = '{}{}.conf'.format(self.__nginx_servers_directory, upstream[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.dump_file_comment())
            file.write('upstream {} {{\n'.format(upstream[NAME_KEY]))
            file.write(upstream[CONFIG_KEY])
            file.write('}\n')

    def dump_upstreams(self):
        _logger.debug('dump_upstreams()')
        for upstream in self.upstreams():
            self.dump_upstream(upstream)

    def dump_servers(self):
        _logger.debug('dump_servers()')
        for server in self.servers():
            self.dump_server(server)

    def dump_server(self, server):
        _logger.debug('dump_server()')
        file_name = '{}{}.conf'.format(self.__nginx_servers_directory, server[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.dump_file_comment())
            file.write('server {\n')
            file.write(server[CONFIG_KEY])
            file.write('\n\ninclude {}{}/*.conf;\n'.format(self.__nginx_servers_directory, server[NAME_KEY]))
            file.write('}')

    def dump_locations(self):
        _logger.debug('dump_locations()')

        for location in self.locations():
            self.dump_location(location)

    def dump_location(self, location):
        _logger.debug('dump_location()')

        server_folder = '{}{}/'.format(self.__nginx_servers_directory, location[SERVER_KEY])
        if not os.path.exists(server_folder):
            os.makedirs(name=server_folder, exist_ok=True)
            _logger.debug('Created server folder [{}]'.format(server_folder))

        file_name = '{}{}.conf'.format(server_folder, location[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.dump_file_comment())
            file.write('location {} {} {{\n'.format(location[LOCATION_MATCH_MODIFIER], location[LOCATION_LOCATION_MATCH]))
            file.write(location[CONFIG_KEY])
            file.write('}\n')

    def test(self):
        result: bool = False
        try:
            _logger.debug('Testing NGINX configuration')
            # Load the run prefix in via the args.
            completed_process: subprocess.CompletedProcess = subprocess.run(['docker', 'exec', '-i', 'nginx', 'nginx', '-t'])
            _logger.debug('Completed process returned [{}]'.format(completed_process))
            result = completed_process.returncode == 0
        except Exception as called_process_error:
            _logger.warning('Failed to reload NGINX [{}]'.format(called_process_error))
        finally:
            return result

    def nginx_conf(self):
        result: bool = False
        try:
            _logger.debug('Testing NGINX configuration')
            # Load the run prefix in via the args.
            completed_process: subprocess.CompletedProcess = subprocess.run(['docker', 'exec', '-i', 'nginx', 'nginx', '-T'])
            _logger.debug('Completed process returned [{}]'.format(completed_process))
            result = completed_process.returncode == 0
        except Exception as called_process_error:
            _logger.warning('Failed to reload NGINX [{}]'.format(called_process_error))
        finally:
            return result

    def reload(self):
        result: bool = False
        try:
            _logger.debug('Testing NGINX configuration')
            # Load the run prefix in via the args.
            completed_process: subprocess.CompletedProcess = subprocess.run(['docker', 'exec', '-i', 'nginx', 'nginx', '-s', 'reload'])
            _logger.debug('Completed process returned [{}]'.format(completed_process))
            result = completed_process.returncode == 0
        except Exception as called_process_error:
            _logger.warning('Failed to reload NGINX [{}]'.format(called_process_error))
        finally:
            return result
