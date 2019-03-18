import collections
import datetime
import logging
import os
import subprocess
import time
import typing
import koolie.tools.common
import uuid
import yaml

import koolie.common.base

_logger = logging.getLogger(__name__)


NGINX_KEY = 'nginx'

CONFIG_KEY = 'config'


NGINX_DIRECTORY_KEY = 'nginx_directory'

NGINX_SERVERS_DIRECTORY_KEY = 'nginx_servers_directory'

NGINX_UPSTREAMS_DIRECTORY_KEY = 'nginx_upstreams_directory'

NGINX_DIRECTORY_DEAFULT = '/tmp/nginx/'


LOAD_POLICY_KEY = 'loadPolicy'

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


NGINX_ROOT_TYPE = 'nginx_root'

NGINX_MAIN_TYPE = 'nginx_main'

NGINX_EVENTS_TYPE = 'nginx_events'

NGINX_HTTP_TYPE = 'nginx_http'

NGINX_SERVER_TYPE = 'nginx_server'
NGINX_SERVER_PREFIX_TYPE = 'nginx_server_prefix'
NGINX_SERVER_SUFFIX_TYPE = 'nginx_server_suffix'

NGINX_LOCATION_TYPE = 'nginx_location'
NGINX_LOCATION_PREFIX_TYPE = 'nginx_location_prefix'
NGINX_LOCATION_SUFFIX_TYPE = 'nginx_location_suffix'

NGINX_UPSTREAM_TYPE = 'nginx_upstream'
NGINX_UPSTREAM_PREFIX_TYPE = 'nginx_upstream_prefix'
NGINX_UPSTREAM_SUFFIX_TYPE = 'nginx_upstream_suffix'


NGINX_SERVER_PREFIX_FQN = '{}.{}'.format(NGINX_SERVER_TYPE, '_prefix')

NGINX_SERVER_SUFFIX_FQN = '{}.{}'.format(NGINX_SERVER_TYPE, '_suffix')

NGINX_UPSTREAM_PREFIX_FQN = '{}.{}'.format(NGINX_LOCATION_TYPE, '_prefix')

NGINX_UPSTREAM_SUFFIX_FQN = '{}.{}'.format(NGINX_LOCATION_TYPE, '_suffix')


LOAD_METADATA_KEY = 'load_metadata'

DUMP_METADATA_KEY = 'dump_metadata'


# Tokens

TOKEN_ADD_TYPE = 'token_add'

VALUE_KEY = 'value'


class Token(koolie.common.base.Base):

    def __init__(self, data: typing.Dict[str, object] = None) -> None:
        super().__init__(data)


class TokenAdd(Token):

    def __init__(self, data: typing.Dict[str, object] = None) -> None:
        super().__init__(data)

    def value(self) -> str:
        return self._data().get(VALUE_KEY)


class NGINX(koolie.common.base.Base):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)

    def load_policy(self) -> str:
        return self._data().get(LOAD_POLICY_KEY, '')

    def config(self, substitute: typing.Dict[str, str] = None, self_in_substitute: bool = True) -> str:
        if substitute is None:
            return self._data().get(CONFIG_KEY, '')

        if self_in_substitute:
            return koolie.tools.common.substitute(self.config(), **collections.ChainMap(substitute, self.tokens()))

        return koolie.tools.common.substitute(self.config(), substitute)


class Root(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Main(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Events(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class HTTP(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Server(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class ServerPrefix(Server):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class ServerSuffix(Server):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Location(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)

    def server(self) -> str:
        return self._data()[SERVER_KEY]

    def fqn(self) -> str:
        return '{}.{}.{}'.format(self.type(), self.server(), self.name())

    def match_modifier(self) -> str:
        return self._data()[LOCATION_MATCH_MODIFIER]

    def location_match(self) -> str:
        return self._data()[LOCATION_LOCATION_MATCH]


class Affix(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


DEFAULT_SERVER_PREFIX = [
    Affix(
        {
            koolie.common.base.TYPE_KEY: NGINX_SERVER_PREFIX_TYPE,
            koolie.common.base.NAME_KEY: 'default',
            CONFIG_KEY: 'server ${nginx_server_prefix__name} {{\n'
        }
    )
]

DEFAULT_SERVER_SUFFIX = [
    Affix(
        {
            koolie.common.base.TYPE_KEY: NGINX_SERVER_SUFFIX_TYPE,
            koolie.common.base.NAME_KEY: 'default',
            CONFIG_KEY: '}\n'
        }
    )
]


DEFAULT_LOCATION_PREFIX = [
    Affix(
        {
            koolie.common.base.TYPE_KEY: NGINX_LOCATION_PREFIX_TYPE,
            koolie.common.base.NAME_KEY: 'default',
            CONFIG_KEY: 'location ${nginx_location__match_modifier} ${nginx_location__location_match} {{\n'
        }
    )
]

DEFAULT_LOCATION_SUFFIX = [
    Affix(
        {
            koolie.common.base.TYPE_KEY: NGINX_LOCATION_SUFFIX_TYPE,
            koolie.common.base.NAME_KEY: 'default',
            CONFIG_KEY: '}\n'
        }
    )
]


DEFAULT_UPSTREAM_PREFIX = [
    Affix(
        {
            koolie.common.base.TYPE_KEY: NGINX_UPSTREAM_PREFIX_TYPE,
            koolie.common.base.NAME_KEY: '_default',
            CONFIG_KEY: 'upstream ${nginx_upstream__name} {\n'
        }
    )
]

DEFAULT_UPSTREAM_SUFFIX = [
    Affix(
        {
            koolie.common.base.TYPE_KEY: NGINX_UPSTREAM_SUFFIX_TYPE,
            koolie.common.base.NAME_KEY: '_default',
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


class Upstream(NGINX):

    def __init__(self, data: dict = None) -> None:
        super().__init__(data)


class Config(object):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

        self.__args = args

        self.__kwargs = kwargs

        self.__items: typing.Dict[str, typing.List] = {}

    def _kwargs(self):
        return self.__kwargs

    def nginx_directory(self) -> str:
        return self._kwargs().get(NGINX_DIRECTORY_KEY, NGINX_DIRECTORY_DEAFULT)

    def nginx_servers_directory(self) -> str:
        return self._kwargs().get(NGINX_SERVERS_DIRECTORY_KEY, '{}servers/'.format(self.nginx_directory()))

    def items(self) -> typing.Dict[str, typing.List]:
        return self.__items

    def add_unique_item(self, item: NGINX):
        _logger.debug('add_unique()')
        assert isinstance(item, NGINX)
        _logger.debug('add_unique() item=[{}]'.format(item))
        assert item.fqn() not in self.items().keys()
        # Add a new list containing the item.
        self.items()[item.fqn()] = [item]

    def append_item(self, item: NGINX):
        _logger.debug('append_item()')
        assert isinstance(item, NGINX)
        _logger.debug('append_item() item=[{}]'.format(item))
        if item.fqn() in self.items().keys():
            self.items()[item.fqn()].append(item)
        else:
            self.items()[item.fqn()] = [item]

    load_dispatcher: typing.Dict[str, typing.Callable] = {LOAD_POLICY_APPEND: append_item, LOAD_POLICY_UNIQUE: add_unique_item}

    def add_item(self, item: NGINX):
        assert isinstance(item, NGINX)
        self.load_dispatcher[item.load_policy()](self, item)

    item_creator: typing.Dict[str, typing.Callable[[typing.Dict], NGINX]] = {
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
                        nginx: NGINX = NGINX(item)

                        self.add_item(self.item_creator[nginx.type()](nginx._data()))
                    except Exception as exception:
                        _logger.warning('load() Item exception [{}]'.format(koolie.tools.common.decode_exception(exception)))
            except Exception as exception:
                _logger.warning('load() File exception [{}]'.format(koolie.tools.common.decode_exception(exception)))
    # Dump

    def write(self, directory: str, name: str, *args: str):
        _logger.debug('write()')
        koolie.tools.common.ensure_directory(directory)
        file = '{}{}'.format(directory, name)
        with open(file=file, mode='w') as out:
            if out.tell() == 0:
                out.write('# koolie\n\n')
            for line in args:
                out.write(line)

    def dump_config(self, bases: typing.List[NGINX], prefixes: typing.List[NGINX], suffixes: typing.List[NGINX], tokens: typing.Dict[str, str]) -> str:
        _logger.debug('dump_config()')

        prefix = '' if prefixes is None else self.dump_config(prefixes, None, None, tokens)

        config = '\n'.join('# FQN [{}]\n{}'.format(base.fqn(), base.config(tokens, True)) for base in bases)

        suffix = '' if suffixes is None else self.dump_config(suffixes, None, None, tokens)

        return '{}\n{}\n{}'.format(prefix, config, suffix)

    def dump_root(self, roots: typing.List[Root], tokens: typing.Dict[str, str]):
        _logger.debug('dump_root()')
        line = self.dump_config(roots, None, None, tokens)
        self.write(self.nginx_directory(), '{}.conf'.format(roots[0].name()), line)

    def dump_main(self, roots: typing.List[Root], tokens: typing.Dict[str, str]):
        _logger.debug('dump_root()')
        line = self.dump_config(roots, None, None, tokens)
        self.write(self.nginx_directory(), '{}.conf'.format(roots[0].name()), line)

    def dump_events(self, roots: typing.List[Root], tokens: typing.Dict[str, str]):
        _logger.debug('dump_root()')
        line = self.dump_config(roots, None, None, tokens)
        self.write(self.nginx_directory(), '{}.conf'.format(roots[0].name()), line)

    def dump_http(self, roots: typing.List[Root], tokens: typing.Dict[str, str]):
        _logger.debug('dump_root()')
        line = self.dump_config(roots, None, None, tokens)
        self.write(self.nginx_directory(), '{}.conf'.format(roots[0].name()), line)

    def dump_server(self, servers: typing.List[Server], tokens: typing.Dict[str, str]):
        _logger.debug('dump_server()')
        line = self.dump_config(servers, self.items().get(NGINX_SERVER_PREFIX_TYPE, DEFAULT_SERVER_PREFIX), self.items().get(NGINX_SERVER_SUFFIX_TYPE, DEFAULT_SERVER_SUFFIX), tokens)
        self.write(self.nginx_servers_directory(), '{}.conf'.format(servers[0].name()), line)

    def dump_ignore(self, nginx: NGINX, tokens: typing.Dict[str, str]):
        _logger.warning('dump_ignore() nginx [{}]'.format(nginx))

    def dump(self, **kwargs: typing.Dict[str, str]):
        _logger.debug('dump()')

        dump_tokens: typing.Dict[str, str] = {
            'config__nginx_directory': NGINX_DIRECTORY_DEAFULT
        }

        # def dump_server(server: Server):
        #     _logger.debug('dump_server()')
        #     write('{}servers/'.format(nginx_directory()), server.name(), self.items()[NGINX_SERVER_PREFIX_FQN])
        #     write('{}servers/'.format(nginx_directory()), server.name(), [server])
        #     write('{}servers/'.format(nginx_directory()), server.name(), self.items()[NGINX_SERVER_SUFFIX_FQN])

        dump_dispatcher: typing.Dict[str, typing.Callable[['Config', NGINX, typing.Dict[str, str]], None]] = {
            NGINX_ROOT_TYPE: self.dump_root,
            NGINX_MAIN_TYPE: self.dump_main,
            NGINX_EVENTS_TYPE: self.dump_events,
            NGINX_HTTP_TYPE: self.dump_http,
            NGINX_SERVER_TYPE: self.dump_server,
            # NGINX_LOCATION_TYPE: dump_location,
            # NGINX_UPSTREAM_TYPE: dump_upstream
        }

        for nginx_list in self.items().values():
            _logger.debug('NGINX [{}]'.format(nginx_list[0].fqn()))
            dump_dispatcher.get(nginx_list[0].type(), self.dump_ignore)(nginx_list, dump_tokens)


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
