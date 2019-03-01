import datetime
import koolie.core
import logging
import os
import subprocess
import shutil
import sys
import time
import yaml

_logging = logging.getLogger(__name__)

NGINX_KEY = 'nginx'
CONFIG_KEY = 'config'

NGINX_FOLDER_ENV = 'NGINX_FOLDER'
NGINX_FOLDER_DEFAULT = '/etc/nginx/'
nginx_folder = os.getenv(NGINX_FOLDER_ENV, NGINX_FOLDER_DEFAULT)
_logging.info('NGINX folder [{}]'.format(nginx_folder))

SERVERS_FOLDER_ENV = 'SERVERS_FOLDER'
SERVERS_FOLDER_DEFAULT = '{}servers/'.format(nginx_folder)
servers_folder = os.getenv(SERVERS_FOLDER_ENV, SERVERS_FOLDER_DEFAULT)
_logging.info('Servers folder [{}]'.format(servers_folder))

UPSTREAMS_FOLDER_ENV = 'UPSTREAMS_FOLDER'
UPSTREAMS_FOLDER_DEFAULT = '{}upstreams/'.format(nginx_folder)
upstreams_folder = os.getenv(UPSTREAMS_FOLDER_ENV, UPSTREAMS_FOLDER_DEFAULT)
_logging.info('Upstreams folder [{}]'.format(upstreams_folder))


MAIN_TYPE = '{}/main'.format(NGINX_KEY)

EVENTS_TYPE = '{}/events'.format(NGINX_KEY)

HTTP_TYPE = '{}/http'.format(NGINX_KEY)

SERVER_TYPE = '{}/server'.format(NGINX_KEY)

LOCATION_TYPE = '{}/location'.format(NGINX_KEY)

UPSTREAM_TYPE = '{}/upstream'.format(NGINX_KEY)

TAG_KEY = 'tag'

TYPE_KEY = 'type'

NAME_KEY = 'name'

LOAD_POLICY = 'loadPolicy'

LOAD_POLICY_UNIQUE = 'unique'

LOAD_POLICY_APPEND = 'append'

SERVER_KEY = 'server'

SERVERS_KEY = 'servers'

LOCATIONS_KEY = 'locations'

UPSTREAMS_KEY = 'upstreams'

LOCATION_MATCH_MODIFIER = 'matchModifier'
LOCATION_LOCATION_MATCH = 'locationMatch'


# class WithData(object):
#
#     def __init__(self):
#         self.data = None
#
#     @property
#     def data(self) -> dict:
#         return self.__data
#
#     @data.setter
#     def data(self, data: dict):
#         assert data is None or isinstance(data, dict)
#         self.__data = data


class NGINXConfig(object):

    def __init__(self) -> None:
        super().__init__()
        self.__data = None
        self.reset()

        self.__load_from_list = dict()
        self.__load_from_list[MAIN_TYPE] = self.load_main
        self.__load_from_list[EVENTS_TYPE] = self.load_events
        self.__load_from_list[HTTP_TYPE] = self.load_http
        self.__load_from_list[SERVER_TYPE] = self.load_server
        self.__load_from_list[LOCATION_TYPE] = self.load_location
        self.__load_from_list[UPSTREAM_TYPE] = self.load_upstream

    def __str__(self) -> str:
        return str(self.__data)

    @property
    def data(self) -> dict:
        return self.__data

    @data.setter
    def data(self, data: dict):
        assert data is not None and isinstance(data, dict)
        self.__data = data

    def common_nginx_conf_comments(self):
        return '# Created by Koolie [{}]\n\n'.format(datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

    def create_servers_folder(self):
        if not os.path.exists(servers_folder):
            os.makedirs(name=servers_folder, exist_ok=True)

    def write_conf(self, name, data):
        _logging.debug('write_conf()')
        try:
            with open(file=name, mode='w') as file:
                file.write(self.common_nginx_conf_comments())
                file.write(data)
        except Exception as exception:
            _logging.warning('Failed to write file [{}] [{}]'.format(name, exception))

    def get_tag(self, source: dict) -> str:
        assert source is not None and isinstance(source, dict)
        return source.get(TAG_KEY, '')

    def get_location_id(self, source) -> str:
        assert source is not None and isinstance(source, dict)
        return '({}){}/{}'.format(self.get_tag(source), source.get(SERVER_KEY), source.get(NAME_KEY))

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
        return '({}){}'.format(source.get(TAG_KEY, ''), source.get(NAME_KEY))

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

    def reset(self):
        self.__data = dict()
        self.data[SERVERS_KEY] = list()
        self.data[LOCATIONS_KEY] = list()
        self.data[UPSTREAMS_KEY] = list()

    def reset_upstreams_folder(self):
        _logging.debug('reset_upstreams_folder()')
        try:
            if os.path.exists(upstreams_folder):
                shutil.rmtree(upstreams_folder)
                _logging.debug('Removed upstreams folder [{}]'.format(upstreams_folder))
            os.makedirs(upstreams_folder)
            _logging.debug('Created upstreams folder [{}]'.format(upstreams_folder))
        except Exception as exception:
            _logging.warning('Failed to reset upstreams folder.\nException [{}]'.format(exception))

    def reset_servers_folder(self):
        _logging.debug('reset_servers_folder()')
        try:
            if os.path.exists(servers_folder):
                shutil.rmtree(servers_folder)
                _logging.debug('Removed servers folder [{}]'.format(servers_folder))
            os.makedirs(servers_folder)
            _logging.debug('Created servers folder [{}]'.format(servers_folder))
        except Exception as exception:
            _logging.warning('Failed to reset servers folder.\nException [{}]'.format(exception))

    # Load.

    def load(self, source):
        _logging.debug('load()')
        try:
            if isinstance(source, bytes):
                self.load_from_bytes(source)
            elif isinstance(source, list):
                self.load_from_list(source)
            elif isinstance(source, str):
                self.load_from_file(source)
            else:
                assert TypeError('Unknown source type [{}]'.format(type(source)))
        except Exception as exception:
            _logging.warning('Failed to load from file [{}]'.format(exception))

    def load_main(self, source: dict):
        _logging.debug('load_main()')
        assert isinstance(source, dict)
        self.data[MAIN_TYPE] = source

    def load_events(self, source: dict):
        _logging.debug('load_events()')
        assert isinstance(source, dict)
        self.data[EVENTS_TYPE] = source

    def load_http(self, source: dict):
        _logging.debug('load_http()')
        assert isinstance(source, dict)
        self.data[HTTP_TYPE] = source

    def load_server(self, source: dict):
        _logging.debug('load_server()')
        assert isinstance(source, dict)
        _logging.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))
        if source[LOAD_POLICY] == LOAD_POLICY_UNIQUE:
            server = self.get_server(source[NAME_KEY])
            if server is None:
                self.servers().append(source)
                _logging.info('Created server [{}]'.format(source[NAME_KEY]))
            else:
                _logging.warning('Failed to load server [{}], already exists.\n{}'.format(source[NAME_KEY], server))
        elif source[LOAD_POLICY] == LOAD_POLICY_APPEND:
            server = self.get_server(source[NAME_KEY])
            if server is None:
                self.servers().append(source)
                _logging.info('Created server [{}]'.format(source[NAME_KEY]))
            else:
                server[CONFIG_KEY] = '{}\n# Appended [{}]\n{}'.format(server[CONFIG_KEY], self.get_tag(source), source[CONFIG_KEY])
                _logging.info('Appended server [{}]'.format(source[NAME_KEY]))
        else:
            _logging.warning('Failed to load location [{}] [{}], unknown load policy [{}]'.format(source[SERVER_KEY], source[NAME_KEY], source[LOAD_POLICY]))

    def load_location(self, source: dict):
        try:
            _logging.debug('load_location()')
            assert isinstance(source, dict)
            _logging.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))
            if source[LOAD_POLICY] == LOAD_POLICY_UNIQUE:
                location = self.get_location(source[SERVER_KEY], source[NAME_KEY])
                if location is None:
                    self.locations().append(source)
                    source[CONFIG_KEY] = '# From [{}]\n{}'.format(self.get_location_id(source), source[CONFIG_KEY])
                    _logging.info('Created location [{}]'.format(self.get_location_id(source)))
                else:
                    _logging.warning('Failed to load Location [{}] already exists as [{}].'.format(self.get_location_id(source), self.get_location_id(location)))
            elif source[LOAD_POLICY] == LOAD_POLICY_APPEND:
                location = self.get_location(source[SERVER_KEY], source[NAME_KEY])
                if location is None:
                    self.locations().append(source)
                    source[CONFIG_KEY] = '# From [{}]\n{}'.format(self.get_location_id(source), source[CONFIG_KEY])
                else:
                    location[CONFIG_KEY] = '{}\n# From [{}]\n{}'.format(location[CONFIG_KEY], self.get_location_id(source), source[CONFIG_KEY])
                    _logging.debug('Appended location [{}]'.format(self.get_location_id(source)))
            else:
                _logging.warning('Failed to load location [{}], unknown load policy [{}]'.format(self.get_location_id(source), source[LOAD_POLICY]))
        except Exception as exception:
            _logging.warning('Failed to load location.\n{}\n{}'.format(source, exception))

    def load_upstream(self, source: dict):
        try:
            _logging.debug('load_upstream()')
            assert isinstance(source, dict)
            _logging.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))
            if source[LOAD_POLICY] == LOAD_POLICY_UNIQUE:
                upstream = self.get_upstream(source[NAME_KEY])
                if upstream is None:
                    self.upstreams().append(source)
                    _logging.info('Created upstream [{}]'.format(self.get_upstream_id(source)))
                else:
                    _logging.warning('Failed to load upstream [{}] already exists as [{}].'.format(self.get_upstream_id(source), self.get_upstream_id(upstream)))
            elif source[LOAD_POLICY] == LOAD_POLICY_APPEND:
                upstream = self.get_upstream(source[NAME_KEY])
                if upstream is None:
                    self.upstreams().append(source)
                    source[CONFIG_KEY] = '# From [{}]\n{}'.format(self.get_upstream_id(source), source[CONFIG_KEY])
                    _logging.info('Created upstream [{}]'.format(self.get_upstream_id(source)))
                else:
                    upstream[CONFIG_KEY] = '{}\n# From [{}]\n{}'.format(upstream[CONFIG_KEY], self.get_upstream_id(source), source[CONFIG_KEY])
            else:
                _logging.warning('Failed to load upstream [{}], unknown load policy [{}]'.format(self.get_upstream_id(source), source[LOAD_POLICY]))
        except Exception as exception:
            _logging.warning('Failed to load upstream.\nSource {}\nException {}'.format(source, exception))

    def load_from_bytes(self, source: bytes):
        _logging.debug('load_from_bytes()')
        try:
            if source is None:
                _logging.info('Nothing to load')
                return

            assert isinstance(source, bytes)

            self.load(yaml.load(source.decode('utf-8')))

        except Exception as exception:
            _logging.warning('Failed to load from file [{}]'.format(exception))

    def load_from_list(self, source: list):
        _logging.debug('load_from_list()\nsource: [{}]'.format(source))
        try:
            if source is None:
                _logging.info('Nothing to load')
                return

            assert isinstance(source, list)

            _logging.debug('source length [{}]'.format(len(source)))

            for item in source:
                _logging.debug('item type [{}]'.format(type(item)))
                if isinstance(item, dict):
                    loader = self.__load_from_list.get(item[TYPE_KEY])
                    if loader is None:
                        _logging.warning('load_from_list() Unknown item [({}){}]'.format(self.get_tag(item), item[TYPE_KEY]))
                    else:
                        loader(item)
                        _logging.debug('Returned from loader.')
                else:
                    _logging.warning('Failed to load item due to type [{}]'.format(type(item)))

        except Exception as exception:
            _logging.warning('Failed to load from list [{}:{}]'.format(type(exception), exception))

    def load_from_file(self, source: str):
        _logging.debug('load_from_file()')
        try:
            if source is None:
                _logging.info('Nothing to load.')
                return

            assert isinstance(source, str)
            _logging.info('Loading from file [{}]'.format(source))

            with open(file=source, mode='r') as file:
                data = yaml.load(file)

            self.load(data)

        except Exception as exception:
            _logging.warning('Failed to load from file [{}]'.format(exception))

    # Dump.

    def dump(self):
        _logging.debug('dump()')
        try:
            if os.path.exists('{}nginx.conf'.format(nginx_folder)):
                os.remove('{}nginx.conf'.format(nginx_folder))

            self.reset_upstreams_folder()

            self.reset_servers_folder()

            self.dump_nginx()

            self.dump_upstreams()

            self.dump_servers()

            self.dump_locations()

            _logging.info('Dump OK')
        except Exception as exception:
            _logging.warning('update() Failed to update [{}] [{}]'.format(type(exception), exception))

    def dump_nginx(self):
        file_name = '{}nginx.conf'.format(nginx_folder)
        with open(file=file_name, mode='w') as file:
            file.write(self.common_nginx_conf_comments())
            file.write(self.data[MAIN_TYPE][CONFIG_KEY])
            file.write(self.data[EVENTS_TYPE][CONFIG_KEY])
            file.write(self.data[HTTP_TYPE][CONFIG_KEY])

    def dump_upstream(self, upstream):
        _logging.debug('dump_upstream()')

        file_name = '{}{}.conf'.format(upstreams_folder, upstream[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.common_nginx_conf_comments())
            file.write('upstream {} {{\n'.format(upstream[NAME_KEY]))
            file.write(upstream[CONFIG_KEY])
            file.write('}\n')

    def dump_upstreams(self):
        _logging.debug('dump_upstreams()')
        for upstream in self.upstreams():
            self.dump_upstream(upstream)

    def dump_servers(self):
        _logging.debug('dump_servers()')
        for server in self.servers():
            self.dump_server(server)

    def dump_server(self, server):
        _logging.debug('dump_server()')

        file_name = '{}{}.conf'.format(servers_folder, server[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.common_nginx_conf_comments())
            file.write(server[CONFIG_KEY])

    def dump_locations(self):
        _logging.debug('dump_locations()')

        for location in self.locations():
            self.dump_location(location)

    def dump_location(self, location):
        _logging.debug('dump_location()')

        server_folder = '{}{}/'.format(servers_folder, location[SERVER_KEY])
        if not os.path.exists(server_folder):
            os.makedirs(name=server_folder, exist_ok=True)
            _logging.debug('Created server folder [{}]'.format(server_folder))

        file_name = '{}{}.conf'.format(server_folder, location[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.common_nginx_conf_comments())
            file.write('location {} {} {{\n'.format(location[LOCATION_MATCH_MODIFIER], location[LOCATION_LOCATION_MATCH]))
            file.write(location[CONFIG_KEY])
            file.write('}\n')

    def test(self):
        result: bool = False
        try:
            _logging.debug('Testing NGINX configuration')
            completed_process: subprocess.CompletedProcess = subprocess.run(['nginx', '-t'])
            _logging.debug('Completed process returned [{}]'.format(completed_process))
            result = completed_process.returncode == 0
        except Exception as called_process_error:
            _logging.warning('Failed to reload NGINX [{}]'.format(called_process_error))
        finally:
            return result

# location /my-website {
#   content_by_lua_block {
#     os.execute("/bin/myShellScript.sh")
#   }
# }


def test():
    nginx = NGINXConfig()
    nginx.load('nginx_test.yaml')
    nginx.dump()
    print('OK')


if __name__ == '__main__':
    level: str = getattr(logging, os.getenv(koolie.core.KOOLIE_LOGGING_LEVEL_ENV, koolie.core.KOOLIE_LOGGING_LEVEL_DEFAULT).upper())
    print('Level [{}]'.format(level))
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    print('Logging [{}]'.format(_logging))
    test()