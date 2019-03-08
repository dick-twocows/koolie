import datetime
import logging
import os
import subprocess
import shutil
import time
import koolie.tools.common
import uuid
import yaml

_logging = logging.getLogger(__name__)

NGINX_KEY = 'nginx'

CONFIG_KEY = 'config'

NGINX_DIRECTORY_KEY = 'nginx_directory'

NGINX_SERVERS_DIRECTORY_KEY = 'nginx_servers_directory'

NGINX_UPSTREAMS_DIRECTORY_KEY = 'nginx_upstreams_directory'

NGINX_DIRECTORY_DEAFULT = '/etc/nginx/'

MAIN_TYPE = '{}/main'.format(NGINX_KEY)

EVENTS_TYPE = '{}/events'.format(NGINX_KEY)

HTTP_TYPE = '{}/http'.format(NGINX_KEY)

CORE_TYPE = '{}/core'.format(NGINX_KEY)

SERVER_TYPE = '{}/server'.format(NGINX_KEY)

LOCATION_TYPE = '{}/location'.format(NGINX_KEY)

UPSTREAM_TYPE = '{}/upstream'.format(NGINX_KEY)

TAG_KEY = 'tag'

TYPE_KEY = 'type'

NAME_KEY = 'name'

LOAD_POLICY = 'loadPolicy'

LOAD_POLICY_UNIQUE = 'unique'

LOAD_POLICY_APPEND = 'append'

CORE_KEY = 'core'

SERVER_KEY = 'server'

SERVERS_KEY = 'servers'

LOCATIONS_KEY = 'locations'

UPSTREAMS_KEY = 'upstreams'

LOCATION_MATCH_MODIFIER = 'matchModifier'
LOCATION_LOCATION_MATCH = 'locationMatch'


def if_none(v: object, o: object) -> object:
    return koolie.tools.common.if_none(v, o)


class NGINXConfig(object):

    def __init__(self, **kwargs) -> None:
        super().__init__()

        self.__kwargs = kwargs

        self.__nginx_directory = if_none(self.get_from_kwargs('nginx_directory'), NGINX_DIRECTORY_DEAFULT)
        self.__nginx_servers_directory = if_none(self.get_from_kwargs('nginx_servers_directory'), '{}servers/'.format(self.__nginx_directory))
        self.__nginx_upstreams_directory = if_none(self.get_from_kwargs('nginx_upstreams_directory'), '{}upstreams/'.format(self.__nginx_directory))
        _logging.info('NGINX folders\nNGINX [{}]\nServers [{}]\nUpstreams [{}]'.format(self.__nginx_directory, self.__nginx_servers_directory, self.__nginx_upstreams_directory))

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
        _logging.debug('clear()')
        self.__data = dict()
        self.data[SERVERS_KEY] = list()
        self.data[LOCATIONS_KEY] = list()
        self.data[UPSTREAMS_KEY] = list()

    def reset(self):
        self.clear()

    def reset_upstreams_folder(self):
        _logging.debug('reset_upstreams_folder()')
        try:
            koolie.tools.common.clear_directory(self.__nginx_upstreams_directory)
            # if os.path.exists(self.__nginx_upstreams_directory):
            #     shutil.rmtree(self.__nginx_upstreams_directory)
            #     _logging.debug('Removed upstreams folder [{}]'.format(self.__nginx_upstreams_directory))
            # os.makedirs(self.__nginx_upstreams_directory)
            # _logging.debug('Created upstreams folder [{}]'.format(self.__nginx_upstreams_directory))
        except Exception as exception:
            _logging.warning('Failed to reset upstreams folder.\nException [{}]'.format(exception))

    def reset_servers_folder(self):
        _logging.debug('reset_servers_folder()')
        try:
            koolie.tools.common.clear_directory(self.__nginx_upstreams_directory)
            # if os.path.exists(self.__nginx_servers_directory):
            #     shutil.rmtree(self.__nginx_servers_directory)
            #     _logging.debug('Removed servers folder [{}]'.format(self.__nginx_servers_directory))
            # os.makedirs(self.__nginx_servers_directory)
            # _logging.debug('Created servers folder [{}]'.format(self.__nginx_servers_directory))
        except Exception as exception:
            _logging.warning('Failed to reset servers folder.\nException [{}]'.format(exception))

    # Load.

    def load_start(self):
        _logging.debug('load_start()')
        try:
            self.__load_metadata = dict()
            self.__load_metadata['id'] = str(uuid.uuid4())
            self.__load_metadata['started'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            self.__load_metadata['load_count'] = 0
            _logging.info('Load start [{}]'.format(self.__load_metadata))

            self.load_from_file(self.get_from_kwargs('config_load_file', list()))
        except Exception as exception:
            _logging.warning('Failed to load [{}]'.format(exception))

    def load_stop(self):
        _logging.debug('load_stop()')
        try:
            self.__load_metadata['stopped'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
            _logging.info('Load stop [{}]'.format(self.__load_metadata))
        except Exception as exception:
            _logging.warning('Failed to load [{}]'.format(exception))

    def load(self, source):
        _logging.debug('load()')
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

    def load_core(self, source: dict):
        _logging.debug('load_core()')
        assert isinstance(source, dict)
        _logging.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))
        self.__data[CORE_KEY] = source

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
            _logging.warning('Failed to load server [{}] [{}], unknown load policy [{}]'.format(source[SERVER_KEY], source[NAME_KEY], source[LOAD_POLICY]))

    def load_comment_source(self, prefix, source, data):
        return '{}\n# koolie -> [{}]\n{}\n# koolie <- [{}]\n'.format(prefix, source, data, source)

    def load_location(self, source: dict):
        try:
            _logging.debug('load_location()')
            assert isinstance(source, dict)
            _logging.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))
            if source[LOAD_POLICY] == LOAD_POLICY_UNIQUE:
                location = self.get_location(source[SERVER_KEY], source[NAME_KEY])
                if location is None:
                    self.locations().append(source)
                    source[CONFIG_KEY] = self.load_comment_source('', self.get_location_id(source), source[CONFIG_KEY])
                    _logging.info('Created location [{}]'.format(self.get_location_id(source)))
                else:
                    _logging.warning('Failed to load unique location\n[{}]\nExisting location \n[{}]'.format(source, location))
            elif source[LOAD_POLICY] == LOAD_POLICY_APPEND:
                location = self.get_location(source[SERVER_KEY], source[NAME_KEY])
                if location is None:
                    self.locations().append(source)
                    source[CONFIG_KEY] = '# From [{}]\n{}'.format(self.get_location_id(source), source[CONFIG_KEY])
                else:
                    location[CONFIG_KEY] = '{}\n# -> [{}]\n{}\n# <- [{}]'.format(location[CONFIG_KEY], self.get_location_id(source), source[CONFIG_KEY], self.get_location_id(source))
                    _logging.debug('Appended location [{}]'.format(self.get_location_id(source)))
            else:
                _logging.warning('Failed to load location [{}], unknown load policy [{}]'.format(self.get_location_id(source), source[LOAD_POLICY]))
        except Exception as exception:
            _logging.warning('Failed to load location.\nConfig [{}]\nException [{}]'.format(source, exception))

    def load_upstream(self, source: dict):
        try:
            _logging.debug('load_upstream()')

            assert isinstance(source, dict)
            _logging.debug('loadPolicy [{}]'.format(source[LOAD_POLICY]))

            if source[LOAD_POLICY] == LOAD_POLICY_UNIQUE:
                upstream = self.get_upstream(source[NAME_KEY])
                if upstream is None:
                    self.upstreams().append(source)
                    source[CONFIG_KEY] = self.load_comment_source('', self.get_upstream_id(source), source[CONFIG_KEY])
                    _logging.info('Created upstream [{}]'.format(self.get_upstream_id(source)))
                else:
                    _logging.warning('Failed to load upstream [{}] already exists as [{}].'.format(self.get_upstream_id(source), self.get_upstream_id(upstream)))
            elif source[LOAD_POLICY] == LOAD_POLICY_APPEND:
                upstream = self.get_upstream(source[NAME_KEY])
                if upstream is None:
                    self.upstreams().append(source)
                    source[CONFIG_KEY] = self.load_comment_source('', self.get_upstream_id(source), source[CONFIG_KEY])
                    _logging.info('Created upstream [{}]'.format(self.get_upstream_id(source)))
                else:
                    upstream[CONFIG_KEY] = self.load_comment_source(upstream[CONFIG_KEY], self.get_upstream_id(source), source[CONFIG_KEY])
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
                        self.__load_metadata['load_count'] = self.__load_metadata['load_count'] + 1
                        _logging.debug('Returned from loader.')
                else:
                    _logging.warning('Failed to load item due to type [{}]'.format(type(item)))

        except Exception as exception:
            _logging.warning('Failed to load from list [{}:{}]'.format(type(exception), exception))

    def load_from_file(self, source):
        _logging.debug('load_from_file(source=type:{})'.format(type(source)))
        try:
            if source is None:
                _logging.info('Nothing to load.')
                return

            if isinstance(source, str):
                try:
                    with open(file=source, mode='r') as file:
                        data = file.read()
                    self.load(yaml.load(koolie.tools.common.safe_substitute(data, **self.__kwargs)))
                except Exception as exception:
                    _logging.warning('Failed to load file [{}] with exception [{}]'.format(source, exception))
            elif isinstance(source, list):
                for file in source:
                    self.load_from_file(file)
            else:
                _logging.warning('Unable to load source as a file, unknown type [{}]'.format(type(source)))
        except Exception as exception:
            _logging.warning('Failed to load from file [{}]'.format(exception))

    # Dump.

    def dump_start(self):
        self.__dump_metadata.clear()
        self.__dump_metadata['id'] = str(uuid.uuid4())
        self.__dump_metadata['started'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        _logging.info('Dump start [{}]'.format(self.__dump_metadata))

    def dump_stop(self):
        self.__dump_metadata['stopped'] = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        _logging.info('Dump stopped [{}]'.format(self.__dump_metadata))

    def dump(self):
        _logging.debug('dump()')
        try:

            if os.path.exists('{}nginx.conf'.format(self.__nginx_directory)):
                os.remove('{}nginx.conf'.format(self.__nginx_directory))

            self.reset_servers_folder()

            self.reset_upstreams_folder()

            self.dump_core()

            self.dump_upstreams()

            self.dump_servers()

            self.dump_locations()

            _logging.info('Dump OK')
        except Exception as exception:
            _logging.warning('update() Failed to update [{}] [{}]'.format(type(exception), exception))

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
        _logging.debug('dump_upstream()')

        file_name = '{}{}.conf'.format(self.__nginx_servers_directory, upstream[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.dump_file_comment())
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
        file_name = '{}{}.conf'.format(self.__nginx_servers_directory, server[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.common_nginx_conf_comments())
            file.write('server {\n')
            file.write(server[CONFIG_KEY])
            file.write('include {}{}/locations/*.conf;'.format(self.__nginx_servers_directory, server[NAME_KEY]))
            file.write('}')

    def dump_locations(self):
        _logging.debug('dump_locations()')

        for location in self.locations():
            self.dump_location(location)

    def dump_location(self, location):
        _logging.debug('dump_location()')

        server_folder = '{}{}/'.format(self.__nginx_servers_directory, location[SERVER_KEY])
        if not os.path.exists(server_folder):
            os.makedirs(name=server_folder, exist_ok=True)
            _logging.debug('Created server folder [{}]'.format(server_folder))

        file_name = '{}{}.conf'.format(server_folder, location[NAME_KEY])
        with open(file=file_name, mode='w') as file:
            file.write(self.dump_file_comment())
            file.write('location {} {} {{\n'.format(location[LOCATION_MATCH_MODIFIER], location[LOCATION_LOCATION_MATCH]))
            file.write(location[CONFIG_KEY])
            file.write('}\n')

    def test(self):
        result: bool = False
        try:
            _logging.debug('Testing NGINX configuration')
            # Load the run prefix in via the args.
            completed_process: subprocess.CompletedProcess = subprocess.run(['docker', 'exec', '-i', 'nginx', 'nginx', '-t'])
            _logging.debug('Completed process returned [{}]'.format(completed_process))
            result = completed_process.returncode == 0
        except Exception as called_process_error:
            _logging.warning('Failed to reload NGINX [{}]'.format(called_process_error))
        finally:
            return result

    def nginx_conf(self):
        result: bool = False
        try:
            _logging.debug('Testing NGINX configuration')
            # Load the run prefix in via the args.
            completed_process: subprocess.CompletedProcess = subprocess.run(['docker', 'exec', '-i', 'nginx', 'nginx', '-T'])
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
