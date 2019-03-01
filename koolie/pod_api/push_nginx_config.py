import logging
import string
import yaml

import koolie.pod_api.pod_status

print(koolie.pod_api)

_logging = logging.getLogger(__name__)

NGINX: str = 'nginx'

NGINX_LOCATIONS: str = 'locations'
NGINX_SERVERS: str = 'servers'

NGINX_CONFIG_FILE: str = 'NGINX_CONFIG_FILE'
NGINX_CONFIG_FILE_DEFAULT: str = '/koolie_old/set-nginx.yaml'


class PushNGINXConfig(koolie.pod_api.pod_status.PushStatus):

    POD_PUSH_CONFIG_FILE = 'pod_push_config_file'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.__kwargs = kwargs

        self.__zoo_keeper = koolie.zookeeper_api.using_kazoo.ZooKeeper(**kwargs)

        self.file = None
        self.file_cache = None

    def start(self):
        _logging.debug('start()')
        self.cache_file()
        super().start()

    def stop(self):
        _logging.debug('stop()')
        super().stop()

    def go(self):
        _logging.debug('go()')
        super().go()

    @property
    def file(self) -> str:
        return self.__file

    @file.setter
    def file(self, file: str):
        assert file is None or isinstance(file, str)
        self.__file = file

    @property
    def file_cache(self):
        return self.__file_cache

    @file_cache.setter
    def file_cache(self, file_cache):
        self.__file_cache = file_cache

    def cache_file(self):
        try:
            if self.file_cache is None:
                with open(file=self.__kwargs[PushNGINXConfig.POD_PUSH_CONFIG_FILE], mode='r') as y:
                    template = string.Template(y.read())
                    r = template.substitute(self.__kwargs)
                    self.file_cache = yaml.load(r)
                _logging.info('File cache [{}]'.format(self.file_cache))
        except Exception as exception:
            _logging.warning('Exception [{}]'.format(self.file, exception))

    def create_data(self) -> list:
        _logging.debug('create_data()')
        data = super().create_data()
        if self.file_cache is None:
            try:
                with open(file=self.file, mode='r') as y:
                    self.file_cache = yaml.load(y)
            except Exception as exception:
                _logging.warning('Failed to open [{}] exception [{}]'.format(self.file, exception))
        data.extend(self.file_cache)
        return data

    def update_data(self) -> list:
        _logging.debug('update_data()')
        data = super().update_data()
        data.extend(self.file_cache)
        return data

    def __str__(self) -> str:
        return '{} [{}]'.format(super(), self.file)
