import logging
import socket
import string
import sys
import yaml

import koolie.pod_api.pod_status

_logging = logging.getLogger(__name__)

NGINX: str = 'nginx'

NGINX_LOCATIONS: str = 'locations'
NGINX_SERVERS: str = 'servers'

NGINX_CONFIG_FILE: str = 'NGINX_CONFIG_FILE'
NGINX_CONFIG_FILE_DEFAULT: str = '/koolie_old/set-nginx.yaml'


class PushNGINXConfig(koolie.pod_api.pod_status.PushStatus):

    POD_PUSH_CONFIG_FILE = 'koolie_nginx_config_file'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._file_cache = None

    def before_start(self):
        self.cache_file()
        super().before_start()

    def cache_file(self):
        file_name = self.get_kv(PushNGINXConfig.POD_PUSH_CONFIG_FILE)
        try:
            with open(file=file_name, mode='r') as y:
                template = string.Template(y.read())
                r = template.substitute(self._kwargs)
                self._file_cache = yaml.load(r)
        except Exception as exception:
            _logging.warning('Failed to cache file [{}] with exception [{}].'.format(file_name, exception))

    def create_status(self) -> dict:
        data = super().create_status()
        try:
            self.cache_file()
            if self._file_cache is not None:
                data.extend(self._file_cache)
        except Exception as exception:
            _logging.warning('exception [{}]'.format(exception))
        return data

    def update_status(self) -> dict:
        _logging.debug('update_data()')
        data = super().update_status()
        if self._file_cache is not None:
            data.extend(self._file_cache)
        return data


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    service = PushNGINXConfig(
        os_environ_hostname=socket.getfqdn(),
        koolie_nginx_config_file='/home/dick/PycharmProjects/koolie/koolie/pod_api/ydos_upstream.yaml'
    )
    service.start()
