import logging
import socket
import string
import sys
import typing
import yaml

import koolie.pod_api.pod_status

_logger = logging.getLogger(__name__)

NGINX: str = 'nginx'

NGINX_LOCATIONS: str = 'locations'
NGINX_SERVERS: str = 'servers'

NGINX_CONFIG_FILE: str = 'NGINX_CONFIG_FILE'
NGINX_CONFIG_FILE_DEFAULT: str = ''


class PushNGINXConfig(koolie.pod_api.pod_status.PushStatus):

    POD_PUSH_CONFIG_FILE = 'koolie_nginx_config_files'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._file_cache: typing.List[...] = list()

    def clear_files(self):
        self._file_cache.clear()

    def cache_files(self, file_names: typing.List[str]):
        """Cache the given files."""
        for file_name in file_names:
            try:
                with open(file=file_name, mode='r') as y:
                    template = string.Template(y.read())
                    r = template.substitute(self._kwargs)
                    self._file_cache.append(yaml.load(r))
            except Exception as exception:
                _logger.warning('Failed to cache file [{}] with exception [{}].'.format(file_name, exception))

    def create_status(self) -> dict:
        data = super().create_status()
        try:
            self.cache_files(self.get_kv(PushNGINXConfig.POD_PUSH_CONFIG_FILE).split(','))
            data.extend(self._file_cache)
        except Exception as exception:
            _logger.warning('exception [{}]'.format(exception))
        return data

    def update_status(self) -> dict:
        return super().update_status()

    def __str__(self) -> str:
        return '{}\nFiles cache [{}]'.format(super().__str__(), len(self._file_cache))


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    service = PushNGINXConfig(
        os_environ_hostname=socket.getfqdn(),
        koolie_nginx_config_files='/home/dick/PycharmProjects/koolie/koolie/pod_api/ydos_upstream.yaml,/home/dick/PycharmProjects/koolie/koolie/pod_api/pod_1_upstream.yaml,,/home/dick/PycharmProjects/koolie/koolie/pod_api/pod_2_upstream.yaml'
    )
    _logger.info(service)
    service.start()
