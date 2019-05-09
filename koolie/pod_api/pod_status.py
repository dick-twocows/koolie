import koolie
import logging
import string
import sys
import time
import yaml

import koolie.tools.abstract_service
import koolie.tools.common
import koolie.zookeeper_api.koolie_zookeeper

_logger = logging.getLogger(__name__)

POD_KEY = 'pod'

STATUS_TYPE = '{}/status'.format(POD_KEY)

STATUS_CREATED_KEY: str = 'created'  # Immutable created timestamp.
STATUS_MODIFIED_KEY: str = 'modified'  # Changed whenever the status is modified.
STATUS_HEARTBEAT_KEY: str = 'heartbeat'  # How often the status is updated in seconds.


def encode_data(data) -> str:
    """Encode the given data returning a YAML UTF-8 string."""
    try:
        return yaml.dump(data, default_flow_style=False, default_style='|').encode('utf-8')
    except Exception as exception:
        _logger.warning('Failed to encode data with exception, type [{}] value [{}]'.format(exception, type(data), data))
        return None


def decode_data(data) -> object:
    """Decode the given YAML UTF-8 encoded data, returning an object."""
    try:
        return yaml.load(data.decode('utf-8'))
    except Exception as exception:
        _logger.warning('Failed to decode data with exception, type [{}] value [{}]'.format(exception, type(data), data))
        return None


class PushStatus(koolie.tools.abstract_service.SleepService):

    TYPE = 'pod/status'
    CREATED = 'created'
    MODIFIED = 'modified'

    CONFIG_FILE = 'config_file'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.__kwargs = kwargs

        self.__zoo_keeper = koolie.zookeeper_api.koolie_zookeeper.UsingKazoo(**kwargs)

        self.__path = '/koolie/pods/{}'.format(kwargs.get('k8s_pod_name', self.name()))

        self.__data = None

        self.__status = None

        self.__config_files = None

        _logger.info('[{}] pushing status to [{}].'.format(self.name(), self.__path))

    def before_start(self):
        try:
            self.__zoo_keeper.start()
            super().before_start()
        except Exception as exception:
            _logger.warning('Exception [{}]'.format(exception))

    def before_stop(self):
        self.__zoo_keeper.stop()
        super().before_stop()

    def wake(self):
        _logger.info('wake()')
        try:
            if self.__data is None:
                self.__data = self.create_status()
                self.__zoo_keeper.create_ephemeral_node(self.__path, encode_data(self.__data))
            else:
                self.__data = self.update_status()
                self.__zoo_keeper.set_node_value(self.__path, encode_data(self.__data))
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
        finally:
            super().wake()

    def create_status(self) -> dict:
        _logger.debug('create_status()')

        data = list()

        timestamp = time.time()
        self.__status = {
            'type': PushStatus.TYPE,
            PushStatus.CREATED: timestamp,
            PushStatus.MODIFIED: timestamp,
            'hostname': self.__kwargs.get('os_environ_hostname')
        }
        data.append(self.__status)
        _logger.debug('Status [{}]'.format(self.__status))

        try:
            self.__config_files = list()
            config_files = self.__kwargs.get('config_files', list())
            assert isinstance(config_files, list)
            for config_file in config_files:
                try:
                    _logger.debug('Config file [{}]'.format(config_file))

                    assert isinstance(config_file, str)
                    with open(file=config_file, mode='r') as file:
                        config_file_data = file.read()

                    config_file_yaml = yaml.load(self.substitute(config_file_data))
                    _logger.debug('YAML [{}]'.format(config_file_yaml))

                    assert isinstance(config_file_yaml, list)
                    for d in config_file_yaml:
                        assert isinstance(d, dict)
                        d[PushStatus.CONFIG_FILE] = config_file

                    self.__config_files.extend(config_file_yaml)
                except Exception as exception:
                    _logger.warning('Failed to load config file [{}] exception [{}]'.format(config_file, exception))
        except Exception as exception:
            _logger.warning('Failed to load config files exception [{}]'.format(exception))

        data.extend(self.__config_files)
        _logger.debug('Data [{}]'.format(data))

        return data

    def update_status(self) -> dict:
        _logger.debug('update()')
        self.__status[PushStatus.MODIFIED] = time.time()
        _logger.debug('Data [{}]'.format(self.__data))
        return self.__data

    def substitute(self, data):
        _logger.debug('substitute()')
        template = string.Template(data)
        _logger.debug('Template [{}]'.format(template))
        result = template.substitute(self.__kwargs)
        _logger.debug('result [{}]'.format(result))
        return result


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    push_status: PushStatus = PushStatus()
    push_status.start()
