import koolie
import logging
import os
import sys
import time
import uuid
import yaml

import koolie.go
import koolie.tools.service
import koolie.zookeeper_api.using_kazoo

_logging = logging.getLogger(__name__)

POD_KEY = 'pod'

STATUS_TYPE = '{}/status'.format(POD_KEY)

STATUS_CREATED_KEY: str = 'created'  # Immutable created timestamp.
STATUS_MODIFIED_KEY: str = 'modified'  # Changed whenever the status is modified.
STATUS_HEARTBEAT_KEY: str = 'heartbeat'  # How often the status is updated in seconds.


def encode_data(data) -> str:
    return yaml.dump(data, default_flow_style=False, default_style='|').encode('utf-8')


class PushStatus(koolie.tools.service.SleepService):

    TYPE = 'pod/status'
    CREATED = 'created'
    MODIFIED = 'modified'

    CONFIF_FILE = 'config_file'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.__kwargs = kwargs

        self.__zoo_keeper = koolie.zookeeper_api.using_kazoo.ZooKeeper(**kwargs)

        self.__path = '/koolie/pods/{}'.format(kwargs.get('k8s_pod_name', str(uuid.uuid4())))

        self.__data = None

        self.__status = None

        self.__config_files = None

    def start(self):
        _logging.debug('start()')
        self.__zoo_keeper.open()
        try:
            super().start()
        except Exception as exception:
            _logging.warning('Exception [{}]'.format(exception))
        finally:
            self.__zoo_keeper.close()

    def stop(self):
        _logging.debug('stop()')
        super().stop()

    def go(self):
        _logging.debug('go()')
        if self.__data is None:
            self.__data = self.create()
            self.__zoo_keeper.create_ephemeral_node(self.__path, encode_data(self.__data))
        else:
            self.__data = self.update()
            self.__zoo_keeper.set_node_value(self.__path, encode_data(self.__data))
        _logging.debug(self.__data)
        super().go()

    def create(self) -> dict:
        _logging.debug('create()')

        data = list()

        timestamp = time.time()
        self.__status = {'type': PushStatus.TYPE, PushStatus.CREATED: timestamp, PushStatus.MODIFIED: timestamp, 'hostname': self.__kwargs.get('os_environ_hostname')}
        data.append(self.__status)
        _logging.debug('Status [{}]'.format(self.__status))

        try:
            self.__config_files = list()
            config_files = self.__kwargs.get('config_files', list())
            assert isinstance(config_files, list)
            for config_file in config_files:
                try:
                    _logging.debug('Config file [{}]'.format(config_file))
                    assert isinstance(config_file, str)
                    with open(file=config_file, mode='r') as file:
                        config_file_data = yaml.load(file)
                    _logging.debug('Data [{}]'.format(config_file_data))
                    assert isinstance(config_file_data, list)
                    for d in config_file_data:
                        assert isinstance(d, dict)
                        d[PushStatus.CONFIF_FILE] = config_file
                    self.__config_files.extend(config_file_data)
                except Exception as exception:
                    _logging.warning('Failed to load config file [{}] exception [{}]'.format(config_file, exception))
        except Exception as exception:
            _logging.warning('Failed to load config files exception [{}]'.format(exception))

        data.extend(self.__config_files)
        _logging.debug('Data [{}]'.format(data))

        return data

    def update(self) -> dict:
        _logging.debug('update()')
        self.__status[PushStatus.MODIFIED] = time.time()
        _logging.debug('Data [{}]'.format(self.__data))
        return self.__data
