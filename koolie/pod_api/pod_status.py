import koolie
import logging
import os
import sys
import time
import uuid
import yaml

import koolie.go
import koolie.zookeeper_api.using_kazoo

_logging = logging.getLogger(__name__)

POD_KEY = 'pod'

STATUS_TYPE = '{}/status'.format(POD_KEY)

STATUS_CREATED_KEY: str = 'created'  # Immutable created timestamp.
STATUS_MODIFIED_KEY: str = 'modified'  # Changed whenever the status is modified.
STATUS_HEARTBEAT_KEY: str = 'heartbeat'  # How often the status is updated in seconds.


class Status(yaml.YAMLObject):

    time_format = '%H:%M:%S'

    def __init__(self) -> None:
        yaml.YAMLObject.__init__(self)

    @classmethod
    def to_yaml(cls, dumper, data):
        dict_representation = {
            'val': 'fred'
        }
        node = dumper.represent_mapping(u'!A', dict_representation)
        return node

    @classmethod
    def from_yaml(cls, loader, node):
        dict_representation = loader.construct_mapping(node)
        val = dict_representation['val']
        return Status()


class PodStatus(koolie.zookeeper_api.using_kazoo.WithZooKeeper):

    def __init__(self, args):
        _logging.debug('PodStatus.__init()')
        koolie.zookeeper_api.using_kazoo.WithZooKeeper.__init__(self, args)

        self.__args = args
        self.__config = vars(args)

        self.__data = list

        self.pod_name = self.__config.get('pod_name', str(uuid.uuid4()))

        self.heartbeat = 10  # int(os.getenv(KOOLIE_POD_HEARTBEAT, KOOLIE_POD_HEARTBEAT_DEFAULT))

        self.node_name = self.create_node_name(self.pod_name)

        self.zoo_keeper.hosts = self.__config.get('zookeeper_hosts')  # os.getenv(KOOLIE_ZOOKEEPER_HOSTS,KOOLIE_ZOOKEEPER_HOSTS_DEFAULT)

        self.__created = time.time()
        self.__status = dict()

        _logging.debug(self)

    @property
    def _created(self):
        return self.__created

    @property
    def pod_name(self):
        return self.__pod_name

    @pod_name.setter
    def pod_name(self, pod_name: str):
        assert pod_name is None or isinstance(pod_name, str)
        self.__pod_name = pod_name

    @property
    def heartbeat(self) -> int:
        return self.__heartbeat

    @heartbeat.setter
    def heartbeat(self, heartbeat: int):
        assert heartbeat is not None and isinstance(heartbeat, int)
        self.__heartbeat = heartbeat

    @property
    def node_name(self):
        return self.__node_name

    @node_name.setter
    def node_name(self, node_name: str):
        assert node_name is None or isinstance(node_name, str)
        self.__node_name = node_name

    # Protected stuff.

    @property
    def _status(self) -> dict:
        return self.__status

    @_status.setter
    def _status(self, status: dict):
        assert status is not None and isinstance(status, dict)
        self.__status = status

    def create_node_name(self, suffix: str) -> str:
        assert suffix is not None and isinstance(suffix, str)
        return '{}{}'.format(koolie.go.ZOOKEEPER_PODS, self.pod_name)

    def start(self):
        _logging.debug('start()')

        if self.pod_name is None:
            raise ValueError('Pod name not defined')

        # self.__created = time.time()
        # self.__status = self.create_status()
        self.__data = self.create_data()
        # if not self.__status[STATUS_VALID_KEY]:
        #     _logging.warning('Status not valid [%s].', yaml.dump(self.__status))
        #     return
        try:
            while True:
                if self.zoo_keeper.open():
                    self.zoo_keeper.create_ephemeral_node(self.node_name, self.encode_data())
                    _logging.debug('Created ephemeral node [{}]'.format(self.node_name))
                    while True:
                        _logging.debug('Sleeping')
                        time.sleep(self.heartbeat)
                        if koolie.go.signalled_to_exit:
                            _logging.info('Signal exit.')
                            break
                        self.__data = self.update_data()
                        self.zoo_keeper.set_node_value(self.node_name, self.encode_data())
                        _logging.debug('Updated node')
                    break
                else:
                    _logging.warning('Failed to open ZooKeeper, waiting for ? seconds.')
                    time.sleep(60)
        except KeyboardInterrupt:
            _logging.info('Ctrl+C exit.')
        except Exception:
            _logging.error('Unexpected [{}].'.format(sys.exc_info()[0]))
            raise
        finally:
            self.zoo_keeper.close()

    def stop(self):
        _logging.info('stop()')
        self._graceful_killer.kill_now = True

    # Data.

    def data(self) -> list:
        return self.__data

    def create_data(self) -> list:
        _logging.debug('create_data()')
        data = list()
        self.__status = self.create_status()
        data.append(self.__status)
        return data

    def update_data(self) -> list:
        _logging.debug('update_data()')
        data = list()
        self.__status = self.update_status()
        data.append(self.__status)
        return data

    def encode_data(self):
        return yaml.dump(self.__data, default_flow_style=False, default_style='|').encode('utf-8')

    # Status.

    def encode_status(self):
        return yaml.dump(self._status, default_flow_style=False, default_style='|').encode('utf-8')

    def create_status(self) -> dict:
        _logging.debug('create_status()')
        status: dict = dict()
        status[koolie.go.KOOLIE_STATUS_TYPE] = STATUS_TYPE
        status[POD_KEY] = self.__pod_name
        status[STATUS_CREATED_KEY] = self.__created
        status[STATUS_MODIFIED_KEY] = time.time()
        status[STATUS_HEARTBEAT_KEY] = self.__heartbeat
        return status

    def update_status(self) -> dict:
        _logging.debug('update_status()')
        status: dict = self.__status
        status[STATUS_MODIFIED_KEY] = time.time()
        return status

    def __str__(self) -> str:
        return 'Pod name [{}] Node name [{}] Heartbeat [{}] ZooKeeper [{}]'.format(self.pod_name, self.node_name, self.heartbeat, self.zoo_keeper)
