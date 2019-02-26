import logging
import sys
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException


_logging = logging.getLogger(__name__)

ZOOKEEPER_HOSTS: str = 'ZOOKEEPER_HOSTS'
ZOOKEEPER_HOSTS_DEFAULT: str = 'zookeeper.default.svc.cluster.local'
ZOOKEEPER_NODE_PATH: str = 'ZOOKEEPER_NODE_PATH'
ZOOKEEPER_NODE_PATH_DEFAULT: str = '/'


class ZooKeeper(object):

    def __init__(self, args):
        _logging.debug('ZooKeeper.__init__()')

        self.__args = args

        self.__open: bool = False

        self.__kazoo_client = None

    def args(self, args) -> 'ZooKeeper':
        self.__args = args
        return self

    @property
    def kazoo_client(self):
        return self.__kazoo_client

    def open(self) -> bool:
        _logging.debug('ZooKeeper.open()')
        try:
            self.__kazoo_client = KazooClient(hosts=self.__args.zookeeper_hosts)
            self.__kazoo_client.start(timeout=5)
            self.__open = True
        except KazooException as exception:
            _logging.warning('Failed to open [{}] [{}]'.format(sys.exc_info()[0], exception))
            self.__open = False
        return self.__open

    def close(self):
        _logging.debug('ZooKeeper.close()')
        try:
            self.__open = False
            self.__kazoo_client.stop()
            self.__kazoo_client.close()
            self.__kazoo_client = None
        except KazooException:
            _logging.warning('Failed to close [{}]'.format(sys.exc_info()[0]))

    def create_node(self, path, value=b'', acl=None, ephemeral=False, sequence=False, make_path=False):
        _logging.debug('create_node()')
        self.__kazoo_client.create(path, value, acl, ephemeral, sequence, make_path)

    def create_ephemeral_node(self, path, value=b''):
        _logging.debug('create_ephemeral_node()')
        self.create_node(path, value, ephemeral=True, make_path=True)

    def get_node_value(self, path) -> tuple:
        _logging.debug('get_node_value(path [%s])', path)
        return self.__kazoo_client.get(path)

    def set_node_value(self, path: str, value=b''):
        _logging.debug('ZooKeeper.set_node_value()')
        assert path is not None and isinstance(path, str)
        assert value is not None and isinstance(value, bytes)
        self.__kazoo_client.set(path, value, -1)

    def __str__(self) -> str:
        return 'Hosts [{}] Open [{}]'.format(self.hosts, self.__open)


class WithZooKeeper(object):

    def __init__(self, args) -> None:
        super().__init__()
        self.zoo_keeper = ZooKeeper(args)

    @property
    def zoo_keeper(self) -> ZooKeeper:
        return self.__zoo_keeper

    @zoo_keeper.setter
    def zoo_keeper(self, zoo_keeper: ZooKeeper):
        assert zoo_keeper is None or isinstance(zoo_keeper, ZooKeeper)
        self.__zoo_keeper = zoo_keeper
