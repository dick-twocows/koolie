import abc
import contextlib
import logging
import sys
import typing
import uuid

import kazoo.recipe.watchers
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException


_logging = logging.getLogger(__name__)

ZOOKEEPER_HOSTS: str = 'ZOOKEEPER_HOSTS'
ZOOKEEPER_LOCALHOST: str = '127.0.0.1:2181'
ZOOKEEPER_HOSTS_DEFAULT: str = 'zookeeper.default.svc.cluster.local'
ZOOKEEPER_NODE_PATH: str = 'ZOOKEEPER_NODE_PATH'
ZOOKEEPER_NODE_PATH_DEFAULT: str = '/'


class AbstractKoolieZooKeeper(contextlib.AbstractContextManager):

    """An abstract class for accessing ZooKeeper
    Based on Kazoo because that's what was used first..."""

    def __init__(self, **kwargs):
        super().__init__()
        self._kwargs = kwargs

    def __enter__(self):
        self.start()
        return super().__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()
        pass

    def hosts(self) -> str:
        """Convenience method to return the hosts from `_kwargs`"""
        return self._kwargs.get(ZOOKEEPER_HOSTS, ZOOKEEPER_LOCALHOST)

    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    @abc.abstractmethod
    def get_node_value(self, path: str) -> bytes:
        pass

    @abc.abstractmethod
    def set_node_value(self, path: str, value: bytes):
        pass

    @abc.abstractmethod
    def get_children(self, path: str) -> typing.List[str]:
        pass

    @abc.abstractmethod
    def watch_children(self, path: str, func: callable):
        pass

    @abc.abstractmethod
    def create_node(self, path, value=b'', acl=None, ephemeral=False, sequence=False, make_path=False) -> str:
        """Create a node with regard to ZooKeeper restrictions."""
        pass

    def create_ephemeral_node(self, path, value=b'', acl=None, sequence=False, make_path=False):
        """Convenience method to create an ephemeral node."""
        return self.create_node(path, value, acl, True, sequence, make_path)

    def create_uuid_node(self, prefix: str = '/', value=b'', acl=None, ephemeral=False, sequence=False, make_path=False) -> str:
        """Convenience method to create an UUID(4) node."""
        return self.create_node('{}{}'.format(prefix, str(uuid.uuid4())), value, acl, ephemeral, sequence, make_path)

    @abc.abstractmethod
    def delete_node(self, path, version=-1, recursive=False):
        """Delete a node with regard to ZooKeeper restrictions."""
        pass

    def delete_node_and_children(self, path, version=-1):
        """Convenience method to delete a node and it's children."""
        self.delete_node(path, version, True)


class UsingKazoo(AbstractKoolieZooKeeper):

    """Concrete class to access ZooKeeper using Kazoo."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__open: bool = False

        self._kazoo_client: KazooClient = None

    @property
    def kazoo_client(self):
        return self._kazoo_client

    def start(self) -> bool:
        _logging.debug('ZooKeeper.open()')
        try:
            self._kazoo_client = KazooClient(hosts=self.hosts())
            self._kazoo_client.start(timeout=5)
            self.__open = True
        except KazooException as exception:
            _logging.warning('Failed to open [{}] [{}]'.format(sys.exc_info()[0], exception))
            self.__open = False
        return self.__open

    def stop(self):
        _logging.debug('ZooKeeper.close()')
        try:
            self.__open = False
            children_watch: kazoo.recipe.watchers.ChildrenWatch
            self._kazoo_client.stop()
            self._kazoo_client = None
        except KazooException:
            _logging.warning('Failed to close [{}]'.format(sys.exc_info()[0]))

    def get_node_value(self, path) -> bytes:
        try:
            return self._kazoo_client.get(path)[0]
        except Exception as exception:
            _logging.warning('Failed to get value for path [{}] with exception [{}]'.format(path, exception))
            return None

    def set_node_value(self, path: str, value=b''):
        _logging.debug('ZooKeeper.set_node_value()')
        assert path is not None and isinstance(path, str)
        assert value is not None and isinstance(value, bytes)
        self._kazoo_client.set(path, value, -1)

    def get_children(self, path: str) -> typing.List[str]:
        return self._kazoo_client.get_children(path)

    def watch_children(self, path: str, func: callable):
        self._kazoo_client.ChildrenWatch(path, func)

    def create_node(self, path, value=b'', acl=None, ephemeral=False, sequence=False, make_path=False):
        _logging.debug('create_node()')
        self._kazoo_client.create(path, value, acl, ephemeral, sequence, make_path)

    def delete_node(self, path, version=-1, recursive=False):
        self._kazoo_client.delete(path, version, recursive)


class WithZooKeeper(object):

    def __init__(self, **kwargs) -> None:
        super().__init__()
        self.koolie_zookeeper = UsingKazoo(**kwargs)

    @property
    def koolie_zookeeper(self) -> UsingKazoo:
        return self.__zoo_keeper

    @koolie_zookeeper.setter
    def koolie_zookeeper(self, zoo_keeper: UsingKazoo):
        assert zoo_keeper is None or isinstance(zoo_keeper, UsingKazoo)
        self.__zoo_keeper = zoo_keeper

    def __str__(self) -> str:
        return str(self.koolie_zookeeper)


if __name__ == '__main__':

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    koolie_zookeeper: AbstractKoolieZooKeeper
    with UsingKazoo(ZOOKEEPER_HOSTS=ZOOKEEPER_LOCALHOST) as koolie_zookeeper:
        for child in koolie_zookeeper.get_children('/'):
            _logging.info(child)
        koolie_zookeeper.create_ephemeral_node(path='/foo', value='bar'.encode('utf-8'))
        for child in koolie_zookeeper.get_children('/'):
            _logging.info(child)
        _logging.info(koolie_zookeeper.get_node_value(path='/foo').decode('utf-8'))

        def children(children: list):
            for child in children:
                print(child)

        koolie_zookeeper.watch_children('/', children)

        _logging.info(koolie_zookeeper.create_uuid_node())
        for child in koolie_zookeeper.get_children('/'):
            _logging.info(child)




    # _logging.info(with_zookeeper)
