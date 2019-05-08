import abc
import logging
import sys
import yaml
import kazoo.protocol.states
import koolie.tools.abstract_service

import koolie.zookeeper_api.koolie_zookeeper


_logging = logging.getLogger(__name__)

KOOLIE_NODE_WATCH_PATH: str = 'koolie_node_watch_path'


class AbstractNodeWatch(koolie.tools.abstract_service.SleepService):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__zoo_keeper: koolie.zookeeper_api.koolie_zookeeper.AbstractKoolieZooKeeper = koolie.zookeeper_api.koolie_zookeeper.UsingKazoo(**kwargs)

        self.__change_count = 0;

    def zoo_keeper(self):
        return self.__zoo_keeper

    def zookeeper_node_path(self) -> str:
        return self.getKV(KOOLIE_NODE_WATCH_PATH)

    def before_start(self):
        _logging.debug('start()')
        self.__zoo_keeper.start()
        try:
            self.__zoo_keeper.watch_children(self.zookeeper_node_path(), self.change)
        except Exception as exception:
            _logging.warning('Exception [{}]'.format(exception))

    def before_stop(self):
        try:
            self.__zoo_keeper.stop()
        except Exception as exception:
            _logging.warning('Exception [{}]'.format(exception))

    @abc.abstractmethod
    def change(self, children):
        """SubClasses need to override this method and do something.
        By default it increments change count by 1."""
        self.__change_count += 1


class DeltaNodeWatch(AbstractNodeWatch):

    """ZooKeeper node watch that performs a delta when changes occur using added() and removed().
    The delta is calculated based on the current nodes."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__current = set()

        self._added = 0
        self._removed = 0

    def current(self) -> set:
        return self.__current

    def change(self, children):
        new: set = set(children)  # Sanity check!
        assert len(children) == len(new)
        self.removed(self.__current.difference(new))
        self.added(new.difference(self.__current))
        self.__current = new
        super().change(children)

    def added(self, children) -> object:
        _logging.debug('added()')
        self._added += len(children)

    def removed(self, children) -> object:
        _logging.debug('removed()')
        self._removed += len(children)


class EchoNodeWatch(DeltaNodeWatch):

    """ZooKeeper node watch that echos changes."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def added(self, children) -> object:
        _logging.info('EchoNodeWatch.added()')
        for child in children:
            _logging.info(child)

    def removed(self, children) -> object:
        _logging.info('EchoNodeWatch.removed()')
        for child in children:
            _logging.info(child)


class YAMLNodeWatch(DeltaNodeWatch):

    def __init__(self, args):
        super().__init__(args)
        _logging.debug('YAMLNodeWatch.__init__()')

    def added(self, children) -> object:
        for child in children:
            _logging.info(child)
            try:
                data: tuple = self.zoo_keeper.get_node_value('{}{}'.format(self.args.get('zookeeper_node_path'), child))
                if isinstance(data, tuple):
                    _logging.info('Tuple length [{}]'.format(len(data)))

                    if len(data) >= 1:
                        value: bytes = data[0]
                        j = yaml.load(value.decode('utf-8'))
                        _logging.debug(j)
                        if isinstance(j, list):
                            _logging.info('Items [{}]'.format(len(j)))
                            for item in j:
                                if isinstance(item, dict):
                                    _logging.info('Item type [{}] tag [{}]'.format(item.get(koolie.go.KOOLIE_STATUS_TYPE), item.get(koolie.go.KOOLIE_STATUS_TAG)))
                                else:
                                    _logging.info('Item type [{}]'.format(type(item)))
                        else:
                            _logging.warning('Expected list got [{}]'.format(type(j)))

                    if len(data) >= 2:
                        if isinstance(data[1], kazoo.protocol.states.ZnodeStat):
                            _logging.info('ZnodeStat [{}]'.format(data[1]))
                        else:
                            _logging.warning('Expected ZnodeStat got [{}]'.format(type(data[1])))
                else:
                    _logging.warning('Expected tuple got [{}]'.format(type(data)))
            except Exception as exception:
                _logging.warning('Exception [{}}'.format(exception))

    def removed(self, children) -> object:
        for child in children:
            _logging.info(child)


class StatusTypeWatch(DeltaNodeWatch):

    ADD = 'StatusTypeWatch_add'

    REMOVE = 'StatusTypeWatch_remove'

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        _logging.debug('StatusTypeWatch.__init__()')
        self.__kwargs = kwargs

    def added(self, children) -> object:
        for child in children:
            _logging.info(child)
            try:
                data: tuple = self.zoo_keeper.get_node_value('{}{}'.format(self.__kwargs.get('zookeeper_node_path'), child))
                if isinstance(data, tuple):
                    _logging.info('Tuple length [{}]'.format(len(data)))

                    if len(data) >= 1:
                        value: bytes = data[0]
                        j = yaml.load(value.decode('utf-8'))
                        _logging.debug(j)
                        if isinstance(j, list):
                            _logging.info('Items [{}]'.format(len(j)))
                            for item in j:
                                if isinstance(item, dict):
                                    handle = self.__kwargs.get(StatusTypeWatch.ADD).get(item.get(koolie.go.KOOLIE_STATUS_TYPE))
                                    if handle is None:
                                        _logging.info('No handle for type [{}]'.format(item.get(koolie.go.KOOLIE_STATUS_TYPE)))
                                    else:
                                        handle(item)
                                else:
                                    _logging.info('Item type [{}]'.format(type(item)))
                        else:
                            _logging.warning('Expected list got [{}]'.format(type(j)))

                    if len(data) >= 2:
                        if isinstance(data[1], kazoo.protocol.states.ZnodeStat):
                            _logging.info('ZnodeStat [{}]'.format(data[1]))
                        else:
                            _logging.warning('Expected ZnodeStat got [{}]'.format(type(data[1])))
                else:
                    _logging.warning('Expected tuple got [{}]'.format(type(data)))
            except Exception as exception:
                _logging.warning('Exception [{}]'.format(exception))

    def removed(self, children) -> object:
        return super().removed(children)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    echo_node_watch = EchoNodeWatch(koolie_node_watch_path='/')
    echo_node_watch.start()
