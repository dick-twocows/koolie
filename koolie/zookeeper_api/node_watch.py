import logging
import os
import sys
import time
import yaml
import kazoo.protocol.states
import koolie.go

import koolie.zookeeper_api.using_kazoo


_logging = logging.getLogger(__name__)


class NodeWatch(koolie.zookeeper_api.using_kazoo.WithZooKeeper):

    def __init__(self, args):
        koolie.zookeeper_api.using_kazoo.WithZooKeeper.__init__(self, args)

        self.__args = args

    @property
    def args(self):
        return self.__args

    def start(self):
        _logging.debug('NodeWatch.start()')
        if self.zoo_keeper.open():
            try:
                @self.zoo_keeper.kazoo_client.ChildrenWatch(self.__args.zookeeper_node_path)
                def watch_children(children):
                    self.change(children)
                    return True

                while True:
                    time.sleep(1)
                    if koolie.go.signalled_to_exit:
                        _logging.debug('Signalled to exit.')
                        break
            except KeyboardInterrupt:
                _logging.info('Ctrl+C exit.')
            except Exception as exception:
                _logging.error('Unexpected [{}]\nException [{}]'.format(sys.exc_info()[0], exception))
            finally:
                self.zoo_keeper.close()

    def stop(self):
        _logging.debug('NodeWatch.stop()')
        # self.__graceful_killer.kill_now = True

    # Override in subclasses to do something with the children.
    def change(self, children):
        return True


class DeltaNodeWatch(NodeWatch):

    def __init__(self, args):
        NodeWatch.__init__(self, args)

        self.__current = set()

    def change(self, children):
        new: set = set(children)  # Sanity check!
        self.removed(self.__current.difference(new))
        self.added(new.difference(self.__current))
        self.__current = new
        return True

    def after_change(self):
        pass

    def added(self, children) -> object:
        pass

    def removed(self, children) -> object:
        pass


class EchoNodeWatch(DeltaNodeWatch):

    def __init__(self, args):
        DeltaNodeWatch.__init__(self, args)

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
        DeltaNodeWatch.__init__(self, args)
        _logging.debug('YAMLNodeWatch.__init__()')

    def added(self, children) -> object:
        for child in children:
            _logging.info(child)
            try:
                data: tuple = self.zoo_keeper.get_node_value('{}{}'.format(self.args.zookeeper_node_path, child))
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

    def __init__(self, args):
        super().__init__(args)
        _logging.debug('StatusTypeWatch.__init__()')
        self.__config = vars(args)

    def added(self, children) -> object:
        for child in children:
            _logging.info(child)
            try:
                data: tuple = self.zoo_keeper.get_node_value('{}{}'.format(self.args.zookeeper_node_path, child))
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
                                    handle = self.__config.get(StatusTypeWatch.ADD).get(item.get(koolie.go.KOOLIE_STATUS_TYPE))
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

