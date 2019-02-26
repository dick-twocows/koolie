import logging
import os
import sys
import time
import yaml

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
                    print(koolie.go.signalled_to_exit)
                    if koolie.go.signalled_to_exit:
                        _logging.info('Signal exit.')
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


class GetYAMLNodeWatch(DeltaNodeWatch):

    def __init__(self, args):
        DeltaNodeWatch.__init__(self, args)

    def added(self, children) -> object:
        for child in children:
            _logging.info(child)
            data: tuple = self.zoo_keeper.kazoo_client.get(self.path + child)
            value: bytes = data[0]
            j = yaml.load(value.decode('utf-8'))
            _logging.debug(j)
            if isinstance(j, list):
                _logging.info('Items [{}]'.format(len(j)))
                for item in j:
                    _logging.debug('Item type [{}]'.format(type(item)))
                    if isinstance(item, dict):
                        _logging.info('Item type [{}] tag [{}]'.format(item.get(koolie.TYPE_KEY), item.get(koolie.TAG_KEY)))
            else:
                _logging.warning('Unknown type [{}]'.format(type(j)))

    def removed(self, children) -> object:
        for child in children:
            _logging.info(child)
