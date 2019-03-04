import logging
import yaml

import koolie.nginx.config
import koolie.zookeeper_api.node_watch

_logger = logging.getLogger(__name__)


class Consume(koolie.zookeeper_api.node_watch.DeltaNodeWatch):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__all_nodes = set()

        self.__nginx_nodes = set()

        self.__change_nginx_config = koolie.nginx.config.NGINXConfig(**kwargs)

        self.__added_all_nodes = set()

        self.__added_nginx_nodes = set()

    def change(self, children):
        self.__change_nginx_config.clear()
        self.__added_all_nodes.clear()
        self.__added_nginx_nodes.clear()

        self.__change_nginx_config.load_start()
        super().change(children)
        self.__change_nginx_config.load_stop()

        self.__change_nginx_config.dump_start()
        self.__change_nginx_config.dump()
        self.__change_nginx_config.dump_stop()

        self.__change_nginx_config.test()

    def added(self, children):
        _logger.debug('added()')

        for child in children:
            _logger.debug('Child [{}]'.format(child))

            self.__added_all_nodes.add(child)

            # Kazoo returns a tuple, [0] is the data (b array) and [1] is the ZooKeeper info (ZnodeStat).
            data: tuple = self.zoo_keeper().kazoo_client.get(self.zookeeper_node_path() + '/' + child)

            j = yaml.load(data[0].decode('utf-8'))
            _logger.debug(j)

            loaded = self.__change_nginx_config.load(j)
            if loaded > 0:
                self.__added_nginx_nodes.add(child)
                _logger.debug('Added child [{}] to NGINX nodes'.format(child))

        _logger.debug('NGINX config [{}]'.format(self.__change_nginx_config))

    def removed(self, children) -> object:
        _logger.debug('removed()')
        return super().removed(children)
