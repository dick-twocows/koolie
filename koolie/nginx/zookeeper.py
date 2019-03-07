import logging
import yaml

import koolie.nginx.config
import koolie.pod_api.pod_status
import koolie.zookeeper_api.node_watch

_logger = logging.getLogger(__name__)


class Consume(koolie.zookeeper_api.node_watch.DeltaNodeWatch):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.__kwargs = kwargs

        self.__nginx_config = None

        self.__all_nodes = set()

        self.__nginx_nodes = set()

        self.__change_nginx_config = None

        self.__added_all_nodes = None

        self.__added_nginx_nodes = None

        self.__removed_nginx_nodes: set = None

    def change(self, children):
        self.__change_nginx_config = koolie.nginx.config.NGINXConfig(**self.__kwargs)
        self.__added_all_nodes = set()
        self.__added_nginx_nodes = set()

        self.__removed_nginx_nodes = set()

        self.__change_nginx_config.load_start()
        super().change(children)
        self.__change_nginx_config.load_stop()
        _logger.info('NGINX changes, added [{}], removed [{}]'.format(len(self.__added_nginx_nodes), len(self.__removed_nginx_nodes)))

        _logger.info('Loaded count [{}]'.format(self.__change_nginx_config.loaded_count()))

        self.__all_nodes = self.__added_all_nodes
        self.__added_all_nodes = None

        self.__nginx_nodes = self.__added_nginx_nodes
        self.__added_nginx_nodes = None

        self.__removed_nginx_nodes = None

        if self.__change_nginx_config.load_metadata()['load_count'] > 0:
            self.__change_nginx_config.dump_start()
            self.__change_nginx_config.dump()
            self.__change_nginx_config.dump_stop()

            self.__change_nginx_config.test()

        self.__change_nginx_config = None

    def added(self, children):
        _logger.debug('added()')

        for child in children:
            _logger.debug('Child [{}]'.format(child))

            self.__added_all_nodes.add(child)

            # Kazoo returns a tuple, [0] is the data (b array) and [1] is the ZooKeeper info (ZnodeStat).
            data: tuple = self.zoo_keeper().get_node_value(self.zookeeper_node_path() + '/' + child)
            if data is None:
                _logger.warning('Failed to get value for child [{}]'.format(child))
                continue

            j = koolie.pod_api.pod_status.decode_data(data[0])
            _logger.debug(j)
            if j is None:
                _logger.warning('Failed to decode child value')
                continue

            load_count = self.__change_nginx_config.load(j)
            if load_count > 0:
                self.__added_nginx_nodes.add(child)
                _logger.debug('Added child [{}] to NGINX nodes'.format(child))

    def removed(self, children):
        _logger.debug('removed()')

        for child in children:
            _logger.debug('Child [{}]'.format(child))

            if child in self.__nginx_nodes:
                self.__removed_nginx_nodes.add(child)
