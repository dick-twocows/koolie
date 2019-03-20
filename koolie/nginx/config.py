import collections
import logging
import sys
import typing

import koolie.config.item

_logger = logging.getLogger(__name__)


class Item(koolie.config.item.Item):

    CONFIG_KEY = 'config'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def config(self) -> typing.List[str]:
        return self.data().get(Item.CONFIG_KEY)


class Root(Item):

    ROOT_TYPE = 'nginx_root'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


class Server(Item):

    SERVER_TYPE = 'nginx_server'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)


class Location(Item):

    LOCATION_TYPE = 'nginx_location'

    SERVER_KEY = 'server'

    MATCH_MODIFIER_KEY = ' match_modifier'

    LOCATION_MATCH_KEY = 'location_match'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def fqn(self) -> str:
        return '{}server[{}]'.format(super().fqn(), self.server())

    def server(self) -> str:
        return self.data().get(Location.SERVER_KEY)

    def match_modifier(self) -> str:
        return self.data().get(Location.MATCH_MODIFIER_KEY)

    def location_match(self) -> str:
        return self.data().get(Location.LOCATION_MATCH_KEY)


class Load(koolie.config.item.Load):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.creators().update(
            {
                Root.ROOT_TYPE: Root,
                Server.SERVER_TYPE: Server,
                Location.LOCATION_TYPE: Location
            }
        )


class Dump(koolie.config.item.Dump):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.dispatchers().update(
            {
                Server.SERVER_TYPE: self.item
            }
        )

        self.__configs = dict()

    def configs(self):
        return self.__configs

    def item(self, item: Item):
        config = self.configs().get(item.fqn())
        if config is None:
            ntuple = collections.namedtuple('Config', ['item', 'config'])
            ntuple.item = item
            ntuple.config = item.config()
            self.configs()[item.fqn()] = ntuple
        else:
            config.config = '{}\n\n{}'.format(config.config, item.config)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    load = Load()
    load.load('base_nginx_config.yaml')
    _logger.info(load.items())
    dump = Dump()
    dump.dump(load)
    _logger.info('\n'.join(config.config for config in dump.configs().values()))