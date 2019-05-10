"""
Classes to handle config as an Item or subclass thereof.
"""

import abc
import logging
import re
import typing
import yaml

import koolie.tools.common

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

# Raw item type.
Raw_Item_Type = typing.Dict[str, typing.Any]

# Raw items type.
Raw_Items_Type = typing.List['Item']


class Item(abc.ABC):

    TYPE_KEY = 'type'

    NAME_KEY = 'name'

    TAG_KEY = 'tag'

    ID_PATTERN = re.compile('[a-zA-Z][a-zA-Z0-9_-]+')

    def __init__(self, **kwargs) -> None:
        super().__init__()

        if kwargs is None:
            self.__data = {}
        else:
            self.__data = kwargs

    def data(self) -> typing.Dict[str, object]:
        return self.__data

    def type(self) -> str:
        return self.data().get(Item.TYPE_KEY, '')

    def name(self) -> str:
        return self.data().get(Item.NAME_KEY, '')

    def tag(self) -> object:
        return self.data().get(Item.TAG_KEY, None)

    def fqn(self) -> str:
        return 'type[{}]name[{}]'.format(self.type(), self.name())

    def get(self, k: str, v: object = None) -> object:
        return self.data().get(k, v)

    def tokens(self) -> typing.Dict[str, str]:
        tokens = {}
        for k, v in self.data().items():
            tokens['{}__{}'.format(self.type(), k)] = v
        return tokens

    def __str__(self) -> str:
        return ', '.join('[{}]=[{}]'.format(k, v) for k, v in self.__data.items())


class Token(Item):

    VALUE_KEY = 'value'

    TOKEN_TYPE = 'koolie_token'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def value(self) -> object:
        return self.data().get(Token.VALUE_KEY)


class Items(object):

    """Base class for items."""

    def __init__(self, **kwargs) -> None:
        super().__init__()

        # The kwargs given at creation.
        self._kwargs = kwargs

        # The list of Items, which may or may not contain duplicates.
        self._items: Raw_Items_Type = list()

    def get_kwargs(self) -> typing.Dict[str, typing.Any]:
        return self._kwargs

    def get_items(self, fqn: str = None) -> Raw_Items_Type:
        """Return the raw items.
        If fqn is not None return a subset which matches the given fqn."""
        if fqn is None:
            return self._items
        subset: Raw_Items_Type = list()
        for raw_item in self._items:
            if raw_item.fqn() == fqn:
                subset.append(raw_item)
        return subset

    def clear_items(self) -> Raw_Items_Type:
        self._items.clear()
        return self._items

    def add_item(self, item: typing.Union[Item, Raw_Item_Type]) -> Raw_Items_Type:
        if isinstance(item, dict):
            item = Item(**item)
        assert isinstance(item, Item)
        self._items.append(item)
        return self._items


class ReadItems(Items):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def read(self, *args: typing.List[typing.Union[str]]):
        """Read the given list of files as YAML and append to __data."""
        _logger.debug('read()')
        for name in args:
            try:
                assert isinstance(name, str)
                with open(file=name, mode='r') as file:
                    raw = file.read()
                items = yaml.safe_load(raw)
                # If its not a list...
                assert isinstance(items, typing.List)
                _logger.debug('Read [{}] items.'.format(len(items)))
                for item in items:
                    self._items.append(item)
            except Exception as exception:
                koolie.tools.common.log_exception(exception, logger=_logger)


class Load(object):

    LOAD_KEY = 'load'

    LOAD_APPEND = 'append'

    LOAD_UNIQUE = 'unique'

    AdderSignature = typing.Callable[[typing.Type[Item]], None]

    def __init__(self, **kwargs) -> None:
        super().__init__()

        self.__kwargs = kwargs

        self.__keys: typing.Set[str] = set()

        self.__items: typing.List[typing.Type[Item]] = list()

        self.__creators = {
            Token.TOKEN_TYPE: Token
        }

        self.__adders: typing.Dict[str, Load.AdderSignature] = {
            Load.LOAD_APPEND: self.append,
            Load.LOAD_UNIQUE: self.unique
        }

    def keys(self) -> typing.Set[str]:
        return self.__keys

    def items(self) -> typing.List[typing.Type[Item]]:
        return self.__items

    def creators(self) -> typing.Dict[str, typing.Callable[..., Item]]:
        return self.__creators

    def creator_for(self, **kwargs) -> typing.Type[Item]:
        item: typing.Type[Item] = self.__creators.get(kwargs[Item.TYPE_KEY])
        if item is None:
            item = Item
            _logger.warning('Failed to get creator for [{}]'.format(kwargs))
        return item

    def adders(self) -> typing.Dict[str, AdderSignature]:
        return self.__adders

    def adder_for(self, item: typing.Type[Item]) -> AdderSignature:
        loader: Load.AdderSignature = self.adders().get(self.load_for(item))
        if loader is None:
            _logger.warning('Failed to get adder for [{}]'.format(item))
        return loader

    def load_for(self, item: typing.Type[Item]) -> str:
        load: str = item.get(Load.LOAD_KEY)
        if load is None:
            load = Load.LOAD_UNIQUE
            _logger.warning('Defaulting [{}] to [{}] for [{}]'.format(Load.LOAD_KEY, load, item.fqn()))
        return load

    def create(self, **kwargs) -> typing.Type[Item]:
        return self.creator_for(**kwargs)(**kwargs)

    def unique(self, base: typing.Type[Item]):
        if base.fqn() in self.keys():
            _logger.warning('Failed to add unique, FQN [{}] already exists'.format(base.fqn()))
            return
        self.keys().add(base.fqn())
        self.items().append(base)

    def append(self, base: typing.Type[Item]):
        if base.fqn() not in self.keys():
            self.keys().add(base.fqn())
        self.items().append(base)

    def add(self, item: typing.Type[Item]):
        adder: Load.AdderSignature = self.adder_for(item)
        if adder is None:
            _logger.warning('Failed to add, no adder for [{}]'.format(item))
            return
        adder(item)

    def load(self, *args: typing.List[typing.Union[str]]):
        _logger.debug('load()')
        for name in args:
            try:
                assert isinstance(name, str)
                _logger.info('Loading [{}]'.format(name))
                with open(file=name, mode='r') as file:
                    raw = file.read()
                items: typing.List[typing.Dict] = yaml.load(raw)
                _logger.info('[{}] items to load'.format(len(items)))
                for data in items:
                    try:
                        self.add(self.create(**data))
                    except Exception as exception:
                        _logger.warning('load() Item exception [{}]'.format(koolie.tools.common.decode_exception(exception, logger=_logger)))
            except Exception as exception:
                _logger.warning('load() File exception [{}]'.format(koolie.tools.common.decode_exception(exception, logger=_logger)))

    def __str__(self) -> str:
        return 'Items [{}]'.format(len(self.items()))


class Dump(object):

    def __init__(self, **kwargs) -> None:
        super().__init__()

        self.__kwargs = kwargs

        self.__dispatchers: typing.Dict[str, typing.Callable[[typing.Type[Item]], None]] = {}

    def null(self, item: Item):
        pass

    def dispatchers(self) -> typing.Dict[str, typing.Callable[[typing.Type[Item]], None]]:
        return self.__dispatchers

    def dispatcher_for(self, item: Item) -> typing.Callable[[typing.Type[Item]], None]:
        dumper = self.dispatchers().get(item.type())
        if dumper is None:
            dumper = self.null
            _logger.warning('Defaulting dispatcher to [{}] for type [{}]'.format(dumper, item.type()))
        return dumper

    def dump(self, load: Load):
        for item in load.items():
            self.dispatcher_for(item)(item)
