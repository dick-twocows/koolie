import abc
import logging
import re
import typing
import yaml

import koolie.tools.common

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

Items_List = typing.List[typing.Any]


class Item(abc.ABC):

    TYPE_KEY = 'type'

    NAME_KEY = 'name'

    TAG_KEY_PREFIX = 'tag'

    ID_PATTERN = re.compile('[a-zA-Z][a-zA-Z0-9_-]+')

    def __init__(self, **kwargs) -> None:
        super().__init__()

        if kwargs is None:
            self._data = {}
        else:
            self._data = kwargs

    def data(self) -> typing.Dict[str, object]:
        return self._data

    def type(self) -> str:
        return self.data().get(Item.TYPE_KEY)

    def name(self) -> str:
        return self.data().get(Item.NAME_KEY)

    def tag(self) -> object:
        return self.data().get(Item.TAG_KEY_PREFIX)

    def tags(self) -> typing.List[str]:
        """Return all the data items where the key starts with 'tag'."""
        tags: dict()
        for k, v in self.data().items():
            if k.startswith(Item.TAG_KEY_PREFIX):
                tags[k] = v
        return tags

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
        return ', '.join('[{}]=[{}]'.format(k, v) for k, v in self._data.items())


class Token(Item):

    """Extend an Item with the concept of a Token.
    eg Add a value() method."""

    VALUE_KEY = 'value'

    TOKEN_TYPE = 'koolie_token'

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def value(self) -> object:
        return self.data().get(Token.VALUE_KEY)


class Items(object):

    LOAD_KEY = 'load'

    LOAD_APPEND = 'APPEND'

    LOAD_UNIQUE = 'UNIQUE'

    TAG_LOAD_SOURCE_KEY = 'tagLoadSource'

    LoaderSignature = typing.Callable[[typing.Type[Item]], None]

    ITEM_FILES_KEY: 'item_files'

    def __init__(self, **kwargs) -> None:
        super().__init__()

        self._kwargs = kwargs

        # List of Items.
        # The implied structure depends on how the Items were loaded, eg RawItems(), StrictItems().
        self._items: Items_List = list()

        # FQN keys.
        self._fqns: typing.Set[str] = set()

        # Map of Item creators. Use 'Item.type' as key.
        self._creators = {
            Token.TOKEN_TYPE: Token
        }

        # Map of Item loaders. Use 'Item.load' as key.
        self._loaders: typing.Dict[str, Items.LoaderSignature] = {
            Items.LOAD_APPEND: self.load_item_append,
            Items.LOAD_UNIQUE: self.load_item_unique
        }

    def get_kwargs(self) -> typing.Dict[str, typing.Any]:
        """The kwargs given when this Items was created."""
        return self._kwargs

    def get_items(self) -> Items_List:
        """The current list of Items."""
        return self._items

    def clear_items(self) -> Items_List:
        self._items.clear()
        return self._items

    def debug(self):
        print('FQNs;')
        for item in self.get_fqns():
            print(item)
        print('Items;')
        for item in self.get_items():
            print(item)

    def get_fqns(self) -> typing.Set[str]:
        return self._fqns

    def get_creators(self) -> typing.Dict[str, typing.Callable[..., Item]]:
        """Return the Item creators."""
        return self._creators

    def get_item_creator(self, **kwargs) -> typing.Type[Item]:
        """Get the item creator for the given 'kwargs'.
        Return None is no item creator defined."""
        return self._creators.get(kwargs[Item.TYPE_KEY])

    def create_item(self, **kwargs) -> typing.Type[Item]:
        """Create an Item with the given **kwargs."""
        return self.get_item_creator(**kwargs)(**kwargs)

    def get_item_loaders(self) -> typing.Dict[str, LoaderSignature]:
        """Return the loaders."""
        return self._loaders

    def get_item_loader(self, item: typing.Type[Item]) -> LoaderSignature:
        """Get the Loader for the given Item."""
        loader: Items.LoaderSignature = self.get_item_loaders().get(self.load_for(item))
        if loader is None:
            _logger.warning('Failed to get adder for [{}]'.format(item))
        return loader

    def load_item_append(self, item: typing.Type[Item]) -> typing.Type[Item]:
        try:
            self.get_fqns().add(item.fqn())
            self.get_items().append(item)
            return item
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
            return None

    def load_item_unique(self, item: typing.Type[Item]) -> typing.Type[Item]:
        try:
            if item.fqn() in self.get_fqns():
                _logger.warning('Failed to add unique, FQN [{}] already exists'.format(item.fqn()))
                return
            self.get_fqns().add(item.fqn())
            self.get_items().append(item)
            return item
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
            return None

    def load_item(self, item: Item):
        """Called by load() for each 'Item'.
        By default do nothing."""
        pass

    def load_items(self, items: typing.List[typing.Dict]):
        _logger.debug('load_items()')
        try:
            assert isinstance(items, list)
            for item in items:
                try:
                    assert isinstance(item, dict)
                    self.load_item(self.create_item(**item))
                except Exception as exception:
                    koolie.tools.common.log_exception(exception, logger=_logger)
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)

    def load_file(self, name: str):
        _logger.debug('load_file()')
        try:
            assert isinstance(name, str)
            with open(file=name, mode='r') as file:
                raw = file.read()
            self.load_items(yaml.safe_load(raw))
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)

    def load(self, *args: typing.List[typing.Union[str]]):
        """Load the given YAML files."""
        _logger.debug('load()')
        for name in args:
            try:
                assert isinstance(name, str)
                self.load_file(name)
            except Exception as exception:
                koolie.tools.common.log_exception(exception, logger=_logger)


class RawItems(Items):

    """Load items by appending them to the list of items.
    The result is a list of items as loaded.
    The Item.load value is not used."""

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def get_item_creator(self, **kwargs) -> typing.Type[Item]:
        """Return Item as we just need a raw Item."""
        return Item

    def load_item(self, item: typing.Type[Item]) -> Item:
        """Append the given 'Item', ie ignore the defined 'Item.load' value."""
        try:
            assert isinstance(item, Item)
            self.load_item_append(item)
            return item
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
            return None


class StrictItems(Items):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def load_item(self, item: Item):
        """Load the given 'Item' using the defined 'Item.load' value."""
        try:
            assert isinstance(item, Item)
            self.get_item_loaders()[item[Items.LOAD_KEY]](item)
        except Exception as exception:
            koolie.tools.common.log_exception(exception, logger=_logger)
