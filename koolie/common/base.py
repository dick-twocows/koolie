import abc
import logging
import re
import typing
import yaml

import koolie.tools.common

_logger = logging.getLogger(__name__)

TYPE_KEY = 'type'

NAME_KEY = 'name'

LOAD_KEY = 'load'

SOURCE_KEY = 'source'

TAG_KEY = 'tag'

VALUE_KEY = 'value'


LOAD_APPEND = 'append'

LOAD_UNIQUE = 'unique'


TOKEN_TYPE = 'koolie_token'


class Base(abc.ABC):

    ID_PATTERN = re.compile('[a-zA-Z0-9-]+')

    def __init__(self, **kwargs) -> None:
        super().__init__()

        if kwargs is None:
            self.__data = {}
        else:
            self.__data = kwargs

    def _data(self) -> typing.Dict[str, object]:
        return self.__data

    def type(self) -> str:
        return self._data().get(TYPE_KEY, '')

    def name(self) -> str:
        return self._data().get(NAME_KEY, '')

    def load(self) -> str:
        return self._data().get(LOAD_KEY, LOAD_UNIQUE)

    def source(self) -> str:
        return self._data().get(SOURCE_KEY, '')

    def tag(self) -> str:
        return self._data().get(TAG_KEY, '')

    def fqn(self) -> str:
        return '{}__{}'.format(self.type(), self.name())

    def tokens(self) -> typing.Dict[str, str]:
        tokens = {}
        for k, v in self._data().items():
            tokens['{}__{}'.format(self.type(), k)] = v
        return tokens

    def __str__(self) -> str:
        return ', '.join('[{}]=[{}]'.format(k, v) for k, v in self.__data.items())


class Token(Base):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def value(self, default: str = '') -> str:
        return self._data().get(VALUE_KEY, default)


class Load(object):

    LoaderSignature = typing.Callable[[typing.Type[Base]], None]

    def __init__(self) -> None:
        super().__init__()

        self.__keys: typing.Set[str] = set()

        self.__items: typing.List[typing.Type[Base]] = list()

        self.__creators = {
            TOKEN_TYPE: Token
        }

        self.__loaders: typing.Dict[str, Load.LoaderSignature] = {
            LOAD_APPEND: self.append,
            LOAD_UNIQUE: self.unique
        }

    def keys(self) -> typing.Set[str]:
        return self.__keys

    def items(self):
        return self.__items

    def creators(self) -> typing.Dict[str, typing.Callable[..., Base]]:
        return self.__creators

    def loaders(self) -> typing.Dict[str, typing.Callable[[typing.Type[Base]], None]]:
        return self.__loaders

    def loader_for(self, base: typing.Type[Base]) -> LoaderSignature:
        loader: Load.LoaderSignature = self.loaders().get(base.load())
        if loader is None:
            _logger.warning('Failed to get loader for [{}]'.format(base))
        return loader

    def create(self, **kwargs) -> typing.Type[Base]:
        return self.__creators.get(kwargs[TYPE_KEY], Base)

    def unique(self, base: typing.Type[Base]):
        _logger.debug('unique()')
        if base.fqn() in self.keys():
            _logger.warning('Failed to load unique, FQN [{}] already exists'.format(base.fqn()))
            return
        self.keys().add(base.fqn())
        self.items().append(base)

    def append(self, base: typing.Type[Base]):
        _logger.debug('append()')
        if base.fqn() not in self.keys():
            self.keys().add(base.fqn())
        self.items().append(base)

    def add(self, base: typing.Type[Base]):
        loader: Load.LoaderSignature = self.loader_for(base)
        if loader is not None:
            loader(base)

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
                        self.add(self.create(**data)(**data))
                    except Exception as exception:
                        _logger.warning('load() Item exception [{}]'.format(koolie.tools.common.decode_exception(exception, logger=_logger)))
            except Exception as exception:
                _logger.warning('load() File exception [{}]'.format(koolie.tools.common.decode_exception(exception, logger=_logger)))

    def __str__(self) -> str:
        return len(self.items())
