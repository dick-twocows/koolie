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
        return self.fqn()


class Token(Base):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

    def value(self, default: str = '') -> str:
        return self._data().get(VALUE_KEY, default)


class Load(object):

    def __init__(self) -> None:
        super().__init__()

        self.__items: typing.Dict[str, typing.List[Base]] = dict()

        self.__base_creator = dict()
        self.__base_creator[TOKEN_TYPE] = Token

        self.__add_dispatcher: typing.Dict[str, typing.Callable[[Base], None]] = dict()
        self.__add_dispatcher[LOAD_APPEND] = self.append
        self.__add_dispatcher[LOAD_UNIQUE] = self.unique

    def items(self):
        return self.__items

    def create(self, **kwargs):
        return self.__base_creator.get(kwargs[TYPE_KEY], Base)

    def unique(self, base: Base):
        _logger.debug('unique()')
        assert isinstance(base, Base)
        assert base.fqn() not in self.items().keys()
        self.items()[base.fqn()] = [base]

    def append(self, base: Base):
        _logger.debug('append_item()')
        assert isinstance(base, Base)
        if base.fqn() in self.items().keys():
            self.items()[base.fqn()].append(base)
        else:
            self.items()[base.fqn()] = [base]

    def add(self, base: Base):
        self.__add_dispatcher[base.load()](base)

    def load(self, *args: typing.List[typing.Union[str]]):
        _logger.debug('load()')
        for name in args:
            try:
                assert isinstance(name, str)
                _logger.debug('add_file() name=[{}]'.format(name))
                with open(file=name, mode='r') as file:
                    raw = file.read()
                items: typing.List[typing.Dict] = yaml.load(raw)
                for data in items:
                    try:
                        b = self.create(**data)
                        self.add(b(**data))
                    except Exception as exception:
                        _logger.warning('load() Item exception [{}]'.format(koolie.tools.common.decode_exception(exception)))
            except Exception as exception:
                _logger.warning('load() File exception [{}]'.format(koolie.tools.common.decode_exception(exception)))
