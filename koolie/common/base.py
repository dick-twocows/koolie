import abc
import re
import typing

TYPE_KEY = 'type'

NAME_KEY = 'name'

SOURCE_KEY = 'source'

TAG_KEY = 'tag'


class Base(abc.ABC):

    ID_PATTERN = re.compile('[a-zA-Z0-9-]+')

    def __init__(self, data: typing.Dict[str, object] = None) -> None:
        super().__init__()

        if data is None:
            self.__data = {}
        else:
            assert isinstance(data, typing.Dict)
            self.__data = data

    def data(self) -> typing.Dict[str, object]:
        return self.__data

    def type(self) -> str:
        return self.data().get(TYPE_KEY, '')

    def name(self) -> str:
        return self.data().get(NAME_KEY, '')

    def fqn(self) -> str:
        """
        The FQN which by default is {type__name}
        :return: str
        """
        return '{}.{}'.format(self.type(), self.name())

    def source(self) -> str:
        return self.data().get(SOURCE_KEY, '')

    def tag(self) -> str:
        return self.data().self(TAG_KEY, '')

    def tokens(self) -> typing.Dict[str, str]:
        tokens = {}
        for k, v in self.data().items():
            tokens['{}__{}'.format(self.type(), k)] = v
        return tokens

    def __str__(self) -> str:
        return self.fqn()
