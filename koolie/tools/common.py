import logging
import string

_logger = logging.getLogger(__name__)


def get_from_kwargs(k: str, v: str = None, **kwargs):
    r = None
    if k in kwargs.keys():
        _logger.debug('Returning [{}]'.format(k))
        r = kwargs.get(k)
    else:
        _logger.debug('Defaulting [{}] tp [{}]'.format(k, v))
        r = v
    return r


def if_none(v: object, o: object) -> object:
    return o if v is None else v


def substitute(source, **kwargs):
    template = string.Template(source)
    result = template.substitute(kwargs)
    _logger.debug('Source [{}]\nResult [{}]'.format(source, result))
    return result
