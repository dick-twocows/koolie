import logging
import string

_logger = logging.getLogger(__name__)


def get_from_kwargs(k: str, v: str, **kwargs):
    r = None
    if k in kwargs.keys():
        _logger.debug('Returning [{}]'.format(k))
        r = kwargs.get(k)
    else:
        _logger.debug('Defaulting [{}] tp [{}]'.format(k, v))
        r = v
    return r


def substitute(source, **kwargs):
    template = string.Template(source)
    result = template.substitute(kwargs)
    _logger.debug('Source [{}]\nResult [{}]'.format(source, result))
    return result
