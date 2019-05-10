import linecache
import logging
import os
import string
import sys
import traceback

_logger = logging.getLogger(__name__)


def get_from_kwargs(k: str, v: str = None, **kwargs):
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


def safe_substitute(source, **kwargs):
    template = string.Template(source)
    result = template.safe_substitute(kwargs)
    _logger.debug('Source [{}]\nResult [{}]'.format(source, result))
    return result


def ensure_directory(path: str):
    os.makedirs(path, exist_ok=True)


def clear_directory(path: str):
    _logger.debug('clear_directory(path=[{}])'.format(path))
    for folderName, sub_folders, file_names in os.walk(path):
        print('The current folder is ' + folderName)

        for sub_folder in sub_folders:
            print('SUBFOLDER OF ' + folderName + ': ' + sub_folder)
        for file_name in file_names:
            print('FILE INSIDE ' + folderName + ': ' + file_name)

        # print('')


def decode_exception(exception: Exception, **kwargs) -> str:
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)

    logger: logging.Logger = kwargs.get('logger')
    if logger is None:
        return '{}'.format(exception)
    else:
        if logger.isEnabledFor(logging.DEBUG):
            return '{}\n{}'.format(exception, traceback.format_exc())
        else:
            return '{}'.format(exception)


def log_exception(exception: Exception, **kwargs):
    """Log the given exception and kwargs.
    If an exception occurs log a CATCH-22 warning with this exception and the given exception, then run away..."""
    try:
        kwargs.get('logger', _logger).warning(
            '{}\n{}\n{}'.format(
                exception,
                '\n'.join(['{}: {}'.format(key, value) for key, value in kwargs.items()]),
                traceback.format_exc()
            )
        )
    except Exception as catch_22:
        _logger.warning('CATCH-22 [{}] logging exception [{}].'.format(catch_22, exception))
