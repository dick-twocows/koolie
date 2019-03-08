import logging
import os
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


def safe_substitute(source, **kwargs):
    template = string.Template(source)
    result = template.safe_substitute(kwargs)
    _logger.debug('Source [{}]\nResult [{}]'.format(source, result))
    return result


def clear_directory(path: str):
    _logger.debug('clear_directory(path=[{}])'.format(path))
    for folderName, sub_folders, file_names in os.walk(path):
        print('The current folder is ' + folderName)

        for sub_folder in sub_folders:
            print('SUBFOLDER OF ' + folderName + ': ' + sub_folder)
        for file_name in file_names:
            print('FILE INSIDE ' + folderName + ': ' + file_name)

        # print('')
