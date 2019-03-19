import logging
import sys

import koolie.common.base

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)


class Load(koolie.common.base.Load):

    def __init__(self) -> None:
        super().__init__()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    load: Load = Load()
    load.load('base_nginx_config.yaml')
