import logging
import unittest

import koolie.nginx.config

_logger = logging.getLogger(__name__)


class TestLoad(unittest.TestCase):

    def test_load(self):
        load = koolie.nginx.config.Load()
        load.load('base_nginx_config.yaml')
        _logger.info(load)


if __name__ == '__main__':
    unittest.main()
