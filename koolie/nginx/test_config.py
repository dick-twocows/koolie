import logging

import koolie.nginx.config

_logger = logging.getLogger(__name__)

if __name__ == '__main__':
    load_config = koolie.nginx.config.LoadConfig()
    load_config.load('base_nginx_config.yaml')