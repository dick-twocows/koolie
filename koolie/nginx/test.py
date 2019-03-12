import logging
import sys

import koolie.nginx.config
import koolie.nginx.load
import koolie.nginx.dump


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    config = koolie.nginx.config.Config()
    config.load('base_nginx_config.yaml', 'example_nginx_config.yaml')

    print(config)

    print(config.items())

    config.dump()

    # load = koolie.nginx.load.Load(config)
    # load.start()
    # load.load_file('example_nginx_config.yaml')
    # load.stop()
    # print(load)
    #
    # load.config().kwargs()[koolie.nginx.config.NGINX_DIRECTORY_KEY] = '/tmp/nginx/'
    #
    # dump = koolie.nginx.dump.Dump(config)
    # dump.start()
    # dump.dump()
    # dump.stop()
    # print(dump)
