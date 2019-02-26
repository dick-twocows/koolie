import argparse
import koolie.nginx.zookeeper
import koolie.version
import koolie.zookeeper_api.using_kazoo
import koolie.zookeeper_api.node_watch
import logging
import os
import signal
import sys
import time

_logger = logging.getLogger(__name__)

signalled_to_exit = False


def signal_to_exit(signum, frame):
    koolie.go.signalled_to_exit = True
    print(koolie.go.signalled_to_exit)
    print('Waiting to exit...', file=sys.stdout, flush=True)


signal.signal(signal.SIGINT, signal_to_exit)
signal.signal(signal.SIGTERM, signal_to_exit)


NGINX_DIRECTORY = '/etc/ngninx/'
NGINX_SERVERS_DIRECTORY = '{}servers/'.format(NGINX_DIRECTORY)
NGINX_UPSTREAMS_DIRECTORY = '{}upstreams/'.format(NGINX_DIRECTORY)

ZOOKEEPER_HOSTS = 'zookeeper.default.svc.cluster.local'
ZOOKEEPER_KUBERNETES_PODS = '/koolie/pods/'
ZOOKEEPER_ROOT_NODE = '/'


def default(name, value) -> str:
    return os.getenv(name, value)


def suffix_help(args):
    parser.parse_args(args.help_prefix.split().append('--help'))


def sleep(args):
    while True:
        time.sleep(10)
        if signalled_to_exit:
            break


def nginx_consume_zookeeper(args):
    koolie.nginx.zookeeper.Consume().args(args).start()


def zookeeper_test(args):
    zookeeper = koolie.zookeeper_api.using_kazoo.ZooKeeper(args)
    try:
        zookeeper.open()
        zookeeper.close()
    except Exception as exception:
        _logger.warning('Test failed [{}]'.format(exception))


def zookeeper_watch(args):
    watch = koolie.zookeeper_api.node_watch.EchoNodeWatch(args)
    try:
        watch.start()
        try:
            while True:
                time.sleep(10)
                print('.')
        finally:
            watch.stop()
    except Exception as exception:
        _logger.warning('Test failed [{}]'.format(exception))


parser = argparse.ArgumentParser(description='Koolie CLI')
parser.set_defaults(func=suffix_help, help_prefix='')

parser.add_argument('--logging-level', type=str, help='Logging level', default=default('LOGGING_LEVEL', logging.getLevelName(logging.DEBUG)))

parser.add_argument('--version', action='store_true')

subparsers = parser.add_subparsers()

# sleep

sleep_parser = subparsers.add_parser('sleep', help='Wait')
sleep_parser.set_defaults(func=sleep)

# nginx

nginx_parser = subparsers.add_parser('nginx', help='NGINX')
nginx_parser.set_defaults(func=suffix_help, help_prefix='nginx')

nginx_parser.add_argument('--nginx-directory', type=str, default=default('NGINX_DIRECTORY', NGINX_DIRECTORY))
nginx_parser.add_argument('--nginx-servers-directory', type=str, default=default('NGINX_SERVERS_DIRECTORY', NGINX_SERVERS_DIRECTORY))
nginx_parser.add_argument('--nginx-upstreams-directory', type=str, default=default('NGINX_UPSTREAMS_DIRECTORY', NGINX_UPSTREAMS_DIRECTORY))

nginx_subparsers = nginx_parser.add_subparsers()

# nginx consume

nginx_consume_parser = nginx_subparsers.add_parser('consume', help='Consume')
nginx_consume_parser.set_defaults(func=suffix_help, help_prefix='nginx consume')

nginx_consume_subparsers = nginx_consume_parser.add_subparsers()

# nginx watch zookeeper

nginx_consume_zookeeper_parser = nginx_consume_subparsers.add_parser('zookeeper', help='ZooKeeper')
nginx_consume_zookeeper_parser.add_argument('--zookeeper-hosts', type=str, default=default('ZOOKEEPER_HOSTS', ZOOKEEPER_HOSTS))
nginx_consume_zookeeper_parser.add_argument('--zookeeper-kubernetes-pods', type=str, default=default('ZOOKEEPER_KUBERNETES_PODS', ZOOKEEPER_KUBERNETES_PODS))
nginx_consume_zookeeper_parser.set_defaults(func=nginx_consume_zookeeper)

# ZooKeeper

zookeeper_parser = subparsers.add_parser('zookeeper', help='ZooKeeper')
zookeeper_parser.add_argument('--zookeeper-hosts', type=str, default=default('ZOOKEEPER_HOSTS', ZOOKEEPER_HOSTS))
zookeeper_parser.set_defaults(func=suffix_help, hep_prefix='zookeeper')

zookeeper_subparsers = zookeeper_parser.add_subparsers()

zookeeper_test_parser = zookeeper_subparsers.add_parser('test', help='Test')
zookeeper_test_parser.set_defaults(func=zookeeper_test)

zookeeper_watch_parser = zookeeper_subparsers.add_parser('watch', help='Watch')
zookeeper_watch_parser.add_argument('--zookeeper-node-path', type=str, default=default('ZOOKEEPER_NODE_PATH', ZOOKEEPER_ROOT_NODE))
zookeeper_watch_parser.set_defaults(func=zookeeper_watch)


if __name__ == '__main__':
    # args = parser.parse_args(args='nginx watch zookeeper --zookeeper-hosts=foo'.split())
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=args.logging_level)

    try:
        args.func(args)
    except AttributeError as attribute_error:
        print('{}\n{} ...'.format(attribute_error, args))

    print('OK', file=sys.stdout, flush=True)
