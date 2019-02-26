import argparse
import koolie.nginx.zookeeper
import koolie.version
import logging
import os
import signal
import sys
import time

_logger = logging.getLogger(__name__)

signalled_to_exit = False


def signal_to_exit(signum, frame):
    global signalled_to_exit
    signalled_to_exit = True
    print('Waiting to exit...', file=sys.stdout, flush=True)


signal.signal(signal.SIGINT, signal_to_exit)
signal.signal(signal.SIGTERM, signal_to_exit)


NGINX_DIRECTORY = '/etc/ngninx/'
NGINX_SERVERS_DIRECTORY = '{}servers/'.format(NGINX_DIRECTORY)
NGINX_UPSTREAMS_DIRECTORY = '{}upstreams/'.format(NGINX_DIRECTORY)

ZOOKEEPER_HOSTS = 'zookeeper.default.svc.cluster.local'
ZOOKEEPER_KUBERNETES_PODS = '/koolie/pods/'


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


if __name__ == '__main__':
    # args = parser.parse_args(args='nginx watch zookeeper --zookeeper-hosts=foo'.split())
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=args.logging_level)

    try:
        args.func(args)
    except AttributeError as attribute_error:
        print('{}\n{} ...'.format(attribute_error, args))

    print('OK', file=sys.stdout, flush=True)
