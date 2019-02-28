import argparse
import koolie.pod_api.pod_status
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
    _logger.info('Signalled to exit...')


signal.signal(signal.SIGINT, signal_to_exit)
signal.signal(signal.SIGTERM, signal_to_exit)


# Koolie variables.

KOOLIE_STATUS_TYPE = 'type'
KOOLIE_STATUS_TAG = 'tag'

NGINX_DIRECTORY = '/etc/ngninx/'
NGINX_SERVERS_DIRECTORY = '{}servers/'.format(NGINX_DIRECTORY)
NGINX_UPSTREAMS_DIRECTORY = '{}upstreams/'.format(NGINX_DIRECTORY)

ZOOKEEPER_HOSTS = 'localhost:2181'
ZOOKEEPER_PODS = '/koolie/pods/'
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


def pod_status(args):
    koolie.pod_api.pod_status.PodStatus(args).start()


def zookeeper_test(args):
    zookeeper = koolie.zookeeper_api.using_kazoo.ZooKeeper(args)
    try:
        zookeeper.open()
        zookeeper.close()
    except Exception as exception:
        _logger.warning('Test failed [{}]'.format(exception))


def zookeeper_watch(args):

    def pod_status(child: dict):
        _logger.info('add [{}]'.format(child))

    add = dict()
    add[koolie.pod_api.pod_status.STATUS_TYPE] = pod_status

    config = vars(args)
    config[koolie.zookeeper_api.node_watch.StatusTypeWatch.ADD] = add

    watch = koolie.zookeeper_api.node_watch.StatusTypeWatch(args)
    try:
        try:
            watch.start()
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

# Pod

pod_parser = subparsers.add_parser('pod', help='Pod')
pod_parser.set_defaults(func=suffix_help, help_prefix='pod')

pod_subparsers = pod_parser.add_subparsers()

# pod status

pod_status_parser = pod_subparsers.add_parser('status', help='Status')
pod_status_parser.add_argument('--zookeeper-hosts', type=str, default=default('ZOOKEEPER_HOSTS', ZOOKEEPER_HOSTS))
pod_status_parser.set_defaults(func=pod_status)

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

# nginx status zookeeper

nginx_consume_zookeeper_parser = nginx_consume_subparsers.add_parser('zookeeper', help='ZooKeeper')
nginx_consume_zookeeper_parser.add_argument('--zookeeper-hosts', type=str, default=default('ZOOKEEPER_HOSTS', ZOOKEEPER_HOSTS))
nginx_consume_zookeeper_parser.add_argument('--zookeeper-kubernetes-pods', type=str, default=default('ZOOKEEPER_KUBERNETES_PODS', ZOOKEEPER_PODS))
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
    args: argparse.Namespace = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=args.logging_level)

    try:
        args.func(args)
    except AttributeError as attribute_error:
        print('{}\n{} ...'.format(attribute_error, args))

    print('OK', file=sys.stdout, flush=True)
