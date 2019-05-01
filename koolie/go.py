import argparse
import koolie.pod_api.pod_status
import koolie.nginx.zookeeper
import koolie.version
import koolie.zookeeper_api.koolie_zookeeper
import koolie.zookeeper_api.koolie_node_watch
import logging
import os
import sys
import time

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
_logger = logging.getLogger(__name__)

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


def nginx_consume_zookeeper(**kwargs):
    koolie.nginx.zookeeper.Consume(**kwargs).start()


def pod_status(**kwargs):
    _logger.debug('kwargs [{}]'.format(kwargs))
    koolie.pod_api.pod_status.PushStatus(**kwargs).start()


def pod_push(**kwargs):
    _logger.debug('pod_push-config({})'.format(kwargs))
    koolie.pod_api.pod_status.PushConfig(**kwargs).start()


def zookeeper_test(args):
    zookeeper = koolie.zookeeper_api.koolie_zookeeper.UsingKazoo(args)
    try:
        zookeeper.start()
        zookeeper.stop()
    except Exception as exception:
        _logger.warning('Test failed [{}]'.format(exception))


def zookeeper_watch(**kwargs):

    def pod_status(child: dict):
        _logger.info('add [{}]'.format(child))

    add = dict()
    add[koolie.pod_api.pod_status.STATUS_TYPE] = pod_status

    kwargs[koolie.zookeeper_api.koolie_node_watch.StatusTypeWatch.ADD] = add

    watch = koolie.zookeeper_api.koolie_node_watch.StatusTypeWatch(**kwargs)
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
pod_status_parser.add_argument('--config-files', type=str, nargs='*')
pod_status_parser.set_defaults(func=pod_status)

# nginx

nginx_parser = subparsers.add_parser('nginx', help='NGINX')
nginx_parser.set_defaults(func=suffix_help, help_prefix='nginx')

nginx_parser.add_argument('--nginx-directory', type=str)
nginx_parser.add_argument('--nginx-servers-directory', type=str)
nginx_parser.add_argument('--nginx-upstreams-directory', type=str)

nginx_subparsers = nginx_parser.add_subparsers()

# nginx consume

nginx_consume_parser = nginx_subparsers.add_parser('consume', help='Consume')
nginx_consume_parser.set_defaults(func=suffix_help, help_prefix='nginx consume')

nginx_consume_subparsers = nginx_consume_parser.add_subparsers()

# nginx status zookeeper

nginx_consume_zookeeper_parser = nginx_consume_subparsers.add_parser('zookeeper', help='ZooKeeper')
nginx_consume_zookeeper_parser.add_argument('--zookeeper-hosts', type=str, default=default('ZOOKEEPER_HOSTS', ZOOKEEPER_HOSTS))
nginx_consume_zookeeper_parser.add_argument('--zookeeper-kubernetes-pods', type=str, default=default('ZOOKEEPER_KUBERNETES_PODS', ZOOKEEPER_PODS))
nginx_consume_zookeeper_parser.add_argument('--zookeeper-node-path', type=str, default=default('ZOOKEEPER_NODE_PATH', ZOOKEEPER_ROOT_NODE))
nginx_consume_zookeeper_parser.add_argument('--config-load-file', type=str, nargs='*')
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

    _logger.info('Loading environment variables')
    kwargs = dict()
    # Add in all the environment variables.
    for k, v in os.environ.items():
        kwargs['os_environ_{}'.format(k.replace('-', '_').lower())] = v

    _logger.info('Parsing CLI arguments')
    args = parser.parse_args()
    cli_args = dict(vars(args))

    _logger.info('Merging CLI arguments')
    for k in cli_args.keys():
        if k in kwargs.keys():
            _logger.warning('Overriding [{}] [{}] from CLI [{}]'.format(k, kwargs.get(k), cli_args.get(k)))
        kwargs[k] = cli_args[k]

    logging.basicConfig(level=kwargs['logging_level'])

    # # Take the unknowns and for any --k=v entries add to the kwargs.
    # # This isn't fool proof but it is defined.
    # values = list()
    # for arg in unknowns:
    #     if arg.startswith('--'):
    #         if arg.index('=') > 0:
    #             # Strip the leading '--' and replace '-' with '_' as per argparse.
    #             # eg --some-key=value would result in kwargs['some_key'] = value
    #             kwargs[arg[2:arg.index('=')].strip().replace('-', '_')] = arg[arg.index('=') + 1:].strip()
    #         else:
    #             # Replace the '-' with '_' and set the value to True as per argparse.
    #             kwargs[arg[2:].replace('-', '_')] = True
    #     else:
    #         # _logger.warning('Skipping [{}]'.format(arg))
    #         values.append(arg)
    # # Add a values key.
    # kwargs['values'] = values

    # Dump the kwargs for sanity.
    for k in sorted(kwargs.keys()):
        _logger.info('{} = [{}]'.format(k, kwargs[k]))

    try:
        args.func(**kwargs)
    except AttributeError as attribute_error:
        print('{}\n{} ...'.format(attribute_error, args))

    print('OK', file=sys.stdout, flush=True)
