import koolie.tools.abstract_service

import kubernetes
import logging
import os
import sys

_logger = logging.getLogger(__name__)


class UIDLabel(koolie.tools.abstract_service.QueueService):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        kubernetes.config.load_kube_config(
            config_file=os.path.expanduser('~/YellowDog/k8s/clusters/oci/uk-london-1/new_tenancy/preprod/kubeconfig')
        )

        self._api = kubernetes.client.CoreV1Api()

    def producer(self):
        _logger.debug('producer()')
        w = kubernetes.watch.Watch()
        pods = w.stream(
            self._api.list_namespaced_pod,
            'dev',
            label_selector='app.kubernetes.io/instance = dev, app.kubernetes.io/name = objectstore'
        )
        while self.state() is koolie.tools.abstract_service.ServiceState.STARTED and self.pending_state() is not koolie.tools.abstract_service.ServiceState.STOPPED:
            self._queue.put(next(pods, ))
        _logger.debug('producer() END')

    def item(self, item):
        print(type(item))


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

    _logger.info('PID is [{}].'.format(os.getpid()))

    uid_label = UIDLabel()
    uid_label.duration(5)
