import os
import typing
import uuid
from kubernetes import client, config, watch

LABELS = typing.Dict[str, str]

# Configs can be set in Configuration class directly or using helper utility
# config.load_kube_config(config_file=os.path.expanduser('~/YellowDog/k8s/clusters/oci/uk-london-1/new_tenancy/preprod/kubeconfig'))
#
# api = client.CoreV1Api()

# # print("Listing pods with their IPs:")
# # ret = v1.list_pod_for_all_namespaces(watch=False)
# # for i in ret.items:
# #     print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
#
# print("Listing nodes with their IPs:")
# node_list: client.V1NodeList = api.list_node(watch=False)
# node_item: client.V1Node
# for node_item in node_list.items:
#     metadata: client.V1ObjectMeta = node_item.metadata
#     print('{} {}'.format(metadata.uid, metadata.name))
#
# koolie_service_list: client.V1ServiceList = client.V1ServiceList(items=list())
# service_list: client.V1ServiceList = api.list_service_for_all_namespaces(watch=False)
# service_item: client.V1Service
# for service_item in service_list.items:
#     metadata: client.V1ObjectMeta = service_item.metadata
#     print('{} {} {}'.format(metadata.uid, metadata.name, metadata.namespace))
#     labels: LABELS = metadata.labels
#     if 'koolie_type' in labels.keys():
#         koolie_service_list.items.append(service_item)
#
# print('Koolie services {}'.format(len(koolie_service_list.items)))
#
# for node_item in node_list.items:
#     metadata: client.V1ObjectMeta = node_item.metadata
#     labels = metadata.labels
#     if 'koolie_type' in labels.keys():
#         pass
#     else:
#         print('No koolie_type')


def create_service():
    config.load_kube_config(
        config_file=os.path.expanduser('~/YellowDog/k8s/clusters/oci/uk-london-1/new_tenancy/preprod/kubeconfig')
    )

    api = client.CoreV1Api()

    https = client.V1ServicePort(name='https', port=443, target_port=443, protocol='TCP')

    metadata = client.V1ObjectMeta(name='fred', labels=dict())
    metadata.labels['app.kubernetes.io/name'] = 'fred'
    metadata.labels['app.kubernetes.io/instance'] = 'Dynamic'
    metadata.labels['app.kubernetes.io/managed-by'] = 'Koolie'

    spec = client.V1ServiceSpec(
        type='NodePort',
        ports=[https],
        selector={'app.kubernetes.io/name': 'fred'}
    )

    service = client.V1Service()
    service.metadata = metadata
    service.spec = spec

    api.create_namespaced_service('dev', service)


def pods():
    config.load_kube_config(
        config_file=os.path.expanduser('~/YellowDog/k8s/clusters/oci/uk-london-1/new_tenancy/preprod/kubeconfig'))

    api = client.CoreV1Api()

    pod_list: client.V1PodList = api.list_pod_for_all_namespaces(watch=False, label_selector='app.kubernetes.io/instance = dev, app.kubernetes.io/name = objectstore')
    pod_item: client.V1Pod
    for pod_item in pod_list.items:
        print('{} {}'.format(pod_item.metadata.uid, pod_item.metadata.labels))
        service_list: client.V1ServiceList = api.list_service_for_all_namespaces(
            watch=False,
            label_selector='app.kubernetes.io/instance = dev, koolie/pod_uid = {}'.format(pod_item.metadata.uid)
        )
        print('{}'.format(len(service_list.items)))
        service_item: client.V1Service
        for service_item in service_list.items:
            print('{}'.format(service_item.metadata.uid))


def uid_label():
    config.load_kube_config(
        config_file=os.path.expanduser('~/YellowDog/k8s/clusters/oci/uk-london-1/new_tenancy/preprod/kubeconfig'))

    api = client.CoreV1Api()

    pod_list = api.list_namespaced_pod('dev', watch=False, label_selector='app.kubernetes.io/instance = dev, app.kubernetes.io/name = objectstore')
    pod_item: client.V1Pod
    for pod_item in pod_list.items:
        uid = pod_item.metadata.labels.get('koolie.yellowdog.co/uid')
        print(uid)
        if uid is None:
            body = {'metadata': {'labels': {'koolie.yellowdog.co/uid': pod_item.metadata.uid}}}
            returned_pod: client.V1Pod = api.patch_namespaced_pod(name=pod_item.metadata.name, namespace='dev', body=body)
            print(returned_pod)


def watch_pods():
    config.load_kube_config(
        config_file=os.path.expanduser('~/YellowDog/k8s/clusters/oci/uk-london-1/new_tenancy/preprod/kubeconfig'))

    api = client.CoreV1Api()

    w = watch.Watch()
    for e in w.stream(api.list_namespaced_pod, 'dev', label_selector='app.kubernetes.io/instance = dev, app.kubernetes.io/name = objectstore'):
        type = e['type']
        print(type)
        object: client.V1Namespace = e['object']  # object is one of type return_type
        print(object.metadata.name)
        # print(object)
        raw_object = e['raw_object']  # raw_object is a dict
        # print(raw_object)

        # w.stop()


def watch_namespace():
    config.load_kube_config(
        config_file=os.path.expanduser('~/YellowDog/k8s/clusters/oci/uk-london-1/new_tenancy/preprod/kubeconfig'))

    api = client.CoreV1Api()

    w = watch.Watch()
    for e in w.stream(api.list_namespace):
        type = e['type']
        print(type)
        object: client.V1Namespace = e['object']  # object is one of type return_type
        print(object.metadata.name)
        # print(object)
        raw_object = e['raw_object']  # raw_object is a dict
        # print(raw_object)

        # w.stop()


if __name__ == '__main__':
    # pods()
    # create_service()
    # uid_label()
    # watch_namespace()
    watch_pods()
