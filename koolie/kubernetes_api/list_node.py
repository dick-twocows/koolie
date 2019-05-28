import os
from kubernetes import client, config

# Configs can be set in Configuration class directly or using helper utility
config.load_kube_config(config_file=os.path.expanduser('~/YellowDog/k8s/clusters/oci/uk-london-1/new_tenancy/preprod/kubeconfig'))

v1 = client.CoreV1Api()

# print("Listing pods with their IPs:")
# ret = v1.list_pod_for_all_namespaces(watch=False)
# for i in ret.items:
#     print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))

print("Listing nodes with their IPs:")
ret = v1.list_node(watch=False)
for i in ret.items:
    # print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))
    print(i)