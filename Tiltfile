update_settings(max_parallel_updates=6)

docker_build('migration',
             context='.',
             dockerfile='./go/Dockerfile.migration'
)

docker_build('coordinator',
             context='.',
             dockerfile='./go/Dockerfile'
)

docker_build('server',
             context='.',
             dockerfile='./Dockerfile',
)

docker_build('worker',
             context='.',
             dockerfile='./rust/worker/Dockerfile'
)

# Actual chroma
k8s_yaml(helm('k8s/distributed-chroma',
              namespace='chroma',
              values=['k8s/distributed-chroma/values.yaml']
              ))

# Extra stuff to make debugging and testing easier
k8s_yaml('k8s/test/coordinator_service.yaml')
k8s_yaml('k8s/test/minio.yaml')
k8s_yaml('k8s/test/pulsar_service.yaml')
k8s_yaml('k8s/test/segment_server_service.yaml')
k8s_yaml('k8s/test/test_memberlist_cr.yaml')

# Lots of things assume the cluster is in a basic state, so we need to get it
# into a basic state before we can start deploying things.
k8s_resource(
  objects=['chroma:Namespace',
           'worker-memberlist-readerwriter:ClusterRole',
           'pod-watcher:Role',
           'coordinator-serviceaccount-rolebinding:RoleBinding',
           'segmentserver-serviceaccount-rolebinding:RoleBinding',
           'memberlists.chroma.cluster:CustomResourceDefinition',
           'worker-memberlist:MemberList',
           'test-memberlist:MemberList',
           'test-memberlist-reader:ClusterRole',
           'test-memberlist-reader-binding:ClusterRoleBinding',
           ],
  new_name='k8s_setup',
  labels=["infrastructure"]
)

k8s_resource('postgres', resource_deps=['k8s_setup'], labels=["infrastructure"])
k8s_resource('pulsar', resource_deps=['k8s_setup'], labels=["infrastructure"], port_forwards=['6650:6650', '8080:8080'])
k8s_resource('migration', resource_deps=['postgres'], labels=["chroma"])
k8s_resource('logservice', resource_deps=['migration'], labels=["chroma"])
k8s_resource('frontend-server', resource_deps=['pulsar'],labels=["chroma"], port_forwards=8000 )
k8s_resource('coordinator', resource_deps=['pulsar'], labels=["chroma"], port_forwards=50051)
k8s_resource('worker', resource_deps=['coordinator'],labels=["chroma"])
