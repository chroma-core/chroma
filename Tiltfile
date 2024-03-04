update_settings(max_parallel_updates=6)

docker_build(
  'migration',
  context='.',
  dockerfile='./go/Dockerfile.migration'
)

docker_build(
  'coordinator',
  context='.',
  dockerfile='./go/Dockerfile'
)

docker_build(
  'server',
  context='.',
  dockerfile='./Dockerfile',
)

docker_build(
  'worker',
  context='.',
  dockerfile='./rust/worker/Dockerfile'
)

k8s_yaml(
  helm(
    'k8s/distributed-chroma',
    namespace='chroma',
    values=[
      'k8s/distributed-chroma/values.yaml'
    ]
  )
)

# Lots of things assume the cluster is in a basic state. Get it into a basic
# state before deploying anything else.
k8s_resource(
  objects=[
    'chroma:Namespace',
    'pod-watcher:Role',
    'memberlists.chroma.cluster:CustomResourceDefinition',
    'worker-memberlist:MemberList',

    'coordinator-serviceaccount:serviceaccount',
    'coordinator-serviceaccount-rolebinding:RoleBinding',
    'coordinator-worker-memberlist-binding:clusterrolebinding',

    'logservice-serviceaccount:serviceaccount',

    'worker-serviceaccount:serviceaccount',
    'worker-serviceaccount-rolebinding:RoleBinding',
    'worker-memberlist-readerwriter:ClusterRole',
    'worker-worker-memberlist-binding:clusterrolebinding',
    'worker-memberlist-readerwriter-binding:clusterrolebinding',

    'test-memberlist:MemberList',
    'test-memberlist-reader:ClusterRole',
    'test-memberlist-reader-binding:ClusterRoleBinding',
  ],
  new_name='k8s_setup',
  labels=["infrastructure"]
)

# Production Chroma
k8s_yaml([
  'k8s/test/postgres.yaml',
])
k8s_resource('postgres', resource_deps=['k8s_setup'], labels=["infrastructure"])
k8s_resource('jaeger', resource_deps=['k8s_setup'], labels=["infrastructure"])
k8s_resource('pulsar', resource_deps=['k8s_setup'], labels=["infrastructure"], port_forwards=['6650:6650', '8080:8080'])
k8s_resource('migration', resource_deps=['postgres'], labels=["infrastructure"])
k8s_resource('logservice', resource_deps=['migration'], labels=["chroma"], port_forwards='50052:50051')
k8s_resource('coordinator', resource_deps=['pulsar', 'migration'], labels=["chroma"], port_forwards='50051:50051')
k8s_resource('frontend-server', resource_deps=['pulsar', 'coordinator', 'logservice'],labels=["chroma"], port_forwards='8000:8000')
k8s_resource('worker', resource_deps=['coordinator', 'pulsar'], labels=["chroma"])

# Extra stuff to make debugging and testing easier
k8s_yaml([
  'k8s/test/coordinator_service.yaml',
  'k8s/test/jaeger_service.yaml',
  'k8s/test/logservice_service.yaml',
  'k8s/test/minio.yaml',
  'k8s/test/pulsar_service.yaml',
  'k8s/test/worker_service.yaml',
  'k8s/test/test_memberlist_cr.yaml',
])
k8s_resource(
  objects=[
    # I don't know why but Tilt denies the existence of 'coordinator:service' et al
    # when you try to add them here.
    'worker:service',
  ],
  new_name='debug_setup',
  resource_deps=['worker'],
  labels=["debug"],
)


# Local S3
k8s_resource('minio-deployment', resource_deps=['k8s_setup'], labels=["debug"], port_forwards='9000:9000')
