update_settings(max_parallel_updates=6)

docker_build(
  'local:sysdb-migration',
  context='.',
  dockerfile='./go/Dockerfile.migration'
)

docker_build(
  'local:sysdb',
  context='.',
  dockerfile='./go/Dockerfile'
)

docker_build(
  'local:frontend-service',
  context='.',
  dockerfile='./Dockerfile',
)

docker_build(
  'local:query-service',
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

k8s_yaml([
  'k8s/test/postgres.yaml',
])

# Extra stuff to make debugging and testing easier
k8s_yaml([
  'k8s/test/sysdb-service.yaml',
  'k8s/test/jaeger-service.yaml',
  'k8s/test/logservice-service.yaml',
  'k8s/test/minio.yaml',
  'k8s/test/query-service-service.yaml',
  'k8s/test/test-memberlist-cr.yaml',
])

# Lots of things assume the cluster is in a basic state. Get it into a basic
# state before deploying anything else.
k8s_resource(
  objects=['chroma:Namespace'],
  new_name='namespace',
  labels=["infrastructure"],
)
k8s_resource(
  objects=[
    'pod-watcher:Role',
    'memberlists.chroma.cluster:CustomResourceDefinition',
    'query-service-memberlist:MemberList',

    'sysdb-serviceaccount:serviceaccount',
    'sysdb-serviceaccount-rolebinding:RoleBinding',
    'sysdb-query-service-memberlist-binding:clusterrolebinding',

    'logservice-serviceaccount:serviceaccount',

    'query-service-serviceaccount:serviceaccount',
    'query-service-serviceaccount-rolebinding:RoleBinding',
    'query-service-memberlist-readerwriter:ClusterRole',
    'query-service-query-service-memberlist-binding:clusterrolebinding',
    'query-service-memberlist-readerwriter-binding:clusterrolebinding',

    'test-memberlist:MemberList',
    'test-memberlist-reader:ClusterRole',
    'test-memberlist-reader-binding:ClusterRoleBinding',
  ],
  new_name='k8s_setup',
  labels=["infrastructure"],
  resource_deps=['namespace'],
)

# Production Chroma
k8s_resource('postgres', resource_deps=['k8s_setup', 'namespace'], labels=["infrastructure"])
k8s_resource('pulsar', resource_deps=['k8s_setup', 'namespace'], labels=["infrastructure"], port_forwards=['6650:6650', '8080:8080'])
k8s_resource('sysdb-migration', resource_deps=['postgres', 'namespace'], labels=["infrastructure"])
k8s_resource('logservice', resource_deps=['sysdb-migration'], labels=["chroma"], port_forwards='50052:50051')
k8s_resource('sysdb', resource_deps=['pulsar', 'sysdb-migration'], labels=["chroma"], port_forwards='50051:50051')
k8s_resource('frontend-service', resource_deps=['pulsar', 'sysdb', 'logservice'],labels=["chroma"], port_forwards='8000:8000')
k8s_resource('query-service', resource_deps=['sysdb', 'pulsar'], labels=["chroma"])

# I have no idea why these need their own lines but the others don't.
k8s_resource(objects=['query-service:service'], new_name='query-service-service', resource_deps=['query-service'], labels=["chroma"])
k8s_resource(objects=['jaeger-lb:Service'], new_name='jaeger-service', resource_deps=['k8s_setup'], labels=["debug"])

# Local S3
k8s_resource('minio-deployment', resource_deps=['k8s_setup'], labels=["debug"], port_forwards='9000:9000')
