update_settings(max_parallel_updates=6)

docker_build(
  'local:postgres',
  context='./k8s/test/postgres',
  dockerfile='./k8s/test/postgres/Dockerfile'
)

docker_build(
  'local:log-service',
  '.',
  only=['go/'],
  dockerfile='./go/Dockerfile',
  target='logservice'
)


docker_build(
  'local:sysdb-migration',
  '.',
  only=['go/'],
  dockerfile='./go/Dockerfile.migration',
  target='sysdb-migration'
)

docker_build(
  'local:logservice-migration',
  '.',
  only=['go/'],
  dockerfile='./go/Dockerfile.migration',
  target="logservice-migration"
)

docker_build(
  'local:sysdb',
  '.',
  only=['go/', 'idl/'],
  dockerfile='./go/Dockerfile',
  target='sysdb'
)

docker_build(
  'local:frontend-service',
  '.',
  only=['chromadb/', 'idl/', 'requirements.txt', 'bin/'],
  dockerfile='./Dockerfile',
)

docker_build(
  'local:query-service',
  '.',
  only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
  dockerfile='./rust/worker/Dockerfile',
  target='query_service'
)

docker_build(
  'local:compaction-service',
  '.',
  only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
  dockerfile='./rust/worker/Dockerfile',
  target='compaction_service'
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
  'k8s/test/jaeger.yaml',
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
    'compaction-service-memberlist:MemberList',

    'sysdb-serviceaccount:serviceaccount',
    'sysdb-serviceaccount-rolebinding:RoleBinding',
    'sysdb-query-service-memberlist-binding:clusterrolebinding',

    'logservice-serviceaccount:serviceaccount',

    'query-service-serviceaccount:serviceaccount',
    'query-service-serviceaccount-rolebinding:RoleBinding',
    'query-service-memberlist-readerwriter:ClusterRole',
    'query-service-query-service-memberlist-binding:clusterrolebinding',
    'query-service-memberlist-readerwriter-binding:clusterrolebinding',

    'compaction-service-memberlist-readerwriter:ClusterRole',
    'compaction-service-compaction-service-memberlist-binding:clusterrolebinding',
    'compaction-service-memberlist-readerwriter-binding:clusterrolebinding',
    'compaction-service-serviceaccount:serviceaccount',
    'compaction-service-serviceaccount-rolebinding:RoleBinding',

    'test-memberlist:MemberList',
    'test-memberlist-reader:ClusterRole',
    'test-memberlist-reader-binding:ClusterRoleBinding',
  ],
  new_name='k8s_setup',
  labels=["infrastructure"],
  resource_deps=['namespace'],
)

# Production Chroma
k8s_resource('postgres', resource_deps=['k8s_setup', 'namespace'], labels=["infrastructure"], port_forwards='5432:5432')
k8s_resource('sysdb-migration', resource_deps=['postgres', 'namespace'], labels=["infrastructure"])
k8s_resource('logservice-migration', resource_deps=['postgres', 'namespace'], labels=["infrastructure"])
k8s_resource('logservice', resource_deps=['sysdb-migration'], labels=["chroma"], port_forwards='50052:50051')
k8s_resource('sysdb', resource_deps=['sysdb-migration'], labels=["chroma"], port_forwards='50051:50051')
k8s_resource('frontend-service', resource_deps=['sysdb', 'logservice'],labels=["chroma"], port_forwards='8000:8000')
k8s_resource('query-service', resource_deps=['sysdb'], labels=["chroma"])
k8s_resource('compaction-service', resource_deps=['sysdb'], labels=["chroma"])

# I have no idea why these need their own lines but the others don't.
k8s_resource(objects=['query-service:service'], new_name='query-service-service', resource_deps=['query-service'], labels=["chroma"])
k8s_resource('jaeger', resource_deps=['k8s_setup'], labels=["debug"])

# Local S3
k8s_resource('minio-deployment', resource_deps=['k8s_setup'], labels=["debug"], port_forwards='9000:9000')
