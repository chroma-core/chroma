update_settings(max_parallel_updates=6)

custom_build(
  'local:postgres',
  'docker build -t $EXPECTED_REF --load -f ./k8s/test/postgres/Dockerfile k8s/test/postgres',
  ["./k8s/test/postgres"]
)

custom_build(
  'local:log-service',
  'docker build -t $EXPECTED_REF --load -f ./go/Dockerfile --target logservice .',
  ["./go"]
)

custom_build(
  'local:sysdb-migration',
  'docker build -t $EXPECTED_REF --load -f ./go/Dockerfile.migration --target sysdb-migration .',
  ["./go"]
)

custom_build(
  'local:logservice-migration',
  'docker build -t $EXPECTED_REF --load -f ./go/Dockerfile.migration --target logservice-migration .',
  ["./go"]
)

custom_build(
  'local:sysdb',
  'docker build -t $EXPECTED_REF --load -f ./go/Dockerfile --target sysdb .',
  ["./go", "./idl"]
)


custom_build(
  'local:frontend-service',
  'docker build -t $EXPECTED_REF --load .',
  ["./chromadb", "./idl", "./requirements.txt", "./bin"]
)

custom_build(
  'local:query-service',
  'docker build -t $EXPECTED_REF --load -f ./rust/worker/Dockerfile --target query_service .',
  ["./rust", "./idl", "./Cargo.toml", "./Cargo.lock"]
)

custom_build(
  'local:compaction-service',
  'docker build -t $EXPECTED_REF --load -f ./rust/worker/Dockerfile --target compaction_service .',
  ["./rust", "./idl", "./Cargo.toml", "./Cargo.lock"]
)

k8s_resource(
  objects=['chroma:Namespace'],
  new_name='namespace',
  labels=["infrastructure"],
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
  'k8s/test/jaeger-service.yaml',
  'k8s/test/jaeger.yaml',
  'k8s/test/minio.yaml',
  'k8s/test/test-memberlist-cr.yaml',
])

# Lots of things assume the cluster is in a basic state. Get it into a basic
# state before deploying anything else.
k8s_resource(
  objects=[
    'pod-watcher:Role',
    'memberlists.chroma.cluster:CustomResourceDefinition',
    'query-service-memberlist:MemberList',
    'compaction-service-memberlist:MemberList',

    'sysdb-serviceaccount:serviceaccount',
    'sysdb-serviceaccount-rolebinding:RoleBinding',
    'sysdb-query-service-memberlist-binding:clusterrolebinding',
    'sysdb-compaction-service-memberlist-binding:clusterrolebinding',

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
    'lease-watcher:role',
    'logservice-serviceaccount-rolebinding:rolebinding',
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
k8s_resource('query-service', resource_deps=['sysdb'], labels=["chroma"], port_forwards='50053:50051')
k8s_resource('compaction-service', resource_deps=['sysdb'], labels=["chroma"])

# I have no idea why these need their own lines but the others don't.
k8s_resource('jaeger', resource_deps=['k8s_setup'], labels=["debug"])

# Local S3
k8s_resource('minio-deployment', resource_deps=['k8s_setup'], labels=["debug"], port_forwards='9000:9000')
