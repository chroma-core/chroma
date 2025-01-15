update_settings(max_parallel_updates=6)

def format_buildx_command(dockerfile, context = None, target = None):
  build_str = 'docker buildx build -t $EXPECTED_REF --file {}'.format(dockerfile)

  if os.environ.get('CI'):
    build_str += ' --cache-from=type=gha --cache-to=type=gha --load'

  if target:
    build_str += ' --target {}'.format(target)
  if context:
    build_str += ' {}'.format(context)
  else:
    build_str += ' .'

  return build_str

custom_build(
  'local:postgres',
  format_buildx_command(dockerfile='k8s/test/postgres/Dockerfile', context='./k8s/test/postgres'),
  deps=[]
)

custom_build(
  'local:logservice',
  format_buildx_command(dockerfile='go/Dockerfile', target='logservice'),
  tag="logservice",
  deps=["go/", "idl/"]
)

custom_build(
  'local:logservice-migration',
  format_buildx_command(dockerfile='go/Dockerfile.migration', target='logservice-migration'),
  tag="logservice-migration",
  deps=["go/"]
)

custom_build(
  'local:sysdb',
  format_buildx_command(dockerfile='go/Dockerfile', target='sysdb'),
  tag="sysdb",
  deps=["go/", "idl/"]
)

custom_build(
  'local:sysdb-migration',
  format_buildx_command(dockerfile='go/Dockerfile.migration', target='sysdb-migration'),
  tag="sysdb-migration",
  deps=["go/"]
)

custom_build(
  'local:frontend-service',
  format_buildx_command(dockerfile='Dockerfile'),
  tag="frontend-service",
  deps=['chromadb/', 'idl/', 'requirements.txt', 'bin/'],
  ignore=['**/*.pyc', 'chromadb/test/'],
)

custom_build(
  'local:query-service',
  format_buildx_command(dockerfile='rust/worker/Dockerfile', target='query_service'),
  tag="query-service",
  deps=['rust/', 'idl/', 'Cargo.toml', 'Cargo.lock'],
)

custom_build(
  'local:compaction-service',
  format_buildx_command(dockerfile='rust/worker/Dockerfile', target='compaction_service'),
  tag="compaction-service",
  deps=['rust/', 'idl/', 'Cargo.toml', 'Cargo.lock'],
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
  'k8s/test/namespace.yaml',
  'k8s/test/otel-collector.yaml',
  'k8s/test/grafana-service.yaml',
  'k8s/test/grafana.yaml',
  'k8s/test/jaeger-service.yaml',
  'k8s/test/jaeger.yaml',
  'k8s/test/minio.yaml',
  'k8s/test/prometheus.yaml',
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
)

# Production Chroma
k8s_resource('postgres', resource_deps=['k8s_setup'], labels=["infrastructure"], port_forwards='5432:5432')
# Jobs are suffixed with the image tag to ensure they are unique. In this context, the image tag is defined in k8s/distributed-chroma/values.yaml.
k8s_resource('sysdb-migration-sysdb-migration', resource_deps=['postgres'], labels=["infrastructure"])
k8s_resource('logservice-migration-logservice-migration', resource_deps=['postgres'], labels=["infrastructure"])
k8s_resource('logservice', resource_deps=['sysdb-migration-sysdb-migration'], labels=["chroma"], port_forwards='50052:50051')
k8s_resource('sysdb', resource_deps=['sysdb-migration-sysdb-migration'], labels=["chroma"], port_forwards='50051:50051')
k8s_resource('frontend-service', resource_deps=['sysdb', 'logservice'],labels=["chroma"], port_forwards='8000:8000')
k8s_resource('query-service', resource_deps=['sysdb'], labels=["chroma"], port_forwards='50053:50051')
k8s_resource('compaction-service', resource_deps=['sysdb'], labels=["chroma"])

# I have no idea why these need their own lines but the others don't.
k8s_resource('jaeger', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('grafana', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('prometheus', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('otel-collector', resource_deps=['k8s_setup'], labels=["observability"])

# Local S3
k8s_resource('minio-deployment', resource_deps=['k8s_setup'], labels=["debug"], port_forwards=['9000:9000', '9005:9005'])
