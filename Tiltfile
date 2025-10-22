update_settings(max_parallel_updates=6)

# *:ci images are defined in .github/actions/tilt/docker-bake.hcl and used for .github/actions/tilt/action.yaml.

if config.tilt_subcommand == "ci":
  custom_build(
    'chroma-postgres',
    'docker build -t $EXPECTED_REF -f k8s/test/postgres/Dockerfile k8s/test/postgres',
    ['./k8s/test/postgres/'],
    disable_push=True
  )
else:
  docker_build(
    'chroma-postgres',
    context='./k8s/test/postgres',
    dockerfile='./k8s/test/postgres/Dockerfile'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'rust-log-service',
    'docker image tag rust-log-service:ci $EXPECTED_REF',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock'],
    disable_push=True
  )
else:
  docker_build(
    'rust-log-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/Dockerfile',
    target='log_service'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'heap-tender-service',
    'docker image tag heap-tender-service:ci $EXPECTED_REF',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock'],
    disable_push=True
  )
else:
  docker_build(
    'heap-tender-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/Dockerfile',
    target='heap_tender_service'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'sysdb',
    'docker image tag sysdb:ci $EXPECTED_REF',
    ['./go/', './idl/'],
    disable_push=True
  )
else:
  docker_build(
    'sysdb',
    '.',
    only=['go/', 'idl/'],
    dockerfile='./go/Dockerfile',
    target='sysdb'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'sysdb-migration',
    'docker image tag sysdb-migration:ci $EXPECTED_REF',
    ['./go/'],
    disable_push=True
  )
else:
  docker_build(
    'sysdb-migration',
    '.',
    only=['go/'],
    dockerfile='./go/Dockerfile.migration',
    target='sysdb-migration'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'rust-frontend-service',
    'docker image tag rust-frontend-service:ci $EXPECTED_REF',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock'],
    disable_push=True
  )
else:
  docker_build(
    'rust-frontend-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/Dockerfile',
    target='cli'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'query-service',
    'docker image tag query-service:ci $EXPECTED_REF',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock'],
    disable_push=True
  )
else:
  docker_build(
    'query-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/Dockerfile',
    target='query_service'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'compaction-service',
    'docker image tag compactor-service:ci $EXPECTED_REF',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock'],
    disable_push=True
  )
else:
  docker_build(
    'compaction-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/Dockerfile',
    target='compaction_service'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'garbage-collector',
    'docker image tag garbage-collector:ci $EXPECTED_REF',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock'],
    disable_push=True
  )
else:
  docker_build(
    'garbage-collector',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/Dockerfile',
    target='garbage_collector'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'load-service',
    'docker image tag load-service:ci $EXPECTED_REF',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock'],
    disable_push=True
  )
else:
  docker_build(
    'load-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/Dockerfile',
    target='load_service'
  )


# First install the CRD
k8s_yaml(
  ['k8s/distributed-chroma/crds/memberlist_crd.yaml'],
)


# We manually call helm template so we can call set-file
k8s_yaml(
  local(
    'helm template --set-file rustFrontendService.configuration=rust/frontend/sample_configs/distributed.yaml,rustLogService.configuration=rust/worker/chroma_config.yaml,heapTenderService.configuration=rust/worker/chroma_config.yaml,compactionService.configuration=rust/worker/chroma_config.yaml,queryService.configuration=rust/worker/chroma_config.yaml,garbageCollector.configuration=rust/worker/chroma_config.yaml --values k8s/distributed-chroma/values.yaml,k8s/distributed-chroma/values.dev.yaml k8s/distributed-chroma'
  ),
)
watch_file('rust/frontend/sample_configs/distributed.yaml')
watch_file('rust/worker/chroma_config.yaml')
watch_file('k8s/distributed-chroma/values.yaml')
watch_file('k8s/distributed-chroma/values.dev.yaml')
watch_file('k8s/distributed-chroma/*.yaml')


# Extra stuff to make debugging and testing easier
k8s_yaml([
  'k8s/test/otel-collector.yaml',
  'k8s/test/grafana-service.yaml',
  'k8s/test/grafana-dashboards.yaml',
  'k8s/test/grafana.yaml',
  'k8s/test/jaeger-service.yaml',
  'k8s/test/jaeger.yaml',
  'k8s/test/load-service.yaml',
  'k8s/test/minio.yaml',
  'k8s/test/prometheus.yaml',
  'k8s/test/test-memberlist-cr.yaml',
  'k8s/test/postgres.yaml',
])

# Lots of things assume the cluster is in a basic state. Get it into a basic
# state before deploying anything else.
k8s_resource(
  objects=[
    'pod-watcher:Role',
    'memberlists.chroma.cluster:CustomResourceDefinition',
    'query-service-memberlist:MemberList',
    'compaction-service-memberlist:MemberList',
    'garbage-collection-service-memberlist:MemberList',
    'rust-log-service-memberlist:MemberList',

    'sysdb-serviceaccount:serviceaccount',
    'sysdb-serviceaccount-rolebinding:RoleBinding',
    'sysdb-query-service-memberlist-binding:clusterrolebinding',
    'sysdb-compaction-service-memberlist-binding:clusterrolebinding',

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
    'rust-frontend-service-config:ConfigMap',
  ],
  new_name='k8s_setup',
  labels=["infrastructure"],
)

# Production Chroma
k8s_resource('postgres', resource_deps=['k8s_setup'], labels=["infrastructure"], port_forwards='5432:5432')
# Jobs are suffixed with the image tag to ensure they are unique. In this context, the image tag is defined in k8s/distributed-chroma/values.yaml.
k8s_resource('sysdb-migration-latest', resource_deps=['postgres'], labels=["infrastructure"])
k8s_resource('rust-log-service', labels=["chroma"], port_forwards=['50054:50051', '50052:50052'], resource_deps=['minio-deployment'])
k8s_resource('sysdb', resource_deps=['sysdb-migration-latest'], labels=["chroma"], port_forwards='50051:50051')
k8s_resource('rust-frontend-service', resource_deps=['sysdb', 'rust-log-service'], labels=["chroma"], port_forwards='8000:8000')
k8s_resource('query-service', resource_deps=['sysdb'], labels=["chroma"], port_forwards='50053:50051')
k8s_resource('compaction-service', resource_deps=['sysdb'], labels=["chroma"])
k8s_resource('load-service', resource_deps=['k8s_setup'], labels=["infrastructure"], port_forwards='3001:3001')
k8s_resource('jaeger', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('grafana', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('prometheus', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('otel-collector', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('garbage-collector', resource_deps=['k8s_setup', 'minio-deployment'], labels=["chroma"], port_forwards='50055:50055')
# Local S3
k8s_resource('minio-deployment', resource_deps=['k8s_setup'], labels=["debug"], port_forwards=['9000:9000', '9005:9005'])
