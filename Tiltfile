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

rfe_config_file = os.environ.get('RFE_CONFIG_FILE') or "rust/frontend/sample_configs/distributed.yaml"

distributed_chroma_values = "k8s/distributed-chroma/values.yaml,k8s/distributed-chroma/values.dev.yaml"
if os.environ.get('ADDITIONAL_DISTRIBUTED_CHROMA_VALUES'):
  distributed_chroma_values += ',' + os.environ.get('ADDITIONAL_DISTRIBUTED_CHROMA_VALUES')

# We manually call helm template so we can call set-file
k8s_yaml(
  local(
    'helm template --set-file rustFrontendService.configuration=' + rfe_config_file + ',rustLogService.configuration=rust/worker/chroma_config.yaml,heapTenderService.configuration=rust/worker/chroma_config.yaml,compactionService.configuration=rust/worker/chroma_config.yaml,queryService.configuration=rust/worker/chroma_config.yaml,garbageCollector.configuration=rust/worker/chroma_config.yaml --values ' + distributed_chroma_values + ' k8s/distributed-chroma'
  ),
)

rfe2_config_file = os.environ.get('RFE2_CONFIG_FILE') or "rust/frontend/sample_configs/distributed2.yaml"

distributed_chroma2_values = "k8s/distributed-chroma/values2.yaml,k8s/distributed-chroma/values2.dev.yaml"
if os.environ.get('ADDITIONAL_DISTRIBUTED_CHROMA2_VALUES'):
  distributed_chroma2_values += ',' + os.environ.get('ADDITIONAL_DISTRIBUTED_CHROMA2_VALUES')

k8s_yaml(
  local(
    'helm template --set-file rustFrontendService.configuration=' + rfe2_config_file + ',rustLogService.configuration=rust/worker/chroma_config2.yaml,heapTenderService.configuration=rust/worker/chroma_config2.yaml,compactionService.configuration=rust/worker/chroma_config2.yaml,queryService.configuration=rust/worker/chroma_config2.yaml,garbageCollector.configuration=rust/worker/chroma_config2.yaml --values ' + distributed_chroma2_values + ' k8s/distributed-chroma'
  ),
)

watch_file('rust/frontend/sample_configs/distributed.yaml')
watch_file('rust/frontend/sample_configs/distributed2.yaml')
watch_file('rust/worker/chroma_config.yaml')
watch_file('rust/worker/chroma_config2.yaml')
watch_file('k8s/distributed-chroma/values.yaml')
watch_file('k8s/distributed-chroma/values.dev.yaml')
watch_file('k8s/distributed-chroma/values2.yaml')
watch_file('k8s/distributed-chroma/values2.dev.yaml')
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
  'k8s/test/spanner.yaml',
  'k8s/test/prometheus.yaml',
  'k8s/test/test-memberlist-cr.yaml',
  'k8s/test/postgres.yaml',
  'k8s/test/postgres2.yaml',
])

# Lots of things assume the cluster is in a basic state. Get it into a basic
# state before deploying anything else.
k8s_resource(
  objects=[
    'memberlists.chroma.cluster:CustomResourceDefinition',

    'pod-watcher:Role:chroma',
    'query-service-memberlist:MemberList:chroma',
    'compaction-service-memberlist:MemberList:chroma',
    'garbage-collection-service-memberlist:MemberList:chroma',
    'rust-log-service-memberlist:MemberList:chroma',

    'sysdb-serviceaccount:ServiceAccount:chroma',
    'sysdb-serviceaccount-rolebinding:RoleBinding:chroma',
    'sysdb-query-service-memberlist-binding:RoleBinding:chroma',
    'sysdb-compaction-service-memberlist-binding:RoleBinding:chroma',

    'query-service-serviceaccount:ServiceAccount:chroma',
    'query-service-serviceaccount-rolebinding:RoleBinding:chroma',
    'query-service-memberlist-readerwriter:Role:chroma',
    'query-service-query-service-memberlist-binding:RoleBinding:chroma',
    'query-service-memberlist-readerwriter-binding:RoleBinding:chroma',

    'compaction-service-memberlist-readerwriter:Role:chroma',
    'compaction-service-compaction-service-memberlist-binding:RoleBinding:chroma',
    'compaction-service-memberlist-readerwriter-binding:RoleBinding:chroma',
    'compaction-service-serviceaccount:ServiceAccount:chroma',
    'compaction-service-serviceaccount-rolebinding:RoleBinding:chroma',

    'test-memberlist:MemberList:chroma',
    'test-memberlist-reader:Role:chroma',
    'test-memberlist-reader-binding:RoleBinding:chroma',
    'lease-watcher:Role:chroma',
    'rust-frontend-service-config:ConfigMap:chroma',
  ],
  new_name='k8s_setup',
  labels=["infrastructure"],
)

# Lots of things assume the cluster is in a basic state. Get it into a basic
# state before deploying anything else.
k8s_resource(
  objects=[
    'pod-watcher:Role:chroma2',
    'query-service-memberlist:MemberList:chroma2',
    'compaction-service-memberlist:MemberList:chroma2',
    'garbage-collection-service-memberlist:MemberList:chroma2',
    'rust-log-service-memberlist:MemberList:chroma2',

    'sysdb-serviceaccount:ServiceAccount:chroma2',
    'sysdb-serviceaccount-rolebinding:RoleBinding:chroma2',
    'sysdb-query-service-memberlist-binding:RoleBinding:chroma2',
    'sysdb-compaction-service-memberlist-binding:RoleBinding:chroma2',

    'query-service-serviceaccount:ServiceAccount:chroma2',
    'query-service-serviceaccount-rolebinding:RoleBinding:chroma2',
    'query-service-memberlist-readerwriter:Role:chroma2',
    'query-service-query-service-memberlist-binding:RoleBinding:chroma2',
    'query-service-memberlist-readerwriter-binding:RoleBinding:chroma2',

    'compaction-service-memberlist-readerwriter:Role:chroma2',
    'compaction-service-compaction-service-memberlist-binding:RoleBinding:chroma2',
    'compaction-service-memberlist-readerwriter-binding:RoleBinding:chroma2',
    'compaction-service-serviceaccount:ServiceAccount:chroma2',
    'compaction-service-serviceaccount-rolebinding:RoleBinding:chroma2',

    'lease-watcher:Role:chroma2',
    'rust-frontend-service-config:ConfigMap:chroma2',
  ],
  new_name='k8s_setup2',
  labels=["infrastructure2"],
)

# Production Chroma
k8s_resource('postgres:deployment:chroma', resource_deps=['k8s_setup'], labels=["infrastructure"], port_forwards='5432:5432')
# Jobs are suffixed with the image tag to ensure they are unique. In this context, the image tag is defined in k8s/distributed-chroma/values.yaml.
k8s_resource('sysdb-migration-latest:job:chroma', resource_deps=['postgres:deployment:chroma'], labels=["infrastructure"])
k8s_resource('rust-log-service:statefulset:chroma', labels=["chroma"], port_forwards=['50054:50051', '50052:50052'], resource_deps=['minio-deployment'])
k8s_resource('sysdb:deployment:chroma', resource_deps=['sysdb-migration-latest:job:chroma'], labels=["chroma"], port_forwards='50051:50051')
k8s_resource('rust-frontend-service:deployment:chroma', resource_deps=['sysdb:deployment:chroma', 'rust-log-service:statefulset:chroma'], labels=["chroma"], port_forwards='8000:8000')
k8s_resource('query-service:statefulset:chroma', resource_deps=['sysdb:deployment:chroma'], labels=["chroma"], port_forwards='50053:50051')
k8s_resource('compaction-service:statefulset:chroma', resource_deps=['sysdb:deployment:chroma'], labels=["chroma"])
k8s_resource('garbage-collector:statefulset:chroma', resource_deps=['k8s_setup', 'minio-deployment'], labels=["chroma"], port_forwards='50055:50055')
k8s_resource('load-service', resource_deps=['k8s_setup'], labels=["infrastructure"], port_forwards='3001:3001')

# Production Chroma 2
k8s_resource('postgres:deployment:chroma2', resource_deps=['k8s_setup2'], labels=["infrastructure2"], port_forwards='6432:5432')
# Jobs are suffixed with the image tag to ensure they are unique. In this context, the image tag is defined in k8s/distributed-chroma/values.yaml.
k8s_resource('sysdb-migration-latest:job:chroma2', resource_deps=['postgres:deployment:chroma2'], labels=["infrastructure2"])
k8s_resource('rust-log-service:statefulset:chroma2', labels=["chroma2"], port_forwards=['60054:50051', '60052:50052'], resource_deps=['minio-deployment'])
k8s_resource('sysdb:deployment:chroma2', resource_deps=['sysdb-migration-latest:job:chroma2'], labels=["chroma2"], port_forwards='60051:50051')
k8s_resource('rust-frontend-service:deployment:chroma2', resource_deps=['sysdb:deployment:chroma2', 'rust-log-service:statefulset:chroma2'], labels=["chroma2"], port_forwards='8001:8000')
k8s_resource('query-service:statefulset:chroma2', resource_deps=['sysdb:deployment:chroma2'], labels=["chroma2"], port_forwards='60053:50051')
k8s_resource('compaction-service:statefulset:chroma2', resource_deps=['sysdb:deployment:chroma2'], labels=["chroma2"])
k8s_resource('garbage-collector:statefulset:chroma2', resource_deps=['k8s_setup2', 'minio-deployment'], labels=["chroma2"], port_forwards='60055:50055')

# Observability
k8s_resource('jaeger', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('grafana', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('prometheus', resource_deps=['k8s_setup'], labels=["observability"])
k8s_resource('otel-collector', resource_deps=['k8s_setup'], labels=["observability"])

# Local S3
k8s_resource('minio-deployment', resource_deps=['k8s_setup'], labels=["debug"], port_forwards=['9000:9000', '9005:9005'])
# Local Spanner
k8s_resource('spanner-deployment', resource_deps=['k8s_setup'], labels=["debug"], port_forwards=['9010:9010', '9020:9020'])


# Set the enabled resources
# - Basic resources are always enabled.
# - Multi-region resources are only enabled if the env var MULTI_REGION is set to true.
config.clear_enabled_resources()

groups = {
  'basic': [
    'k8s_setup',
    'postgres:deployment:chroma',
    'sysdb-migration-latest:job:chroma',
    'rust-log-service:statefulset:chroma',
    'sysdb:deployment:chroma',
    'rust-frontend-service:deployment:chroma',
    'query-service:statefulset:chroma',
    'compaction-service:statefulset:chroma',
    'load-service',
    'garbage-collector:statefulset:chroma',
    'jaeger',
    'grafana',
    'prometheus',
    'otel-collector',
    'minio-deployment',
  ],
  'multi_region': [
    'k8s_setup2',
    'postgres:deployment:chroma2',
    'sysdb-migration-latest:job:chroma2',
    'rust-log-service:statefulset:chroma2',
    'sysdb:deployment:chroma2',
    'rust-frontend-service:deployment:chroma2',
    'query-service:statefulset:chroma2',
    'compaction-service:statefulset:chroma2',
    'garbage-collector:statefulset:chroma2',
    'spanner-deployment',
  ],
}

if os.environ.get('MULTI_REGION') == 'true':
  config.set_enabled_resources(groups['basic'] + groups['multi_region'])
else:
  config.set_enabled_resources(groups['basic'])
