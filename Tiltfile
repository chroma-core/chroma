update_settings(max_parallel_updates=6)

docker_build(
  'chroma-postgres',
  context='./k8s/test/postgres',
  dockerfile='./k8s/test/postgres/Dockerfile'
)

if config.tilt_subcommand == "ci":
  custom_build(
    'logservice',
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF --target logservice -f ./go/Dockerfile . --load',
    ['./go/', './idl/']
  )
else:
  docker_build(
    'logservice',
    '.',
    only=['go/', 'idl/'],
    dockerfile='./go/Dockerfile',
    target='logservice'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'logservice-migration',
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF --target logservice-migration -f ./go/Dockerfile.migration . --load',
    ['./go/']
  )
else:
  docker_build(
    'logservice-migration',
    '.',
    only=['go/'],
    dockerfile='./go/Dockerfile.migration',
    target="logservice-migration"
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'rust-log-service',
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF  -f ./rust/log/Dockerfile . --load',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock']
  )
else:
  docker_build(
    'rust-log-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/log/Dockerfile',
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'sysdb',
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF --target sysdb -f ./go/Dockerfile . --load',
    ['./go/', './idl/']
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
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF --target sysdb-migration -f ./go/Dockerfile.migration . --load',
    ['./go/']
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
    'frontend-service',
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF -f ./Dockerfile . --load',
    ['chromadb/', 'idl/', 'requirements.txt', 'bin/']
  )
else:
  docker_build(
    'frontend-service',
    '.',
    only=['chromadb/', 'idl/', 'requirements.txt', 'bin/'],
    dockerfile='./Dockerfile',
    ignore=['**/*.pyc', 'chromadb/test/'],
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'rust-frontend-service',
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF -f ./rust/cli/Dockerfile . --load',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock']
  )
else:
  docker_build(
    'rust-frontend-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/cli/Dockerfile',
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'query-service',
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF --target query_service -f ./rust/worker/Dockerfile . --load ',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock']
  )
else:
  docker_build(
    'query-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/worker/Dockerfile',
    target='query_service'
  )

if config.tilt_subcommand == "ci":
  custom_build(
    'compaction-service',
    'depot build --project $DEPOT_PROJECT_ID -t $EXPECTED_REF --target compaction_service -f ./rust/worker/Dockerfile . --load ',
    ['./rust/', './idl/', './Cargo.toml', './Cargo.lock']
  )
else:
  docker_build(
    'compaction-service',
    '.',
    only=["rust/", "idl/", "Cargo.toml", "Cargo.lock"],
    dockerfile='./rust/worker/Dockerfile',
    target='compaction_service'
  )


# First install the CRD
k8s_yaml(
  ['k8s/distributed-chroma/crds/memberlist_crd.yaml'],
)


# We manually call helm template so we can call set-file
k8s_yaml(
  local(
    'helm template --set-file rustFrontendService.configuration=rust/frontend/sample_configs/distributed.yaml --values k8s/distributed-chroma/values.yaml,k8s/distributed-chroma/values.dev.yaml k8s/distributed-chroma'
  ),
)
watch_file('rust/frontend/sample_configs/distributed.yaml')
watch_file('k8s/distributed-chroma/values.yaml')
watch_file('k8s/distributed-chroma/values.dev.yaml')
watch_file('k8s/distributed-chroma/*.yaml')


# Extra stuff to make debugging and testing easier
k8s_yaml([
  'k8s/test/otel-collector.yaml',
  'k8s/test/grafana-service.yaml',
  'k8s/test/grafana.yaml',
  'k8s/test/jaeger-service.yaml',
  'k8s/test/jaeger.yaml',
  'k8s/test/minio.yaml',
  'k8s/test/prometheus.yaml',
  'k8s/test/test-memberlist-cr.yaml',
  'k8s/test/postgres.yaml',
])

k8s_resource(
  objects=[
    # needed for memberlists
    'memberlists.chroma.cluster:CustomResourceDefinition',
    # needed for initial deployment
    'chroma:Namespace',
    # used by compaction-service, query-service, rust-frontend-service, and sysdb-service
    'pod-watcher:Role',
  ],
  new_name='k8s_setup',
  labels=["infrastructure"],
)

k8s_resource(
  workload='postgres',
  resource_deps=['k8s_setup'],
  labels=["infrastructure"],
  port_forwards='5432:5432'
)

k8s_resource(
  workload='sysdb-migration-latest',
  resource_deps=['postgres'],
  labels=["infrastructure"]
)

k8s_resource(
  workload='logservice-migration-latest',
  resource_deps=['postgres'],
  labels=["infrastructure"]
)

k8s_resource(
  workload='logservice',
  objects=[
    'lease-watcher:role',
    'logservice-serviceaccount:serviceaccount',
    'logservice-serviceaccount-rolebinding:rolebinding'
  ],
  resource_deps=['sysdb-migration-latest'],
  labels=["chroma"],
  port_forwards='50052:50051'
)

k8s_resource(
  workload='rust-log-service',
  objects=[
    'rust-log-service-config:configmap'
  ],
  labels=["chroma"],
  port_forwards='50054:50051'
)

k8s_resource(
  objects=[
    'query-service-memberlist:MemberList',
    'query-service-memberlist-readerwriter:ClusterRole',
    'query-service-memberlist-readerwriter-binding:clusterrolebinding',
  ],
  new_name="query-service-memberlist",
  resource_deps=['k8s_setup'],
  labels=["infrastructure"]
)

k8s_resource(
  objects=[
    'compaction-service-memberlist:MemberList',
    'compaction-service-memberlist-readerwriter:ClusterRole',
    'compaction-service-memberlist-readerwriter-binding:clusterrolebinding',
  ],
  new_name="compaction-service-memberlist",
  resource_deps=['k8s_setup'],
  labels=["infrastructure"]
)

k8s_resource(
  objects=[
    'test-memberlist:MemberList',
    'test-memberlist-reader:ClusterRole',
    'test-memberlist-reader-binding:ClusterRoleBinding',
  ],
  new_name="test-memberlist",
  resource_deps=['k8s_setup'],
  labels=["infrastructure"]
)

k8s_resource(
  workload='sysdb',
  objects=[
    'sysdb-serviceaccount:serviceaccount',
    'sysdb-serviceaccount-rolebinding:RoleBinding',
    'sysdb-query-service-memberlist-binding:clusterrolebinding',
    'sysdb-compaction-service-memberlist-binding:clusterrolebinding',
  ],
  resource_deps=[
    'sysdb-migration-latest',
    'query-service-memberlist',
    'compaction-service-memberlist',
  ],
  labels=["chroma"],
  port_forwards='50051:50051'
)

k8s_resource(
  workload='frontend-service',
  resource_deps=[
    'sysdb',
    'logservice'
  ],
  labels=["chroma"],
  port_forwards='8000:8000'
)

k8s_resource(
  workload='rust-frontend-service',
  objects=[
    'rust-frontend-service-serviceaccount:serviceaccount',
    'rust-frontend-service-rolebinding:rolebinding',
    'rust-frontend-service-query-service-memberlist-binding:clusterrolebinding',
    'rust-frontend-service-config:ConfigMap',
  ],
  resource_deps=[
    'sysdb',
    'logservice',
    'rust-log-service',
    'query-service-memberlist'
  ],
  labels=["chroma"],
  port_forwards='3000:8000'
)

k8s_resource(
  workload='query-service',
  objects=[
    'query-service-serviceaccount:serviceaccount',
    'query-service-serviceaccount-rolebinding:RoleBinding',
    'query-service-query-service-memberlist-binding:clusterrolebinding',
  ],
  resource_deps=[
    'sysdb',
    'query-service-memberlist',
  ],
  labels=["chroma"],
  port_forwards='50053:50051'
)

k8s_resource(
  workload='compaction-service',
  objects=[
    'compaction-service-serviceaccount:serviceaccount',
    'compaction-service-serviceaccount-rolebinding:RoleBinding',
    'compaction-service-compaction-service-memberlist-binding:clusterrolebinding',
  ],
  resource_deps=[
    'sysdb',
    'query-service-memberlist',
  ],
  labels=["chroma"]
)

k8s_resource(
  workload='jaeger',
  resource_deps=['k8s_setup'],
  labels=["observability"]
)

k8s_resource(
  workload='grafana',
  objects=[
    'grafana-config:configmap',
    'grafana-prometheus-config:configmap'
  ],
  resource_deps=['k8s_setup'],
  labels=["observability"]
)

k8s_resource(
  workload='prometheus',
  objects=[
    'prometheus-config:configmap'
  ],
  resource_deps=['k8s_setup'],
  labels=["observability"]
)

k8s_resource(
  workload='otel-collector',
  objects=[
    'otel-collector-config:configmap'
  ],
  resource_deps=['k8s_setup'],
  labels=["observability"]
)

k8s_resource(
  workload='minio-deployment',
  resource_deps=['k8s_setup'],
  labels=["debug"],
  port_forwards=[
    '9000:9000',
    '9005:9005'
  ]
)
