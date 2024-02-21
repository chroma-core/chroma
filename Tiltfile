docker_build('migration',
             context='.',
             dockerfile='./go/coordinator/Dockerfile.migration'
)

docker_build('coordinator',
             context='.',
             dockerfile='./go/coordinator/Dockerfile'
)

docker_build('server',
             context='.',
             dockerfile='./Dockerfile',
)

docker_build('worker',
             context='.',
             dockerfile='./rust/worker/Dockerfile'
)


k8s_yaml(['k8s/dev/setup.yaml'])
k8s_resource(
  objects=['chroma:Namespace', 'memberlist-reader:ClusterRole', 'memberlist-reader:ClusterRoleBinding', 'pod-list-role:Role', 'pod-list-role-binding:RoleBinding', 'memberlists.chroma.cluster:CustomResourceDefinition','worker-memberlist:MemberList'],
  new_name='k8s_setup',
  labels=["infrastructure"]
)
k8s_yaml(['k8s/dev/pulsar.yaml'])
k8s_resource('pulsar', resource_deps=['k8s_setup'], labels=["infrastructure"])
k8s_yaml(['k8s/dev/postgres.yaml'])
k8s_resource('postgres', resource_deps=['k8s_setup'], labels=["infrastructure"])
k8s_yaml(['k8s/dev/migration.yaml'])
k8s_resource('migration', resource_deps=['postgres'], labels=["chroma"])
k8s_yaml(['k8s/dev/server.yaml'])
k8s_resource('server', resource_deps=['k8s_setup'],labels=["chroma"], port_forwards=8000 )
k8s_yaml(['k8s/dev/coordinator.yaml'])
k8s_resource('coordinator', resource_deps=['pulsar', 'server', 'migration'], labels=["chroma"], port_forwards=50051 )
k8s_yaml(['k8s/dev/logservice.yaml'])
k8s_resource('logservice', resource_deps=['migration'], labels=["chroma"], port_forwards='50052:50051')
k8s_yaml(['k8s/dev/worker.yaml'])
k8s_resource('worker', resource_deps=['coordinator'],labels=["chroma"])
