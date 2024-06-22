module "gce-container" {
  source = "terraform-google-modules/container-vm/google"

  container = {
    image = "${var.region}-docker.pkg.dev/${var.project}/${var.registry}/${var.image}:latest"
    tty : true
    env = [
      {
        name  = "IS_PERSISTENT"
        value = "1"
      },
      {
        name  = "PERSIST_DIRECTORY"
        value = "/data"
      }
    ]

    volumeMounts = [
      {
        mountPath = "/data"
        name      = "data-disk-0"
        readOnly  = false
      },
    ]
  }


  volumes = [
    {
      name = "data-disk-0"

      gcePersistentDisk = {
        pdName = "data-disk-0"
        fsType = "ext4"
      }
    },
  ]

  restart_policy = "Always"
}
