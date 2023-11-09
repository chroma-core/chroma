data "external_schema" "gorm" {
  program = [
    "go",
    "run",
    "-mod=mod",
    "ariga.io/atlas-provider-gorm",
    "load",
    "--path", "./internal/metastore/db/dbmodel",
    "--dialect", "mysql", // | postgres | sqlite
  ]
}

env "gorm" {
  src = data.external_schema.gorm.url
  dev = "mysql://root:@localhost:3306/dev"
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}
