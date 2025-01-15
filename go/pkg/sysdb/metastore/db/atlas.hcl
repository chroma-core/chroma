data "external_schema" "gorm" {
  program = [
    "go",
    "run",
    "-mod=mod",
    "ariga.io/atlas-provider-gorm",
    "load",
    "--path", "./dbmodel",
    "--dialect", "postgres",
  ]
}

env "dev" {
  src = data.external_schema.gorm.url
  dev = "postgres://localhost:5432/chroma?sslmode=disable"
  migration {
    dir = "file://migrations"
  }
  format {
    migrate {
      diff = "{{ sql . \"  \" }}"
    }
  }
}
