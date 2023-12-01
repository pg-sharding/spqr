locals {
  count = 64
}

resource "yandex_mdb_postgresql_cluster" "foo" {
  count       = local.count 
  name        = format("spqr-shard%02d", count.index + 1)
  description = "denchick"
  environment = "PRODUCTION"
  network_id  = "_PGAASINTERNALNETS_"

  config {
    version = 14
    resources {
      resource_preset_id = "s3.medium"
      disk_type_id       = "local-ssd"
      disk_size          = 100
    }

    pooler_config {
      pooling_mode = "TRANSACTION"
      pool_discard = false
    }
  }



  host {
    zone      = "sas"
  }
}

resource "yandex_mdb_postgresql_user" "sbtest" {
  count      = local.count 
  cluster_id = yandex_mdb_postgresql_cluster.foo[count.index].id
  name       = "sbtest"
  password   = "password"
}


resource "yandex_mdb_postgresql_database" "sbtest" {
  count      = local.count 
  cluster_id = yandex_mdb_postgresql_cluster.foo[count.index].id
  name       = "sbtest"
  owner = yandex_mdb_postgresql_user.sbtest[count.index].name
}

resource "yandex_mdb_postgresql_user" "pgbench" {
  count      = local.count 
  cluster_id = yandex_mdb_postgresql_cluster.foo[count.index].id
  name       = "pgbench"
  password   = "password"
}

resource "yandex_mdb_postgresql_database" "pgbench" {
  count      = local.count 
  cluster_id = yandex_mdb_postgresql_cluster.foo[count.index].id
  name       = "pgbench"
  owner = yandex_mdb_postgresql_user.pgbench[count.index].name
}

resource "yandex_mdb_postgresql_user" "denchick" {
  count      = local.count 
  cluster_id = yandex_mdb_postgresql_cluster.foo[count.index].id
  name       = "denchick"
  password   = "password"
}

resource "yandex_mdb_postgresql_database" "denchick" {
  count      = local.count 
  cluster_id = yandex_mdb_postgresql_cluster.foo[count.index].id
  name       = "denchick"
  owner = yandex_mdb_postgresql_user.denchick[count.index].name
}

