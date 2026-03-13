resource "yandex_mdb_postgresql_cluster_v2" "shards" {
  count       = var.shards_count

  name        = format("spqr-shard-${var.pr_number}-%02d", count.index + 1)
  environment = "PRODUCTION"
  network_id  = yandex_vpc_network.spqr-net.id

  config {
    version = 16
    resources {
      resource_preset_id = "s3-c8-m32"
      disk_type_id       = "network-ssd"
      disk_size          = 300
    }

    postgresql_config = {
      max_connections = 1600
    }
    pooler_config = {
      pooling_mode = "TRANSACTION"
      pool_discard = false
    }
  }

  hosts = {
    "host1" = {
        zone             = "ru-central1-d"
        subnet_id        = yandex_vpc_subnet.spqr-subnet-d.id
        assign_public_ip = true
    }
  }
}

resource "yandex_mdb_postgresql_user" "user" {
  count      = var.shards_count

  cluster_id = yandex_mdb_postgresql_cluster_v2.shards[count.index].id
  name       = "user1"
  password   = "password"
  conn_limit = 1500
  settings = {
    pool_mode = "transaction"
    prepared_statements_pooling = true
  }
}


resource "yandex_mdb_postgresql_database" "db" {
  count      = var.shards_count

  cluster_id = yandex_mdb_postgresql_cluster_v2.shards[count.index].id
  name       = "db1"
  owner      = yandex_mdb_postgresql_user.user[count.index].name
}
