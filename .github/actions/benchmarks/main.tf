terraform {
  required_providers {
    yandex = {
      source = "registry.terraform.io/yandex-cloud/yandex"
    }
  }

  backend "s3" {
    bucket   = "spqr-benchmark-resources"
    region   = "us-east-1"
    endpoint = "https://storage.yandexcloud.net"

    skip_region_validation      = true
    skip_credentials_validation = true
    force_path_style = true
  }
}

provider "yandex" {
    cloud_id  = "b1g8717fm8f009puc67r"
    folder_id = "b1gugeumtm75bnd8jqhi"
}

resource "yandex_vpc_network" "spqr-net" {}

resource "yandex_vpc_subnet" "spqr-subnet-d" {
  zone           = "ru-central1-d"
  network_id     = yandex_vpc_network.spqr-net.id
  v4_cidr_blocks = ["10.3.0.0/24"]
}

data "yandex_compute_image" "container_optimized_image" {
  family = "container-optimized-image"
}

resource "yandex_compute_instance" "routers" {
  count = var.router_count

  hostname = "spqr-benchmarks-router-${var.pr_number}-${count.index}"
  name     = "spqr-benchmarks-router-${var.pr_number}-${count.index}"
  zone = "ru-central1-d"
  
  boot_disk {
    initialize_params {
      image_id = data.yandex_compute_image.container_optimized_image.id
    }
  }
  network_interface {
    subnet_id = yandex_vpc_subnet.spqr-subnet-d.id
    nat = true
  }

  platform_id = "standard-v3"
  resources {
    cores = 4
    memory = 8
    core_fraction = 100
  }

  service_account_id = "ajeas2r9lvv8l4qmlo15"

  metadata = {
    docker-compose = file("${path.module}/docker-compose.yaml")
    user-data = templatefile("${path.module}/user-data.yaml", {
      router_config = local.router_config
      init_sql = local.init_sql
    })
  }
}

resource "yandex_compute_instance" "benchmark-loader" {
  hostname = "spqr-benchmarks-loader-${var.pr_number}"
  name     = "spqr-benchmarks-loader-${var.pr_number}"
  zone = "ru-central1-d"
  
  boot_disk {
    initialize_params {
      name       = "disk-ubuntu-24-04-lts-1771340993035"
      type       = "network-ssd"
      size       = 20
      block_size = 4096
      image_id   = "fd8q1krrgc5pncjckeht"
    }
    auto_delete = true
  }
  network_interface {
    subnet_id = yandex_vpc_subnet.spqr-subnet-d.id
    nat = true
  }

  platform_id = "standard-v3"
  resources {
    cores = 4
    memory = 8
    core_fraction = 100
  }

  service_account_id = "ajeas2r9lvv8l4qmlo15"

  metadata = {
    user-data = <<-EOT
#cloud-config
users:
- name: diphantxm
  sudo: ALL=(ALL) NOPASSWD:ALL
  shell: /bin/bash
  ssh_authorized_keys:
  - ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBIYJu4nrqghhdnmV1hsvueRbF9mXkygFTuoY737g5a2nbypzn0b6tySunng1vFpiBfmnRx71yOcHJw2btdF8DZc= # Skotty key legacy on yubikey

write_files:
  - path: /etc/benchbase/spqr.xml
    permissions: '0644'
    content: |
          ${indent(10, local.rendered_benchbase)}

  - path: /usr/local/upload.sh
    permissions: '0750'
    content: |
      #!/bin/bash

      export AWS_DEFAULT_REGION="ru-central1"
      ENDPOINT="https://storage.yandexcloud.net"
      BUCKET="spqr-benchmark-reports"
      PR_NUMBER="${var.pr_number}"
      FOLDER="$1"

      for FILE in "$FOLDER"/*; do
        FILE_NAME=$(basename "$FILE")
        NAME=$${FILE_NAME#*.}

        /root/yandex-cloud/bin/yc storage s3api put-object --bucket $BUCKET --key $PR_NUMBER/$NAME --body $FILE
      done

  - path: /usr/local/run.sh
    permissions: '0755'
    content: |
      #!/bin/bash
      set -e
      set -x

      BUCKET="spqr-benchmark-reports"
      PR_NUMBER="${var.pr_number}"

      change_status() {
        echo $1 > /tmp/status
        /root/yandex-cloud/bin/yc storage s3api put-object --bucket $BUCKET --key $PR_NUMBER/status --body /tmp/status
      }

      error_exit() {
          change_status failed
      }

      trap 'error_exit' ERR

      echo "installing yc-cli"
      curl -sSL -o install.sh https://storage.yandexcloud.net/yandexcloud-yc/install.sh
      HOME=/root bash install.sh

      change_status pending

      echo "preparing, installing tools"
      git clone https://github.com/JoBeR007/benchbase-spqr.git
      apt update && sudo apt upgrade -y
      apt install default-jdk -y
      apt install openjdk-21-jdk -y

      echo "preparing benchmark tests"
      cd benchbase-spqr
      ./mvnw clean package -P spqr -DskipTests
      cd target
      tar xvzf benchbase-spqr.tgz
      cd ..

      change_status running

      echo "running benchmarks"
      java -jar target/benchbase-spqr/benchbase.jar -b tpcc -c /etc/benchbase/spqr.xml --create=true --load=true --execute=true > /var/log/benchbase.log

      echo "copying results to s3 bucket"
      /usr/local/upload.sh results/

      change_status done

runcmd:
  - /usr/local/run.sh > /var/log/benchmark-loader.log 2>&1
EOT
  }

  depends_on = [yandex_compute_instance.routers]
}

locals {
  router_public_ips = [
    for vm in yandex_compute_instance.routers :
    vm.network_interface[0].nat_ip_address
  ]
  router_config = templatefile("${path.module}/router.yaml", {
    shard_ips = local.shard_public_ips
  })
  init_sql = file("${path.module}/init.sql")
  rendered_benchbase = templatefile("${path.module}/benchbase.xml.tpl", {
    router_ips = local.router_public_ips
    shard_ips = local.shard_public_ips
  })
  shard_public_ips = [
    for c in yandex_mdb_postgresql_cluster_v2.shards :
    c.hosts["host1"].fqdn
  ]
}
