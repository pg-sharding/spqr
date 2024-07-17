terraform {
  required_providers {
    yandex = {
      source = "yandex-cloud/yandex"
      version = "<VERSION>"
    }
  }
  required_version = ">= 0.13"
}

provider "yandex" {
  token     = "<TOKEN>" 
  cloud_id  = "<CLOUD_ID>"
  folder_id = "<FOLDER_ID>"
  zone      = "<ZONE_ID>"
  endpoint  = "<ENDPOINT>"
}
