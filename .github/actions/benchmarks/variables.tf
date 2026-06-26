variable "image_id" {
  description = "Image ID of SPQR in docker registry"
  type        = string
}

variable "router_count" {
  default = 1
}

variable "router_cpu" {
  description = "Number of CPU cores on router"
  type        = number
  default     = 32
}

variable "router_mem" {
  description = "RAM in GB on router"
  type        = number
  default     = 32
}

variable "shards_count" {
  default = 4
}

variable "shard_resource_preset" {
  description = "Shard's resource preset"
  type        = string
  default     = "s3-c8-m32"
}

variable "pr_number" {
  type = string
  default = "master"
}

variable "image_tag" {
  type = string
}

variable "cr_registry" {
  type = string
}
