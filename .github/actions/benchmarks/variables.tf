variable "image_id" {
  description = "Image ID of SPQR in docker registry"
  type        = string
}

variable "router_count" {
  default = 1
}

variable "shards_count" {
  default = 3
}

variable "pr_number" {
  type = string
}

variable "image_tag" {
  type = string
}

variable "cr_registry" {
  type = string
}
