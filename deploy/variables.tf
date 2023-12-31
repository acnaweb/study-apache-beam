variable "project_id" {
  description = "The ID of the project in which to provision resources."
  type        = string
}

variable "region" {
  description = "Default region"
  type = string
}

variable "credentials" {
  description = "Path for Google Credentials"
  type = string
}

variable "prefix" {
  description = "Prefix for object names"
  type = string
}