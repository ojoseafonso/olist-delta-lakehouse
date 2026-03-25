variable "databricks_host" {
  description = "URL do workspace Databricks"
  type        = string
}

variable "databricks_token" {
  description = "Personal Access Token do Databricks"
  type        = string
  sensitive   = true
}