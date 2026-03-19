provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "data_lake" {
  name          = "fraud-detection-de-${var.project_id}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

# Create a BigQuery dataset for data warehouse
resource "google_bigquery_dataset" "data_warehouse" {
  dataset_id                  = "fraud_detection"
  location                    = var.region
  delete_contents_on_destroy  = true
}

# Create a Service Account for local Airflow and dbt execution
resource "google_service_account" "pipeline_sa" {
  account_id   = "fraud-pipeline-sa"
  display_name = "Service Account for Data Pipeline"
}

# Grant Storage Admin role to the Service Account
resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Grant BigQuery Admin role to the Service Account
resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Generate a JSON key for the Service Account to be used locally
resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
}

# Output the private key to decode and save locally
output "service_account_key" {
  value     = google_service_account_key.pipeline_sa_key.private_key
  sensitive = true
}