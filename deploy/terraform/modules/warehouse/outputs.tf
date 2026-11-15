output "datasets" {
  value = { for id, dataset in google_bigquery_dataset.layer : id => dataset.id }
}
