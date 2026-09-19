# Cloud mapping

Local paths under `data/` map 1:1 to object storage prefixes.

| Local | AWS | GCP |
|-------|-----|-----|
| `data/bronze/...` | `s3://adstream-prod/bronze/...` | `gs://adstream-prod/bronze/...` |
| `data/silver/...` | S3 + Glue catalog / EMR | GCS + Dataproc |
| Spark jobs | EMR / Glue | Dataproc |
| Kafka | MSK | Managed Kafka / Pub/Sub (different contract) |
| Airflow | MWAA | Cloud Composer |
| dbt target | Redshift / Snowflake / Athena | BigQuery |
| API | ECS / EKS | Cloud Run / GKE |

Glue note: you can swap the PySpark silver for a Glue job reading the same bronze prefix; keep the silver schema identical so dbt doesn't care.
