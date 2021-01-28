
# first curl request to create a job using the appropriate databricks Rest API (the most recent one):
#DATABRICKS_TOKEN= $1

curl -X POST -H "Authorization: Bearer dapibxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"  -d @/path/dbs_api.json    https://greenflex.cloud.databricks.com/api/2.0/jobs/create > file.json
curl -X POST -H "Authorization: Bearer dapibxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" -d @file.json https://company.databricks.com/api/2.0/jobs/run-now