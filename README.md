# Create a ETL data pipeline on Google Cloud Platform

Follow the steps laid out in the <a href='https://medium.com/@rydernguyen/how-to-set-up-a-covid-19-workflow-and-dashboard-using-the-google-cloud-platform-b0e5165333e5?sk=6266ffdb056466e73a7bc38a23748722' target='_blank'>Medium story</a> & clone the repository.

# Useful BigQuery python commands
## Write to a BigQuery table
pd.to_gbq('table_name',if_exists='param')
## Read from a BigQuery table using legacy syntax
pd.read_gbq(sql, dialect='legacy')
## Run queries on BigQuery directly from Jupyter 
query_job = bigquery_client.query("""[SQL CODE]""") <br>
results = query_job.result()

# Useful Google Shell Command
## Set up working environment
export PROJECT_ID='covid-jul25' <br>
gcloud config set project $PROJECT_ID <br>
export REGION=us-west3 <br>
export ZONE=us-west3-a <br>
export BUCKET_LINK=gs://us-west3-<b>{BUCKET_NAME}</b> <br>
export BUCKET=us-west3-<b>{BUCKET_NAME}</b> <br>
export TEMPLATE_ID=daily_update_template

## Naming the cluster & create a template
export cluster_name=covid-cluster <br>
gcloud dataproc workflow-templates create \
  $TEMPLATE_ID --region $REGION

## Delete existing workflow templates
gcloud dataproc workflow-templates delete <b>{TEMPLATE_NAME}</b> --region=us-west3

## Attach managed cluster + Pandas to template
gcloud dataproc workflow-templates set-managed-cluster \ <br>
  $TEMPLATE_ID \ <br>
    --region $REGION \ <br>
    --zone $ZONE \ <br>
    --cluster-name $cluster_name \ <br>
    --optional-components=ANACONDA \ <br>
    --master-machine-type n1-standard-4 \ <br>
    --master-boot-disk-size 20 \ <br>
    --worker-machine-type n1-standard-4 \ <br>
    --worker-boot-disk-size 20 \ <br>
    --num-workers 2 \ <br>
    --image-version 1.4 \ <br>
    --metadata='PIP_PACKAGES=pandas google.cloud pandas-gbq' \ <br>
    --initialization-actions gs://us-west3-<b>{BUCKET_NAME}</b>/pip-install.sh

## Add Python-based PySpark job
export STEP_ID=arima_update <br>
gcloud dataproc workflow-templates add-job pyspark \ <br>
  $BUCKET_LINK/daily_update.py \ <br>
  --step-id $STEP_ID \ <br>
  --workflow-template $TEMPLATE_ID \ <br>
  --region $REGION

## See jobs in template
gcloud dataproc workflow-templates list --region $REGION

## Instantiate & time the workflow
export REGION=us-west3 <br>
export TEMPLATE_ID=daily_update_template <br>
time gcloud dataproc workflow-templates instantiate \ <br>
  $TEMPLATE_ID --region $REGION #--async
