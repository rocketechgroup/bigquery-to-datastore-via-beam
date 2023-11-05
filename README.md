# BigQuery to Datastore via Beam

This is a sample project to show how to use Apache Beam to read data from BigQuery and write to Datastore.

## How does this work

1. Poetry is used here to make the development process easier to manage
2. Poetry is then used in the build process to get all the dependencies build in order to submit the dataflow job
3. Cloud Build is used to orchestrate the build process and submit the job to Dataflow

The thought behind this build process is to keep things as simple as possible. Without needing a dataflow template
or containers. This is a simple python script that is submitted to dataflow.

## Submit via Cloud Build

```
export DATETIME=`date +%Y%m%d-%H%M%S`

gcloud builds submit --config cloudbuild.yaml --project rocketech-de-pgcp-sandbox --region europe-west1 \
    --substitutions _DATETIME=${DATETIME}
```

## Issues
A few things currently unresolved
- `--extra_packages ./dist/*.tar.gz` this doesn't seem required, but it should be, how does the library.py file got included when it's not packaged?
- `setup.py` does not seem to be doing anything