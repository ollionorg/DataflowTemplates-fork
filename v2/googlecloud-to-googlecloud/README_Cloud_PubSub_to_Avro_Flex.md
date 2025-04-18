
Pub/Sub to Avro Files on Cloud Storage template
---
The Pub/Sub to Avro files on Cloud Storage template is a streaming pipeline that
reads data from a Pub/Sub topic and writes Avro files into the specified Cloud
Storage bucket.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/pubsub-to-avro)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=Cloud_PubSub_to_Avro_Flex).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **outputDirectory**: The path and filename prefix for writing output files. Must end with a slash. DateTime formatting is used to parse directory path for date & time formatters.
* **avroTempDirectory**: Directory for temporary Avro files.

### Optional parameters

* **inputSubscription**: Pub/Sub subscription to read the input from, in the format of 'projects/your-project-id/subscriptions/your-subscription-name' For example, `projects/your-project-id/subscriptions/your-subscription-name`.
* **inputTopic**: Pub/Sub topic to read the input from, in the format of 'projects/your-project-id/topics/your-topic-name'.
* **outputFilenamePrefix**: The prefix to place on each windowed file. Defaults to: output.
* **outputFilenameSuffix**: The suffix to place on each windowed file. Typically a file extension such as .txt or .csv. Defaults to empty.
* **outputShardTemplate**: The shard template defines the dynamic portion of each windowed file. By default, the pipeline uses a single shard for output to the file system within each window. This means that all data outputs into a single file per window. The `outputShardTemplate` defaults to `W-P-SS-of-NN` where `W` is the window date range, `P` is the pane info, `S` is the shard number, and `N` is the number of shards. In case of a single file, the `SS-of-NN` portion of the `outputShardTemplate` is `00-of-01`.
* **numShards**: The maximum number of output shards produced when writing. A higher number of shards means higher throughput for writing to Cloud Storage, but potentially higher data aggregation cost across shards when processing output Cloud Storage files. Defaults to: 0.
* **windowDuration**: The window duration is the interval in which data is written to the output directory. Configure the duration based on the pipeline's throughput. For example, a higher throughput might require smaller window sizes so that the data fits into memory. Defaults to `5m` (5 minutes), with a minimum of `1s` (1 second). Allowed formats are: `[int]s` (for seconds, example: `5s`), `[int]m` (for minutes, example: `12m`), `[int]h` (for hours, example: `2h`). For example, `5m`.
* **yearPattern**: Pattern for formatting the year. Must be one or more of `y` or `Y`. Case makes no difference in the year. The pattern can be optionally wrapped by characters that aren't either alphanumeric or the directory (`/`) character. Defaults to `YYYY`.
* **monthPattern**: Pattern for formatting the month. Must be one or more of the `M` character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory (`/`) character. Defaults to `MM`.
* **dayPattern**: Pattern for formatting the day. Must be one or more of `d` for day of month or `D` for day of year. Case makes no difference in the year. The pattern can be optionally wrapped by characters that aren't either alphanumeric or the directory (`/`) character. Defaults to `dd`.
* **hourPattern**: Pattern for formatting the hour. Must be one or more of the `H` character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory (`/`) character. Defaults to `HH`.
* **minutePattern**: Pattern for formatting the minute. Must be one or more of the `m` character. The pattern can be optionally wrapped by characters that aren't alphanumeric or the directory (`/`) character. Defaults to `mm`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/googlecloud-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/templates/PubsubToAvro.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
and [Configure Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates)
for more information.

#### Staging the Template

If the plan is to just stage the template (i.e., make it available to use) by
the `gcloud` command or Dataflow "Create job from template" UI,
the `-PtemplatesStage` profile should be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="Cloud_PubSub_to_Avro_Flex" \
-f v2/googlecloud-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/Cloud_PubSub_to_Avro_Flex
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/Cloud_PubSub_to_Avro_Flex"

### Required
export OUTPUT_DIRECTORY=<outputDirectory>
export AVRO_TEMP_DIRECTORY=<avroTempDirectory>

### Optional
export INPUT_SUBSCRIPTION=<inputSubscription>
export INPUT_TOPIC=<inputTopic>
export OUTPUT_FILENAME_PREFIX=output
export OUTPUT_FILENAME_SUFFIX=""
export OUTPUT_SHARD_TEMPLATE=W-P-SS-of-NN
export NUM_SHARDS=0
export WINDOW_DURATION=5m
export YEAR_PATTERN=YYYY
export MONTH_PATTERN=MM
export DAY_PATTERN=dd
export HOUR_PATTERN=HH
export MINUTE_PATTERN=mm

gcloud dataflow flex-template run "cloud-pubsub-to-avro-flex-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "inputSubscription=$INPUT_SUBSCRIPTION" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "outputDirectory=$OUTPUT_DIRECTORY" \
  --parameters "outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX" \
  --parameters "outputFilenameSuffix=$OUTPUT_FILENAME_SUFFIX" \
  --parameters "avroTempDirectory=$AVRO_TEMP_DIRECTORY" \
  --parameters "outputShardTemplate=$OUTPUT_SHARD_TEMPLATE" \
  --parameters "numShards=$NUM_SHARDS" \
  --parameters "windowDuration=$WINDOW_DURATION" \
  --parameters "yearPattern=$YEAR_PATTERN" \
  --parameters "monthPattern=$MONTH_PATTERN" \
  --parameters "dayPattern=$DAY_PATTERN" \
  --parameters "hourPattern=$HOUR_PATTERN" \
  --parameters "minutePattern=$MINUTE_PATTERN"
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
export OUTPUT_DIRECTORY=<outputDirectory>
export AVRO_TEMP_DIRECTORY=<avroTempDirectory>

### Optional
export INPUT_SUBSCRIPTION=<inputSubscription>
export INPUT_TOPIC=<inputTopic>
export OUTPUT_FILENAME_PREFIX=output
export OUTPUT_FILENAME_SUFFIX=""
export OUTPUT_SHARD_TEMPLATE=W-P-SS-of-NN
export NUM_SHARDS=0
export WINDOW_DURATION=5m
export YEAR_PATTERN=YYYY
export MONTH_PATTERN=MM
export DAY_PATTERN=dd
export HOUR_PATTERN=HH
export MINUTE_PATTERN=mm

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="cloud-pubsub-to-avro-flex-job" \
-DtemplateName="Cloud_PubSub_to_Avro_Flex" \
-Dparameters="inputSubscription=$INPUT_SUBSCRIPTION,inputTopic=$INPUT_TOPIC,outputDirectory=$OUTPUT_DIRECTORY,outputFilenamePrefix=$OUTPUT_FILENAME_PREFIX,outputFilenameSuffix=$OUTPUT_FILENAME_SUFFIX,avroTempDirectory=$AVRO_TEMP_DIRECTORY,outputShardTemplate=$OUTPUT_SHARD_TEMPLATE,numShards=$NUM_SHARDS,windowDuration=$WINDOW_DURATION,yearPattern=$YEAR_PATTERN,monthPattern=$MONTH_PATTERN,dayPattern=$DAY_PATTERN,hourPattern=$HOUR_PATTERN,minutePattern=$MINUTE_PATTERN" \
-f v2/googlecloud-to-googlecloud
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/googlecloud-to-googlecloud/terraform/Cloud_PubSub_to_Avro_Flex
terraform init
terraform apply
```

To use
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly:

```terraform
provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

resource "google_dataflow_flex_template_job" "cloud_pubsub_to_avro_flex" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/Cloud_PubSub_to_Avro_Flex"
  name              = "cloud-pubsub-to-avro-flex"
  region            = var.region
  parameters        = {
    outputDirectory = "<outputDirectory>"
    avroTempDirectory = "<avroTempDirectory>"
    # inputSubscription = "<inputSubscription>"
    # inputTopic = "<inputTopic>"
    # outputFilenamePrefix = "output"
    # outputFilenameSuffix = ""
    # outputShardTemplate = "W-P-SS-of-NN"
    # numShards = "0"
    # windowDuration = "5m"
    # yearPattern = "YYYY"
    # monthPattern = "MM"
    # dayPattern = "dd"
    # hourPattern = "HH"
    # minutePattern = "mm"
  }
}
```
