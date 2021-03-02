# Data Culpa Validator Getting Started

Welcome to Data Culpa Validator, a data quality monitoring solution for all your data pipelines. You can use Data Culpa Validator to ensure the proper long-running operation of data pipelines, whether those pipelines are supporting AI analysis, BI reporting, ETL and ELT operations, or some other data-intensive process. Validator captures inputs from upstream, from data pipeline output, and from specific values used in the pipeline's operation. Validator automatically raises alerts when it detects anomalies that could jeopardize results. Validator makes it easy for you to compare operations from present to past and to visualize changes in operations over time across a variety of dimensions: runtime throughput, schema changes, and even type and value changes within fields.
 
Data Culpa Validator is built for flexible operations and scale. You can push data from your pipeline into Validator asynchronously for high-throughput operation and post facto analysis, or you can call Validator in synchronous mode to capture and react to problematic data before operations continue.


## Operation Overview 
 
Data Culpa Validator is distributed as a Docker container that includes both the user interface and data processing code. Validator uses an internal database for its private cache of values.
 
Validator includes an open source python client library that enables JSON-formatted HTTP REST calls to the Validator processing server. We expect to provide other IPC mechanisms in later releases (such as message queues), and we welcome feedback from customers about other transports to support.
 
Pipeline endpoints authenticate using an API key + token mechanism in Validator, which you can secure using your existing secrets infrastructure for your pipeline.
 
Calling Validator from your pipeline is as simple as modifying the pipeline code to include the following:
 
    from dataculpa import DataCulpaValidator
    
    def run():   # your existing "pipeline run" function
          …
          data = [] # array of dictionaries suitable for JSON encoding
                    # if you want to send multiple data feeds to Validator,
                    # you can push those in via different pipeline_stage names
 
          dc = DataCulpaValidator(DataCulpaValidator.HTTP, yourServerHost, yourServerPort)
          dc.validator_async(pipeline_name, 
                             pipeline_environment, 
                             pipeline_stage, 
                             data,
                             extra_metadata)
          …

The `pipeline_name`, `_environment`, `_stage`, and `_version` values are all strings of your choosing. You may wish to call Validator from both test and production environments, for example, or at multiple stages in pipeline processing, such as "ingest", "post_cleaning", "outputs." These top-level metadata fields help to organize your dashboards and visualizations when analyzing the results. 

Note that `_name`, `_environment`, and `_stage` are used to select a specific pipeline; `_version` is merely top-level metadata and is not used as a selector.
 
The `validate_async` and `validate_sync` calls capture both the data you want Validator to analyze and any extra metadata that you want to tag on the data. Metadata might include your software version, memory usage, uptime, CPU load, or any other potentially-useful attributes.
 
The async call will immediately return after connecting to Validator. The client call will return a job_id that you can later query for to discover the job's status or to append additional metadata:
 
        dc.validate_update(job_id, metadata, is_finished)
 
When monitoring data quality, it can be useful to know the approximate run time for your pipeline. You can mark a job (specified as job_id) as finished by setting is_finished to true. Once the job finishes, Validator calculates the run time for the pipeline, which can be useful to resolving errors. Validator can also mark time offsets when using the pipeline_stage fields, so that you can understand times of processing through the different stages of pipeline operation.

### Validation Result

Validation results can be provided back to the pipeline by checking `validation_status(queue_id, wait_for_processing=True)` after commiting the queue (or sending a batch of data). Getting the results inline means that your pipeline can decide whether to proceed or push errors into other streams. You can also pass identifiers to some other process to check/monitor for results in your own watchdog system.

| Field Name | Type | Value Range | Description|
|------------|------|-------------|------------|
|input_record_format | string | json \| csv | the detected format of the data |
|input_num_records   |  int | >= 0 | the number of records Validator processed/found |
|schema_notes        | string array | null or list of values | a list of human-readable debugging notes about the input data's schema |
|schema_similarity_list | float array | [0,1] | one per data item, indicating the similarity of each input data record's schema to recently observed schemas that have been sent to Validator. |
|schema_confidence | float | [0,1] | the confidence Validator has in the object schema; this is the average of `schema_similarity_list` |
|dupe_entries | int | >= 0 | the number of data records that were exact duplicates of previously-seen records. This is useful for identifying when an upstream data feed is sending stale records. |
| dupe_warnings |string array | null or any | a list of human-readable debugging notes about any duplicates. |
|record_value_confidence | float array | [0,1] | one per data item (row), indicating overall confidence in the values of the record |
|record_value_notes | string array | null or a list | human-readable debugging notes |
| field_value_confidence | dict<string, float> | floats are [0,1] | dictionary of fields (columns) and our confidence that the values are good across the records. |
|processing_time | float | > 0.0 | wall-clock seconds that Validator took to process the record set |
|internal_error | boolean | T/F | True if Validator had an internal error while processing the data. If this is set, please contact Data Culpa support at hello@dataculpa.com |
|internal_error_notes | string array | null or a list | List of internal error descriptions

### Data Queueing

The usual `validate_*` calls assume a batch of data is ready to be processed. This is great for jobs that are processing batches of new data, but it's less convenient for one-offs, such as an event that dispatches and wants Validator's interpretion on a single record. Validator provides a queuing set of calls for this mode of operation:

    (queue_id, queue_count, queue_age) = 
    dc.queue_record(record,
                    pipeline_name, 
                    pipeline_environment="default", 
                    pipeline_stage="default", 
                    pipeline_version="default",
                    extra_metadata)
    
Returns the queue id, the number of items in the queue, and the age of the queue in seconds. The age value can be used to detect issues, such as an old queue that never got flushed with the `queue_commit()` call below
     
    dc.queue_interim_validate(queue_id) 
    
Returns a validation record of the items in the queue so far without commiting the queued data.
		    
    dc.queue_commit(queue_id) 
    
Commits the contents of the queue, erases the queue, and returns a validation record.

Note that only one queue may be operational at a time for a given pipeline name + stage + environment combination; calling `queue_record()` will append to an existing queue.


### Arbitrary Metadata

From a diagnostics perspective, your pipeline may want to include additional metadata with the queued data. For example, you might want to tie parameters, or number of records, error codes, or whatever with the queued data for viewing in Data Culpa later. You can include arbitrary metadata with the queue by calling `queue_metadata(queue_id, meta)` where meta is a dictionary of JSON-encodeable objects (strings, etc). We generally recommend keeping this fairly flat unless you have a strong need for some kind of hierarchy in retrieving this later.

### Batch Data

Validator supports batch data and streaming processing for sending large CSV files (for example).  In the client library, this is implement with the `load_csv_file()` call, which takes a path to a CSV file.

The `load_flat_array()` call can be used to load 2D arrays, whether they are Python native, pandas, or numpy arrays. In the current implementation, these are encoded as JSON and passed to the Validator service.

### Pipeline Configuration

You can configure alerts for pipeline data with a YAML file.

    import yaml
    config = yaml.load(file, yaml.SafeLoader)

    dc.configure_pipeline(pipeline_name, config=config)

Any client with permissions to edit the pipeline can load a new YAML file; the configuration only needs to be applied once. The configuration is viewable in the Validator UI.

### Logging into the Validator UI

To view the operations of your pipeline, login to the Validator UI.  You can see the status of your Validator installation by running the command:

    $ dcsh --status

The dcsh program is an interaction shell that simplifies management of the Validator software, letting you test the Mongo configuration, query for the status of the Validator services, and so on.

The --status command will print out the running services and such:

    $ dcsh --status
    Data Culpa Validator is running.
    UI: http://localhost:7777/

Open the UI via a web browser. Validator uses Okta for authentication, even with your local installation. 

### Create API Tokens for Pipeline Callers

In the UI, navigate to _Manager > API Users_ to set up new data pipeline users and generate access tokens that you can integrate into your secrets manager. We recommend creating a unique user for each pipeline.


### Pipeline Metadata

Validator pipeline interactions include an ability for your pipeline code to augment the data sent to Validator with metadata that may be useful for your analysis. You may wish to include source control revision information, runtime environment information, or other application-specific data.

For database and file tree monitoring, Validator includes "example pipelines" that include information about the database system and file system tree.

### Pipeline Dashboards

The pipeline dashboards page provides a high level view of pipeline activity within your Validator instance. The main graph shows the number of records processed and when, going back 14 days. Initially the graph will be sparse until you have some running history with Validator.

"You don’t have any pipelines yet; learn how to set one up now!"

By default, Validator auto-registers a new pipeline when a client connects with information about that pipeline. 

### Drilling into a Specific Pipeline

Once you have a pipeline connecting to Validator and pushing data, the data are immediately available in the pipeline screen to review.

You can perform the following operations to evaluate pipeline performance:
* View records throughput over time
* View schema changes over time.
  * Compare schema layouts to others in a multi-way diff
* View value changes over time (min, max, avg)
* Compare multiple time periods by manually selecting time offsets or keying on metadata or field data to select windows for comparison.


### Questions? Feedback?

We want to build products that people want to use. If you have feedback, suggestions, or feature requests, please get in touch by writing to hello@dataculpa.com or opening an issue in this GitHub repository.
