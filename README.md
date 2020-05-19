# Data Culpa Validator Getting Started

Welcome to Data Culpa Validator, a data quality monitoring solution for all your data pipelines. You can use Data Culpa Validator to ensure the proper long-running operation of data pipelines, whether those pipelines are supporting AI analysis, BI reporting, ETL operations, or some other data-intensive process. Validator captures inputs from upstream, from data pipeline output, and from specific values used in the pipeline's operation. Validator automatically raises alerts when it detects anomalies that could jeopardize results. Validator makes it easy for you to compare operations from present to past and to visualize changes in operations over time across a variety of dimensions: runtime throughput, schema changes, and even type and value changes within fields.
 
Data Culpa Validator is built for flexible operations and scale. You can push data from your pipeline into Validator asynchronously for high-throughput operation and post facto analysis, or you can call Validator in synchronous mode to capture and react to problematic data before operations continue.




## Operation Overview 
 
Data Culpa Validator is distributed as an Ubuntu package that contains both the user interface and data processing code. Validator uses a Mongo database for its private cache of values.
 
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
                             pipeline_stage, 
                             pipeline_environment, 
                             data,
                             extra_metadata)
          …

The `pipeline_name`, `_stage`, and `_environment` values are all strings of your choosing. You may wish to call Validator from both test and production environments, for example, or at multiple stages in pipeline processing, such as "ingest", "post_cleaning", "outputs." These top-level metadata fields help to organize your dashboards and visualizations when analyzing the results.
 
The `validate_async` and `validate_sync` calls capture both the data you want Validator to analyze and any extra metadata that you want to tag on the data. Metadata might include your software version, memory usage, uptime, CPU load, or any other potentially-useful attributes.
 
The async call will immediately return after connecting to Validator. The client call will return a job_id that you can later query for to discover the job's status or to append additional metadata:
 
        dc.validate_update(job_id, metadata, is_finished)
 
When monitoring data quality, it can be useful to know the approximate run time for your pipeline. You can mark a job (specified as job_id) as finished by setting is_finished to true. Once the job finishes, Validator calculates the run time for the pipeline, which can be useful to resolving errors. Validator can also mark time offsets when using the pipeline_stage fields, so that you can understand times of processing through the different stages of pipeline operation.

### Validation Result

Pushing a batch of records into Validator currently provides a strong hint of a data window to process. The processing of the data's consistency is compared to recently observed data; we may adjust windowing and history comparison windows, or offer them as parameters, based on customer feedback.

Calling `validate_sync` or `validate_update(..., is_finished=True)` will kick off consistency processing on the data set and return a structure with the following members:

    input_record_format -- string - the detected format of the data (e.g., json, csv)
    input_num_records   -- int - the number of records Validator processed/found

    schema_notes        -- null | [string] array -- a list of human-readable debugging notes about the input data's schema
    schema_similarity_list -- array of floats [0,1] -- one per data item, indicating the similarity of each input data record's schema to recently observed schemas that have been sent to Validator.
    schema_confidence   -- [0,1] float - the confidence Validator has in the object schema; this is the average of schema_similarity_list
    
    dupe_entries -- int - the number of data records that were exact duplicates of previously-seen records. This is useful for identifying when an upstream data feed is sending stale records.
    dupe_warnings -- string array - a list of human-readable debugging notes about any duplicates.
    
    record_value_confidence -- array of floats [0,1] -- one per data item, indicating whether we believe the actual value of a given record
    record_value_notes -- human-readable debugging notes
    
    field_value_confidence - dictionary of fields across all the records and our confidence that the values are good across the records.
    
    processing_time - float - wall-clock seconds that Validator took to process the record set
    
    internal_error - boolean - True if Validator had an internal error while processing the data. If this is set, please contact Data Culpa support at hello@dataculpa.com
    internal_error_notes - array of strings - 


### Data Queueing

The usual `validate_*` calls assume a batch of data is ready to be processed. This is great for jobs that are processing batches of new data, but it's not useful for one-offs, such as an event that dispatches and wants Validator's interpreation on a single record. Validator provides a queuing set of calls, both async and synchrnous for this mode of operation:

    dc.queue_record(record,
                    pipeline_name, 
                    pipeline_stage, 
                    pipeline_environment, 
                    extra_metadata) --> { "queue_count": 1, "queue_start": <datetime of first element> }
     
    dc.queue_interim_validate(pipeline_name, 
                              pipeline_stage, 
                              pipeline_environment) --> { returns a validation record of the items in the queue so far without commiting the queued data }
		    
    dc.queue_commit(pipeline_name, 
                    pipeline_stage, 
                    pipeline_environment) --> { <validation record> }

Note that only one queue may be operational at a time for a given pipeline name + stage + environment combination.

We welcome feedback on this feature.

### Logging into the Validator UI

To view the operations of your pipeline, login to the Validator UI.  You can see the status of your Validator installation by running the command:

    $ dcsh --status

The dcsh program is an interaction shell that simplifies management of the Validator software, letting you test the Mongo configuration, query for the status of the Validator services, and so on.

The --status command will print out the running services and such:

	
    $ dcsh --status
    Data Culpa Validator is running.
    UI: http://localhost:7777/

Open the UI via a web browser. Validator uses Okta for authentication, even with your local installation. 


### Pipeline Dashboards

The pipeline dashboards page provides a high level view of pipeline activity within your Validator instance. The main graph shows the number of records processed and when, going back 14 days. Initially the graph will be sparse until you have some running history with Validator.

"You don’t have any pipelines yet; learn how to set one up now!"

### Drilling into a Specific Pipeline

Once you have a pipeline connecting to Validator and pushing data, the data are immediately available in the pipeline screen to review.

You can perform the following operations to evaluate pipeline performance:
* View records throughput over time
* View schema changes over time.
  * Compare schema layouts to others in a multi-way diff
* View value changes over time (min, max, avg)
* Compare multiple time periods by manually selecting time offsets or keying on metadata or field data to select windows for comparison.
