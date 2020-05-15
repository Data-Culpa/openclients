# Data Culpa Validator Getting Started

Welcome to Data Culpa Validator! You can use Data Culpa Validator to ensure proper long-running operation of your data pipelines by capturing inputs from upstream, by capturing outputs of code your pipeline runs, and even by capturing specific values used in the operation of the pipeline.  When things go wrong, Data Culpa Validator lets you easily compare operations from present to past and visualize the changes in these operations over time across a variety of dimensions: runtime throughput, schema changes, and even type and value changes within fields.
 
Data Culpa Validator is built for flexible operations and scale. You may push data from your pipeline into Validator asynchronously for high-throughput operation and post facto analysis, or you can call Data Culpa Validator in synchronous mode to capture and react to problematic data before operations continue.




## Operation Overview 
 
Data Culpa Validator is distributed as an Ubuntu package that contains both the user interface and data processing code. Validator uses a Mongo database for its private cache of values.
 
Validator includes an open source python client library that enables JSON-formatted HTTP REST calls to the Validator processing server; we expect to provide other IPC mechanisms in later releases (such as message queues) and welcome feedback on customers need for transports.
 
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

The `pipeline_name`, `_stage`, and `_environment` values are all strings of your choosing--you may wish to call Validator from both test and production environments, for example, or at multiple stages in pipeline processing, such as "ingest", "post_cleaning", "outputs"--and these top-level metadata fields help to organize your dashboards and visualizations when analyzing the results.
 
The `validate_async` and `validate_sync` calls capture both the data you want Validator to analyze and any extra metadata that you want to tag on the data.  Metadata might include your software version, memory usage, uptime, CPU load, or any other potentially-useful attributes.
 
The async call will immediately return after connecting to Validator and the client call will return a job_id that you can later query for status or append additional metadata:
 
        dc.validate_update(job_id, metadata, is_finished)
 
Marking the job_id as finished by setting is_finished to true will let Validator calculate an approximate run time for your pipeline, which can be an important attribute for correlating some times of errors.  Validator can also mark the time offsets when using the pipeline_stage fields so that you can understand times of processing through the different stages of pipeline operation.
 

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

The pipeline dashboards page provides a high level view of pipeline activity within your Validator instance. The main graph shows the number of records processed and when, going back 14 days.  Initially the graph will be sparse until you have some running history with Validator.

"You don’t have any pipelines yet; learn how to set one up now!"

### Drilling into a Specific Pipeline

Once you have a pipeline connecting to Validator and pushing data, the data are immediately available in the pipeline screen to review.

