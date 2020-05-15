# python-client
Python client for calling Data Culpa services from data pipelines

# Data Culpa Validator Getting Started

Welcome to Data Culpa Validator! You can use Data Culpa Validator to ensure proper long-running operation of your data pipelines by capturing inputs from upstream, by capturing outputs of code your pipeline runs, and even by capturing specific values used in the operation of the pipeline.  When things do wrong, Data Culpa Validator lets you easily compare operations from present to past and visualize the changes in these operations over time across a variety of dimensions: runtime throughput, schema changes, and even type and value changes within fields.
 
Data Culpa Validator is built for flexible operations and scale. Your calls to push data from your pipeline into Validator may be asynchronous for high-throughput operation and post facto analysis, or you can call Data Culpa Validator in synchronous mode to capture and react to problematic data before operations continue.
