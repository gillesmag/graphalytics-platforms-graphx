# Graphalytics GraphX platform extension

[![Build Status](http://jenkins.tribler.org/buildStatus/icon?job=Graphalytics_GraphX_master_tester)](http://jenkins.tribler.org/job/Graphalytics_GraphX_master_tester/)


## Getting started

Please refer to the documentation of the Graphalytics core (`graphalytics` repository) for an introduction to using Graphalytics.


## GraphX-specific benchmark configuration

The `graphx` benchmark uses YARN version 2.4.1 or later (earlier versions have not been attempted) to deploy Spark. Before launching the benchmark, ensure Hadoop is running in either pseudo-distributed or distributed mode. Next, edit `config/platform.properties` and change the following settings:

 - `platform.graphx.job.num-executors`: Set to the number of Spark workers to use.
 - `platform.graphx.job.executor-memory`: Set to the amount of memory to reserve in YARN for each worker.
 - `platform.graphx.job.executor-cores`: Set to the number of cores available to each worker.
 - `platform.hadoop.home`: Set to the root of your Hadoop installation (`$HADOOP_HOME`).

