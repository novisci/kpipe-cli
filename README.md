# `kafka-datapipe`

A proof-of-concept project using Kafka to implement an event-based data pipeline performing ETLs of source data to CDF data.

The project is basically a toolkit which provides modular functions for use in ETL processes. It supports reading and writing to stdio, filesystem, Amazon S3, and Kafka topics. All operations are implemented as streams and are intended to be connected via pipe(), in a pipeline(), or via stdio for piping in shell scripts. The project provides a commandline utility `dpipe` which allows access to the datapipe functions for shell scripting, exploration, or testing.

Within the project are additional scripts used to provision AWS EC2 instances as members of a Kafka cluster.  See README.md under `./aws` for details.

## Sub-Folders

- `aws`: Contains bash scripts used by EC2 instances during instance provisioning. These get copied to S3 and are loaded by cluster instances when booting for the first time.
- `bin`: Utility bash scripts for deploying init scripts to S3, redeploying autoscaling groups, etc.
- `docker`: docker-compose script for a single-node kafka cluster (for local development)
- `schemas`: Example schema files describing fields of interest in source datasets
- `src`: NodeJS source files for the data pipline functions
  - `cmds`: Defines the main commands (using yargs) for the `dpipe` utility
  - `pipelines`: Provides various assemblies of stream operations implemented as a promise which executes a pipeline() and resolves when the pipeline completes
  - `reader`: Modules which provide sourcing of data from various storage. Each module returns a function which can be used to create Readable streams from the data source.
  - `sequences`: Provides assemblies of step-wise operations, usually pipelines, which execute in sequence. Used to group related operations together such as creating a topic, transferring data to the topic (using a pipeline), creating a new topic, performing some pipeline, etc.
  - `transform`: A collection of transform streams intended to perform data manipulation after data has been read into a stream (and before being written). The perform all sorts of functions such as JSON parse/stringify, conversion of source data rows into discrete events, progress indicators, value extraction, etc.
  - `writer`: Modules which allow for writing data to various destinations such as the filesystem, S3, Kafka topics, stdio. Same as readers, the module provides a function used to create Writable streams to the desired destination.
- `tests`: Various scripts used to test functionality _UNDER DEVELOPMENT_

## Stream Types

| Name | Type | Reads | Writes | Notes |
|--|--|--|--|--|--|
| `reader/buffer` | reaable | buffers | | |
| `reader/stdio` | readable | buffers | | |
| `reader/fs` | readable | buffers | | |
| `reader/s3` | readable | buffers | | |
| `reader/kafka` | readable | lines | | |
| `transform/gunzip` | duplex | buffers | | |
| `transform/delineate` | duplex | buffers | lines | _Split buffer on newlines_ |
| `transform/jsonparse` | duplex | lines | objects | _JSON => object_ |
| `transform/compact` | duplex | objects | objects | _Convert objects to arrays of values_ |
| `transform/head` | duplex | lines | lines | _Only process first N lines_ |
| `transform/progress` | duplex | objects<br/>buffer | objects<br/>buffer | _Emit `.` to stderr for every 10k objects <br/> Emit `.` to stderr for every 10k `\n` (newlines)_ |
| `transform/value` | duplex | objects | lines | _Extract object value property as a string_ |
| `transform/jsonstringify` | duplex | objects | lines | _object => JSON_ |
| `transform/lineate` | duplex | lines | buffers | _Join lines with newlines_ |
| `transform/gzip` | duplex | | buffers | |
| `writer/kafka` | writable | | lines | |
| `writer/s3` | writable | | buffers | |
| `writer/fs` | writable | | buffers | |
| `writer/stdio` | writable | | buffers | |
| `writer/buffer` | writable | | buffers | |

> _Lines are strings which are terminated by newlines. Streams operating on lines are assumed to be in object mode. Each chunk is a string (with the terminating newline removed)_

## Structure of data

The main data format transferred by the datapipe functions are assumed to be streaming JSON. That is, the data are organized as parseable JSON objects which are contained in a single line (non-prettified). Newlines in the stream mark the end of each JSON object. In this way, data may be streamed line by line as a string when simply transferring data, but can be parsed into a object when required.

Though some stream functions are agnostic to the underlying structure of the stream, the interpretation of the stream as topic events assumes particular structure, which can be one of the following:

### Object form

The event is serialized as a JSON object. If the object contains a property `key` at the root, then its value is used as a the topic key for a produced event.

```
{"prop1":"value1","key":"1","prop2":"value1_detail","prop3":["a","nested","array"]}
```

### Array form

The event is serialized as a JSON array. In this format, the values are distinguised by their order in the array. When this format is used, the first element of the array is assumed to be the topic key. (Un-keyed data should contain a `null` in the first element position)

```
["1","value1","value1_detail",["a","nested","array"],{"a":"nested","b":"object"}]
```

## Examples

> A note about IO: `stdout` is reserved for transmission of data. All messaging, status, and progress output is emitted to `stderr`

```
// Read from S3 and write to stdout
require('stream').pipeline(
    new (require('./reader))({ 
      type: 's3',
      region: 'us-east-1',
      bucket: 'a-bucket'
    })('path/to/object'),
    new (require('./writer'))({ type: 'stdio' })(),
    (err) => {
      if (err) {
        console.error(err)
        process.exit(-1)
      }
    }
  )
```

## `dpipe` utility

This commandline toolkit exposes the underlying functions of the datapipe as a shell command. In general, the `dpipe` data transfer operations either stream from `stdin` and write to some data location, or read form some data source and stream to `stdout`. Chaining these commands together allows to simple commandline access to functions of the datapipe. In addition, `dpipe` provides commands to manage the Kafka cluster and perform administrative functions.

### Example

```
# Create topic someTopic with 10 partitions
dpipe kafka create -p 10 someTopic 

# Copy from S3 and write it to a someTopic
dpipe read s3 us-east-1 source-bucket path/to/file | dpipe write kafka someTopic 

# Read 100 events from someTopic and write to local filesystem
dpipe read kafka -n 100 someTopic | dpipe write fs local/file

# Echo the first 10 events from someTopic to stdout
dpipe read kafka -n 10 someTopic
```