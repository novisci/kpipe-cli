# `kpipe-cli`


## `kpipe` utility

This commandline toolkit exposes the underlying functions of the datapipe as a shell command. In general, the `kpipe` data transfer operations either stream from `stdin` and write to some data location, or read form some data source and stream to `stdout`. Chaining these commands together allows to simple commandline access to functions of the datapipe. In addition, `kpipe` provides commands to manage the Kafka cluster and perform administrative functions.

### Example

```
# Create topic someTopic with 10 partitions
kpipe kafka create -p 10 someTopic 

# Copy from S3 and write it to a someTopic
kpipe read s3 us-east-1 source-bucket path/to/file | kpipe write kafka someTopic 

# Read 100 events from someTopic and write to local filesystem
kpipe read kafka -n 100 someTopic | kpipe write fs local/file

# Echo the first 10 events from someTopic to stdout
kpipe read kafka -n 10 someTopic
```