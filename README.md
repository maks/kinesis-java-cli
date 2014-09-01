A simple command line client useful for testing fetching data from AWS Kinesis.

This is really nothing more than the example code from 
[Amazons Kinesis developer guide](http://docs.aws.amazon.com/kinesis/latest/dev/introduction.html)
wrapped into a single Java class with some basic params.

## Usage

on OS's with a posix-y shell use:
```
./kinesis -h
```

## Limitations:

* only supports reading data from a single (first) shard


