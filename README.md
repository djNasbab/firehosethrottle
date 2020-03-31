# firehosethrottle
Simple Demo for Firehose Throttling

## abstract

Demos is to showcase how to handle async calls in aws java sdk2 and how to see if your messages towards firehose kinect are 
being throttled. If they are being throttled showcase the ways of handling this

### installing

```sh
$ git clone <<repo>>
$ cd firehosethrottle
$ mvn clean install 
```

### Configure

Configure the application.yaml with your sources, currently supporting S3 as a destination. 

### running

no docker yet... will be :) 

```sh
$ cd firehosethrottle/target
$ java -jar pusher-1.0.0-SNAPSHOT.jar
``` 
