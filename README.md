#Flume interceptor for Filebeat events

Objective behind writing this Interceptor is to:
  - Make the Apache Flume pipeline work with both Goibibo's Kafka-Tailer and Filebeat inputs.

How?
  - The event Kafka tailer produces is a simple logline while Filebeat produces event encoded in JSON.
  - The first character of the Filebeat input is always '{' ( Because it is JSON ).
  - Interceptor will keep the Kafka-tailer input unmodified. 
  - For Filebeat input
    - It will parse json and convert it to below format,
       - beat.name + '\t' + message + '\t' + beat.hostname
    - It will also add timestamp header from the @timestamp field of filebeat event.

Config:
   - The interceptor can be configured in flume config file as,
``` Config
a1.sources.src1.interceptors = interceptor1
a1.sources.src1.interceptors.interceptor1.type = com.goibibo.filebeat_flume_interceptor.FilebeatInterceptor
#Set preserveExisting to true, if you want to preseve the existing timestamp header
a1.sources.src1.interceptors.interceptor1.preserveExisting = false
```