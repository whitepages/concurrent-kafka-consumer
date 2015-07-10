
About
=====

Kafka message processing concurrency is limited by the partition count.
The high-level consumer breaks down as the partition count gets high.

This facts mean that if you have a really high thoughput, your message processing time
needs to be low, or the concurrency limit will put a limit on your rate.

You can work around this by putting another layer of concurrency on top of the partition,
and process multiple messages asynchronously. 

This works fine, but if you also care about making sure each message gets processed, you 
now have to track all the messages currently in flight, and have a mechanism for handling failure.

This project is intended to convert a single-threaded high-level kafka consumer into an asynchronous 
message processing API. All the details of kafka batch commits and single-threaded processing are 
hidden.

This is a work in progress, obviously.
