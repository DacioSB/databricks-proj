# Thinking

## EventPublisher

* Receives a connection string and the eh name
* then proceeds to create an EventHubProducerClient passing conn_str and eventhub_name to it.
* Send batch ops
  * receives the readings which is a list of TrafficReading
  * creates an event batch
  * iterate over readings, create event data from reading and try to add to batch
  * if unsuccessful in adding to batch (batch full), send the current batch and create a new one, then add the event data to the new batch
  * after iterating over all readings, if there are any remaining in the batch, send it
* close the producer client

## Main

* Create connection string for Event Hub and eventhubname
* initialize citygrid with city_center and grid_size
* initialize traffic sensor simulator with citygrid
* initialize event publisher with conn_str and eventhubname
* try and control the iteration
* iterate forever, increase the iteration count
  * for each intersection 
    * generate readings from the traffic sensor simulator 
    * append
  * publish batch
  * stats
  * sleep for some time (30 secs)
* except KeyboardInterrupt
  * print exiting message
* finally
  * close event publisher