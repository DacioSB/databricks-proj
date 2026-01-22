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