
OnlineMQ Python API

Version: 1.0
License: LGPLv3

A python API for OnlineMQ's services.
This library uses httplib for connecting over HTTPS to onlineMQ.
This release includes a basic implementation of OnlineMQ's RESTful
Web-Service API.

The library defines the following classes:
- OmqException:
  This class represents exceptions that occur locally in the API.

- OmqServerException
  Inherits from OmqException. represents an error response from the server.

- OmqMessage
  Represents an OnlineMQ message (with body, encoding type, body type,
  priority, description, sender,queue id and message id)
  queue id and message ID are populated only for messages retrieved from
  the server, and should not be populated for newly created messages.

- OmqQueue
  Represents an OnlineMQ queue. contains the following fields:
  name, queue manager id, max depth, max message length, send enabled,
  receive enabled, description, queue id, current depth
  and visibility timeout.
  Currently the API doesn't support creation of new queues. only retrival.


- OmqConnection
  This class represents a connection to the OnlineMQ server.
  methods:
  - open_transaction()
    returns a new transaction ID from the server
  - commit(transaction_id)
    commits the given transaction ID
  - rollback(transaction_id)
    rolls back the given transaction ID
  - send_message(queue_id, msg,[transaction_id])
    sends a message (OmqMessage object) to the specified queue id.
    if transaction_id is specified - this operation will be part of the
    transaction.
  - receive_message(queue_id,[transaction_id])
    receive a message (FIFO) from the queue specified by queue_id.
    if transaction_id is specified, this operation will be part of the
    transaction. an OmqServerException may be raised if the queue is empty.
  - get_queue(queue_id)
    returns an OmqQueue object, describing the given queue_id.
  - get_queue_depth(queue_id)
    returns the current depth of the queue (0 means that the queue is empty)

  all methods may raise OmqException or OmqServerException if a problem
  occurs during call.

example usage:

>>> connection = OmqConnection('myuser@mail.com','mypass')
>>> my_queue_id = 30
>>> transaction_id = connection.open_transaction()
>>> my_message = OmqMessage('this is the message body!',encoding=OmqMessage.ENC_UTF8)
>>> connection.send_message(my_queue_id,my_message,transaction_id)
>>> connection.commit(transaction_id)
>>>

