"""
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
"""


import httplib #Used for connection to OnlineMQ's server
from xml.sax.saxutils import escape, unescape # Escaping XML content
from xml.dom import minidom # Parsing and creating XML documents

# base64 encoding, with support for legacy base64 python module
try:
    from base64 import b64encode as b64encode
except:
    from base64 import encodestring as b64encode



class Callable:
    """
    Helper class used for creating class functions
    """
    def __init__(self, anycallable):
        self.__call__ = anycallable


class OmqException(Exception):
    """
    Generic OnlineMQ exception
    """
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)


class OmqServerException(OmqException):
    """
    Represents an exception returned by the server
    """
    def __init__(self,xml):
        'parse an error message XML returned by the server'
        doc = minidom.parseString(xml)
        error_tags = doc.getElementsByTagName('error') # get <error> tags
        if len(error_tags) > 0:
            self.error = error_tags.pop().firstChild.data #take last <error>
        else:
            self.error = 'Unknown server error'

    def __str__(self):
        return self.error


class XmlHelper:
    """
    Helper class for building and parsing XML documents
    """
    def str_as_bool(str_val):
        'convert string to boolean value'
        if str_val.lower() == 'true':
            return 1
        else:
            return 0
    str_as_bool = Callable(str_as_bool)

    def bool_as_str(bool_val):
        'convert a boolean value to string'
        if bool_val:
            return 'true'
        else:
            return 'false'
    bool_as_str = Callable(bool_as_str)

    def add_tag_with_value(root_tag,tag_name,tag_value,type=None,escape_xml=1):
        'add to the root tag - a tag with the given value and type attribute'
        if tag_value is None:
            return
        xml_doc = root_tag.ownerDocument
        xml_tag = xml_doc.createElement(tag_name)
        if type:
            xml_tag.setAttribute('type', str(type)) #set the type attribute
        text_content = str(tag_value)
        if escape_xml:
            text_content = escape(text_content) # escape XML chars
        xml_tag_value = xml_doc.createTextNode('%s' % (text_content))
        xml_tag.appendChild(xml_tag_value)
        root_tag.appendChild(xml_tag)
    add_tag_with_value = Callable(add_tag_with_value)

    def get_tag_content(doc,tag_name,unescape_xml=1):
        'get a tags value by tag name. looks at the type attr. default is string'
        tags = doc.getElementsByTagName(tag_name)
        if len(tags) > 0:
            tag = tags.pop() # Take the last occurance
            text_node = tag.firstChild
            if text_node:
                if len(text_node.data) > 0:
                    if tag.hasAttribute('type'):
                        type = tag.getAttribute('type')
                        if type == 'integer':
                            inner_text = int(text_node.data)
                        elif type == 'boolean':
                            inner_text =  XmlHelper.str_as_bool(text_node.data)
                        else:
                            # If type is not recognized, assume it's a string
                            inner_text = str(text_node.data)
                            if unescape_xml:
                                inner_text = unescape(inner_text)
                    else:
                        # If no type is given, assume it's a string
                        inner_text = str(text_node.data)
                        if unescape_xml:
                            inner_text = unescape(inner_text)
                    return inner_text
        return None
    get_tag_content = Callable(get_tag_content)


class OmqMessage:
    """
    Representation of an OnlineMQ message
    """
    # Message encoding types:
    ENC_UTF8 = 1
    ENC_CDATA = 2
    ENC_XML_ESCAPED = 3
    ENC_BASE64 = 4

    # Message body types
    BODY_TYPE_XML = 1
    BODY_TYPE_YAML = 2
    BODY_TYPE_JSON = 3
    BODY_TYPE_SDL = 4
    BODY_TYPE_CSV = 5
    BODY_TYPE_OTHER = 6

    def __init__(self,body=None,encoding=None,body_type=None,
                 priority=None,description=None,sender=None,
                 queue_id=None,mid=None):
        self.body = body
        if encoding:
            self.encoding = encoding
        else:
            # Default encoding: UTF-8
            self.encoding = OmqMessage.ENC_UTF8
        self.priority = priority
        self.body_type = body_type
        self.description = description
        self.sender = sender
        self.queue_id = queue_id
        self.id = mid

    def get_message_as_xml(self):
        'return the XML representation of this message object'
        self.xml = minidom.Document()
        msg_root = self.xml.createElement('message')
        XmlHelper.add_tag_with_value(msg_root,'body_encoding_id',self.encoding)
        XmlHelper.add_tag_with_value(msg_root,'priority',self.priority)
        XmlHelper.add_tag_with_value(msg_root,'body_type_id',self.body_type)
        XmlHelper.add_tag_with_value(msg_root,'description',self.description)
        # Only escape XML for UTF-8 and escaped XML types
        if self.encoding == OmqMessage.ENC_UTF8 or self.encoding == OmqMessage.ENC_XML_ESCAPED:
            XmlHelper.add_tag_with_value(msg_root,'body',self.body,escape_xml=1)
        else:
            XmlHelper.add_tag_with_value(msg_root,'body',self.body,escape_xml=0)
        # Build structure
        self.xml.appendChild(msg_root)
        return self.xml.toxml("UTF-8")

    def from_xml(xml_str):
        'build an OmqMessage object from an XML document. returns None on error'
        try:
            doc = minidom.parseString(xml_str)
            encoding = XmlHelper.get_tag_content(doc,'body_encoding_id')
            priority = XmlHelper.get_tag_content(doc,'priority')
            mtype = XmlHelper.get_tag_content(doc,'body_type_id')
            desc = XmlHelper.get_tag_content(doc,'description')
            sender = XmlHelper.get_tag_content(doc,'sender')
            qid = XmlHelper.get_tag_content(doc,'queue_id')
            mid = XmlHelper.get_tag_content(doc,'id')
            if encoding == OmqMessage.ENC_UTF8 or self.encoding == OmqMessage.ENC_XML_ESCAPED:
                # Escape XML content for messages with encodings: UTF-8, XML-Escaped
                body = XmlHelper.get_tag_content(doc,'body',unescape_xml=1)
            else:
                body = XmlHelper.get_tag_content(doc,'body',unescape_xml=0)
            return OmqMessage(body,encoding,mtype,priority,desc,sender,qid,mid)
        except:
            return None
    from_xml = Callable(from_xml)


class OmqQueue:
    """
    Representation of an OnlineMQ queue
    """
    def __init__(self,name,queue_manager_id,max_depth=None,max_msg_length=None,
                 send_enabled=1,receive_enabled=1,description=None,
                 qid=None,depth=None,visibility_timeout=None):
        self.id = qid
        self.name = name
        self.queue_manager_id = queue_manager_id
        self.max_depth = max_depth
        self.max_msg_length = max_msg_length
        self.send_enabled = send_enabled
        self.receive_enabled = receive_enabled
        self.description = description
        self.depth = depth
        self.visibility_timeout = visibility_timeout

    def get_queue_as_xml(self):
        'return the XML representation of this queue object'
        self.xml = minidom.Document()
        msg_root = self.xml.createElement('queue')
        XmlHelper.add_tag_with_value(msg_root,'description',self.description)
        XmlHelper.add_tag_with_value(msg_root,'id',self.id,'integer')
        XmlHelper.add_tag_with_value(msg_root,'max_depth',self.max_depth,'integer')
        XmlHelper.add_tag_with_value(msg_root,'max_message_length',self.max_msg_length,'integer')
        XmlHelper.add_tag_with_value(msg_root,'name',self.name)
        XmlHelper.add_tag_with_value(msg_root,'queue_manager_id',self.queue_manager_id,'integer')
        XmlHelper.add_tag_with_value(msg_root,'receive_enabled',XmlHelper.bool_as_str(self.receive_enabled),'boolean')
        XmlHelper.add_tag_with_value(msg_root,'send_enabled',self.send_enabled)
        XmlHelper.add_tag_with_value(msg_root,'visibility_timeout',self.visibility_timeout)
        # Build structure
        self.xml.appendChild(msg_root)
        return self.xml.toxml("UTF-8")

    def from_xml(xml_str):
        'build an OmqQueue object from an XML document. returns None on error'
        try:
            doc = minidom.parseString(xml_str)
            desc = XmlHelper.get_tag_content(doc,'description')
            qid = XmlHelper.get_tag_content(doc,'id')
            mdepth = XmlHelper.get_tag_content(doc,'max_depth')
            mlength = XmlHelper.get_tag_content(doc,'max_message_length')
            name = XmlHelper.get_tag_content(doc,'name')
            qmgrid = XmlHelper.get_tag_content(doc,'queue_manager_id')
            recenabled = XmlHelper.get_tag_content(doc,'receive_enabled')
            send_enabled = XmlHelper.get_tag_content(doc,'send_enabled')
            visibility = XmlHelper.get_tag_content(doc,'visibility_timeout')
            depth = XmlHelper.get_tag_content(doc,'depth')
            return OmqQueue(name,qmgrid,mdepth,mlength,
                            send_enabled,recenabled,desc,qid,depth,visibility)
        except Exception, e:
            raise OmqException(repr(e))
    from_xml = Callable(from_xml)


class OmqConnection:
    """
    Represents a connection to OnlineMQ.
    Since calls are made over HTTPS which is stateless,
    there are no connect() or disconnect() methods.
    each call to the server takes care of the connection on it's own.
    """
    HTTPS_ADDR = 'mq.onlinemq.com'; # OnlineMQ's address
    CONTENT_TYPE = 'application/xml' # used in the HTTP headers
    ACCEPTS = 'application/xml' # used in the HTTP headers
    URL_POSTFIX = '.xml' # append this to all URI's
    REST_VERSION = '1.0' # Server's REST version

    def __init__(self,user,password):
        self.user = user
        self.password = password
        self.credentials = self.get_basic_auth(self.user,self.password)

    def get_basic_auth(self,user,password):
        'return a base64 string for HTTP Basic Auth'
        basic_string = '%s:%s' % (user,password)
        based_string = b64encode(basic_string)
        credentials = based_string.strip('\n') # For legacy base64 module
        return 'Basic %s' % (credentials)

    def _append_postfix(self,path):
        'appends postfix to URL'
        return '%s%s' % (path,OmqConnection.URL_POSTFIX)

    def _handle_error(self,connection,response):
        'closes the HTTP connection and raises an OmqServerException'
        data = response.read()
        connection.close()
        raise OmqServerException(data)

    def _request(self,path,method='GET',body=None):
        'build the HTTP connection, setting appropriate headers'
        headers = {}
        headers['Content-Type'] = OmqConnection.CONTENT_TYPE
        headers['Accept'] = OmqConnection.ACCEPTS
        headers['Authorization'] = self.credentials
        headers['X-Rest-Interface-Version'] = OmqConnection.REST_VERSION
        connection = httplib.HTTPSConnection(OmqConnection.HTTPS_ADDR)
        connection.request(method,path,body,headers)
        return connection

    def open_transaction(self):
        'creates a new OnlineMQ transaction'
        base_path = '/transactions'
        path = self._append_postfix(base_path)
        method = 'POST'
        connection = self._request(path,method)
        response = connection.getresponse()
        if response.status < 400:
            response.read()
            # HTTP header containing the new transaction ID
            location = response.getheader('location')
            if location is not None:
                full_url = 'https://%s/%s/' % (OmqConnection.HTTPS_ADDR,base_path)
                # Transaction ID is 'stripped' out of the URL
                transaction_id = int(location.strip(full_url))
                return transaction_id
            else:
                return None
        else:
            self._handle_error(connection,response)

    def _transaction_action(self,transaction_id,action):
        'commit or rollback a transaction by ID'
        base_path = '/transactions/%d/%s' % (transaction_id,action)
        path = self._append_postfix(base_path)
        method = 'POST'
        connection = self._request(path,method)
        response = connection.getresponse()
        # HTTP: OK
        if response.status == 200:
            response.read()
            connection.close()
        else:
            self._handle_error(connection,response)

    def commit(self,transaction_id):
        'commit a transaction by ID'
        self._transaction_action(transaction_id,'commit')

    def rollback(self,transaction_id):
        'rollback a transaction by ID'
        self._transaction_action(transaction_id,'rollback')

    def send_message(self,queue_id, msg, transaction_id=None):
        'send an OmqMessage to the queue. with optional transaction support'
        base_path = '/queues/%d/messages' % (queue_id)
        path = self._append_postfix(base_path)
        if transaction_id:
            # Set the GET parameter for the transaction
            path = '%s?transaction_id=%d' % (path,transaction_id)
        connection = self._request(path,'POST',msg.get_message_as_xml())
        response = connection.getresponse()
        # HTTP: CREATED
        if response.status == 201:
            response.read()
            connection.close()
            return
        else:
            self._handle_error(connection,response)

    def receive_message(self,queue_id,transaction_id=None):
        'receive an OmqMessage from the queue. with optional transaction support'
        base_path = '/queues/%d/messages/receive' % (queue_id)
        path = self._append_postfix(base_path)
        path = base_path
        if transaction_id:
            path = '%s?transaction_id=%d' % (path,transaction_id)
        connection = self._request(path,'POST')
        response = connection.getresponse()
        # HTTP BAD REQUEST
        if response.status < 400:
            data = response.read()
            connection.close()
            return OmqMessage.from_xml(data)
        else:
            self._handle_error(connection,response)

    def get_queue(self,queue_id):
        'get the OmqQueue object for the given queue ID'
        base_path ='/queues/%d' % (queue_id)
        path = self._append_postfix(base_path)
        connection = self._request(path,'GET')
        response = connection.getresponse()
        # HTTP BAD REQUEST
        if response.status < 400:
            data = response.read()
            connection.close()
            return OmqQueue.from_xml(data)
        else:
            self._handle_error(connection,response)

    def get_queue_depth(self,queue_id):
        'returns the current depth of the given queue ID'
        try:
            queue = self.get_queue(queue_id)
            return queue.depth
        except:
            # Return None if operation failed
            return None
