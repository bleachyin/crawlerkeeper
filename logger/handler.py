from logging import Handler

class ThriftHandler(Handler):
    """
    A handler class which writes logging records, appropriately formatted,
    to a stream. Note that this class does not close the stream, as
    sys.stdout or sys.stderr may be used.
    """

    def __init__(self, stream=None):
        """
        Initialize the handler.

        If stream is not specified, sys.stderr is used.
        """
        Handler.__init__(self)
        self._thrift_client = None
#         if stream is None:
#             stream = sys.stderr
#         self.stream = stream
        
    def set_thrift_client(self, thrift_client):
        self._thrift_client = thrift_client

#     def flush(self):
#         """
#         Flushes the stream.
#         """
#         if self.stream and hasattr(self.stream, "flush"):
#             self.stream.flush()

    def emit(self, record):
        """
        Emit a record.

        If a formatter is specified, it is used to format the record.
        The record is then written to the stream with a trailing newline.  If
        exception information is present, it is formatted using
        traceback.print_exception and appended to the stream.  If the stream
        has an 'encoding' attribute, it is used to determine how to do the
        output to the stream.
        """
        try:
            msg = self.format(record)
            fs = "%s\n"
            if self._thrift_client:
                self._thrift_client.logo(record.levelno,fs % msg)
            else:
                raise Exception('thrift_client must be none you need to use [add_thrift_client] to add client')
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as ex:
            self.handleError(record)