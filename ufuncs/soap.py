import uuid
import zeep


class InsolvenciesProvider(object):
    """
    """
    url = ''
    _wsa_nsa = 'http://schemas.xmlsoap.org/ws/2004/08/addressing'
    _url_to = ''
    _services_url = ''
    _base_headers = [
        ElementMaker(namespace=_wsa_nsa).MessageID('urn:uuid:{}'.format(uuid.uuid4())),
        ElementMaker(namespace=_wsa_nsa).To(_url_to)
    ]

    def __init__(self, username, password):
        """Initialize the client with a WS security header

        The history attribute can be use to extract the request and
        response from each call for debugging purposes.
        """
        self.history = HistoryPlugin()
        self.client = zeep.Client(wsdl="{}?WSDL".format(self.url),
                                  wsse=UsernameToken(username, password),
                                  plugins=[self.history])

    def __make_headers(self, service):
        """Complete the header for the addressing schema

        From the base headers we still need to add the specific service
        to be used in the action.

        Args:
            service (str): Name of the service to use.

        Returns:
            Complete heaers to add to the SOAP call
        """
        headers = self._base_headers.copy()
        action = ElementMaker(namespace=self._wsa_nsa)
        headers.append(action.Action(self._services_url + service))
        return headers

    def _make_soap_request(self, service, **kwargs):
        """Make request

        Kwargs represent the paramaters to be filled in the request.

        Args:
            service (str): Name of the service to use.

        Returns:
            XML response given by the server
        """
        headers = self.__make_headers(service)
        logging.info("Making insolvency request for service {}, with "
                     "keyword arguments {}".format(service, kwargs))
        element = self.client.service[service](_soapheaders=headers, **kwargs)
