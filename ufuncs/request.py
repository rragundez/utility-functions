import requests
import asyncio
import logging

@asyncio.coroutine
def post_request_async(session, url, request):
    with aiohttp.Timeout(30):
        response = yield from session.post(url, json=request)
        try:
            return (yield from response.json())
        except Exception as e:
            print("Error")
            raise(e)
        finally:
            yield from response.release()


def post_request(*, url, request):
    """Make an API post call and return response as dictionary.

    Exceptions, logging, messages and other things should be handel
    in here.

    Args:
        request (dict):

    Returns:
        response (dict): Response from the API.
    """
    logging.info("url: {}, request: {}".format(url, request))
    if isinstance(request, dict):
        response = requests.post(url, json=request)
    elif isinstance(request, str):
        response = requests.post(url, data=request)
    else:
        msg = ("`request` argument expeted as `dictionary` or "
               "`string`, received `{}`".format(type(request).__name__))
        logging.error(msg)
        raise TypeError(msg)
    response.raise_for_status()  # raise exception if call not succesfull
    return response
