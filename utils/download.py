import time
from threading import Lock

import cbor
import requests

from utils.response import Response

time_lock = Lock()


def download(url, config, logger=None):
    host, port = config.cache_server
    with time_lock:
        resp = requests.get(
            f"http://{host}:{port}/",
            params=[("q", f"{url}"), ("u", f"{config.user_agent}")])
        time.sleep(config.time_delay)
    if resp:
        return Response(cbor.loads(resp.content))
    logger.error(f"Spacetime Response error {resp} with url {url}.")
    return Response({
        "error": f"Spacetime Response error {resp} with url {url}.",
        "status": resp.status_code,
        "url": url})
