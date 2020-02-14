import pickle

class Response(object):
    def __init__(self, resp_dict):
        self.url = resp_dict["url"]
        self.status = resp_dict["status"]
        self.error = resp_dict.get('error')
        try:
            self.raw_response = (
                pickle.loads(resp_dict["response"])
                if "response" in resp_dict else None)
        except TypeError:
            self.raw_response = None
        if self.raw_response:
            self.http_code = self.raw_response.status_code
            self.is_redirected = len(self.raw_response.history) > 0
            self.final_url = self.raw_response.url if self.is_redirected else ""
            self.raw_response.raise_for_status()
            self.http_headers = dict(self.raw_response.headers)