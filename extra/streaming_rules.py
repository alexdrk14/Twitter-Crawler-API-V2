"""--------------------------------------------------------------------------------------------------------------------
#   Streaming class that implement basic communication steps required by Twitter API.
Author: Alexander Shevtsov (shevtsov@ics.forth.gr)
--------------------------------------------------------------------------------------------------------------------"""
import json, requests, sys
from datetime import datetime


class Streaming:
    def __init__(self, bearer_token, com_type, rules, search_attributes, debug=False):
        self.debug = debug
        self.bearer_token = bearer_token
        self.known_com_types = {"stream": "v2FilteredStreamPython"}
        self.com_type = com_type
        self.rules = rules
        self.search_attributes = search_attributes
        if self.com_type not in self.known_com_types:
            raise Exception(f'Twitter communication Error: communication type is not in implemented list. Class can accept one of the following: {self.known_com_types.keys()}')


    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f'Bearer {self.bearer_token}'
        r.headers["User-Agent"] = self.known_com_types[self.com_type]
        return r


    def __get_rules(self):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream/rules", auth=self.bearer_oauth
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
            )

        if self.debug: print(f'Known Rules:{json.dumps(response.json())}')

        return response.json()


    def __delete_rules(self, rules):
        if rules is None or "data" not in rules:
            return None

        ids = list(map(lambda rule: rule["id"], rules["data"]))
        payload = {"delete": {"ids": ids}}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload
        )
        if response.status_code != 200:
            raise Exception(
                "Cannot delete rules (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        if self.debug: print(f'Removing Rules responce: {json.dumps(response.json())}')


    def __set_rules(self):
        stream_rules = [{"tag": tag, "value": self.rules[tag]} for tag in self.rules]

        payload = {"add": stream_rules}
        response = requests.post(
            "https://api.twitter.com/2/tweets/search/stream/rules",
            auth=self.bearer_oauth,
            json=payload,
        )
        if response.status_code != 201:
            raise Exception(
                "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
            )
        if self.debug: print(f'Set new Rules response: {json.dumps(response.json())}')

    def fix_rules(self):
        old_rules = self.__get_rules()
        self.__delete_rules(old_rules)
        self.__set_rules()

    def initiate_stream(self, callback_recieved):
        response = requests.get(
            "https://api.twitter.com/2/tweets/search/stream" + "?" + self.search_attributes,
            auth=self.bearer_oauth,
            stream=True,
        )
        if self.debug: print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Cannot get stream (HTTP {}): {}".format(
                    response.status_code, response.text
                )
            )
        for response_line in response.iter_lines():
            if response_line:
                json_response = json.loads(response_line)
                probe_time = datetime.now()
                if self.debug: print(f'{json.dumps(json_response, indent=2)}\n' + '-'*10)

                callback_recieved(json_response, probe_time)

                if self.debug:
                    sys.exit(-1)
