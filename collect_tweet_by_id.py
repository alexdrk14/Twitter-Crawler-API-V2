import requests, time
import extra.configfile as cnf


class CollectTweetsByID:
    def __init__(self, fields):
        self.tweet_fields = fields

    def create_url(self, tweets):
        ids = ",".join([f'{twid}' for twid in tweets])
        url = f'https://api.twitter.com/2/tweets?ids={ids}&{self.tweet_fields}'
        return url


    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """
        r.headers["Authorization"] = f'Bearer {cnf.bearer_token}'
        r.headers["User-Agent"] = "v2TweetLookupPython"
        return r


    def connect_to_endpoint(self, url):
        response = requests.request("GET", url, auth=self.bearer_oauth)
        #print(response.status_code)
        if response.status_code != 200:
            raise Exception(
                "Request returned an error: {} {}".format(
                    response.status_code, response.text
                )
            )
        return response.json()


    def collect_tweets(self, tweets):

        url = self.create_url(tweets)
        json_response = self.connect_to_endpoint(url)
        time.sleep(len(tweets) * 2)
        return json_response
        #return self.api.get_tweets(tweets, self.tweet_fields) 
