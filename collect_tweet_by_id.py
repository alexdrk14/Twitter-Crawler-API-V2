import requests
import os
import json
import extra.configfile as cnf


class CollectTweetsByID:
    def __init__(self):
        self.tweet_fields = "tweet.fields=created_at,attachments,author_id,context_annotations,conversation_id,edit_controls,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld&" \
                       "expansions=author_id,referenced_tweets.id,edit_history_tweet_ids,in_reply_to_user_id,attachments.media_keys,attachments.poll_ids,geo.place_id,entities.mentions.username,referenced_tweets.id.author_id&" \
                       "user.fields=created_at,description,entities,location,pinned_tweet_id,profile_image_url,protected,public_metrics,url,verified,withheld"


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
        print(response.status_code)
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
        return json_response
