"""Python Script that collect user ids from mongo collection of v2 and collect for them v1 user object via twitter API"""

import tweepy, time
from datetime import datetime
from dateutil.parser import parse


class userV1object:

    def __init__(self, consumer_key, consumer_secret, access_token_key, access_token_secret):
        """Connect to Twitter API via O-Auth"""
        self.auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        self.auth.set_access_token(access_token_key, access_token_secret)
        self.api = tweepy.API(self.auth)

    def get_profile(self, uid):
        requests = 0
        while requests < 3:
            try:
                requests += 1
                uobject = self.api.get_user(uid)
                break
            except tweepy.error.TweepError as e:
                if "User has been suspended." in e.args[0][0]['message'] or "User not found." in e.args[0][0]['message']:
                    return {"ERROR": e.args[0][0]['message']}

                elif "Rate limit exceeded" in e.args[0][0]['message']:
                    print("Sleep time: Rate limit issue")
                    time.sleep(5 * 60)
                else:
                    print(e.args[0][0]['message'])
                    time.sleep(5)
                    continue

        if requests == 3 or uobject is None:
            return None

        """Extract json from the object and fix the created_at from text to datetime object and also create probe_at datetime"""
        uobject = uobject._json
        uobject['id'] = int(uobject['id'])
        uobject['created_at'] = parse(uobject['created_at']).replace(tzinfo=None) if type(
            uobject['created_at']) == str else uobject['created_at']
        uobject['probe_at'] = datetime.now()

        return uobject
