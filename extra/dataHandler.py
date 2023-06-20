"""--------------------------------------------------------------------------------------------------------------------
#   Streaming Crawling of Tweets using pre-deined diltering rules (hashtags, keywords , mentions).
#    Crawler manage to parse raw json data provided by V2 Twitter API and store proper data in mongoDB
#    Particular implementation allow flexible crawling based on Twitter API fields described in configuration file
Author: Alexander Shevtsov (shevtsov@ics.forth.gr)
--------------------------------------------------------------------------------------------------------------------"""
import extra.configfile as cnf

from datetime import datetime
from collections import Counter
from extra.getProfileV1 import userV1object
from extra.help_functions import write_log, user_fields_fix, tweet_fields_fix

class dataHandler:

    def __init__(self, dataLoader, handlerName, dbg=False, get_V1=False):
        self.debug = dbg

        self.loader = dataLoader
        self.handlerName = handlerName

        ####
        #self.users = set([]) if self.loader is None else set(self.loader.get_user_ids())
        #self.tweets = set([]) if self.loader is None else set(self.loader.get_parsed())

        """Keep empty without loading since some of the collections contain millions of entries
        Alternatively we will ask for unseen elements the mongo and pay the latency every time we see new tweets or users"""
        self.users = set([])
        self.tweets = set([])

        self.seen_tw_references = set([])
        self.user_buffer = []
        self.tweet_buffer = []

        self.get_V1 = get_V1
        if get_V1:
            self.V1 = userV1object(cnf.consumer_key, cnf.consumer_secret, cnf.access_token_key, cnf.access_token_secret)

        self.limits = cnf.LIMITS


    """Should be called every time when main filtered stream get response from Twitter API"""
    def push(self, force=False):
        """Store collected users and tweets in db"""
        if len(self.user_buffer) > self.limits["users"] or len(self.tweet_buffer) > self.limits["tweets"] or force:
            msg = f"Handler {self.handlerName}: pushing data into mongo users:{len(self.user_buffer)} tweets:{len(self.tweet_buffer)} {datetime.now()}"
            print(msg)
            write_log(msg)

            self.loader.store_users(self.user_buffer)
            self.user_buffer = []

            self.loader.store_tweets(self.tweet_buffer)
            self.tweet_buffer = []

    def parse_tweet_objects(self, tweets, matching_rules, probe_time):
        seen_tw_references = set()

        for tweet in tweets:
            """If tweet already exists in DB just skip it (It probably will happen only while we pare reference 
            tweets and not the filtered stream, since filtered stream bring only new tweets)"""
            if int(tweet['id']) in self.tweets:
                continue

            """Check if the tweet id is in mongo collection and we just haven't seen this item yet"""
            if self.loader.has_tweet(tweet["id"]):
                """If the tweet is already in our database, 
                just store the tweet id in local set in order to avoid requesting again this id.
                Without pushing the tweet into buffer in order to avoid duplicates."""
                self.tweets.add(int(tweet["id"]))
                continue

            """Fix fields of tweet Object"""
            tweet = tweet_fields_fix(tweet, probe_time)

            tweet['matching_rules'] = matching_rules

            if "conversation_id" in tweet:
                seen_tw_references.add(tweet["conversation_id"])#(tweet["conversation_id"], tweet['matching_rules'], tweet["id"]))

            if "referenced_tweets" in tweet:
                for item in tweet["referenced_tweets"]:
                    seen_tw_references.add(item["id"])#(item["id"], tweet['matching_rules'], tweet["id"]))

            """Store tweet data (id, object) in our buffer in order to push them later into DB storage"""
            self.tweets.add(tweet['id'])
            self.tweet_buffer.append(tweet)

        return seen_tw_references

    def parse_user_objects(self, users_list, matching_rules, probe_time):

        for user_object in users_list:

            """If we already have this user ignore the object"""
            if int(user_object["id"]) in self.users:
                continue

            """Check if the user id is in mongo collection and we just haven't seen this item yet"""
            if self.loader.has_user(user_object["id"]):
                """If the user is already in our database, 
                just store the user id in local set in order to avoid requesting again this id.
                Without pushing the tweet into buffer in order to avoid duplicates."""
                self.users.add(int(user_object["id"]))
                continue

            user_object = user_fields_fix(user_object, probe_time)

            if matching_rules is None:
                authors_tweets = [tweet for tweet in self.tweet_buffer if tweet['author_id'] == user_object["id"]]
                if len(authors_tweets) > 1:
                    """Find most frequent matching rule"""
                    matching_rules = Counter([tweet['matching_rules'] for tweet in authors_tweets]).most_common(1)[0][0]
                else:
                    matching_rules = authors_tweets[0]['matching_rules']

            user_object['matching_rules'] = matching_rules


            if self.get_V1:
                v1_user_object = self.V1.get_profile(user_object['id'])

                if v1_user_object is not None:
                    user_object["v1_profile"] = v1_user_object

            self.users.add(user_object["id"])
            self.user_buffer.append(user_object)


