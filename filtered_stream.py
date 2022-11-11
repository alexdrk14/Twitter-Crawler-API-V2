"""--------------------------------------------------------------------------------------------------------------------
#   Streaming Crawling of Tweets using pre-deined diltering rules (hashtags, keywords , mentions).
#    Crawler manage to parse raw json data provided by V2 Twitter API and store proper data in mongoDB
#    Particular implementation allow flexible crawling based on Twitter API fields described in configuration file
Author: Alexander Shevtsov (shevtsov@ics.forth.gr)
--------------------------------------------------------------------------------------------------------------------"""


import sys
import extra.configfile as cnf

from datetime import datetime
from collect_tweet_by_id import CollectTweetsByID
from extra.mongo_connector import MongoLoader
from extra.streaming_rules import Streaming
from extra.help_functions import write_log, user_fields_fix, tweet_fields_fix, get_keywords

class StreamCrawler:

    def __init__(self, dbg=False):
        self.debug = dbg

        """Api in order collect specific tweets by their tweet ID"""
        self.tweetCollectAPI = CollectTweetsByID()

        self.search_attributes = cnf.GETFILEDS

        if self.debug:
            """In case of debug mode we dont need the mongo DB and we output the collected tweets into terminal stdout"""
            self.loader = None
            self.users = set([])
            self.tweets = set([])
        else:
            self.loader = MongoLoader(destination="tweets")
            self.users = set(self.loader.get_user_ids())
            self.tweets = set(self.loader.get_parsed())

        self.seen_tw_references = set([])
        self.user_buffer = []
        self.user_counter = 0
        self.tweet_buffer = []
        self.tweet_counter = 0

        """Initiate execution since instance is ready"""
        self.start()


    def dump_data(self):
        """Store collected tweets in DB"""
        if self.tweet_counter > 0:
            unknown_tweets = self.seen_tw_references - self.tweets
            if len(unknown_tweets) != 0:
                """Require to collect tweets that appear in our dataset as reference only"""
                json_response = self.tweetCollectAPI.collect_tweets(unknown_tweets)
                if "includes" in json_response:
                    if "tweets" in json_response["includes"]: self.parse_tweet_objects(json_response["includes"]["tweets"])
                    if "users" in json_response["includes"]: self.parse_user_objects(json_response["includes"]["users"])

            self.loader.store_tweets(self.tweet_buffer)
            self.tweet_buffer = []
            self.tweet_counter = 0
            self.seen_tw_references = set([])

        """Store collected users in db"""
        if self.user_counter > 0:
            self.loader.store_users(self.user_buffer)
            self.user_buffer = []
            self.user_counter = 0


    def parse_user_objects(self, users_list, probe_time):
        for user_object in users_list:
            if user_object["id"] in self.users:
                continue

            user_object = user_fields_fix(user_object, probe_time)

            self.users.add(user_object["id"])
            self.user_buffer.append(user_object)
            self.user_counter += 1


    def parse_tweet_objects(self, tweets, probe_time):

        for tweet in tweets:
            if int(tweet["id"]) in self.tweets:
                continue

            """Fix fields of tweet Object"""
            tweet = tweet_fields_fix(tweet, probe_time)

            if "conversation_id" in tweet:
                self.seen_tw_references.add(tweet["conversation_id"])

            if "referenced_tweets" in tweet:
                for item in tweet["referenced_tweets"]:
                    self.seen_tw_references.add(item["id"])

            """Store tweet data (id, object) in our buffer in order to push them later into DB storage"""
            self.tweets.add(tweet["id"])
            self.tweet_buffer.append(tweet)
            self.tweet_counter += 1

    """Callback function, that is called by streamer when new data is received"""
    def get_streaming_data(self, json_response, probe_time):
        if "includes" not in json_response:
            return
        if "tweets" in json_response["includes"]: self.parse_tweet_objects(json_response["includes"]["tweets"], probe_time)
        if "users" in json_response["includes"]: self.parse_user_objects(json_response["includes"]["users"], probe_time)

        if self.debug and self.tweet_counter >= 5:
            print(f'End of tweets in debug mode')
            sys.exit(-1)

        if self.tweet_counter > 200 or self.user_counter > 200:
            msg = f'Stream Crawler: dump data into mongo users:{self.user_counter} tweets:{self.tweet_counter}'
            print(f'{datetime.now()} ' + msg)
            write_log(msg)
            self.dump_data()


    def start(self):
        write_log('Crawler process started.')
        print('Starting stream')
        """Load keywords for rule description and connect to end point with updating of data collection rules"""

        self.streamer = Streaming(cnf.bearer_token, "stream",
                                  get_keywords(), self.search_attributes,
                                  debug=self.debug)

        self.streamer.fix_rules()
        self.streamer.initiate_stream(self.get_streaming_data)


if __name__ == "__main__":
    instance = StreamCrawler(dbg=False)
