#!/usr/bin/python
import traceback
import os.path, twitter, time, sys
from datetime import datetime
import tweepy
from mongo_connector import MongoLoader
from extra import parse_string_date, write_log
import configfile as cnf



class Crawler:
    def __init__(self):
        self.loader = MongoLoader(destination="tweets")

        if not os.path.isfile(cnf.KEYCONFIG):
            raise Exception("Crawler: keywords file not found")

        with open(cnf.KEYCONFIG, 'r') as f:
            keywords = [line.strip() for line in f.readlines()]
            if len(keywords) == 0:
                raise Exception("Crawler: Keywords are zero length.")
            self.query = " OR ".join(keywords)

        # self.users = set(self.loader.get_user_ids())
        # comment the next line
        self.users = set()
        self.user_buffer = []
        self.user_counter = 0
        self.tweet_buffer = []
        self.tweet_counter = 0


    def dump_data(self):
        if self.user_counter > 0:
            self.loader.store_users(self.user_buffer)
            self.user_buffer = []
            self.user_counter = 0
        if self.tweet_counter > 0:
            self.loader.store_tweets(self.tweet_buffer)
            self.tweet_buffer = []
            self.tweet_counter = 0


    def fix_tweet(self, tweet):
        if "created_at" in tweet:
            # parse created datetime if exists
            tweet["created_at"] = parse_string_date(tweet["created_at"])

        if "user" in tweet:
            tweet["user"]['created_at'] = parse_string_date(tweet["user"]['created_at'])
            user_object = tweet["user"]
            user_object["id"] = int(user_object["id"])
            tweet["user_id"] = user_object["id"]

            if user_object["id"] not in self.users:
                user_object["created_at"] = parse_string_date(user_object["created_at"])
                user_object["probe_at"] = datetime.now()
                self.users.add(int(tweet["user_id"]))
                self.user_buffer.append(user_object)
                self.user_counter += 1

        """Recursively check quoted status and retweeted_status of tweet object"""
        if "quoted_status" in tweet:
            tweet["quoted_status"] = self.fix_tweet(tweet["quoted_status"])
        if "retweeted_status" in tweet:
            tweet["retweeted_status"] = self.fix_tweet(tweet["retweeted_status"])
        return tweet


    def fix_user_v2(self, author_id, includes):
        print("storing user")
        if author_id not in self.users:
            for inc_user in includes["users"]:
                uid = int(inc_user["data"]["id"])
                if uid == author_id:
                    user_obj = inc_user["data"]
                    user_obj["id"] = author_id
                    if "created_at" in user_obj:
                        user_obj["created_at"] = parse_string_date(user_obj["created_at"])
                    self.users.add(user_obj["id"])
                    self.user_buffer.append(user_obj)
                    self.user_counter += 1
                    break


    def fix_tweet_v2(self, tweet_obj, includes,flag):

        # parse created datetime if exists
        if "created_at" in tweet_obj:
            tweet_obj["created_at"] = parse_string_date(tweet_obj["created_at"])

        # store user obj to user_buffer if exists
        if "author_id" in tweet_obj:
            tweet_obj["author_id"] = int(tweet_obj["author_id"])
            self.fix_user_v2(tweet_obj["author_id"], includes)

        if flag == 0:
            print("checking replies,quotes,retweets")
            # recursively check quoted, replied, retweeted tweets
            if "referenced_tweets" in tweet_obj:
                tweet_obj["type"] = tweet_obj["referenced_tweets"][0]["type"]
                tweet_obj["referenced_tweet_id"] = int(tweet_obj["referenced_tweets"][0]["id"])

                for inc_tweet in includes["tweets"]:
                    if inc_tweet["id"] == tweet_obj["referenced_tweet_id"]:
                        print('recursion')
                        self.fix_tweet_v2(inc_tweet.data, includes, flag=1)
                        break

        if flag == 0:
            print("checking mentions")
            # recursively check users mentions
            if "entities" in tweet_obj:
                if "mentions" in tweet_obj["entities"]:
                    mentions = []
                    for mention in tweet_obj["entities"]["mentions"]:
                        mentions.append(int(mention["id"]))
                        self.fix_user_v2(int(mention["id"]), includes)

                    tweet_obj["mentions"] = mentions

                del tweet_obj["entities"]

        self.tweet_buffer.append(tweet_obj)
        self.tweet_counter += 1


    def collect(self):

        try:
            # create authentication twitter.API
            client = tweepy.Client(bearer_token=cnf.bearer_token)

            # number of tweets per stream
            numtweet = 10
            search = client.search_recent_tweets(
                query="#ConspiracyTheories OR #Conspiracy",
                tweet_fields=["created_at", "text", "source", "referenced_tweets"],
                user_fields=["name", "username", "location", "verified", "description"],
                max_results=numtweet,
                expansions=['author_id', 'entities.mentions.username',
                            'in_reply_to_user_id', 'referenced_tweets.id',
                            'referenced_tweets.id.author_id'])



            # for each tweet collected by stream filter insert them into mongoDB collection
            for tweet in search.data:
                self.fix_tweet_v2(tweet.data, search.includes, flag=0)
                if self.tweet_counter > 9 or self.user_counter > 200:
                    print('dumping data')
                    # self.dump_data()


            print('End of first batch')

        except Exception as e:
            #stack = ''.join(traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__))
            stack = e
            write_log(f'Error occured: {e} and stack: {stack}')
            print(traceback.format_exc())
            time.sleep(60 * 30)

    def start(self):
        write_log('Crawler process started.')
        while (True):
            self.collect()
            write_log('Crawler process restarted.')
            time.sleep(8)


if __name__ =="__main__":
    instance = Crawler()
    instance.start()
