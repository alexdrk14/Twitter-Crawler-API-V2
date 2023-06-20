import requests, sys
import extra.configfile as cnf

from datetime import datetime
from collect_tweet_by_id import CollectTweetsByID
from extra.mongo_connector import MongoLoader
from extra.help_functions import write_log, user_fields_fix, tweet_fields_fix


search_url = "https://api.twitter.com/2/tweets/search/all"

# Optional params: start_time,end_time,since_id,until_id,max_results,next_token,
# expansions,tweet.fields,media.fields,poll.fields,place.fields,user.fields


query_params = {'query':    "SOME QUERY TEXT",
                'tweet.fields': "created_at,attachments,author_id,context_annotations,conversation_id,edit_controls,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld",
                'expansions': "author_id,referenced_tweets.id,edit_history_tweet_ids,in_reply_to_user_id,attachments.media_keys,attachments.poll_ids,geo.place_id,entities.mentions.username,referenced_tweets.id.author_id",
                'user.fields': "created_at,description,entities,location,pinned_tweet_id,profile_image_url,protected,public_metrics,url,verified,withheld",
                'start_time': datetime(START_DATE_YEAR,
                                       START_DATE_MONTH,
                                       START_DATE_DAY).astimezone().isoformat()}



def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """
    r.headers["Authorization"] = f"Bearer {cnf.bearer_token}"
    r.headers["User-Agent"] = "v2FullArchiveSearchPython"
    return r


def connect_to_endpoint(params):
    response = requests.request("GET", search_url, auth=bearer_oauth, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()

class ArchiveAPI:

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
        self.main()


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



    def parse_user_objects(self, users_list, probe_time=None):
        for user_object in users_list:
            if user_object["id"] in self.users:
                continue

            user_object = user_fields_fix(user_object, probe_time)

            self.users.add(user_object["id"])
            self.user_buffer.append(user_object)
            self.user_counter += 1


    def parse_tweet_objects(self, tweets, probe_time=None):

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



    def main(self):
        json_response = connect_to_endpoint(query_params)

        self.get_streaming_data(json_response, datetime.now())
        if self.tweet_counter != 0 or self.user_counter != 0:
            msg = f'Stream Crawler: dump data into mongo users:{self.user_counter} tweets:{self.tweet_counter}'
            print(f'{datetime.now()} ' + msg)
            write_log(msg)
            self.dump_data()



if __name__ == "__main__":
    item = ArchiveAPI() 
    
