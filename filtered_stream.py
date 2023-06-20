"""--------------------------------------------------------------------------------------------------------------------
#   Streaming Crawling of Tweets using pre-deined diltering rules (hashtags, keywords , mentions).
#    Crawler manage to parse raw json data provided by V2 Twitter API and store proper data in mongoDB
#    Particular implementation allow flexible crawling based on Twitter API fields described in configuration file
Author: Alexander Shevtsov (shevtsov@ics.forth.gr)
--------------------------------------------------------------------------------------------------------------------"""
import traceback
import sys, time, signal
import extra.configfile as cnf

from datetime import datetime
from extra.dataHandler import dataHandler
from extra.streaming_rules import Streaming
from extra.mongo_connector import MongoLoader
from collect_tweet_by_id import CollectTweetsByID
from extra.help_functions import write_log, get_keywords


class StreamCrawler:

    def __init__(self, dbg=False):
        self.debug = dbg

        """Api in order collect specific tweets by their tweet ID"""
        self.tweetCollectAPI = CollectTweetsByID(cnf.GETFILEDS)

        self.search_attributes = cnf.GETFILEDS

        """Create data handlers for each matching rule"""
        self.handlers = { match: dataHandler(dataLoader=MongoLoader(cnf.DESTINATIONS[match][0], cnf.DESTINATIONS[match][1]), handlerName=match, dbg=dbg, get_V1=match in cnf.V1_OBJECTS) for match in cnf.DESTINATIONS}


    """Collect from TwitterAPI tweets that appears in the original filtered stream responce as the reference tweets"""
    def collect_the_references(self, ref_tweets, matching_rules):

        """Require to collect tweets that appear in our dataset as reference only"""
        json_response = self.tweetCollectAPI.collect_tweets(ref_tweets)
        json_response["matching_rules"] = matching_rules

        """Parse the references that seen from previous round. 
        If we collect references that has their own references to other unseen tweets 
        we also should collected them too"""
        ref_tweets, _ = self.parse_response(json_response, probe_time=datetime.now())

    """Parse the json response from Twitter API v2"""
    def parse_response(self, json_response, probe_time):
        seen_tw_references = set()

        if "includes" not in json_response or "matching_rules" not in json_response:
            return seen_tw_references, None
        
        print(f"{datetime.now()} resp. recieved")
        matching_rules = json_response["matching_rules"] if type(json_response["matching_rules"]) == str else json_response["matching_rules"][0]["tag"]

        """Parse the tweet objects from response"""
        if "tweets" in json_response["includes"]:
            seen_tw_references = self.handlers[matching_rules.split("_")[0]].parse_tweet_objects(
                json_response["includes"]["tweets"],
                matching_rules=matching_rules,
                probe_time=probe_time)

        """Parse the user objects from response"""
        if "users" in json_response["includes"]:
            self.handlers[matching_rules.split("_")[0]].parse_user_objects(
                json_response["includes"]["users"],
                matching_rules=matching_rules,
                probe_time=probe_time)

        return seen_tw_references, matching_rules

    """Callback function, that is called by streamer when new data is received"""
    def get_streaming_data(self, json_response, probe_time):
        """Parse the original stream response"""
        seen_tw_references, matching_rules = self.parse_response(json_response, probe_time)

        """In case of the reference tweets, we should collect the referenced tweet"""
        if len(seen_tw_references) != 0:
            self.collect_the_references(seen_tw_references, matching_rules)

        if self.debug:
            print(f'End of tweets in debug mode')
            sys.exit(-1)

        """Call handler for particular matching rule. The handler will store parsed data and decide when push the data into collections.
        Data will be stored when internal buffers (tweets or users) will be full to dump the data into MongoDB"""
        self.handlers[matching_rules.split("_")[0]].push()

    """Force push of the handlers data into mongo in case of error/exception/kill signal"""
    def force_push_of_data(self):

        for handler_name in self.handlers:
            self.handlers[handler_name].push(force=True)

    """The main function that initiates streaming"""
    def start(self):
        write_log('Filtered Stream started.')
        print('Starting stream')
        """Load keywords for rule description and connect to end point with updating of data collection rules"""

        self.streamer = Streaming(cnf.bearer_token, "stream",
                                  get_keywords(), self.search_attributes,
                                  debug=self.debug)

        self.streamer.fix_rules()
        self.streamer.initiate_stream(self.get_streaming_data)




StreamInstance = StreamCrawler(dbg=False)

"""Handlers of signals"""
def handler_stop_signals(signum, frame):
    global StreamInstance
    print(f"Signal recieved: {signum}")

    StreamInstance.force_push_of_data()

    time.sleep(5)
    sys.exit(-1)

"""CTRL+C signal"""
signal.signal(signal.SIGINT, handler_stop_signals)

if __name__ == "__main__":
    while True:
        try:
            StreamInstance.start()
        except Exception as e:
            msg = f'Streamer Exception: {e} {traceback.format_exc()}{datetime.now()}'
            print(msg)
            write_log(msg)
            StreamInstance.force_push_of_data()
        print("Sleep for 5 minutes")
        time.sleep(5 * 60)
