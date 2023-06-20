from pymongo import MongoClient
import extra.configfile as config
from datetime import datetime, timedelta
from tqdm import tqdm

"""
def choose_collection(obj, type):
    if 'matching_rules' not in obj:
        return None, None
    elif 'matching_rules' in obj and obj['matching_rules'] is None:
        return None, None
    rule = obj['matching_rules']
    for value, destination in config.DESTINATIONS.items():
        if rule == value:
            if type == 'user':
               destination = destination[0]
            elif type == 'tweet':
                destination = destination[1]
            db, collection = destination.split('.')
            return db, collection
"""

class MongoLoader:

    def __init__(self, db_users, db_tweets):
        self.uids = set()
        self.client = None
        if config.DBCONFIG["address"] is None:
            raise Exception("MongoLoader: Configuration file has 'None' value for server IP address.")
        if config.DBCONFIG["port"] is None:
            raise Exception("MongoLoader: Configuration file has 'None' value for server port number.")

        self.db_name = db_users.split('.')[0]
        self.col_users = db_users.split('.')[1]
        self.col_tweets = db_tweets.split('.')[1]

        self._connect_to_db_()

    """Check if tweet is already in the database"""
    def has_tweet(self, tweetid):
        return False if self.db[self.col_tweets].find_one({"id": int(tweetid)}, {"_id": 1}) is None else True

    """Check if user is already in the database"""
    def has_user(self, userid):
        return False if self.db[self.col_users].find_one({"id": int(userid)}, {"_id": 1}) is None else True
    
    ##############################
    # Collect tweet ids from MongoDB for only last X period
    ##############################
    def get_parsed(self):
        print("Get tweet ids")
        self._connect_to_db_()
        ids = set()
        for item in tqdm(self.db[self.col_tweets].find({"created_at": {"$gt": datetime.now() - timedelta(days=30)}}, {"_id": 0, "id": 1}, no_cursor_timeout=True)):
            ids.add(int(item["id"]))
        self._disconnect_from_db_()
        return list(ids)
    
    ##############################
    # Collect only user ids from MongoDB
    ##############################
    def get_user_ids(self):
        print("Get user ids")
        self._connect_to_db_()
        ids = set()
        for item in tqdm(self.db[self.col_users].find({}, {"_id": 0, "id": 1}, no_cursor_timeout=True)):
            ids.add(int(item["id"]))
        self._disconnect_from_db_()
        return list(ids)

    ##############################
    # Collect specific user object from MongoDB
    ##############################
    def get_user_profile(self, user_id):
        self._connect_to_db_()
        uObject = self.db[self.col_users].find_one({"id": user_id})
        self._disconnect_from_db_()
        return uObject

    ##############################
    # Store tweets
    ##############################
    def store_tweets(self, tweet_list):
        """
        for tweet in tweet_list:
            db, collection = choose_collection(tweet, 'tweet')
            if collection is None or db is None:
                print("No matching collection found for tweet: " + str(tweet['id']))
                continue
            self._connect_to_db_(db, collection)

            if self.db[collection].find_one({"id": tweet['id']}, {"_id": 1}) is not None:
                continue
            self.db[collection].insert_one(tweet)
            self._disconnect_from_db_()
        """
        if len(tweet_list) > 0:
            self.db[self.col_tweets].insert_many(tweet_list)

    ##############################
    # Store user objects
    ##############################
    def store_users(self, user_list):
        """
        for user in user_list:
            db, collection = choose_collection(user, 'user')
            if collection is None or db is None:
                print("No matching collection found for user: " + str(user['id']))
                continue
            self._connect_to_db_(db, collection)
            if self.db[collection].find_one({"id": user['id']}, {"_id": 1}) is not None:
                continue
            self.db[collection].insert_one(user)
            self._disconnect_from_db_()
        """
        if len(user_list) > 0 :
            self.db[self.col_users].insert_many(user_list)

    # Read objects rules and choose collection based on destinations config

    ##############################
    # Connect to MongoDB
    ##############################
    def _connect_to_db_(self):
        # connect to mongo db collection
        self._disconnect_from_db_()
        self.client = MongoClient(config.DBCONFIG["address"], config.DBCONFIG["port"])

        self.db = self.client[self.db_name]

        #self.db = self.client[db_name if db_name is not None else config.DBCONFIG["db"]]
        #self.collection = self.db[collection_name if collection_name is not None else config.DBCONFIG["collection"]]

    ##############################
    # Disconnect from mongo DB
    ##############################
    def _disconnect_from_db_(self):
        if self.client is not None:
            self.client.close()
            self.client = None
