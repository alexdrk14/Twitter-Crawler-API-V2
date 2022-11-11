from pymongo import MongoClient
import extra.configfile as config

class MongoLoader:

    def __init__(self, destination=None):
        self.db = None
        self.client = None
        self.collection = None
        self.uids = set()
        if config.DBCONFIG["address"] is None:
            raise Exception("MongoLoader: Configuration file has 'None' value for server IP address.")
        if config.DBCONFIG["port"] is None:
            raise Exception("MongoLoader: Configuration file has 'None' value for server port number.")
        if config.DBCONFIG["db"] is None:
            raise Exception("MongoLoader: Configuration file has 'None' value for server database name.")
        if destination is None:
            raise Exception("MongoLoader: Destination collection is None")
        self.dest_col = destination

    ##############################
    #Collect user ids from MongoDB
    ##############################
    def get_user_ids(self):
        self._connect_to_db_()
        ids = set()
        for item in self.collection.find({}, {"_id": 0, "id": 1}, no_cursor_timeout=True):
            ids.add(int(item["id"]))
        self._disconnect_from_db_()
        return list(ids)

    def get_user_profile(self, user_id):
        self._connect_to_db_()
        uObject = self.collection.find_one({"id": user_id})
        self._disconnect_from_db_()
        return uObject


    def get_parsed(self):
        self._connect_to_db_()
        known = set()
        for item in self.db['tweets'].find({}, {"_id": 0, "id": 1},  no_cursor_timeout=True):
            known.add(int(item["id"]))
        self._disconnect_from_db_()
        return list(known)

    def store_parsed(self, item_list):
        self._connect_to_db_()
        self.db[self.dest_col].insert_many(item_list)
        self._disconnect_from_db_()

    def store_tweets(self, tweet_list):
        self._connect_to_db_()
        self.db['tweets'].insert_many(tweet_list)
        self._disconnect_from_db_()

    def store_users(self, user_list):
        self._connect_to_db_()
        self.db["users"].insert_many(user_list)
        self._disconnect_from_db_()


    ##############################
    #Connect to MongoDB
    ##############################
    def _connect_to_db_(self):
        #connect to mongo db collection
        self._disconnect_from_db_()
        self.client = MongoClient(config.DBCONFIG["address"], config.DBCONFIG["port"])
        self.db = self.client[config.DBCONFIG["db"]]
        self.collection = self.db['tweets']
        
    
    ##############################
    #Disconnect from mongo DB
    ##############################
    def _disconnect_from_db_(self):
        if self.client != None:
            self.client.close()
            self.client = None
            


