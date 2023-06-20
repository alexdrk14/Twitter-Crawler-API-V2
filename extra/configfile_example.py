#!/usr/bin/env python

"""Configuration of MongoDB connector, used for storing the data in DB and filtering already seen users and tweets"""
DBCONFIG = {
    "address": '127.0.0.1',  # IP address of MongoDB, like '127.0.0.1' in string type"""
    "port": 27017,  # Port of MongoDB, like 27017 in integer type"""
}

"""Twitter API v2 Bearer Token provided by Twitter Developer API"""
bearer_token = 'YOUR_BEARER_TOKEN'

"""Twitter API V1 authentication tokens"""
consumer_key = 'YOUR_CONSUMER_KEY'
consumer_secret = 'YOUR_CONSUMER_SECRET'
access_token_key = 'YOUR_ACCESS_TOKEN_KEY'
access_token_secret = 'YOUR_ACCESS_TOKEN_SECRET'

"""Filename + path in case if file located in different path. This file contain dictionary of filtering keywords and tags. See example in keywords.txt.
For more complicated rules please check the Twitter Developer V2 manual"""
KEYWORDSFILE = "keywords.txt"

"""Selection of tweet object fields that would be returned by twitter for each object"""
GETFILEDS = "tweet.fields=created_at,attachments,author_id,context_annotations,conversation_id,edit_controls," \
            "entities,geo,id,in_reply_to_user_id,lang,organic_metrics,promoted_metrics,possibly_sensitive," \
            "public_metrics,non_public_metrics,referenced_tweets,reply_settings,source,text,withheld&" \
            "expansions=author_id,referenced_tweets.id,edit_history_tweet_ids,in_reply_to_user_id," \
            "attachments.media_keys,attachments.poll_ids,geo.place_id,entities.mentions.username," \
            "referenced_tweets.id.author_id&" \
            "user.fields=created_at,description,entities,location,pinned_tweet_id,profile_image_url,protected," \
            "public_metrics,url,verified,withheld"

"""Filename + path in case if file located in different path, used for loging the execution of crawler."""
LOGFILE = "crawler_log.txt"


DESTINATIONS = {"ekloges": ['GRElections.users', 'GRElections.tweets'],
                "testing": ['RussiaWar.users', 'RussiaWar.Tweets'],
                }
"""List of destinations where beside V2 user profile, also V1 should be collected"""
V1_OBJECTS = ["testing"]

"""Limitation for dataHandler of how many user objects and tweet objects can be stored in memory before storing in mongoDB"""
LIMITS = {"users": 100, "tweets": 500}
