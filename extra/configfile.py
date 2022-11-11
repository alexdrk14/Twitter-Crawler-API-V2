#!/usr/bin/env python

"""Configuration of MongoDB connector, used for storing the data in DB and filtering already seen users and tweets"""
DBCONFIG={
          "address"   : '127.0.0.1',   #IP address of MongoDB, like '127.0.0.1' in string type"""
          "port"      : 27017, #Port of MongoDB, like 27017 in integer type"""
          "db"        : 'Your_Mongo_DB_Name', #Name of MongoDB Database, like 'CollectedDatabase' in string type"""
          }

"""Bearer Token provided by Twitter Developer API"""
bearer_token = 'Your_bearer_token'

"""Filename + path in case if file located in different path. This file contain dictionary of filtering keywords and tags. See example in keywords.txt.
For more complicated rules please check the Twitter Developer V2 manual"""
KEYWORDSFILE = "keywords.txt"

"""Selection of tweet object fields that would be returned by twitter for each object"""
GETFILEDS = "tweet.fields=created_at,attachments,author_id,context_annotations,conversation_id,edit_controls,entities,geo,id,in_reply_to_user_id,lang,organic_metrics,promoted_metrics,possibly_sensitive,public_metrics,non_public_metrics,referenced_tweets,reply_settings,source,text,withheld&" \
                                 "expansions=author_id,referenced_tweets.id,edit_history_tweet_ids,in_reply_to_user_id,attachments.media_keys,attachments.poll_ids,geo.place_id,entities.mentions.username,referenced_tweets.id.author_id&" \
                                 "user.fields=created_at,description,entities,location,pinned_tweet_id,profile_image_url,protected,public_metrics,url,verified,withheld"

"""Filename + path in case if file located in different path, used for loging the execution of crawler."""
LOGFILE = "crawler_log.txt"
