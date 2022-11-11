import ast
import extra.configfile as cnf
from os.path import isfile
from dateutil.parser import parse
from datetime import datetime

"""Read keyword file"""
def get_keywords():
    if not isfile(cnf.KEYWORDSFILE):
        raise Exception(f'Keywords loading Error, file {cnf.KEYWORDSFILE} not found.')

    query = ast.literal_eval(open(cnf.KEYWORDSFILE, 'r').read())

    if type(query) != dict:
        raise Exception(f'Keywords loading Error, file {cnf.KEYWORDSFILE} is not dictionary/json type.')
    query = {tag: query[tag].strip() for tag in query if query[tag].strip() != ''}
    if len(query) == 0:
        raise Exception(f'Keywords loading Error, after processing the rules are empty.')
    return query

"""Function that parse sting datetime into datetime object"""
def parse_string_date(date):
    return parse(date) if type(date) == str else date

"""Simple writing to file function that take a message and store it in logfile defined in configuration"""
def write_log(msg):
    f_out = open(cnf.LOGFILE, "a+")
    f_out.write(f'{datetime.now()} msg: {msg}\n')
    f_out.close()

"""Fix fields of user Object collection via Twitter API"""
def user_fields_fix(user_object, probe_time=None):
    """Parse string userID in form of integer (NumberLong in mongoDB)"""
    user_object["id"] = int(user_object["id"])

    """Parse string user profile creation date in form of datetime object
        -> Used in order to make possible fast filtering by creation date in mongoDB
    """
    if "created_at" in user_object:
        user_object["created_at"] = parse_string_date(user_object["created_at"])

    """Store also real datetime timestamp showing date of initial user collection"""
    user_object["probe_at"] = probe_time if not (probe_time is None) else datetime.now()
    return user_object


"""Fix fields of tweet Object collection via Twitter API"""
def tweet_fields_fix(tweet, probe_time=None):

    if "created_at" in tweet:
        tweet["created_at"] = parse_string_date(tweet["created_at"])

    for key_value in ["id", "author_id", "in_reply_to_user_id", "conversation_id"]:
        if key_value in tweet:
            tweet[key_value] = int(tweet[key_value])

    """Parse referenced tweetIDs (retweet,quote) as integer instead of string provided by Twitter API"""
    if "referenced_tweets" in tweet:
        for i in range(len(tweet["referenced_tweets"])):
            tweet["referenced_tweets"][i]["id"] = int(tweet["referenced_tweets"][i]["id"])

    """Fix mentioned userIDs , since Twitter API provide them in form of text we store them in form of integer"""
    if "entities" in tweet and "mentions" in tweet["entities"]:
        """check all mention dictionary and fix userId string to int"""
        for i in range(len(tweet["entities"]["mentions"])):
            tweet["entities"]["mentions"][i]["id"] = int(tweet["entities"]["mentions"][i]["id"])

    tweet["probe_at"] = probe_time if not (probe_time is None) else datetime.now()

    # For future to store new sampled tweet after k days or in daily basis
    tweet["samples"] = []
    return tweet
