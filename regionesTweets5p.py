# -*- coding: utf-8 -*-

__author__ = 'luisangel'

import MySQLdb
import Credentials as k
import sys
import re, string
import unicodedata
import tweepy
import glob
from collections import defaultdict
import json
import time
from neo4jrestclient.client import GraphDatabase
from neo4jrestclient import exceptions

BDJSON = "../../twitter-users/"

MIN_TWEETS = 3000
# Twitter API credentials
consumer_key = "n97GTyR9UDKTb3QmKazWSY8Li"
consumer_secret = "iFGJecd8IiOjHMDWt9lqXJzC3J8BCEFZYVezQyBhfwbzlzQTAc"
access_key = "2559575756-gwnpjZL43wjFaWnBrrEvfHOyqs06FdcpJ5VKiDR"
access_secret = "TCe5jzGrkIi3YEt5oAIbPWLVMCO6GKXMzJO1aavC37quE"


def get_connection():
    # Returns a connection object whom will be given to any DB Query function.
    try:
        connection = MySQLdb.connect(host=k.GEODB_HOST, port=3306, user=k.GEODB_USER,
                                     passwd=k.GEODB_KEY, db=k.GEODB_NAME)
        return connection
    except MySQLdb.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# cleanTweet(tweet.text.encode("utf-8")).encode('utf-8')
def count_word_tweet(tw):
    w = tw.entities
    count = {'aroa': len(w['user_mentions']), 'gato': len(w['hashtags']), 'URL': len(w['urls']),
             'RT': int(hasattr(tw, 'retweeted_status'))}
    return count


def insert_tweets(connection, tweet, id_user):
    try:
        data = count_word_tweet(tweet)
        x = connection.cursor()
        x.execute('INSERT INTO Tweets_table VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ', (
            tweet.id_str,
            tweet.created_at,
            tweet.text.encode("utf-8"),
            1 if tweet.favorited is True else 0,
            tweet.favorite_count,
            1 if tweet.truncated is True else 0,
            tweet.retweet_count,
            1 if tweet.retweeted is True else 0,
            data['gato'],
            data['aroa'],
            data['RT'],
            data['URL'],
            id_user))
        connection.commit()
    except MySQLdb.DatabaseError, e:
        print 'Error %s' % e
        connection.rollback()


def close_connection(connection):
    connection.close()


def execute(connection, q_script):
    # executes a mysql script

    try:
        cursor = connection.cursor()
        cursor.execute(q_script)
    except MySQLdb.Error:
        print "Error: unable to execute"
        return -1


def get_all_tweets(conn, id_user, count):
    if count_tweets(conn, id_user) > MIN_TWEETS:
        return False
    # Twitter only allows access to a users most recent 3240 tweets with this method

    # authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # initialize a list to hold all the tweepy Tweets
    all_tweets = []
    since_id = get_since_id(conn, id_user)
    while True:
        try:
            if since_id is None:
                new_tweets = api.user_timeline(user_id=id_user, count=200)
            else:
                new_tweets = api.user_timeline(user_id=id_user, count=200, since_id=since_id)
            break
        except tweepy.TweepError, e:
            if str(e.message) == 'Not authorized.':
                print "Not authorized id: " + str(id_user)
                return False
            if e.message[0]['code'] == 34:
                print "Not found ApiTwitter id: " + str(id_user)
                return False
            if e.message[0]['code'] == 63:
                print 'Usuario suspendido:' + str(id_user)
                return False
            else:
                # hit rate limit, sleep for 15 minutes
                print 'Rate limited. Dormir durante 15 minutos. ' + e.reason
                time.sleep(15 * 60 + 15)
                continue
        except StopIteration:
            return False

    if len(new_tweets) == 0:
        return
    # save most recent tweets
    all_tweets.extend(new_tweets)

    # save the id of the oldest tweet less one
    oldest = all_tweets[-1].id - 1
    since_id = get_since_id(conn, id_user)
    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print "nro: %d, getting tweets before %s" % (count, oldest)

        while True:
            try:
                # all subsiquent requests use the max_id param to prevent duplicates
                new_tweets = api.user_timeline(user_id=id_user, count=200, max_id=oldest, since_id=since_id)
                break
            except tweepy.TweepError, e:
                if str(e.message) == 'Not authorized.':
                    print "Not authorized id: " + str(id_user)
                    return False
                if e.message[0]['code'] == 34:
                    print "Not found ApiTwitter id: " + str(id_user)
                    return False
                if e.message[0]['code'] == 63:
                    print 'Usuario suspendido:' + str(id_user)
                    return False
                else:
                    # hit rate limit, sleep for 15 minutes
                    print 'Rate limited. Dormir durante 15 minutos. ' + e.reason
                    time.sleep(15 * 60 + 15)
                    continue
            except StopIteration:
                return None

        # save most recent tweets
        all_tweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        oldest = all_tweets[-1].id - 1

        print "...%s tweets downloaded so far" % (len(all_tweets))

    for tweet in all_tweets:
        insert_tweets(conn, tweet, id_user)
    return True


def get_since_id(connection, id_user):
    query = "SELECT idTweet FROM Tweets_table where idUser=%s order by idTweet desc limit 1;"
    try:
        cursor = connection.cursor()
        cursor.execute(query, (id_user,))
        data = cursor.fetchone()
        if data is None:
            return None
        else:
            return data[0]
    except MySQLdb.Error:
        print "Error: unable to fetch data"
        return -1
    pass


def count_tweets(connection, id_user):
    query = "select count(*) from Tweets_table where idUser=%s;"
    try:
        cursor = connection.cursor()
        cursor.execute(query, (id_user,))
        data = cursor.fetchone()
        if data is None:
            return None
        else:
            return data[0]
    except MySQLdb.Error:
        print "Error: unable to fetch data"
        return -1
    pass


##### inicio limpieza de datos #####

def delete_tildes(s=''):
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))


def clean_tweet(text):
    # remove retweets
    text = re.sub('(RT|via)((?:\\b\\W*@\\w+)+)', '', text)
    # remove @otragente
    text = re.sub('@\\w+', '', text)
    # remueve simbolos de puntuacion
    text = re.sub('[%s]' % re.escape(string.punctuation), '', text)
    # remove números
    text = re.sub('\d', '', text)
    # remueve links
    text = re.sub("http([…]+|[\w]+)", '', text)
    # remueve htt...
    text = re.sub("ht[t]?…", "", text)
    # Convierte a minuscula
    text = text.lower()
    # remueve tildes
    text = delete_tildes(text.decode('utf-8'))
    return text


def get_user_by_region(gdb_sql, region=''):
    query = "select * from Users_table where idUser in (select idUser from User_location where region_code = %s) " \
            "order by idUser asc;"
    try:
        cursor = gdb_sql.cursor()
        cursor.execute(query, (region,))
        data = cursor.fetchall()
        if data is None:
            return []
        else:
            return [{
                        'id': x[0],
                        'screen_name': x[1],
                        'time_zone': x[2],
                        'name': x[3],
                        'followers_count': x[4],
                        'geo_enabled': x[5],
                        'description': x[6],
                        'tweet_chile': x[7],
                        'location': x[8],
                        'friends_count': x[9],
                        'verified': x[10],
                        'entities': x[11],
                        'utc_offset': x[12],
                        'statuses_count': x[13],
                        'lang': x[14],
                        'url': x[15],
                        'created_at': x[16],
                        'listed_count': x[17]
                    }
                    for x in data]
    except MySQLdb.Error:
        print "Error: unable to fetch data"
        return -1


def main():
    region = '11'
    gdb_sql = get_connection()
    try:
        init = int(sys.argv[1])
    except IndexError:
        init = 0
    users = get_user_by_region(gdb_sql, region)

    print 'Region: %s' % region
    if len(users) == 0 or users is None:
        print "Not found Users"
        gdb_sql.close()
        sys.exit(1)
    count = init
    for user in users[init:]:
        print 'Nro: %d, ID user: %d' % (count, user['id'])
        get_all_tweets(gdb_sql, user['id'], count)
        count += 1
    gdb_sql.close()


def get_tweets(connection):
    query = "SELECT Tweets_table.text FROM Tweet;"
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        if data is None:
            return None
        else:
            for t in data:
                print t[0]
    except MySQLdb.Error:
        print "Error: unable to fetch data"
        return -1
    pass


if __name__ == '__main__':
    main()