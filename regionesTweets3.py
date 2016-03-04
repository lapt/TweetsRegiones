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

REGIONES = {'2': 'Antofagasta',
            '3': 'Atacama',
            '4': 'Coquimbo',
            '10': 'Los Lagos',
            '7': 'Maule',
            '11': 'Aysen',
            '15': 'Arica y Parinacota',
            '9': 'Araucania',
            '14': 'Los Rios',
            '12': 'Magallanes',
            '1': 'Tarapaca',
            '5': 'Valparaiso',
            '8': 'Biobio',
            '6': 'O\'Higgins',
            '13': 'RM Santiago'
            }

BDJSON = "../../twitter-users/"

MIN_TWEETS = 3000
# Twitter API credentials
KEYS = [['n97GTyR9UDKTb3QmKazWSY8Li', 'iFGJecd8IiOjHMDWt9lqXJzC3J8BCEFZYVezQyBhfwbzlzQTAc',
         '2559575756-gwnpjZL43wjFaWnBrrEvfHOyqs06FdcpJ5VKiDR', 'TCe5jzGrkIi3YEt5oAIbPWLVMCO6GKXMzJO1aavC37quE'],
        ['0YBV04bDmIRrrwc8HcmZmmzEw', 'bx9BpkzPmK6r4rlQmr03QWYSHgg4Y70OfV5DOsXLpVREloJQt9',
         '2559575756-p4hmwMGk7FZA2hlcgRSHGuqBMauzE5PTJ7fdyLv', 'ntu8FlolsgBzRKbgWdVtvqjKZK97d6UhU1T7WeOpnZ2y1'],
        ['2V9TtYm27DdxJ4JZvOXXcdfDz', 'Kwxy9hVWZasxTZLOhgzDlIuwV7vRKR3586xOstcuV15YdclAaw',
         '2559575756-YDqCTQWdiSL2D5ULsReWo1PQsT7GsuXdUID4EwN', 'lG4C3D9aIzrDptH6grubbCyZ5hpWk3T4u3yB0dL0ImfQ0']]
ID_KEY = 0
ID_BAD = 0
auth = tweepy.OAuthHandler(KEYS[ID_KEY][0], KEYS[ID_KEY][1])
auth.set_access_token(KEYS[ID_KEY][2], KEYS[ID_KEY][3])
api = tweepy.API(auth)


def get_new_api():
    global ID_KEY
    ID_KEY = 0 if ID_KEY >= 2 else ID_KEY + 1
    global auth
    auth = tweepy.OAuthHandler(KEYS[ID_KEY][0], KEYS[ID_KEY][1])
    auth.set_access_token(KEYS[ID_KEY][2], KEYS[ID_KEY][3])
    global api
    api = tweepy.API(auth)


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
            print "Primero: " + e.reason + " Termina."
            if e.reason == 'Failed to send request: (\'Connection aborted.\', ' \
                           'gaierror(-2, \'Name or service not known\'))':
                print 'Internet. Dormir durante 1 minuto. ' + e.message
                time.sleep(60)
                continue
            if e.reason == "Failed to send request: ('Connection aborted.', gaierror(-3, " \
                           "'Temporary failure in name resolution'))":
                print 'Internet. Dormir durante 1 minuto. ' + e.message
                time.sleep(60)
                continue
            if e.reason == 'Failed to send request: HTTPSConnectionPool(host=\'api.twitter.com\', port=443): ' \
                           'Read timed out. (read timeout=60)':
                print 'Internet. Dormir durante 1 minuto. ' + e.message
                time.sleep(60)
                continue
            if e.reason == "Failed to send request: ('Connection aborted.', BadStatusLine(\"''\",))":
                print 'Internet. Dormir durante 1 minuto. ' + e.message
                time.sleep(60)
                continue
            if e.reason[:29] == "Failed to parse JSON payload:":
                print 'Internet. Dormir durante 1 minuto. ' + e.message
                time.sleep(15)
                continue
            if e.reason == 'Not authorized.':
                print 'Internet. Dormir durante 1 minuto. ' + e.message
                return
            if e.message[0]['code'] == 34:
                print "Not found ApiTwitter id: " + str(id_user)
                return
            if e.message[0]['code'] == 63:
                print 'Usuario suspendido:' + str(id_user)
                return
            if e.message[0]['code'] == 50:
                print 'User not found:' + str(id_user)
                return
            else:
                # hit rate limit, sleep for 15 minutes
                print 'Rate limited. Dormir durante 15 minutos. code: ' + ' id: ' + str(id_user)
                get_new_api()
                continue
        except StopIteration:
            return False

    if len(new_tweets) == 0:
        return
    # save most recent tweets
    all_tweets.extend(new_tweets)

    # save the id of the oldest tweet less one
    oldest = all_tweets[-1].id - 1

    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print "nro: %d, getting tweets before %s" % (count, oldest)

        while True:
            try:
                if since_id is None:
                    new_tweets = api.user_timeline(user_id=id_user, count=200, max_id=oldest)
                else:
                    new_tweets = api.user_timeline(user_id=id_user, count=200, max_id=oldest, since_id=since_id)
                break
            except tweepy.TweepError, e:
                print "Primero: " + e.reason + " Termina."
                if e.reason == 'Failed to send request: (\'Connection aborted.\', ' \
                               'gaierror(-2, \'Name or service not known\'))':
                    print 'Internet. Dormir durante 1 minuto. ' + e.message
                    time.sleep(60)
                    continue
                if e.reason == "Failed to send request: ('Connection aborted.', gaierror(-3, " \
                               "'Temporary failure in name resolution'))":
                    print 'Internet. Dormir durante 1 minuto. ' + e.message
                    time.sleep(60)
                    continue
                if e.reason == 'Failed to send request: HTTPSConnectionPool(host=\'api.twitter.com\', port=443): ' \
                               'Read timed out. (read timeout=60)':
                    print 'Internet. Dormir durante 1 minuto. ' + e.message
                    time.sleep(60)
                    continue
                if e.reason == "Failed to send request: ('Connection aborted.', BadStatusLine(\"''\",))":
                    print 'Internet. Dormir durante 1 minuto. ' + e.message
                    time.sleep(60)
                    continue
                if e.reason[:29] == "Failed to parse JSON payload:":
                    print 'Internet. Dormir durante 1 minuto. ' + e.message
                    time.sleep(15)
                    continue
                if e.reason == 'Not authorized.':
                    print 'Internet. Dormir durante 1 minuto. ' + e.message
                    return
                if e.message[0]['code'] == 34:
                    print "Not found ApiTwitter id: " + str(id_user)
                    return
                if e.message[0]['code'] == 63:
                    print 'Usuario suspendido:' + str(id_user)
                    return
                if e.message[0]['code'] == 50:
                    print 'User not found:' + str(id_user)
                    return
                else:
                    # hit rate limit, sleep for 15 minutes
                    print 'Rate limited. Dormir durante 15 minutos. code: ' + ' id: ' + str(id_user)
                    get_new_api()
                    continue
            except StopIteration:
                return None

        # save most recent tweets
        all_tweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        oldest = all_tweets[-1].id - 1

        print "...%s tweets downloaded so far" % (len(all_tweets))
    count_tweet = 0
    for tweet in all_tweets:
        insert_tweets(conn, tweet, id_user)
        count_tweet += 1
        print "nro: %d, Insert: %d" % (count, count_tweet)
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
    region = '07'
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