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
users = defaultdict(lambda: {'followers': 0})
MIN_TWEETS = 3000
# Twitter API credentials
consumer_key = "r7ZeHCxlFMWEz5r41Fvfr725d"
consumer_secret = "EkxgeVdnSqn4A90rbc2oypitixb1wBh0DX1fuP7F8Q8gtRjovV"
access_key = "2559575756-w3KfDnUaF7bRb1zYn5JTh3T5tCBSoMSjwNuwgyc"
access_secret = "Vm2CDzis95HozKUWX2hWbzpgAWoKEVcgtoOm7RjSOtx7E"


def get_conecction_neo():
    gdb = GraphDatabase("http://neo4j:123456@localhost:7474/db/data/")
    return gdb


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


def insert_tweets(connection, tweet, id_user, id_region):
    try:
        dato = count_word_tweet(tweet)
        x = connection.cursor()
        x.execute('INSERT INTO Tweet VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ', (
            tweet.id_str, tweet.created_at, tweet.text.encode("utf-8"),
            1 if tweet.favorited is True else 0,
            tweet.favorite_count, 1 if tweet.truncated is True else 0, id_user, tweet.retweet_count,
            1 if tweet.retweeted is True else 0, id_region,
            dato['gato'], dato['aroa'], dato['RT'], dato['URL']))
        connection.commit()
    except MySQLdb.DatabaseError, e:
        print 'Error %s' % e
        connection.rollback()
    pass


def get_id_region(nameRegion, connection):
    query = "SELECT idRegion FROM Twitter.Region where name=%s;"
    try:
        cursor = connection.cursor()
        cursor.execute(query, (nameRegion,))
        data = cursor.fetchone()
        if data is None:
            return None
        else:
            return data[0]
    except MySQLdb.Error:
        print "Error: unable to fetch data"
        return -1
    pass


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


def get_all_tweets(id_user, region):
    conn = get_connection()
    if count_tweets(conn, id_user) > MIN_TWEETS:
        return False
    # Twitter only allows access to a users most recent 3240 tweets with this method

    # authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # initialize a list to hold all the tweepy Tweets
    alltweets = []
    conn = get_connection()
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
    alltweets.extend(new_tweets)

    # save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1
    since_id = get_since_id(conn, id_user)
    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print "getting tweets before %s" % (oldest)

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
        alltweets.extend(new_tweets)

        # update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1

        print "...%s tweets downloaded so far" % (len(alltweets))

    id = get_id_region(region, conn)
    for tweet in alltweets:
        insert_tweets(conn, tweet, id_user, id)
    close_connection(conn)
    return True


def get_since_id(connection, idUser):
    query = "SELECT idTweet FROM Twitter.Tweet where idUser=%s order by idTweet desc limit 1;"
    try:
        cursor = connection.cursor()
        cursor.execute(query, (idUser,))
        data = cursor.fetchone()
        if data is None:
            return None
        else:
            return data[0]
    except MySQLdb.Error:
        print "Error: unable to fetch data"
        return -1
    pass


def count_tweets(connection, idUser):
    query = "select count(*) from Twitter.Tweet where idUser=%s;"
    try:
        cursor = connection.cursor()
        cursor.execute(query, (idUser,))
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


def get_user_by_region(region=''):
    gdb = get_conecction_neo()
    query = "MATCH (n:Chile) WHERE n.region={r} RETURN n.id limit 400"
    param = {'r': region}
    results = gdb.query(query, params=param, data_contents=True)
    return results.rows


def main():
    for region in REGIONES.values():
        ids = get_user_by_region(str(region))
        print 'Region: %s' % region
        if ids is None:
            continue
        for idUser in ids:
            print 'ID Usuario: %d' % idUser[0]
            if get_all_tweets(idUser[0], str(region)) is False:
                continue


def prueba():
    n = get_user_by_region('Arica y Parinacota')

    for x in n:
        print x[0]

    print len(n)


def region():
    region = 'Maule'
    ids = get_user_by_region(region)
    print 'Region: %s' % region
    if ids is None:
        sys.exit(1)
    for idUser in ids:
        print 'ID Usuario: %d' % idUser[0]
        if get_all_tweets(idUser[0], str(region)) is False:
            continue


def get_tweets(connection):
    query = "SELECT Tweet.text FROM Twitter.Tweet;"
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
    region()
