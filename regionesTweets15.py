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
consumer_key = "cZdMNXuova8gEyQiDcuPxF0cv"
consumer_secret = "HGY17uOAa68XIcZLORssIp7Kael7Cb9Yt8SjhxZSat0jEvOnbe"
access_key = "184784339-mnCoevXt8hXqpNRS2qRhgpd56KuanrZtMD3TdAaC"
access_secret = "uoDmsZT7v6tPD4js7arAnTmqLJJSSRSXKczdwFF3LwmDa"


def getConecctionNeo():
    gdb = GraphDatabase("http://neo4j:123456@localhost:7474/db/data/")
    return gdb


def getConnection():
    # Returns a connection object whom will be given to any DB Query function.

    try:
        connection = MySQLdb.connect(host=k.GEODB_HOST, port=3306, user=k.GEODB_USER,
                                     passwd=k.GEODB_KEY, db=k.GEODB_NAME)
        return connection
    except MySQLdb.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


# cleanTweet(tweet.text.encode("utf-8")).encode('utf-8')
def countWordTweet(tw):
    w = tw.entities
    count = {'aroa': len(w['user_mentions']), 'gato': len(w['hashtags']), 'URL': len(w['urls']),
             'RT': int(hasattr(tw, 'retweeted_status'))}
    return count

def insertTweets(connection, tweet, id_user, id_region):
    try:
        dato = countWordTweet(tweet)
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



def getIdRegion(nameRegion, connection):
    query = "SELECT idRegion FROM Region where name=%s;"
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


def closeConnection(connection):
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
    conn = getConnection()
    if countTweets(conn, id_user) > MIN_TWEETS:
        return False
    # Twitter only allows access to a users most recent 3240 tweets with this method

    # authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # initialize a list to hold all the tweepy Tweets
    alltweets = []
    conn = getConnection()
    sinceId = getSinceId(conn, id_user)
    while True:
        try:
            if sinceId is None:
                new_tweets = api.user_timeline(user_id=id_user, count=200)
            else:
                new_tweets = api.user_timeline(user_id=id_user, count=200, since_id=sinceId)
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
    sinceId = getSinceId(conn, id_user)
    # keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        print "getting tweets before %s" % (oldest)

        while True:
            try:
                # all subsiquent requests use the max_id param to prevent duplicates
                new_tweets = api.user_timeline(user_id=id_user, count=200, max_id=oldest, since_id=sinceId)
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

    id = getIdRegion(region, conn)
    for tweet in alltweets:
        insertTweets(conn, tweet, id_user, id)
    closeConnection(conn)
    return True


def getSinceId(connection, idUser):
    query = "SELECT idTweet FROM Tweet where idUser=%s order by idTweet desc limit 1;"
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


def countTweets(connection, idUser):
    query = "select count(*) from Tweet where idUser=%s;"
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

def elimina_tildes(s=''):
    return ''.join((c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn'))


def cleanTweet(text):
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
    text = elimina_tildes(text.decode('utf-8'))
    return text


def getUserByRegion(region=''):
    gdb = getConecctionNeo()
    query = "MATCH (n:Chile) WHERE n.region={r} RETURN n.id limit 400"
    param = {'r': region}
    results = gdb.query(query, params=param, data_contents=True)
    return results.rows


def main():
    for region in REGIONES.values():
        ids = getUserByRegion(str(region))
        print 'Region: %s' % region
        if ids is None:
            continue
        for idUser in ids:
            print 'ID Usuario: %d' % idUser[0]
            if get_all_tweets(idUser[0], str(region)) is False:
                continue


def prueba():
    n = getUserByRegion('RM Santiago')

    for x in n:
        print x[0]

    print len(n)


def region():
    region = 'RM Santiago'
    ids = getUserByRegion(region)
    print 'Region: %s' % region
    if ids is None:
        sys.exit(1)
    for idUser in ids:
        print 'ID Usuario: %d' % idUser[0]
        if get_all_tweets(idUser[0], str(region)) is False:
            continue

def getTweets(connection):

    query = "SELECT Tweet.text FROM Tweet;"
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
