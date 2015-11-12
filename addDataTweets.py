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


def getConnection():
    # Returns a connection object whom will be given to any DB Query function.

    try:
        connection = MySQLdb.connect(host=k.GEODB_HOST, port=3306, user=k.GEODB_USER,
                                     passwd=k.GEODB_KEY, db=k.GEODB_NAME)
        return connection
    except MySQLdb.DatabaseError, e:
        print 'Error %s' % e
        sys.exit(1)


def updateTweets(connection, dato, idTweet):
    try:
        x = connection.cursor()
        x.execute("UPDATE Twitter.Tweet set gato=%s,aroa=%s, RT=%s, URL=%s where idTweet=%s; ", (
            dato['gato'], dato['aroa'], dato['RT'], dato['URL'], idTweet))
        connection.commit()
    except MySQLdb.DatabaseError, e:
        print 'Error %s' % e
        connection.rollback()
    pass


def getTweets(connection):
    query = "SELECT idTweet, Tweet.text FROM Twitter.Tweet;"
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        if data is None:
            return None
        else:
            return data
    except MySQLdb.Error:
        print "Error: unable to fetch data"
        return -1
    pass


def countWordTweet(text=''):
    count = {'aroa': text.count('@'), 'gato': text.count('#'), 'URL': text.count('http'), 'RT': text.count('RT ')}
    return count


def closeConnection(connection):
    connection.close()


def main():
    conn = getConnection()
    data = getTweets(conn)
    i = 0
    for d in data:
        count = countWordTweet(str(d[1]))
        updateTweets(conn, count, int(d[0]))
        i += 1
        print "Nro Tweet: %d" % i
    closeConnection(conn)


if __name__ == '__main__':
    main()
