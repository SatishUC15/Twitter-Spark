# Spark program to identify the proportion of bad words in a tweet
# To run, do: spark-submit --master yarn-client avgTweetLength.py hdfs://hadoop2-0-0/data/twitter/part-03212

from __future__ import print_function, division
import sys, json
from pyspark import SparkContext
from datetime import datetime

# badwords file taken from web.
with open("badwords.txt") as f:
    bad_words_list = f.readline().split(",")


# Given a full tweet object, return the text of the tweet
def getText(line):
    try:
        js = json.loads(line)
        text = js.get('text').encode('ascii', 'ignore').lower()
        time = js.get('user').get('created_at')
        # extracting the hour(as interger) in which the tweet was made
        date_object = datetime.strptime(time, '%a %b %d %H:%M:%S +0000 %Y')
        # generating key,value pairs : (hour_of_tweet, tweet_text)
        data = date_object.hour, text
        return [data]
    except Exception as ex:
        return []


# Generate the proportion of bad words in a given text
# Returns key,value pairs : (hour_of_tweet, proportion of bad words) if any bad words in the tweet.
# Returns value 0 if no bad words are identified
def findWord(hour, text):
    word_count = 0
    bad_words = 0
    try:
        for word in text.split(' '):
            if word in bad_words_list:
                bad_words += 1
            word_count += 1
        return [hour, bad_words / word_count]
    except Exception as ex:
        return [hour, 0]


if __name__ == "__main__":
    sc = SparkContext(appName="BadWordStats")
    # stores tweet data in an RDD
    tweet_texts = sc.textFile("hdfs://hadoop2-0-0/data/twitter/").flatMap(getText)
    # maps the findWord function to each kv pair and filters empty elements generated in findWord function
    bad_word_prop = tweet_texts.map(lambda l: findWord(l[0], l[1])).filter(lambda l: l[1] > 0)
    # generates a RDD containing sum of all proportions and count of the tweets aggregated on the basis of key(hour)
    sumCount = bad_word_prop.combineByKey(lambda value: (value, 1),
                                          lambda x, value: (x[0] + value, x[1] + 1),
                                          lambda x, y: (x[0] + y[0], x[1] + y[1]))

    # Average of the bad words proportion by hour(key) computed
    averageByKey = sumCount.map(lambda (label, (value_sum, count)): (label, value_sum / count))

    # Pints the hourly average of bad word length
    print(averageByKey.collectAsMap())

    sc.stop()
