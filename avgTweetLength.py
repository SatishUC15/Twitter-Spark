# Spark example to print the average tweet length of PrezOno's tweet as compared to others tweet length.
# To run, do: spark-submit --master yarn-client avgTweetLength.py hdfs://hadoop2-0-0/data/twitter/part-03212

from __future__ import print_function
import sys, json, os
from pyspark import SparkContext


# Given a full tweet object, return the text of the tweet
def getText(line):
    try:
        js = json.loads(line)
        #user name required to filter PrezOno's tweet
        user_name = js.get('user').get('screen_name').encode('ascii', 'ignore').lower()
        text = js.get('text').encode('ascii', 'ignore')
        data = user_name, text
        return [data]
    except Exception as a:
        return []


if __name__ == "__main__":
    sc = SparkContext(appName="avgTweetLength")
    texts = sc.textFile("hdfs://hadoop2-0-0/data/twitter/").flatMap(getText)

    #stores PrezOno's tweets and others tweet in separate RDD 
    prezData = texts.filter(lambda l: "prezono" in l[0])
    otherData = texts.filter(lambda l: "prezono" not in l[0])

    # map function to calculate len of each element in the RDD
    prezLen = prezData.map(lambda l: len(l[1]))
    otherLen = otherData.map(lambda l: len(l[1]))

    # stats() - outputs the mean,count,max and min values for PrezOno's and others tweets
    prezStats = prezLen.stats()
    otherStats = otherLen.stats()

    # Print out the stats
    print("PrezOno's Tweet Stats : ", prezStats)
    print("Other's Tweet Stats   : ", otherStats)

    sc.stop()
