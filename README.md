# Spark---P4

Utilizing Apache Spark for data analysis on Twitter data( schema - https://github.com/episod/twitter-api-fields-as-crowdsourced/wiki )

Questions answered:
------------------

1. How does @PrezOno’s tweet length compare to the average of all others?  What is his average length?  All others?
<br/>File - https://github.uc.edu/ravindsn/Spark---P4/blob/master/avgTweetLength.py<br/>shell script - avgTweetLength.sh

2. Detect the proportion of bad words in a tweet.  Plot bad word proportion by hour for all 24 hours.
<br/>File - https://github.uc.edu/ravindsn/Spark---P4/blob/master/BadWordStats.py<br/>shell script - BadWordStats.sh

Note: Please run `chmod u+x <sh file name>.sh` before executing the shell scripts.

Instructions:
-------------
Command for executing the programs using PySpark.
  `spark-submit avgTweetLength.py`

Command for executing the programs on the Hadoop cluster:
  `spark-submit --master yarn-client avgTweetLength.py`
