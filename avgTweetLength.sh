#!/bin/bash

echo "Running avgTweetLength.py"

spark-submit --master yarn-client avgTweetLength.py

echo "Task Completed"