#!/bin/bash

echo "Running BadWordStats.py"

spark-submit --master yarn-client BadWordStats.py

echo "Task Completed"