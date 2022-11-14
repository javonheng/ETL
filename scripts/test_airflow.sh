#!/bin/bash
if [ -e testfile.txt ]; then
  echo "New line inserted for Airflow test!" >> testfile.txt
else
  echo "Apache Airflow running successfully..." > testfile.txt
fi