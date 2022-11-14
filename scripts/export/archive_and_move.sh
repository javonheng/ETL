#!/bin/bash
backup_date=$(date + '%m_%d_%Y')

zip -r $backup_date.zip *.json *.csv
mv $backup_date.zip archived
