#!/bin/bash

NOW=$(date --date last-month +"%b")

echo $NOW

MYPATH="/home/andrew_ho/andrew_workspace/"

SITES="bestprosintown loc8nearme restaurantji"

for site in $SITES
do

  echo "$site"
  LOGFILE=$"${site}.com-${NOW}-2021.gz"

  FULLPATHLOGFILE=$"${MYPATH}${site}.com-${NOW}-2021.gz"

  echo $LOGFILE

  PGRP=$(pgrep -f "analyze_log" | wc -l) # получаем булевое значение 1-работает 0-не работает

  while [ "$PGRP" -eq 1 ] # сидим в слипе пока "=" 1
    do
      echo $PGRP
      echo 'timeout 60s'
      sleep 60
      PGRP=$(pgrep -f "analyze_log" | wc -l) # получаем булевое значение 1-работает 0-не работает
    done

  if ! [ -f $FULLPATHLOGFILE ]; then
    echo 'No File'
  else
    echo 'File exist'
    echo $LOGFILE
    curl -v POST -F TEXT=$LOGFILE --max-time 10 http://firstbankmi.com/andrew_workspace/log_analyzer/analyze_log.py --insecure
  fi

done