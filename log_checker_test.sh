#!/bin/bash

#NOW=$(date --date last-month +"%b")
#
#echo $NOW
#
#SITES="bestprosintown loc8nearme restaurantji"

#for site in $SITES
#do
#  echo "$site"
#  LOGFILE=$"${site}.com-${NOW}-2021.gz"
#
#  echo $LOGFILE
#
#  KEK="bestprosintown.com-Apr-2021.gz"
#
#  PATHH=$(pwd)
#PATHFILE=$PATHH/'log_analyzer/analyze_log.py'
#echo $PATHFILE
#
#
#  if ! [ -f $KEK ]; then
#    echo 'No File'
#  else echo 'File exist'
#  echo $KEK
#  curl -v POST -F TEXT=$KEK -H "Connection: keep-alive"  http://firstbankmi.com/andrew_workspace/log_analyzer/analyze_log.py --insecure
#  fi

#done

#echo $PATHFILE

NOW=$(date --date last-month +"%b")

FILES="bestprosintown.com-Apr-2021.gz bestpros_test_log_feb_small"

for KEK in $FILES
do

  echo $KEK
  PGRP=$(pgrep -f "analyze_log" | wc -l) # получаем булевое значение 1-работает 0-не работает

  while [ "$PGRP" -eq 1 ] # сидим в слипе пока "=" 1
    do
      echo $PGRP
      echo 'timeout 60s'
      sleep 60
      PGRP=$(pgrep -f "analyze_log" | wc -l) # получаем булевое значение 1-работает 0-не работает
    done

  if ! [ -f $KEK ]; then
    echo 'No File'
  else
    echo 'File exist'
    echo $KEK
    curl -v POST -F TEXT=$KEK --max-time 10 http://firstbankmi.com/andrew_workspace/log_analyzer/analyze_log.py --insecure
  fi
done