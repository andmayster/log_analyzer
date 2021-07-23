#!/bin/bash

arr=()

MYPATH="/home/andrew_ho/andrew_workspace/"

elements=$(ls $MYPATH | grep completed)

echo $elements

for element in $elements
do
  arr+=$element' '
done

#MYPWD=$(pwd)

for i in $arr
do
  echo $i

#  if [[ "$MYPWD" = "$MYPATH" ]]
#  then

  username="andrew_ho"
  file_origin=${MYPATH}$i
#  echo $file_origin
  dir_destination="/home/andrew_ho/archive_logs"
  Ip="157.90.211.158"

  echo "Uploading files to remote server...."
  rsync -avzh $file_origin $username@$Ip:$dir_destination
  echo "File upload to remote server completed! ;)"
  # сюда копирование на другой сервер
  # и удаление с ферстбанка
  rm -f $file_origin
  echo $i "was remove from firstbank!"

#  else
#    break
#  fi
done