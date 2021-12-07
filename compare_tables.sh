#!/bin/bash 

while read line
do
	var1=$(awk -F\| '{ print $1 }' <<< "$line")
	var2=$(awk -F\| '{ print $2 }'  <<< "$line")
        var3=$(awk -F\| '{ print $3 }' <<< "$line")
	echo "$var1 $var2 $var3"
done < file.txt
