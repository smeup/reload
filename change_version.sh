#!/bin/sh

if [ -n "$1" ]
then
echo Change Reload version to ${1}
mvn versions:set -DnewVersion=${1}
mvn versions:commit
else
	echo "New Reload version not passed."
fi	