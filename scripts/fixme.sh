#!/bin/sh

FNAME=`echo $1 | awk -F. '{ print $1 }'`


cat ${FNAME}.csv | sed '1,$s/^/'${FNAME}',/'

