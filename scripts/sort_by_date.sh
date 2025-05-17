#!/bin/sh

cd stockdata/stocks

for i in *.csv
do
	sh ../../fixme.sh $i > $i.2
	rm $i
done
sort -t, -k 2  *.csv.2 > stocks_sorted_by_date.csv
