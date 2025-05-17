#!/bin/sh

if
	[ ! -d stockdata ]
then
	mkdir stockdata 2> /dev/null
	cd stockdata
	curl -L -o stock-market-dataset.zip https://www.kaggle.com/api/v1/datasets/download/jacksoncrow/stock-market-dataset
	unzip stock-market-dataset.zip
fi

exit 0

