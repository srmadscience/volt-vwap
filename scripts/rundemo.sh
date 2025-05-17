#!/bin/sh

#
#  Copyright (C) 2025 Volt Active Data Inc.
# 
#  Use of this source code is governed by an MIT
#  license that can be found in the LICENSE file or at
#  https://opensource.org/licenses/MIT.
# 

. $HOME/.profile

TPMS=$1

java  ${JVMOPTS}  -jar ../jars/volt-vwap-client.jar  `cat $HOME/.vdbhostnames` stockdata/stocks/stocks_sorted_by_date.csv  $TPMS


exit 0 
