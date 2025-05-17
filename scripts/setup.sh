#!/bin/sh

#
#  Copyright (C) 2025 Volt Active Data Inc.
# 
#  Use of this source code is governed by an MIT
#  license that can be found in the LICENSE file or at
#  https://opensource.org/licenses/MIT.
# 

. $HOME/.profile


cd
mkdir logs 2> /dev/null
cd voltdb-vwap/ddl

sqlcmd --servers=vdb1 < create_db.sql

cd ../scripts
$HOME/bin/reload_dashboards.sh voltdb-vwap.json

#java  ${JVMOPTS}  -jar $HOME/bin/addtodeploymentdotxml.jar `cat $HOME/.vdbhostnames`  deployment $HOME/voltdb-charglt/scripts/export_and_import.xml

sh getdata.sh
sh sort_by_date.sh 

exit 0 
