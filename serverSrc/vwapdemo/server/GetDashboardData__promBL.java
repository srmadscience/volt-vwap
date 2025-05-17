/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */
package vwapdemo.server;

import java.math.BigDecimal;
import java.math.RoundingMode;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class GetDashboardData__promBL extends VoltProcedure {

     private static final String DAY = "DAY";
    private static final String MONTH = "MONTH";
    private static final String WEEK = "WEEK";
    private static final String ROLLWEEK = "ROLLWEEK";
 
    // @formatter:off


    public static final SQLStmt getMaxdate = new SQLStmt(
            "select max(tickdate) maxdate from stocktick_vmap_summary where timescale = ?;");

    public static final SQLStmt getLatestVolume = new SQLStmt(
            "select 'latest_vwap_volume' STATNAME, 'latest_vwap_volume' STATHELP, timescale,  volume stat_value from stocktick_vmap_summary where timescale = ? and tickdate = ?;");

    public static final SQLStmt getLatestTotalValue = new SQLStmt(
            "select 'latest_vwap_total_value' STATNAME, 'latest_vwap_total_value' STATHELP, timescale,  TOTAL_VALUE stat_value from stocktick_vmap_summary where timescale = ? and tickdate = ?;");

    public static final SQLStmt getLatestVWAP = new SQLStmt(
            "select 'latest_vwap_vwap' STATNAME, 'latest_vwap_vwap' STATHELP, timescale,  TOTAL_VALUE / Volume stat_value from stocktick_vmap_summary where timescale = ? and tickdate = ?;");

   
	// @formatter:on
    final String[] theTimes = { MONTH, ROLLWEEK,  WEEK, DAY };
  
    public VoltTable[] run()
            throws VoltAbortException {

        for (int i = 0; i < theTimes.length; i++) {
            voltQueueSQL(getMaxdate, theTimes[i]);
        }

    
        VoltTable[] currentMaxDates = voltExecuteSQL();

        for (int i = 0; i < theTimes.length; i++) {
          
            if (currentMaxDates[i].advanceRow()) {
                TimestampType theTimestamp = 
                        currentMaxDates[i].getTimestampAsTimestamp("maxdate");
                voltQueueSQL(getLatestVolume, theTimes[i], theTimestamp);
                voltQueueSQL(getLatestTotalValue, theTimes[i], theTimestamp);
                voltQueueSQL(getLatestVWAP, theTimes[i], theTimestamp);
            }
 
        }

        return voltExecuteSQL(true);
    }
}
