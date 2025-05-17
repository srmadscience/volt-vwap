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

public class ReportTick extends VoltProcedure {

    private static final String STOCKTICK_COLUMNS = "(symbol, tickdate, timescale, open_price"
            + ", high_price, low_price, close_price, volume, total_value, vmap) ";
    private static final String DAY = "DAY";
    private static final String MONTH = "MONTH";
    private static final String WEEK = "WEEK";
    private static final String INSERT_TICK_START = "INSERT INTO stocktick_vmap " + STOCKTICK_COLUMNS
            + "VALUES (?,TIME_WINDOW(";
    private static final String INSERT_TICK_END = ",1,?),?,?,?,?,?,?,?,?);";

    // @formatter:off


    public static final SQLStmt getRollingDay = new SQLStmt(
            "SELECT TIME_WINDOW(DAY,1,?) TICKDATE FROM dummy WHERE x = 'X';");

    public static final SQLStmt getWeek = new SQLStmt(
            "SELECT * FROM stocktick_vmap WHERE symbol = ? AND tickdate = TIME_WINDOW("+WEEK+",1,?) AND timescale = '"+WEEK+"';");

    public static final SQLStmt getDay = new SQLStmt(
            "SELECT * FROM stocktick_vmap WHERE symbol = ? AND tickdate = TIME_WINDOW("+DAY+",1,?) AND timescale = '"+DAY+"';");

    public static final SQLStmt getMonth = new SQLStmt(
            "SELECT * FROM stocktick_vmap WHERE symbol = ? AND tickdate = TIME_WINDOW("+MONTH+",1,?) AND timescale = '"+MONTH+"';");

    public static final SQLStmt insertTickDay = new SQLStmt(
            INSERT_TICK_START+DAY+INSERT_TICK_END);

    public static final SQLStmt insertTickWeek = new SQLStmt(
            INSERT_TICK_START+WEEK+INSERT_TICK_END);

    public static final SQLStmt insertTickMonth = new SQLStmt(
            INSERT_TICK_START+MONTH+INSERT_TICK_END);

    public static final SQLStmt updateTick = new SQLStmt(
            "UPDATE stocktick_vmap "
            + "SET low_price = ?, high_price = ?, close_price = ?, volume = ?, total_value = ?, vmap = ? "
            + "WHERE symbol = ? AND tickdate = ? AND timescale = ? ;");

    public static final SQLStmt upsertRollingWeekTick = new SQLStmt(
            "UPSERT INTO stocktick_vmap " + STOCKTICK_COLUMNS +
            " select v.symbol, CAST(? AS TIMESTAMP), 'ROLLWEEK', 0, min(v.low_price) low_price"
            + ", max(v.high_price) high_price,CAST (? AS DECIMAL),  sum(v.volume) volume, sum(v.total_value) total_value, sum(total_value) / sum(volume) vmap  "
            + "from stocktick_vmap  v "
            + "where v.timescale = 'DAY' AND v.symbol = ? "
            + "AND v.tickdate BETWEEN DATEADD(DAY,-7,?) AND ? "
            + "group by v.symbol, 'ROLLWEEK', 0;");


	// @formatter:on
    final String[] theTimes = { MONTH, WEEK, DAY };
    final SQLStmt[] theGets = { getMonth, getWeek, getDay, };
    final SQLStmt[] theInserts = { insertTickMonth, insertTickWeek, insertTickDay };

    public VoltTable[] run(String symbol, TimestampType tickdate, BigDecimal value, BigDecimal volume)
            throws VoltAbortException {

        for (int i = 0; i < theTimes.length; i++) {
            voltQueueSQL(theGets[i], symbol, tickdate);
        }

        voltQueueSQL(getRollingDay, tickdate);

        VoltTable[] currentTicks = voltExecuteSQL();

        for (int i = 0; i < theTimes.length; i++) {
            if (!currentTicks[i].advanceRow()) {
                // Create new entry

                BigDecimal vmap = null;

                try {
                    vmap = value.multiply(volume).divide(volume);
                } catch (java.lang.ArithmeticException e) {
                    vmap = new BigDecimal(0);
                }

                voltQueueSQL(theInserts[i], symbol, tickdate, theTimes[i], value, value, value, value, volume,
                        value.multiply(volume), vmap);

            } else {

                // update existing entry
                final TimestampType actualTickdate = currentTicks[i].getTimestampAsTimestamp("TICKDATE");

                BigDecimal low = currentTicks[i].getDecimalAsBigDecimal("LOW_PRICE");

                if (value.compareTo(low) == -1) {
                    low = value;
                }

                BigDecimal high = currentTicks[i].getDecimalAsBigDecimal("HIGH_PRICE");

                if (value.compareTo(high) == 1) {
                    high = value;
                }

                final BigDecimal newEntryVolume = volume.add(currentTicks[i].getDecimalAsBigDecimal("VOLUME"));
                final BigDecimal extraTotalValue = value.multiply(volume);
                final BigDecimal newEntryTotalValue = extraTotalValue
                        .add(currentTicks[i].getDecimalAsBigDecimal("TOTAL_VALUE"));

                BigDecimal newVmap = null;

                try {
                    newVmap = newEntryTotalValue.divide(newEntryVolume, 12, RoundingMode.HALF_UP);
                } catch (java.lang.ArithmeticException e) {
                    newVmap = new BigDecimal(0);
                }

                voltQueueSQL(updateTick, low, high, value, newEntryVolume, newEntryTotalValue, newVmap, symbol,
                        actualTickdate, theTimes[i]);
            }

        }

        currentTicks[currentTicks.length - 1].advanceRow();
        final TimestampType rollingTickdate = currentTicks[currentTicks.length - 1].getTimestampAsTimestamp("TICKDATE");
        voltQueueSQL(upsertRollingWeekTick, rollingTickdate, value, symbol, rollingTickdate, rollingTickdate);

        return voltExecuteSQL(true);
    }
}
