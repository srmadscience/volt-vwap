/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package vwapdemo.client;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.voltutil.stats.SafeHistogramCache;

public class Demo {

    SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");

    Client mainClient = null;

    // Input text file path.
    String filename = null;

    int tpMs = 1;

    public Demo(String hostnames, String filename, int tpMs) {

        super();

        try {
            mainClient = connectVoltDB(hostnames);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(3);
        }

        this.filename = filename;
        this.tpMs = tpMs;
    }

    private void reset() throws IOException, NoConnectionsException, ProcCallException, InterruptedException {

        msg("Reset Database Starting...");
        mainClient.callProcedure("ResetDatabase");
        Thread.sleep(5000);
        msg("Reset Database Finished.");

    }

    @SuppressWarnings("unused")
    public int load() {

        long lastDashboardTime = 0;
        long lastClockTime = 0;

        SafeHistogramCache shc = SafeHistogramCache.getInstance();

        int maxEventCount = 0;
        int eventCount = 0;
        int skipCount = 0;
        File f = new File(filename);

        long tpThisPerMs = tpMs;

        if (f.exists() && f.isFile() && f.canRead()) {
            BufferedReader br = null;
            try {

                long currentMs = System.currentTimeMillis();
                int transThisMs = 0;

                FileReader fr = new FileReader(f);
                br = new BufferedReader(fr);

                String line;
                Date lastTimeSeen = new Date();
                long startOfThisMinute = 0;
                String lastDaySeen = "";
                int day = 1;

                while ((line = br.readLine()) != null) {

                    if (eventCount++ % 100000 == 0) {
                        msg("Row " + eventCount + " of file");
                    }

                    // Some lines are mostly blank. Others are headers...
                    if (line.endsWith(",,,,,,") || line.endsWith("Date,Open,High,Low,Close,Adj Close,Volume")) {
                        skipCount++;
                    } else {

                        String[] lineContents = line.split(",");
                        try {

                            // As of now we don't use all these values...
                            String symbol = lineContents[0];
                            Date tickdate = df1.parse(lineContents[1]);
                            double open = Double.parseDouble(lineContents[2]);
                            double high = Double.parseDouble(lineContents[3]);
                            double low = Double.parseDouble(lineContents[4]);
                            double close = Double.parseDouble(lineContents[5]);
                            double adjClose = Double.parseDouble(lineContents[6]);
                            double volume = Double.parseDouble(lineContents[7]);

                            if (volume > 0) {
                                // Do not create Zero volume entries
                                ComplainOnErrorWithParamsCallback cwbc = new ComplainOnErrorWithParamsCallback(line,
                                        shc);
                                mainClient.callProcedure(cwbc, "ReportTick", symbol, tickdate, close, volume);
                            }

                        } catch (Exception e) {
                            msg("Event " + eventCount + " is bad. line=" + line + " error=" + e.getMessage());
                        }

                        // If we've already done as many requests this ms
                        // as we're supposed to, sleep...
                        if (transThisMs++ > tpThisPerMs) {

                            // Sleep until the MS has changed...
                            while (currentMs == System.currentTimeMillis()) {
                                Thread.sleep(0, 50000);

                            }

                            // currentMs has changed...
                            currentMs = System.currentTimeMillis();
                            transThisMs = 0;
                        }
                    }

                }

                br.close();
                fr.close();
                mainClient.drain();

            } catch (Exception e) {
                msg(e.getMessage());
            }

            msg("Finished...");
            msg(eventCount + " Reports, " + skipCount + " skipped");

            msg(shc.toString());

        } else {
            msg("Can't load file " + filename);
            System.exit(1);

        }

        return maxEventCount;

    }

    private static Client connectVoltDB(String hostname) throws Exception {
        Client client = null;
        ClientConfig config = null;

        try {
            msg("Logging into VoltDB");

            config = new ClientConfig(); // "admin", "idontknow");
            config.setMaxOutstandingTxns(20000);
            config.setMaxTransactionsPerSecond(200000);
            config.setTopologyChangeAware(true);

            client = ClientFactory.createClient(config);

            String[] hostnameArray = hostname.split(",");

            for (String element : hostnameArray) {
                msg("Connect to " + element + "...");
                try {
                    client.createConnection(element);
                } catch (Exception e) {
                    msg(e.getMessage());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("VoltDB connection failed.." + e.getMessage(), e);
        }

        return client;

    }

    public static void msg(String message) {

        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date now = new Date();
        String strDate = sdfDate.format(now);
        System.out.println(strDate + ":" + message);
    }

    private void closeClients() {

        try {
            mainClient.close();
        } catch (InterruptedException e) {
            Demo.msg(e.getMessage());
        }

    }

    public static void main(String[] args) {

        try {

            if (args.length < 3) {
                msg("Usage: hostnames filename tpms");
                System.exit(1);
            }

            String hostnames = args[0];
            String filename = args[1];
            int tpMsOrSpeedup = Integer.parseInt(args[2]);

            File testFile = new File(filename);

            if (!testFile.exists()) {
                msg("File '" + filename + "' not found");
                System.exit(2);
            }

            msg("Hostnames=" + hostnames);
            msg("File = " + filename);
            msg("transactions per MS = " + tpMsOrSpeedup);

            Demo l = new Demo(hostnames, filename, tpMsOrSpeedup);

            l.reset();
            l.load();
            l.closeClients();

            System.exit(0);

        } catch (Exception e) {

            e.printStackTrace();
        }

    }

}
