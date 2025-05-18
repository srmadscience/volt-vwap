package vwapdemo.client;

/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.voltutil.stats.SafeHistogramCache;


public class ComplainOnErrorWithParamsCallback implements ProcedureCallback {

    long startMs = System.currentTimeMillis();
    SafeHistogramCache shc;
    String params;

    public ComplainOnErrorWithParamsCallback(String params,SafeHistogramCache shc) {
        super();
        this.params = params;
        this.shc = shc;
    }

    
    @Override
    public void clientCallback(ClientResponse arg0) throws Exception {
        
        if (arg0.getStatus() != ClientResponse.SUCCESS) {
            Demo.msg("Error Code :" + arg0.getStatusString());
            Demo.msg("Matching Params :" + params);
        }
        
        shc.reportLatency("REPORT_TICK", startMs, "", 1000);

    }

}
