package com.netflix.evcache.server.startup;

import com.google.inject.Singleton;
import com.netflix.server.base.BaseStatusPage;

import java.io.PrintWriter;

/**
 * Created by senugula on 10/26/16.
 */
@Singleton
public class StatusPage extends BaseStatusPage {

    private static final long serialVersionUID = 1L;

    protected void getDetails(PrintWriter out, boolean htmlize) {
        super.getDetails(out, htmlize);
    }
}
