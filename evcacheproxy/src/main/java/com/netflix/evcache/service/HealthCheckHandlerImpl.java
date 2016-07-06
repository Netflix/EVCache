package com.netflix.evcache.service;

import com.google.inject.Singleton;
import com.netflix.server.base.BaseHealthCheckServlet;

/**
 * Created by senugula on 03/22/15.
 */
@Singleton
public class HealthCheckHandlerImpl extends BaseHealthCheckServlet {

    public int getStatus() {
        return 200; // TODO
    }
 }