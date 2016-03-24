package com.netflix.evcservice.service;

/**
 * Created by senugula on 03/22/15.
 */
public class HealthCheckHandlerImpl implements com.netflix.karyon.spi.HealthCheckHandler {

    @Override
    public int getStatus() {
        return 200; // TODO
    }
}
