package com.netflix.evcache.service;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Created by senugula on 03/22/15.
 */
@Path("/healthcheck")
public class HealthCheckHandlerImpl {

    @GET
    @Path("/")
    @Produces("text/html")
    public Response checkHealth() {
        return Response.ok("pass").build();
    }

}
