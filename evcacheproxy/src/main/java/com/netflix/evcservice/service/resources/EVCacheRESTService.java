package com.netflix.evcservice.service.resources;

import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;
import com.netflix.logging.ILog;
import com.netflix.logging.LogManager;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import com.google.inject.Inject;


/**
 * Created by senugula on 3/22/16.
 */
@Path("/evcache")
public class EVCacheRESTService {

    private ILog logger = LogManager.getLogger(EVCacheRESTService.class);

    private final EVCache.Builder builder;
    private final Map<String, EVCache> evCacheMap;

    @Inject
    public EVCacheRESTService(EVCache.Builder builder) {
        this.builder = builder;
        this.evCacheMap = new HashMap<>();
    }

    @GET
    @Path("get/{appId}/{key}")
    @Produces("text/plain")
    public Response getOperation(@PathParam("appId") String appId,
                                 @PathParam("key") String key) {
        logger.info("Get for application " + appId + " for Key " + key);
        final EVCache evCache = getEVCache(appId);
        try {
            String _response = evCache.<String>get(key);
            return Response.ok(_response).build();
        } catch (EVCacheException e) {
            e.printStackTrace();
        }
        return Response.serverError().build();
    }

    @GET
    @Path("set/{appId}/{key}/{ttl}/{value}")
    @Produces("text/plain")
    public Response setOperation(@PathParam("appId") String appId, @PathParam("key") String key,
                                 @PathParam("ttl") String ttl, @PathParam("value") String value) {
        logger.info("Set for application " + appId + " for Key " + key + " value " + value + "  with ttl " + ttl );
        try {
            final EVCache evcache = getEVCache(appId);
            int timeToLive = Integer.valueOf(ttl).intValue();
            Future<Boolean>[] _future = evcache.set(key, value, timeToLive);
            if (_future.equals(Boolean.TRUE)) {
                logger.info("set key is successful");
            }
            return Response.ok("Set Key Operation successful").build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.serverError().build();
    }
    @POST
    @Path("set/{appId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces("text/plain")
    public Response postSetOperation(@PathParam("appId") String appId,
                                     @QueryParam("key") String key, @QueryParam("ttl") String ttl,
                                     @QueryParam("value") String value) {
        logger.info("Post for application " + appId + " for Key " + key + " value " + value);
        final EVCache evcache = getEVCache(appId);
        try {
            int timeToLive = Integer.getInteger(ttl).intValue();
            Future<Boolean>[] _future = evcache.set(key, value, timeToLive);
            if (_future.equals(Boolean.TRUE)) {
                logger.info("set key is successful");
            }
            return Response.ok("Set Key Operation Successful").build();
        } catch (EVCacheException e) {
            e.printStackTrace();
        }
        return Response.serverError().build();
    }


    @DELETE
    @Path("delete/{appId}/{key}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces("text/plain")
    public Response deleteOperation(@PathParam("appId") String appId, @PathParam("key") String key) {
        logger.info("Get for application " + appId + " for Key " + key);
        final EVCache evCache = getEVCache(appId);
        try {
            Future<Boolean>[] _future = evCache.delete(key);
            if (_future.equals(Boolean.TRUE)) {
                logger.info("set key is successful");
            }
            return Response.ok("Delete Key Operation Successful").build();
        } catch (EVCacheException e) {
            e.printStackTrace();
        }
        return Response.serverError().build();
    }

    private EVCache getEVCache(String appId) {
        EVCache evCache = evCacheMap.get(appId);
        if (evCache != null) return evCache;
        evCache = builder.setAppName(appId).build();
        evCacheMap.put(appId, evCache);
        return evCache;
    }
}