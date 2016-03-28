package com.netflix.evcache.service.resources;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;


/**
 * Created by senugula on 3/22/16.
 */
@Path("/evcache")
public class EVCacheRESTService {

    private Logger logger = LoggerFactory.getLogger(EVCacheRESTService.class);

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
        if(logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        final EVCache evCache = getEVCache(appId);
        try {
            final Object _response = evCache.get(key);
            if(_response == null) {
                return Response.ok("Key " + key + " was not found.").build();
            } else {
                return Response.ok(_response).build();
            }
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
        if(logger.isDebugEnabled()) logger.debug("Set for application " + appId + " for Key " + key + " value " + value + "  with ttl " + ttl );
        try {
            final EVCache evcache = getEVCache(appId);
            int timeToLive = Integer.valueOf(ttl).intValue();
            Future<Boolean>[] _future = evcache.set(key, value, timeToLive);
            if (_future.equals(Boolean.TRUE)) {
                if(logger.isDebugEnabled()) logger.debug("set key is successful");
            }
            return Response.ok("Set Operation for Key - " + key + " was successful.").build();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return Response.serverError().build();
    }

    @GET
    @Path("delete/{appId}/{key}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces("text/plain")
    public Response delete(@PathParam("appId") String appId, @PathParam("key") String key) {
        if(logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        final EVCache evCache = getEVCache(appId);
        try {
            Future<Boolean>[] _future = evCache.delete(key);
            if (_future.equals(Boolean.TRUE)) {
                if(logger.isDebugEnabled()) logger.debug("set key is successful");
            }
            return Response.ok("Deleted Operation for Key - " + key + " was successful.").build();
        } catch (EVCacheException e) {
            e.printStackTrace();
        }
        return Response.serverError().build();
    }

    @POST
    @Path("set/{appId}/{key}/{ttl}")
    @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
    @Produces(MediaType.TEXT_PLAIN)
    public Response postSetOperation(@PathParam("appId") String appId, @PathParam("key") String key,
            @PathParam("ttl") String ttl, @FormParam("value") String value) {
        if(logger.isDebugEnabled()) logger.debug("Post for application " + appId + " for Key " + key + " value " + value);
        final EVCache evcache = getEVCache(appId);
        try {
            int timeToLive = Integer.valueOf(ttl).intValue();
            Future<Boolean>[] _future = evcache.set(key, value, timeToLive);
            if (_future.equals(Boolean.TRUE)) {
                if(logger.isDebugEnabled()) logger.debug("set key is successful");
            }
            return Response.ok("Set Operation for Key - " + key + " was successful.").build();
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
        if(logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        final EVCache evCache = getEVCache(appId);
        try {
            Future<Boolean>[] _future = evCache.delete(key);
            if (_future.equals(Boolean.TRUE)) {
                if(logger.isDebugEnabled()) logger.debug("set key is successful");
            }
            return Response.ok("Deleted Operation for Key - " + key + " was successful.").build();
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