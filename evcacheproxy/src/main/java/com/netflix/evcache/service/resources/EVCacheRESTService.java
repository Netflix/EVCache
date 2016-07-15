package com.netflix.evcache.service.resources;

import com.google.inject.Inject;
import com.netflix.config.DynamicStringSetProperty;
import com.netflix.discovery.shared.Applications;
import com.netflix.discovery.shared.Application;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.service.transcoder.RESTServiceTranscoder;
import net.spy.memcached.CachedData;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;


/**
 * Created by senugula on 3/22/16.
 */
@Path("/evcrest/v1.0")
public class EVCacheRESTService {

    private Logger logger = LoggerFactory.getLogger(EVCacheRESTService.class);

    private final EVCache.Builder builder;
    private static Map<String, EVCache> evCacheMap = new HashMap<>();;
    private final RESTServiceTranscoder evcacheTranscoder = new RESTServiceTranscoder();
    private final EVCacheClientPoolManager evCacheClientPoolManager;
    private final DynamicStringSetProperty ignoreApps = new DynamicStringSetProperty("evcacheproxy.ignore.apps", "EVCACHEPROXY,EVCACHESTATS,EVCACHEPLUGIN");

    @Inject
    public EVCacheRESTService(EVCache.Builder builder, EVCacheClientPoolManager evCacheClientPoolManager) {
        this.builder = builder;
        this.evCacheClientPoolManager = evCacheClientPoolManager;
    }

    @POST
    @Path("{appId}/{key}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response setOperation(final InputStream in, @PathParam("appId") String appId, @PathParam("key") String key,
                                 @QueryParam("ttl") String ttl) {
        try {
            appId = appId.toUpperCase();
            byte[] bytes = IOUtils.toByteArray(in);
            final EVCache evcache = getEVCache(appId);
            if (ttl == null) {
                return Response.status(400).type("text/plain").entity("Please specify ttl for the key " + key + " as query parameter \n").build();
            }
            int timeToLive = Integer.valueOf(ttl).intValue();
            Future<Boolean>[] _future = evcache.set(key, bytes, timeToLive);
            if (_future.equals(Boolean.TRUE)) {
                if (logger.isDebugEnabled()) logger.debug("set key is successful \n");
            }
            return Response.ok("Set Operation for Key - " + key + " was successful. \n").build();
        } catch (EVCacheException e) {
            e.printStackTrace();
            return Response.serverError().build();
        } catch (Throwable t) {
            return Response.serverError().build();
        }
    }

    @PUT
    @Path("{appId}/{key}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response putOperation(final InputStream in, @PathParam("appId") String appId, @PathParam("key") String key,
                                 @QueryParam("ttl") String ttl) {
        try {
            appId = appId.toUpperCase();
            byte[] bytes = IOUtils.toByteArray(in);
            final EVCache evcache = getEVCache(appId);
            if (ttl == null) {
                return Response.status(400).type("text/plain").entity("Please specify ttl for the key " + key + " as query parameter \n").build();
            }
            int timeToLive = Integer.valueOf(ttl).intValue();
            Future<Boolean>[] _future = evcache.set(key, bytes, timeToLive);
            if (_future.equals(Boolean.TRUE)) {
                if (logger.isDebugEnabled()) logger.debug("set key is successful \n");
            }
            return Response.ok("Set Operation for Key - " + key + " was successful. \n").build();
        } catch (EVCacheException e) {
            e.printStackTrace();
            return Response.serverError().build();
        } catch (Throwable t) {
            return Response.serverError().build();
        }
    }

    @GET
    @Path("{appId}/{key}")
    @Produces({MediaType.APPLICATION_OCTET_STREAM})
    public Response getOperation(@PathParam("appId") String appId,
                                 @PathParam("key") String key) {
        appId = appId.toUpperCase();
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        try {
            final EVCache evCache = getEVCache(appId);
            CachedData cachedData = (CachedData) evCache.get(key, evcacheTranscoder);
            if (cachedData == null) {
                return Response.status(404).type("text/plain").entity("Key " + key + " Not Found in cache " + appId + "\n").build();
            }
            byte[] bytes = cachedData.getData();
            if (bytes == null) {
                return Response.status(404).type("text/plain").entity("Key " + key + " Not Found in cache " + appId + "\n").build();
            } else {
                return Response.status(200).type("application/octet-stream").entity(bytes).build();
            }
        } catch (EVCacheException e) {
            e.printStackTrace();
            return Response.serverError().build();

        }
    }


    @DELETE
    @Path("{appId}/{key}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces("text/plain")
    public Response deleteOperation(@PathParam("appId") String appId, @PathParam("key") String key) {
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        appId = appId.toUpperCase();
        final EVCache evCache = getEVCache(appId);
        try {
            Future<Boolean>[] _future = evCache.delete(key);
            if (_future.equals(Boolean.TRUE)) {
                if (logger.isDebugEnabled()) logger.debug("set key is successful");
            }
            return Response.ok("Deleted Operation for Key - " + key + " was successful. \n").build();
        } catch (EVCacheException e) {
            e.printStackTrace();
            return Response.serverError().build();
        }
    }

    private EVCache getEVCache(String appId) {
        EVCache evCache = evCacheMap.get(appId);
        if (evCache != null) return evCache;
        logger.debug("Initialize cache that is not intialized during start up");
        evCache = builder.setAppName(appId).build();
        evCacheMap.put(appId, evCache);
        return evCache;
    }

    public void initializeCaches() {
        final Applications apps = evCacheClientPoolManager.getDiscoveryClient().getApplications();
        if (apps != null) {
            final List<Application> appList = apps.getRegisteredApplications();
            for (Application app : appList) {
                final String appName = app.getName().toUpperCase();
                if (appName.startsWith("EVCACHE") && !(ignoreApps.get().contains(appName))) {
                    logger.debug("Initialize cache for application " + appName);
                    EVCache evCache = evCacheMap.get(appName);
                    evCache = builder.setAppName(appName).build();
                    evCacheMap.put(appName, evCache);
                    logger.debug("Done Initialize cache for appplication " + appName);
                }
            }
        }
    }
}
