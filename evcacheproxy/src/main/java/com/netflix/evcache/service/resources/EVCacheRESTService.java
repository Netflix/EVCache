package com.netflix.evcache.service.resources;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.evcache.EVCache;
import com.netflix.evcache.EVCacheException;
import com.netflix.evcache.EVCacheLatch;
import com.netflix.evcache.EVCacheLatch.Policy;
import com.netflix.evcache.service.transcoder.RESTServiceTranscoder;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;
import net.spy.memcached.CachedData;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


/**
 * Created by senugula on 3/22/16.
 */

@Singleton
@Path("/evcrest/v1.0")
public class EVCacheRESTService {

    private Logger logger = LoggerFactory.getLogger(EVCacheRESTService.class);

    private final EVCache.Builder builder;
    private final Map<String, EVCache> evCacheMap;
    private final RESTServiceTranscoder evcacheTranscoder = new RESTServiceTranscoder();

    @Inject
    public EVCacheRESTService(EVCache.Builder builder) {
        this.builder = builder;
        this.evCacheMap = new HashMap<>();
    }

    @POST
    @Path("{appId}/{key}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response setOperation(final InputStream in, @PathParam("appId") String pAppId, @PathParam("key") String key,
                                 @QueryParam("ttl") String ttl, @DefaultValue("") @QueryParam("flag") String flag) {
        try {
            final String appId = pAppId.toUpperCase();
            final byte[] bytes = IOUtils.toByteArray(in);
            return setData(appId, ttl, flag, key, bytes);
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        } catch (Throwable t) {
            logger.error("Throwable", t);
            return Response.serverError().build();
        }
    }

    @POST
    @Path("{appId}/bulk")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response bulkPostOperation(final InputStream in, @PathParam("appId") String pAppId) {
        JSONArray dataJSON = null;
        try {
            final String appId = pAppId.toUpperCase();
            final String Json = IOUtils.toString(in);
            if(Json.isEmpty() || Json == null) {
                logger.error("Unable to deserialize json");
                return Response.serverError().build();
            }
            JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON(Json);
            if(jsonObject == null || jsonObject.isEmpty()) {
                logger.error("Unable to deserialize json");
                return Response.serverError().build();
            }
            String ttl = (String) jsonObject.get("ttl");
            String flag = (String) jsonObject.get("flag");
            dataJSON = (JSONArray) jsonObject.get("keys");
            if(dataJSON.isEmpty() || dataJSON.size() == 0) {
                logger.error("No Keys to set for this request");
                return Response.serverError().build();
            }
            for(int indx = 0 ; indx < dataJSON.size() ; indx ++) {
                if(logger.isDebugEnabled()) logger.debug(dataJSON.get(indx).toString());
                JSONObject obj = dataJSON.getJSONObject(indx);
                final String key = obj.getString("key");
                final byte[] data = obj.getString("value").getBytes();
                Response response = setData(appId, ttl, flag, key, data);
                if(response.getStatus() >= 400) return Response.serverError().build();
            }
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        } catch (Throwable t) {
            logger.error("Throwable", t);
            return Response.serverError().build();
        }
        return Response.ok("Bulk Set Operation was successful").build();
    }

    @PUT
    @Path("{appId}/bulk")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response bulkPutOperation(final InputStream in, @PathParam("appId") String pAppId) {
        JSONArray dataJSON = null;
        try {
            final String appId = pAppId.toUpperCase();
            final String Json = IOUtils.toString(in);
            if(Json.isEmpty() || Json == null) {
                logger.error("Unable to deserialize json");
                return Response.serverError().build();
            }
            JSONObject jsonObject = (JSONObject) JSONSerializer.toJSON(Json);
            if(jsonObject == null || jsonObject.isEmpty()) {
                logger.error("Unable to deserialize json");
                return Response.serverError().build();
            }
            String ttl = (String) jsonObject.get("ttl");
            String flag = (String) jsonObject.get("flag");
            dataJSON = (JSONArray) jsonObject.get("keys");
            if(dataJSON.isEmpty() || dataJSON.size() == 0) {
                logger.error("No Keys to set for this request");
                return Response.serverError().build();
            }
            for(int indx = 0 ; indx < dataJSON.size() ; indx ++) {
                if(logger.isDebugEnabled()) logger.debug(dataJSON.get(indx).toString());
                JSONObject obj = dataJSON.getJSONObject(indx);
                final String key = obj.getString("key");
                final byte[] data = obj.getString("value").getBytes();
                Response response = setData(appId, ttl, flag, key, data);
                if(response.getStatus() >= 400) return Response.serverError().build();
            }
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        } catch (Throwable t) {
            logger.error("Throwable", t);
            return Response.serverError().build();
        }
        return Response.ok("Bulk Set Operation was successful").build();
    }
    
    @PUT
    @Path("{appId}/{key}")
    @Consumes({MediaType.APPLICATION_OCTET_STREAM})
    @Produces(MediaType.TEXT_PLAIN)
    public Response putOperation(final InputStream in, @PathParam("appId") String pAppId, @PathParam("key") String key,
                                 @QueryParam("ttl") String ttl, @DefaultValue("") @QueryParam("flag") String flag) {
        try {
            final String appId = pAppId.toUpperCase();
            final byte[] bytes = IOUtils.toByteArray(in);
           	return setData(appId, ttl, flag, key, bytes);
        } catch (EVCacheException e) {
        	logger.error("EVCacheException", e);
            return Response.serverError().build();
        } catch (Throwable t) {
        	logger.error("Throwable", t);
            return Response.serverError().build();
        }
    }

    private Response setData(String appId, String ttl, String flag, String key, byte[] bytes) throws EVCacheException, InterruptedException {
        final EVCache evcache = getEVCache(appId);
        if (ttl == null) {
            return Response.status(400).type(MediaType.TEXT_PLAIN).entity("Please specify ttl for the key " + key + " as query parameter \n").build();
        }
        final int timeToLive = Integer.valueOf(ttl).intValue();
        EVCacheLatch latch = null; 
        if(flag != null && flag.length() > 0) {
        	final CachedData cd = new CachedData(Integer.parseInt(flag), bytes, Integer.MAX_VALUE);
        	latch = evcache.set(key, evcacheTranscoder.encode(cd), timeToLive, Policy.ALL_MINUS_1);
        } else {
        	latch = evcache.set(key, bytes, timeToLive, Policy.ALL_MINUS_1);
        }
        
        if(latch != null) {
        	final boolean status = latch.await(2500, TimeUnit.MILLISECONDS);
        	if(status) {
        		return Response.ok("Set Operation for Key - " + key + " was successful. \n").build(); 
        	} else {
        		if(latch.getCompletedCount() > 0) {
        			if(latch.getSuccessCount() == 0){
        				return Response.serverError().build();
        			} else if(latch.getSuccessCount() > 0 ) {
        				return Response.ok("Set Operation for Key - " + key + " was successful in " + latch.getSuccessCount() + " Server Groups. \n").build();
        			}
        		} else {
        			return Response.serverError().build();
        		}
        	}
        }
        return Response.serverError().build();
    }

    @GET
    @Path("incr/{appId}/{key}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response incrOperation(@PathParam("appId") String appId, @PathParam("key") String key, @DefaultValue("1") @QueryParam("by") String byStr, 
    		@DefaultValue("1") @QueryParam("def") String defStr, @DefaultValue("0") @QueryParam("ttl") String ttlStr) {
        appId = appId.toUpperCase();
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        try {
            final EVCache evCache = getEVCache(appId);
            final long by = Long.parseLong(byStr);
            final long def = Long.parseLong(defStr);
            final int ttl = Integer.parseInt(ttlStr);
            final long val = evCache.incr(key, by, def, ttl);
            return Response.status(200).type(MediaType.TEXT_PLAIN).entity(String.valueOf(val)).build();
        } catch (EVCacheException e) {
        	logger.error("EVCacheException", e);
            return Response.serverError().build();
        }
    }


    @GET
    @Path("decr/{appId}/{key}")
    @Produces({MediaType.TEXT_PLAIN})
    public Response decrOperation(@PathParam("appId") String appId, @PathParam("key") String key, @DefaultValue("1") @QueryParam("by") String byStr, 
    		@DefaultValue("1") @QueryParam("def") String defStr, @DefaultValue("0") @QueryParam("ttl") String ttlStr) {
        appId = appId.toUpperCase();
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        try {
            final EVCache evCache = getEVCache(appId);
            final long by = Long.parseLong(byStr);
            final long def = Long.parseLong(defStr);
            final int ttl = Integer.parseInt(ttlStr);
            final long val = evCache.decr(key, by, def, ttl);
            return Response.status(200).type(MediaType.TEXT_PLAIN).entity(String.valueOf(val)).build();
        } catch (EVCacheException e) {
            logger.error("EVCacheException", e);
            return Response.serverError().build();
        }
    }
    
    @GET
    @Path("{appId}/{key}")
    @Produces({MediaType.APPLICATION_OCTET_STREAM})
    public Response getOperation(@PathParam("appId") String appId, @PathParam("key") String key) {
        appId = appId.toUpperCase();
        if (logger.isDebugEnabled()) logger.debug("Get for application " + appId + " for Key " + key);
        try {
            final EVCache evCache = getEVCache(appId);
            CachedData cachedData = (CachedData) evCache.get(key, evcacheTranscoder);
            if (cachedData == null) {
                return Response.status(404).type(MediaType.TEXT_PLAIN).entity("Key " + key + " Not Found in cache " + appId + "\n").build();
            }
            byte[] bytes = cachedData.getData();
            if (bytes == null) {
                return Response.status(404).type(MediaType.TEXT_PLAIN).entity("Key " + key + " Not Found in cache " + appId + "\n").build();
            } else {
                return Response.status(200).type(MediaType.APPLICATION_OCTET_STREAM).entity(bytes).build();
            }
        } catch (EVCacheException e) {
        	logger.error("EVCacheException", e);
            return Response.serverError().build();

        }
    }


    @DELETE
    @Path("{appId}/{key}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
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
        	logger.error("EVCacheException", e);
            return Response.serverError().build();
        }
    }

    private EVCache getEVCache(String appId) {
        EVCache evCache = evCacheMap.get(appId);
        if (evCache != null) return evCache;
        evCache = builder.setAppName(appId).build();
        evCacheMap.put(appId, evCache);
        return evCache;
    }
}
