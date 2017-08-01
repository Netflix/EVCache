package com.netflix.evcache.server.resources;


import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.config.DynamicLongProperty;
import com.netflix.evcache.server.keydump.KeysRequestCache;
import com.netflix.evcache.server.keydump.RequestDAO;
import com.netflix.evcache.server.keydump.RequestProcessor;

/**
 * Created by senugula on 10/12/16.
 */

@Path("/")
@Singleton
public class EVCacheServerRESTAPI {
    private static final Logger logger = LoggerFactory.getLogger(EVCacheServerRESTAPI.class);

    private static final DynamicLongProperty minKeyDumpRate = new DynamicLongProperty("evcache.mnemonic.dumpkeys.rate", 15000000);

    private final RequestProcessor requestProcessor;

    @Inject
    public EVCacheServerRESTAPI(RequestProcessor requestProcessor) {
        this.requestProcessor = requestProcessor;
    }

    @GET
    @Path("/dumpkeys")
    @Produces({MediaType.APPLICATION_JSON})
    public Response dumpKeys(@QueryParam("s3folder") String s3folder,
            @QueryParam("s3file") String s3file,
            @QueryParam("taskid") String taskId,
            @QueryParam("id") String requestId,
            @QueryParam("percentKeys") String percentKeys,
            @QueryParam("numlines") long lines,
            @QueryParam("dumprate") long keyDumpRate, 
            @DefaultValue("false") @QueryParam("includeValue") boolean includeValue) {
        try {
            if (!StringUtils.isEmpty(taskId) && taskId != null) {
                RequestDAO requestDAO = KeysRequestCache.getInstance().getReqStatus(taskId);
                if (requestDAO == null) {
                    return Response.status(404).entity("TasKId " + taskId + " does not exist").build();
                }
                final Gson gson = new Gson();
                final String responseJSON = gson.toJson(requestDAO);
                return Response.status(200).entity(responseJSON).build();
            }
            String pendingTask = KeysRequestCache.getInstance().getPendingTask();
            if (!StringUtils.isEmpty(pendingTask) && pendingTask != null) {
                return Response.ok().entity(pendingTask).build();
            }
            if (StringUtils.isEmpty(s3folder) || s3folder == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S3Folder is " + s3folder);
                }
                return Response.status(400).entity("Keydumps require S3 folder parameter").build();
            }
            if (StringUtils.isEmpty(s3file) || s3file == null) {
                if (logger.isDebugEnabled()) {
                    logger.debug("S3file is " + s3file);
                }
                return Response.status(400).entity("Keydumps require S3 file parameter").build();
            }
            double percent = 0.98; //default 98 percent keys. This will be used only if evcache.keydump.percent is set to true
            if (!StringUtils.isEmpty(percentKeys) || percentKeys != null) {
                percent = Double.parseDouble(percentKeys) / 100;
                if (logger.isDebugEnabled()) {
                    logger.debug("Percentage Keys " + percent);
                }
            }
            String newTaskId = null;
            if (StringUtils.isEmpty(requestId) || requestId == null) {
                newTaskId = getTaskID();
            } else {
                newTaskId = requestId;
            }
            if (keyDumpRate <= 100000) {
                keyDumpRate = minKeyDumpRate.get();
            }
            requestProcessor.handleRequest(newTaskId, s3folder, s3file, percent, lines, keyDumpRate, includeValue);
            return Response.status(202).entity(newTaskId).build();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Failed dumping keys", e);
            return Response.status(500).entity("Could not get Status. Please re-try your request again...").build();
        }
    }

    private String getTaskID() {
        return DateTimeFormat.forPattern("yyyyMMddHHmmss").print(System.currentTimeMillis());
    }
}
