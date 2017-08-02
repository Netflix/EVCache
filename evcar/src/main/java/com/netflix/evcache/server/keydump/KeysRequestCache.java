package com.netflix.evcache.server.keydump;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.netflix.evcache.server.keydump.RequestDAO.TaskStatus;


/**
 * Created by senugula on 9/27/16.
 */
//@Singleton
public class KeysRequestCache {

    private static final Logger log = LoggerFactory.getLogger(KeysRequestCache.class);
    private final Cache<String, RequestDAO> reqCache ;
    private static final KeysRequestCache instance = new KeysRequestCache();

    private KeysRequestCache() {
        reqCache = CacheBuilder.newBuilder().expireAfterWrite(12, TimeUnit.HOURS).build();
    }

    public static KeysRequestCache getInstance() {
        return instance;
    }

    public void addToReqCache(String id, RequestDAO requestDAO) {
        reqCache.put(id, requestDAO);
    }

    public RequestDAO getReqStatus(String id) {
        return reqCache.getIfPresent(id);
    }

    public  String getPendingTask() {
        ConcurrentMap taskMap = reqCache.asMap();
        Iterator iterator = taskMap.keySet().iterator();
        log.info("All keys are " + taskMap.entrySet());
        while (iterator.hasNext()) {
            RequestDAO requestDAO = (RequestDAO) taskMap.get(iterator.next());
            log.info("Request taskid is " + requestDAO.getTaskId());
            TaskStatus status = requestDAO.getStatus();
            log.info("Request status is " + status);
            if (status != null && status == TaskStatus.PENDING) {
                return requestDAO.getTaskId();
            }
        }
        return null;
    }
}
