package com.netflix.evcache.event.write;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.ibm.icu.util.StringTokenizer;
import com.netflix.config.NetflixConfiguration;
import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.EVCacheClientPoolManager;
import com.netflix.evcache.pool.ServerGroup;
import com.netflix.nfkafka.NFKafkaConsumerBuilder;
import com.netflix.nfkafka.schlep.consumer.IncomingMessage;
import com.netflix.nfkafka.schlep.serializer.MessageAttributes;

import net.spy.memcached.CachedData;

public class FailedWriteConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FailedWriteConsumer.class);
    private final KafkaConsumer<Void, IncomingMessage> consumer;
    private final EVCacheClientPoolManager poolManager;
    private final CachedDataTranscoder transcoder;

    @SuppressWarnings("unchecked")
    @Inject
    public FailedWriteConsumer(NFKafkaConsumerBuilder nfKafkaConsumerBuilder, EVCacheClientPoolManager poolManager) {
        this.poolManager = poolManager;
        this.transcoder = new CachedDataTranscoder();
        final String consumerId = "FailWriteFixer-" + System.currentTimeMillis();
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("group.id", consumerId);
        map.put("client.id", consumerId);
        map.put("auto.offset.reset", "earliest");
        map.put("compression.type", "gzip");
        map.put("key.deserializer", "com.netflix.nfkafka.serializer.VoidDeserializer");
        map.put("value.deserializer", "com.netflix.nfkafka.schlep.serialization.SchlepDeserializer");
        map.put("bootstrap.servers",NetflixConfiguration.getInstanceNoThrows().getString("bootstrap.servers", "share.kafka.${EC2_REGION}.dyntest.netflix.net:7101"));
        map.put("enable.auto.commit", "false");
        this.consumer = nfKafkaConsumerBuilder.build("FailWriteFixer",map);
        try {
            consumer.subscribe(ImmutableList.of("evcache-write_failures"));
        } catch(Exception e) {
            log.error("Exception initializing kafka", e);
        }
        
        final Thread consumerThread = new Thread(this);
        consumerThread.start();
    }

    public static void main(String[] args) {
        
    }

    @Override
    public void run() {
        int i = 0;
        while (true) {
            try {
                ConsumerRecords<Void, IncomingMessage> records = consumer.poll(500);
                if(records.isEmpty()) {
                    if(log.isDebugEnabled()) log.debug("No records : sleeping for 30 seconds ");
                    Thread.sleep(10000);
                    continue;
                }
                if(log.isInfoEnabled()) log.info("Got " +records.count() + " records from kafka");
                for (ConsumerRecord<Void, IncomingMessage> record : records) {
                    
                    if(log.isDebugEnabled()) log.debug("\n\nRecord : " + i++ );
                    final IncomingMessage msg = record.value();
                    try {
                        if(log.isDebugEnabled()) log.debug("Key : " + msg.getEntity(String.class));
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    MessageAttributes attribs = msg.getAttributes();
                    for(Entry<String, String> entry : attribs.entrySet()) {
                        if(log.isDebugEnabled()) log.debug("\t" + entry.getKey() + " : " + entry.getValue());
                    }
                    final String key = attribs.get("key");
                    String appName  = attribs.get("cache");
//                    final String prefix = attribs.get("prefix");
                    final String checksumString = attribs.get("checksum");
                    final String lengthString = attribs.get("length");
                    final String flagString = attribs.get("flag");
                    final String failedServerGroups  = attribs.get("FailedServerGroups");
                    final String op = attribs.get("op");
                    if(failedServerGroups != null) {
                        final StringTokenizer st = new StringTokenizer(failedServerGroups, ",");
                        while(st.hasMoreTokens()) {
                            final String serverGroup = st.nextToken();
                            if(appName == null) {
                                final int index = serverGroup.indexOf('-');
                                if(index > 0) {
                                    appName = serverGroup.substring(0, index);
                                }
                            }
                            if(log.isDebugEnabled()) log.debug("appName : " + appName);
                            if(log.isDebugEnabled()) log.debug("serverGroup : " + serverGroup);
                            if(appName != null) {
                                final Map<ServerGroup, List<EVCacheClient>> pools = poolManager.getEVCacheClientPool(appName).getAllInstancesByZone();
                                for(Entry<ServerGroup, List<EVCacheClient>> entry : pools.entrySet()) {
                                    if(entry.getKey().getName().equals(serverGroup)) {
                                        if(entry.getValue() != null && entry.getValue().size() > 0) {
                                            final EVCacheClient client = entry.getValue().get(0);
                                            final CachedData data = client.get(key, transcoder, false, false);
                                            if(data != null) {
                                                if(log.isDebugEnabled()) log.debug("Read data : Length : " + data.getData().length + " ; flags : " + data.getFlags());
                                                if(data.getFlags() != Integer.parseInt(flagString)) {
                                                    if(log.isDebugEnabled()) log.debug("deleting because of incorrect flag : Actual Flag :" + flagString + "; Stored Flag :" + data.getFlags());
                                                    deleteData(key, client, op);
                                                    break;
                                                }
                                                if(data.getData().length != Integer.parseInt(lengthString)) {
                                                    if(log.isDebugEnabled()) log.debug("deleting because of incorrect size : Actual Size :" + lengthString + "; Stored Size :" + data.getData().length);
                                                    deleteData(key, client, op);
                                                    break;
                                                }
                                                final Checksum checksum = new CRC32();
                                                checksum.update(data.getData(), 0, data.getData().length);
                                                if(checksum.getValue() != Long.parseLong(checksumString)) {
                                                    if(log.isDebugEnabled()) log.debug("deleting because of incorrect checksum : Actual checksum :" + checksumString + "; Stored checksum :" + checksum.getValue());
                                                    deleteData(key, client, op);
                                                    break;
                                                }
                                            } else {
                                                if(log.isDebugEnabled()) log.debug("Read data is null. Ignoring this");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch(Exception e) {
                log.error("Exception polling", e);
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e1) {
                    log.error("Exception sleeping", e1);
                }
            }
        }
    }
    
    private void deleteData(String key, EVCacheClient client, String op) throws Exception {
        if(op.equals("SET") || op.equals("DELETE")) {
            client.delete(key);
        }
    }
}
