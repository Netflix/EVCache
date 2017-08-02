package com.netflix.evcache.server.keydump;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Singleton;
import com.netflix.evcache.server.keydump.Task.CheckpointSubTask;
import com.netflix.evcache.server.keydump.Task.CleanupCheckpointSubTask;
import com.netflix.evcache.server.keydump.Task.CleanupKeyDumpSubTask;
import com.netflix.evcache.server.keydump.Task.MonetaDumpKeysSubTask;
import com.netflix.evcache.server.keydump.Task.UploadKeysSubTask;
import com.netflix.evcache.util.Config;

/**
 * Created by senugula on 9/28/16.
 */
@Singleton
public class RequestProcessor {

    private static final String LOCAL_KEYDUMP_PATH = File.separator + "data" + File.separator + "keydump";
    private static final String LOCAL_CHECKPOINT_PARENT_PATH = File.separator + "data" + File.separator + "checkpoint";

    private static final String MNEMONIC_INFO_URL = "http://localhost:11299/dbinfo";
    private static final String MNEMONIC_CHECKPOINT_URL = "http://localhost:11299/checkpoint";

    private static final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, new LinkedBlockingQueue<Runnable>(1));

    private final AtomicReference<MnemonicInfo> mnemonicInfo = new AtomicReference<MnemonicInfo>(null);

    public RequestProcessor() {
    }

    public boolean cancelRequest() {
        // TODO - need to shutdown executor and create new one
        return false;
    }

    // for now, use 1 thread, but maybe later, have dedicated thread for upload task but need to have a better task dependency chain
    public void handleRequest(String taskId, String s3FolderPath, String s3fileName, double percent,  long lines, long keyDumpRate, boolean includeValue) throws Exception {
        if (executor.getActiveCount() > 0 || executor.getQueue().size() > 0) {
            throw new Exception("pre-existing request already being processed");
        }

        final RequestDAO requestDAO = new RequestDAO();
        Task eTask = new Task(taskId, requestDAO);
        if (Config.isMoneta()) {
            addMonetaKeyDumpTasks(eTask, requestDAO, s3FolderPath, s3fileName, keyDumpRate);
        } else {
            addMemcachedKeyDumpTasks(eTask, requestDAO, s3FolderPath, s3fileName, percent, includeValue);
        }

        executor.execute(eTask);
    }

    synchronized MnemonicInfo refreshMnemonicInfo() throws IOException {
        if (mnemonicInfo.get() != null) {
            return mnemonicInfo.get();
        }

        HttpResponse response = HttpClientBuilder.create().build().execute(new HttpGet(MNEMONIC_INFO_URL));
        if (response.getStatusLine().getStatusCode() != 200) {
            throw new IOException("failed to refresh MnemonicInfo from mnemonic");
        }

        final String dbInfoJson = EntityUtils.toString(response.getEntity());
        ObjectMapper mapper = new ObjectMapper();
        MnemonicInfo info = mapper.readValue(dbInfoJson, MnemonicInfo.class);
        if (info == null) {
            throw new IOException("failed to parse MnemonicInfo json from mnemonic");
        }

        mnemonicInfo.set(info);

        return mnemonicInfo.get();
    }


    private static String getFileName(String prefix) {
        return LOCAL_KEYDUMP_PATH + File.separator + prefix;
    }

    private static String getCheckpointPath(String dbId) {
        return LOCAL_CHECKPOINT_PARENT_PATH + File.separator + dbId;
    }

    void addMemcachedKeyDumpTasks(Task eTask, RequestDAO requestDAO, String s3FolderPath, String s3FileNamePrefix, double percent, boolean includeValue) {
        final String localFileName = getFileName(s3FileNamePrefix) + (includeValue ? "" : ".gz");

        eTask.addSubTask(new CleanupKeyDumpSubTask(eTask.getTaskId(), LOCAL_KEYDUMP_PATH));
        eTask.addSubTask(eTask.new MemcachedDumpKeysSubTask(eTask.getTaskId(), localFileName, s3FolderPath, percent, requestDAO, includeValue));
        if(!includeValue) {
            eTask.addSubTask(new UploadKeysSubTask(eTask.getTaskId(), localFileName, s3FolderPath, requestDAO, false));
            eTask.finishSubTaskList();
        }
    }

    void addMonetaKeyDumpTasks(Task eTask, RequestDAO requestDAO, String s3FolderPath, String s3FileNamePrefix, long keyDumpRate) throws IOException{
        if (mnemonicInfo.get() == null) {
            refreshMnemonicInfo();
        }

        // had to change checkpoint to checkpoint all rocksdb paths in mnemonic so have to do some hacks here - checkpoint in first subtask list, then delete each subdirectory to clear space as we go in each list
        List<String> dbInfos = mnemonicInfo.get().getDbPaths();
        for (int i=0; i < dbInfos.size(); ++i) {
            String path = dbInfos.get(i);

            File file = new File(path);
            String name = file.getName();

            String localFileName = getFileName(s3FileNamePrefix + "_" + name);
            String checkpointPath = getCheckpointPath(name);

            if (i==0) {
                eTask.addSubTask(new CleanupCheckpointSubTask(eTask.getTaskId(), LOCAL_CHECKPOINT_PARENT_PATH, true));  // cleanup everything on first pass in case some old task failed
                eTask.addSubTask(new CheckpointSubTask(eTask.getTaskId(), MNEMONIC_CHECKPOINT_URL, LOCAL_CHECKPOINT_PARENT_PATH));
            }

            eTask.addSubTask(new CleanupKeyDumpSubTask(eTask.getTaskId(), LOCAL_KEYDUMP_PATH));

            eTask.addSubTask(new MonetaDumpKeysSubTask(eTask.getTaskId(), mnemonicInfo.get().getMnemonicConfigPath(), checkpointPath, keyDumpRate, localFileName, requestDAO));
            eTask.addSubTask(new UploadKeysSubTask(eTask.getTaskId(), localFileName, s3FolderPath, requestDAO, false));

            eTask.addSubTask(new CleanupKeyDumpSubTask(eTask.getTaskId(), LOCAL_KEYDUMP_PATH));
            eTask.addSubTask(new CleanupCheckpointSubTask(eTask.getTaskId(), checkpointPath, false)); // only cleanup checkpoint of single rocksdb

            if (i==dbInfos.size()-1) {
                eTask.addSubTask(new CleanupCheckpointSubTask(eTask.getTaskId(), LOCAL_CHECKPOINT_PARENT_PATH, true));  // cleanup everything on last pass in case some old task failed
            }


            eTask.finishSubTaskList();
        }
    }
}

