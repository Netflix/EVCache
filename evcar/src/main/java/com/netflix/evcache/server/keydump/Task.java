package com.netflix.evcache.server.keydump;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.ProcessBuilder.Redirect;
import java.net.Socket;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.aws.S3.NFS3Exception;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.evcache.server.keydump.RequestDAO.TaskStatus;
import com.netflix.evcache.util.ByteArrayTranscoder;
import com.netflix.evcache.util.Config;
import com.netflix.evcache.util.EVCacheS3Util;
import com.netflix.servo.monitor.DynamicCounter;
import com.netflix.servo.monitor.DynamicTimer;
import com.netflix.servo.monitor.Stopwatch;
import com.netflix.util.concurrent.NFExecutorPool;

import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.CachedData;
import net.spy.memcached.MemcachedClient;

class Task implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(Task.class);

    private static DynamicBooleanProperty enablePercent = DynamicPropertyFactory.getInstance().getBooleanProperty("evcache.keydump.percent", false);
    private static final Schema _schema = new Schema.Parser().parse("{\"type\":\"record\",\"namespace\":\"com.netflix.evcache\",\"name\":\"item\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"expiration\",\"type\":\"string\"},{\"name\":\"flag\",\"type\":\"string\"},{\"name\":\"data\",\"type\":\"bytes\"}]}");

    private final String taskId;
    private final RequestDAO requestDAO;
    private final List<List<SubTask>> subTasksList;

    public Task(String taskId, RequestDAO requestDAO) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("TaskId " + taskId);
        }
        this.taskId = taskId;
        this.requestDAO = requestDAO;
        this.subTasksList = new LinkedList<List<SubTask>>(Arrays.asList(new LinkedList<SubTask>()));

    }

    public void finishSubTaskList() {
        subTasksList.add(new LinkedList<SubTask>());
    }

    public void addSubTask(SubTask subtask) {
        List<SubTask> callableList = subTasksList.get(subTasksList.size()-1);
        callableList.add(subtask);
    }

    @Override
    public void run() {
        Stopwatch stopwatch = DynamicTimer.start("EVCacheMetric-KeyDump-Latency", "type", (Config.isMoneta()? "moneta": "static classic"));
        try {
            requestDAO.setStatus(TaskStatus.PENDING);
            requestDAO.setTaskId(getTaskId());
            KeysRequestCache.getInstance().addToReqCache(getTaskId(), requestDAO);
            //            DynamicCounter.increment("EVCacheMetric-KeyDump", "Result", "PROCESSED"); // ? metric name? // TODO - use different metric (guage) to indicate state
            processRequest();
            requestDAO.setStatus(TaskStatus.COMPLETED);
            DynamicCounter.increment("EVCacheMetric-KeyDump-Completed", "Result", "SUCCESS");
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(("Task is done " + getTaskId()));
            }
        } catch (Exception e) {
            DynamicCounter.increment("EVCacheMetric-KeyDump-Completed", "Result", "ERROR");
            requestDAO.setStatus(TaskStatus.ERROR);
            LOGGER.error("exception processing Dump Task", e);
        } finally {
            stopwatch.stop();
        }
    }

    private void processRequest() throws Exception {
        Exception ex = null;
        for (List<SubTask> callableList : subTasksList) {
            for (SubTask c : callableList) {
                try {
                    if (ex == null || c.runAfterFailure()) {
                        c.call();
                    }
                } catch (Exception e) {
                    if (ex == null) {
                        LOGGER.error(taskId + ": failed dumping keys, but continuing to run all cleanup tasks", e);
                        ex = e;
                    }
                }
            }
        }

        if (ex != null) {
            LOGGER.error(taskId + ": Task failed");
            throw ex;
        } else {
            LOGGER.info(taskId + ": Completed Task successfully");
        }
    }

    public String getTaskId() {
        return taskId;
    }

    /*
     * ======================================== SubTask ============================================
     */

    interface SubTask extends Callable<Void> {
        boolean runAfterFailure();
    }

    /*
     * creates rocksdb checkpoint
     */
    static class CheckpointSubTask implements SubTask {

        private final String checkpointUrl;
        private final String checkpointPath;
        private final String taskId;

        /*
         * checkpointPath - destination directory of checkpoing
         */
        CheckpointSubTask(String taskId, String checkpointUrl, String checkpointPath) {
            this.taskId = taskId;
            this.checkpointUrl = checkpointUrl;
            this.checkpointPath = checkpointPath;
        }

        @Override
        public Void call() throws Exception {
            LOGGER.info(taskId + ": CheckpointSubTask - " + checkpointPath);
            String params = URLEncoder.encode(checkpointPath, "UTF-8");
            String url = checkpointUrl + "?destpath=" + params;
            HttpGet req = new HttpGet(url);
            HttpResponse response = HttpClientBuilder.create().build().execute(req);
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException(taskId + ": failed to checkpoint db");
            }

            return null;
        }

        @Override
        public boolean runAfterFailure() {
            return false;
        }
    }

    /*
     * Dump Moneta Keys
     */
    static class MonetaDumpKeysSubTask implements SubTask {

        private final String taskId;
        private final RequestDAO requestDAO;
        private final List<String> processParams;

        /*
         * dbPath here should contain an pre-existing checkpoint from CheckpointSubTask; don't use the real db that serves online requests
         */
        MonetaDumpKeysSubTask(String taskId, String configPath, String dbPath, long dumpRate, String keyDumpPath, RequestDAO requestDAO) {
            this(taskId, requestDAO, Arrays.asList("/apps/memento/bin/dumpmeta", "-g", "-c", configPath, "-d", dbPath, "-r", String.valueOf(dumpRate), "-o", keyDumpPath));
        }

        /*
         * for testing
         */
        MonetaDumpKeysSubTask(String taskId, RequestDAO requestDAO, List<String> processParams) {
            this.taskId = taskId;
            this.requestDAO = requestDAO;
            this.processParams = processParams;
        }

        @Override
        public Void call() throws Exception {
            LOGGER.info(taskId + ": MonetaDumpKeysSubTask - " + String.join(" ", processParams));

            ProcessBuilder processBuilder = new ProcessBuilder(processParams)
                    .redirectError(Redirect.INHERIT);
            Process process = processBuilder.start();

            InputStream inputStream = process.getInputStream();
            if (inputStream == null) {
                throw new IOException(taskId + ": unexpected null inputstream");
            }
            InputStreamReader reader = new InputStreamReader(inputStream);

            char[] buf = new char[128];
            int byteCount = reader.read(buf, 0, buf.length);
            if (byteCount < 0 || byteCount >= buf.length) {
                throw new IOException(taskId + ": dumpmeta failed with unexpected output: " + byteCount);
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new IOException(taskId + ": dumpmeta exited with failure code: " + exitCode);
            }

            if (buf[byteCount-1] == '\n') {
                byteCount--;
            }
            Long recordCountNum = Long.valueOf(new String(buf, 0, byteCount));

            requestDAO.addKeyswritten(recordCountNum.longValue());

            return null;
        }

        @Override
        public boolean runAfterFailure() {
            return false;
        }
    }

    /*
     * Dump memcached keys
     */
    class MemcachedDumpKeysSubTask implements SubTask {
        private final String taskId;
        private final String localFilePath;
        private final String s3FolderPath;
        private final double percent;
        private final RequestDAO requestDAO;
        private final boolean includeValue;
        private String hostName = "localhost";
        private ThreadPoolExecutor executor = null;

        MemcachedDumpKeysSubTask(String taskId, String localFilePath, String s3FolderPath, double percent, RequestDAO requestDAO, boolean includeValue) {
            this.taskId = taskId;
            this.localFilePath = localFilePath;
            this.s3FolderPath = s3FolderPath;
            this.percent = percent;
            this.requestDAO = requestDAO;
            this.includeValue = includeValue;
            if(includeValue) {
                executor = new NFExecutorPool("EVCacheS3UploadThreadPool",5, 5, 5, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(100));
                executor.allowCoreThreadTimeOut(true);
            }
        }

        private long getItemCount() throws IOException {
            Socket memSocket = null;
            PrintWriter printWriter = null;
            BufferedReader bufferedReader = null;

            try {
                memSocket = new Socket(hostName, 11211);
                printWriter = new PrintWriter(memSocket.getOutputStream(), true);
                bufferedReader = new BufferedReader(new InputStreamReader(memSocket.getInputStream()));

                printWriter.println("stats");
                String line;
                long count = 0;
                while ((line = bufferedReader.readLine()) != null) {
                    int i = -1;
                    if ((i = line.indexOf("curr_items")) != -1) {
                        LOGGER.info(line);
                        String val = line.substring(i + 11);
                        count = Integer.parseInt(val.trim());
                        LOGGER.info("current items " + count);
                        break;
                    }
                }

                return count;
            } finally {
                IOUtils.closeQuietly(bufferedReader);
                IOUtils.closeQuietly(printWriter);
                IOUtils.closeQuietly(memSocket);
            }
        }

        private long dumpKeysHelper(long itemCount) throws IOException {
            final File file = new File(localFilePath);
            return dumpKeysHelper(itemCount, file, true);
        }

        private long dumpKeysHelper(long itemCount, File file, boolean zip) throws IOException {
            Socket memSocket = null;
            PrintWriter printWriter = null;
            BufferedReader bufferedReader = null;
            OutputStream os = null;
//            GZIPOutputStream zipout = null;
//            FileOutputStream fos = null;
            try {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(taskId + ": File name is : " + localFilePath);
                }

                memSocket = new Socket(hostName, 11211);
                printWriter = new PrintWriter(memSocket.getOutputStream(), true);
                bufferedReader = new BufferedReader(new InputStreamReader(memSocket.getInputStream()));
                if(zip) {
                    os = new GZIPOutputStream(new FileOutputStream(file.getAbsolutePath()));
                } else {
                    os = new FileOutputStream(file.getAbsoluteFile());
                }

                printWriter.print("lru_crawler metadump all \r\n");
                printWriter.print("quit \r\n");
                printWriter.flush();
                int retryCounter = 0;
                while (!bufferedReader.ready()) {
                    if (retryCounter++ > 30) throw new IOException("Failed to start dumping keys.");
                    try {
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        LOGGER.warn(taskId + ": ignoring exception from Thread.sleep: ", e);
                    }
                }

                String line;
                long counter = 0;
                long waitBufferCount = 0;
                long keysWritten = 0;
                long keysExpired = 0;
                for (; ; ) {
                    if (!bufferedReader.ready()) {
                        if (enablePercent.get()) { // the 2 if branches look the same
                            if ((counter >= itemCount && waitBufferCount > 3)) break;
                        } else {
                            if (((keysWritten + keysExpired) >= itemCount && waitBufferCount > 3)) break;
                        }
                        if (waitBufferCount >= 10) {
                            LOGGER.info(taskId + ": Keys Written " + keysWritten + "Keys expired " + keysExpired);
                            LOGGER.info(taskId + ": current count " + counter + " expected " + itemCount + " wait buffer count " + waitBufferCount);
                            throw new IOException(taskId + ": Failed to dump keys");
                        }
                        try {
                            LOGGER.info(taskId + ": waiting for stream to be ready");
                            waitBufferCount++;
                            Thread.sleep(100);
                        } catch (Exception e) {
                            LOGGER.warn(taskId + ": ignoring exception from Thread.sleep: ", e);
                        }
                    } else {
                        waitBufferCount = 0;
                        line = bufferedReader.readLine();
                        if (line.isEmpty()) break;
                        counter++;
                        if (counter % 100000 == 0) {
                            LOGGER.info(taskId + ": processed - " + counter);
                            requestDAO.addKeyswritten(counter);
                        }
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(taskId + ": File name is : " + file.getAbsolutePath());
                            LOGGER.debug(line);
                        }
                        String[] metadata = line.split(" ");
                        String key = null;
                        long exp = 0;
                        for (String part : metadata) {
                            String[] kv = part.split("=");
                            if (kv[0].equals("key")) {
                                //do nothing
                                key = kv[1];
                            } else if (kv[0].equals("exp")) {
                                long currTime = System.currentTimeMillis() / 1000;
                                exp = Long.parseLong(kv[1]) - currTime;
                            }
                            if (key != null && exp > 0) break;
                        }
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(taskId + ": key " + key + " Exp " + exp);
                        }
                        if (key == null || exp <= 0) {
                            // key is null or expired key, increment expired and continue
                            keysExpired++;
                        } else {
                            os.write(line.getBytes());
                            os.write('\n');
                            keysWritten++;
                        }
                    }
                }
                requestDAO.addKeyswritten(keysWritten);
                requestDAO.addKeysExpired(keysExpired);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(taskId + ": Keys written count " + keysWritten);
                    LOGGER.debug(taskId + ": Keys expired count " + keysExpired);
                    LOGGER.debug(taskId + ": Processed done - " + counter);
                }
                os.flush();
                os.close();
                return counter;
            } finally {
                IOUtils.closeQuietly(memSocket);
                IOUtils.closeQuietly(os);
                IOUtils.closeQuietly(bufferedReader);
                IOUtils.closeQuietly(printWriter);
            }
        }
        
        private ParquetWriter<GenericRecord> getWriter(File currentFile ) throws IOException {
            if(LOGGER.isInfoEnabled()) LOGGER.info("Creating new file" + currentFile.getPath());
            final Path outputPath = new Path(currentFile.getPath());
            final int blockSize = 100663296; //ParquetWriter.DEFAULT_BLOCK_SIZE;
            final int pageSize = ParquetWriter.DEFAULT_PAGE_SIZE; //the larger the page size the better the compression
            final ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(outputPath)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .withPageSize(pageSize)
                    .withSchema(_schema)
                    .withRowGroupSize(blockSize)
                    .enableDictionaryEncoding()
                    .build();
            return writer;
        }

        private long snapshotHelper(long itemCount) throws Exception {
            final File file = new File(localFilePath);
            final String folderPath = file.getParent();
            final String pathSeparator = System.getProperty("file.separator");
            final File keydumpFile = new File(folderPath + pathSeparator + "keydump.txt");
            final String fileName = file.getName(); 

            final long count = dumpKeysHelper(itemCount, keydumpFile, false);
            if(count == 0) return 0;

            long counter = 0;
            final long MAX_FILE_SIZE = 1*1024*1024*1024;

            MemcachedClient client = null;
            BufferedReader br = null;
            ParquetWriter<GenericRecord> writer = null;
            File currentFile = null;
            try {
                client = new MemcachedClient(new BinaryConnectionFactory(), AddrUtil.getAddresses(hostName + ":11211"));
                br = new BufferedReader(new InputStreamReader(new FileInputStream(keydumpFile)));
                ByteArrayTranscoder bat = new ByteArrayTranscoder();

                final GenericRecord record = new GenericData.Record(_schema);
                String line = "";
                int index = 0;
                currentFile = new File(folderPath + pathSeparator + fileName + "-" + index++);
                writer = getWriter(currentFile); 
                while((line = br.readLine()) != null) {
                    if(currentFile.exists() && ((long)currentFile.length() >= (long)MAX_FILE_SIZE)) {
                        if(LOGGER.isInfoEnabled()) LOGGER.info(currentFile + " size = " + currentFile.length());
                        IOUtils.closeQuietly(writer);//close old one and get a new one
                        final UploadKeysSubTask task = new UploadKeysSubTask(taskId, currentFile.getAbsolutePath(), s3FolderPath, requestDAO, true);
                        executor.submit(task);
                        currentFile = new File(folderPath + pathSeparator + fileName + "-" + index++);
                        writer = getWriter(currentFile);
                    }
                    counter++;
                    if (counter % 100000 == 0) {
                        LOGGER.info("processed - " + counter);
                    }
                    final String[] metadata = line.split(" ");
                    String key = null;
                    String exp = null;
                    for (String part : metadata) {
                        String[] kv = part.split("=");
                        if (kv[0].equals("key")) {
                            //do nothing
                            key = kv[1];
                        } else if (kv[0].equals("exp")) {
                            exp = kv[1];
                        }
                        if (key != null && exp != null) break;
                    }
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("key " + key + " Exp " + exp);
                    }
                    if (key == null || exp == null) {
                    } else {
                        final String k = URLDecoder.decode(key, "UTF-8"); 
                        final CachedData cd = (CachedData)client.get(k, bat);
                        if(cd != null) {
                            record.put("key", k);
                            record.put("expiration", exp);
                            record.put("flag", String.valueOf(cd.getFlags()));
                            record.put("data", ByteBuffer.wrap(cd.getData()));
                            writer.write(record);
                        }
                    }
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Processed done - " + counter);
                }
                return counter;
            } finally {
                IOUtils.closeQuietly(writer);
                if(currentFile != null) {
                    final UploadKeysSubTask task = new UploadKeysSubTask(taskId, currentFile.getAbsolutePath(), s3FolderPath, requestDAO, true);
                    executor.submit(task);
                }
                if(br != null) IOUtils.closeQuietly(br);
                if(client != null) client.shutdown();
                executor.shutdown();
            }
        }


        @Override
        public Void call() throws Exception {
            try {
                LOGGER.info(taskId + ": MemcachedDumpKeysSubTask - " + localFilePath);

                long count = (long)(getItemCount() * percent);

                if(includeValue) {
                    snapshotHelper(count);
                } else {
                    dumpKeysHelper(count);
                }
                return null;
            } catch (Exception e) {
                LOGGER.error(taskId + ": failed getting keys", e);
                throw e;
            }
        }

        @Override
        public boolean runAfterFailure() {
            return false;
        }
    }

    /*
     * upload keys to S3
     */
    static class UploadKeysSubTask implements SubTask {

        private final String taskId;
        private final String localFilePath;
        private final String s3FolderPath;
        private final RequestDAO requestDAO;
        private final boolean cleanupAfterUpload;

        UploadKeysSubTask(String taskId, String localFilePath, String s3FolderPath, RequestDAO requestDAO, boolean cleanupAfterUpload) {
            this.taskId = taskId;
            this.localFilePath = localFilePath;
            this.s3FolderPath = s3FolderPath;
            this.requestDAO = requestDAO;
            this.cleanupAfterUpload = cleanupAfterUpload;
        }

        @Override
        public Void call() throws Exception {
            LOGGER.info(taskId + ": UploadKeysSubTask - " + localFilePath + ", " + s3FolderPath);

            File localFile = new File(localFilePath);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug((taskId + ": filename for UploadKeysSubTask task is" + localFile.getAbsolutePath()));
            }

            boolean uploadSuccess = false;
            int retryCount = 0;
            int maxRetries = 3;
            final File file = new File(localFilePath);
            while( !uploadSuccess && retryCount ++ < maxRetries) {
                try {
                    EVCacheS3Util.publish(s3FolderPath, file.getName(), file);
                    uploadSuccess = true;
                    if(cleanupAfterUpload && file.exists()) {
                        if (LOGGER.isDebugEnabled()) LOGGER.debug((taskId + ": deleting file after UploadKeysSubTask task " + localFile.getAbsolutePath()));
                        file.delete();
                    }
                } catch(NFS3Exception nfe) {
                    if(LOGGER.isDebugEnabled()) {
                        LOGGER.debug(taskId + ": Failed to upload to S3 , retry count: " + retryCount);
                    }
                }
            }

            String s3FilePath = "s3://" + s3FolderPath + File.separator + localFile.getName();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(taskId + ": success: " + uploadSuccess + ", s3FilePath : " + s3FilePath);
            }

            requestDAO.addFileName(s3FilePath);
            return null;
        }

        @Override
        public boolean runAfterFailure() {
            return false;
        }
    }

    /*
     * Cleanup Moneta checkpoints
     */
    static class CleanupCheckpointSubTask implements SubTask {

        private static final String EXPECTED_LOCAL_CHECKPOINT_PATH_PREFIX1 = File.separator + "data" + File.separator;
        private static final String EXPECTED_LOCAL_CHECKPOINT_PATH_PREFIX2 = File.separator + "tmp" + File.separator; // for testing

        private final String taskId;
        private final String checkpointPath;
        private final boolean allowRecurse;

        CleanupCheckpointSubTask(String taskId, String checkpointPath, boolean allowRecurse) {
            this.taskId = taskId;
            this.checkpointPath = checkpointPath;
            this.allowRecurse = allowRecurse;
        }

        @Override
        public Void call() throws Exception {
            LOGGER.info(taskId + ": CleanupCheckpointSubTask - " + checkpointPath + ", " + allowRecurse);

            // only allow /data or /tmp prefix to limit damage of bug
            File checkpointDir = new File(checkpointPath);
            if ((!checkpointPath.startsWith(EXPECTED_LOCAL_CHECKPOINT_PATH_PREFIX1) && !checkpointPath.startsWith(EXPECTED_LOCAL_CHECKPOINT_PATH_PREFIX2)) ||
                    (checkpointDir.exists() && !checkpointDir.isDirectory())) {
                throw new Exception(taskId + ": invalid checkpoint path");
            }

            if (checkpointDir.exists()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(taskId + ": CleanupCheckpointSubTask - file exists");
                }

                for (File f : checkpointDir.listFiles()) { // delete up to 1 level at most to limit damage caused by bug
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(taskId + ": CleanupCheckpointSubTask - file found: " + f.getAbsolutePath());
                    }

                    if (allowRecurse && f.isDirectory()) {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(taskId + ": CleanupCheckpointSubTask - deleting subdirectory: " + f.getAbsolutePath());
                        }

                        delete(f.listFiles());
                    }
                    f.delete();

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(taskId + ": CleanupCheckpointSubTask - deleted file: " + f.getAbsolutePath());
                    }
                }
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(taskId + ": CleanupCheckpointSubTask - file not exists");
                }
            }
            return null;
        }

        private void delete(File[] files) {
            for (File f : files) {
                f.delete();

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(taskId + ": CleanupCheckpointSubTask - deleted subdirectory file: " + f.getAbsolutePath());
                }
            }
        }

        @Override
        public boolean runAfterFailure() {
            return true;
        }
    }

    /*
     * Cleanup key dumps
     */
    static class CleanupKeyDumpSubTask implements SubTask {
        private static final String EXPECTED_LOCAL_CHECKPOINT_PATH_PREFIX1 = File.separator + "data" + File.separator;
        private static final String EXPECTED_LOCAL_CHECKPOINT_PATH_PREFIX2 = File.separator + "tmp" + File.separator; // for testing

        private final String taskId;
        private final String localKeyDumpDirPath;

        CleanupKeyDumpSubTask(String taskId, String localKeyDumpDirPath) {
            this.taskId = taskId;
            this.localKeyDumpDirPath = localKeyDumpDirPath;
        }

        @Override
        public Void call() throws Exception {
            LOGGER.info(taskId + ": CleanupKeyDumpSubTask - " + localKeyDumpDirPath);

            // only allow /data or /tmp prefix to limit damage of bug
            File keyDumpDir = new File(localKeyDumpDirPath);
            if ((!localKeyDumpDirPath.startsWith(EXPECTED_LOCAL_CHECKPOINT_PATH_PREFIX1) && !localKeyDumpDirPath.startsWith(EXPECTED_LOCAL_CHECKPOINT_PATH_PREFIX2)) ||
                    (keyDumpDir.exists() && !keyDumpDir.isDirectory())) {
                throw new Exception(taskId + ": invalid keydump path: " + localKeyDumpDirPath);
            }

            if (keyDumpDir.exists()) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(taskId + ": CleanupKeyDumpSubTask - keydump directory exists: " + keyDumpDir.getAbsolutePath());
                }

                for (File f : keyDumpDir.listFiles()) {
                    if (!f.isDirectory()) {
                        f.delete();

                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(taskId + ": CleanupKeyDumpSubTask - deleted file: " + f.getAbsolutePath());
                        }
                    } else {
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug(taskId + ": CleanupKeyDumpSubTask - skipping directory: " + f.getAbsolutePath());
                        }
                    }
                }
            }

            return null;
        }

        @Override
        public boolean runAfterFailure() {
            return true;
        }
    }

    public static void main(String[] args) {
        final RequestDAO requestDAO = new RequestDAO();
        Task t = new Task("123", requestDAO);
        MemcachedDumpKeysSubTask task = t.new MemcachedDumpKeysSubTask("123", args[0], "evcache-test/snapshot", 100.0, requestDAO, true);
        task.hostName = args[1];
        try {
            task.snapshotHelper(10);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
