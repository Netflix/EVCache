package com.netflix.evcache.util;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.aws.S3.INFS3;
import com.netflix.aws.S3.NFS3Exception;
import com.netflix.aws.S3.S3Manager;
import com.netflix.aws.S3.S3Object;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by senugula on 8/24/16.
 */
@Singleton
public class EVCacheS3Util {

    private static final Logger log = LoggerFactory.getLogger(EVCacheS3Util.class);
    private static S3Manager s3Manager;

    @Inject
    public EVCacheS3Util(S3Manager s3Manager) {
        this.s3Manager = s3Manager;
    }

    public static void publish(String s3Bucket, String s3Key, File file) throws Exception {
        try {
            // INFS3 nfs3 = InstanceHolder.getInstance(S3Manager.class).getNFS3();
            INFS3 nfs3 = s3Manager.getNFS3();
            log.debug("Will attempt to write using S3 bucket:" + s3Bucket + "/" + s3Key);
            S3Object s3Object = new S3Object(s3Bucket, s3Key, file);
            nfs3.putObject(s3Bucket, s3Object);
        } catch (Exception e) {
            System.out.println("Unable to save " + file.getAbsolutePath() + " to S3");
            e.printStackTrace();
            throw e;
        }
    }

    public static List<String> publish(String s3Bucket, String s3Dir, String directory) {
        final File dir = new File(directory);
        final File[] outFiles = dir.listFiles();
        final List<String> s3Paths = new ArrayList<String>();
        try {
            if (outFiles != null) {
                for (File outFile : outFiles) {
                    final String s3Path = s3Dir + File.separator + outFile.getName();
                    EVCacheS3Util.publish(s3Bucket, s3Path, outFile);
                    s3Paths.add(s3Path);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return s3Paths;
    }

    public static int delete(String s3Bucket, String s3Dir, int maxLength) throws Exception {
        Set<String> s3Keys = new HashSet<String>();
        try {
            s3Keys = getS3Keys(s3Bucket, s3Dir, maxLength);
            // INFS3 nfs3 = InstanceHolder.getInstance(S3Manager.class).getNFS3();
            INFS3 nfs3 = s3Manager.getNFS3();
            for (final String s3Key : s3Keys) {
                nfs3.deleteObject(s3Bucket, s3Key);
            }
        } catch (Exception e) {
            log.error("Unable to delete files from S3", e);
            throw e;
        }
        return s3Keys.size();
    }

    public static Set<String> getS3Keys(String s3Bucket, String s3Dir, int maxLength) throws Exception {
        // INFS3 nfs3 = InstanceHolder.getInstance(S3Manager.class).getNFS3();
        INFS3 nfs3 = s3Manager.getNFS3();
        final Set<String> s3Keys = new HashSet<String>();
        final List<S3Object> s3Objects = nfs3.listObjects(s3Bucket, s3Dir, null, maxLength, null).getObjects();
        for (S3Object baseObject : s3Objects) {
            s3Keys.add(baseObject.getKey());
        }
        return s3Keys;
    }

    public static InputStream getS3FileStream(String s3Bucket, String s3Key) throws Exception {
        //INFS3 nfs3 = InstanceHolder.getInstance(S3Manager.class).getNFS3();
        INFS3 nfs3 = s3Manager.getNFS3();
        S3Object s3Object;
        try {
            s3Object = nfs3.getObject(s3Bucket, s3Key);
        } catch (NFS3Exception e) {
            log.error("s3 file not found %s", s3Key);
            return null;
        }
        return s3Object.getDataInputStream();
    }

    public static File getFile(String s3Bucket, String s3Key) throws Exception {
        final File tempFile = File.createTempFile("FromS3", getFileExtension(s3Key));
        return getFile(s3Bucket, s3Key, tempFile);
    }

    public static File getFile(String s3Bucket, String s3Key, File outFile) throws Exception {
        if (!outFile.getParentFile().exists() && !outFile.getParentFile().mkdirs()) {
            log.debug("Could not create parent directories: " + outFile.getAbsolutePath());
            throw new Exception("Could not create parent directories");
        }
        InputStream inputStream = null;
        OutputStream output = null;
        try {
            inputStream = getS3FileStream(s3Bucket, s3Key);
            if (inputStream == null) {
                log.debug("No file found: " + s3Key);
                throw new Exception("no file found: " + s3Key);
            }
            log.debug("Copying file from S3 path to local temporary file %s", outFile.getAbsolutePath());
            output = new FileOutputStream(outFile);
            IOUtils.copyLarge(inputStream, output);
        } finally {
            if (output != null) {
                output.close();
            }
            if (inputStream != null) {
                inputStream.close();
            }
        }
        return outFile;
    }

    public static void getFiles(String s3Bucket, String s3Dir, int limit, String outputDir) throws Exception {
        final File aFile = new File(outputDir);
        if (!aFile.exists() && !aFile.mkdirs()) {
            throw new Exception("Could not create output directories");
        }
        final Set<String> s3Keys = getS3Keys(s3Bucket, s3Dir, limit);
        for (final String s3Key : s3Keys) {
            final File tempFile = new File(s3Key);
            final File outFile = new File(outputDir + File.separator + tempFile.getName());
            getFile(s3Bucket, s3Key, outFile);
        }
    }

    private static String getFileExtension(String filename) {
        final int index = filename.lastIndexOf('.');
        if (index != -1) {
            return filename.substring(index);
        }
        return "";
    }

}