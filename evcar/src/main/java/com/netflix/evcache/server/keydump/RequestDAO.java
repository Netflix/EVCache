package com.netflix.evcache.server.keydump;

import java.util.List;
import java.util.ArrayList;

/**
 * Created by senugula on 10/26/16.
 */
public class RequestDAO {

    public enum TaskStatus { COMPLETED, PENDING, ERROR };

    private String taskId;
    private String fileName;
    private TaskStatus status;
    private long keyswritten;
    private long keysExpired;
    private List<String> s3FileNames = new ArrayList<String>();

    public List<String> getFileNames() {
        return s3FileNames;
    }

    public void addFileName(String fileName) {
        this.s3FileNames.add(fileName);
    }

    public long getKeysExpired() {
        return keysExpired;
    }

    public void addKeysExpired(long keysExpired) {
        this.keysExpired += keysExpired;
    }

    public long getKeyswritten() {
        return keyswritten;
    }

    public void addKeyswritten(long keyswritten) {
        this.keyswritten += keyswritten;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
