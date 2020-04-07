package com.netflix.evcache.operation;


public class EVCacheItem<T> {
    private final EVCacheItemMetaData item;
    private T data = null;
    private int flag = 0;

    public EVCacheItem() {
        item = new EVCacheItemMetaData();
    }
    public EVCacheItemMetaData getItemMetaData() {
        return item;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return "EVCacheItem [item=" + item + ", data=" + data + ", flag=" + flag + "]";
    }


}
