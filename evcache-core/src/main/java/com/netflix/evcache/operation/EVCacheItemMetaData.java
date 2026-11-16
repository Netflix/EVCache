package com.netflix.evcache.operation;

/**
 * <B><u>Meta </u></B>
 * <br>
 * The meta debug command is a human readable dump of all available internal
 * metadata of an item, minus the value.<br>
 * <br>
 * <b><i>me &lt;key&gt;r\n</i></b><br>
 * <br>
 * <key> means one key string.<br>
 * <br>
 * The response looks like:<br>
 * <br>
 * <b><i>ME &lt;key&gt; &lt;k&gt;=&lt;v&gt;*\r\nEN\r\n</i></b><br>
 * <br>
 * For Ex: <br>
 * <pre>
 *  
    me img:bil:360465414627441161
    ME img:bil:360465414627441161 exp=-549784 la=55016 cas=0 fetch=yes cls=5 size=237
    EN
   </pre>
 * <br>
 * Each of the keys and values are the internal data for the item.<br>
 * <br>
 * exp   = expiration time<br>
 * la    = time in seconds since last access<br>
 * cas   = CAS ID<br>
 * fetch = whether an item has been fetched before<br>
 * cls   = slab class id<br>
 * size  = total size in bytes<br>
 * <br>
 * @author smadappa
 *
 */

public class EVCacheItemMetaData {
    public long secondsLeftToExpire;
    public long secondsSinceLastAccess;
    public long cas;
    public boolean hasBeenFetchedAfterWrite;
    public int slabClass;
    public int sizeInBytes;

    public boolean stale;
    public boolean itemWonRecache;
    public boolean itemLostRecache;

    public void setStale(boolean stale) {
        this.stale = stale;
    }

    public void setItemWonRecache(boolean itemWonRecache) {
        this.itemWonRecache = itemWonRecache;
    }

    public void setItemLostRecache(boolean itemLostRecache) {
        this.itemLostRecache = itemLostRecache;
    }

    public EVCacheItemMetaData() {
        super();
    }

    public void setSecondsLeftToExpire(long secondsLeftToExpire) {
        this.secondsLeftToExpire = secondsLeftToExpire;
    }

    public void setSecondsSinceLastAccess(long secondsSinceLastAccess) {
        this.secondsSinceLastAccess = secondsSinceLastAccess;
    }

    public void setCas(long cas) {
        this.cas = cas;
    }

    public void setHasBeenFetchedAfterWrite(boolean hasBeenFetchedAfterWrite) {
        this.hasBeenFetchedAfterWrite = hasBeenFetchedAfterWrite;
    }

    public void setSlabClass(int slabClass) {
        this.slabClass = slabClass;
    }

    public void setSizeInBytes(int sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }

    public long getSecondsLeftToExpire() {
        return secondsLeftToExpire;
    }

    public long getSecondsSinceLastAccess() {
        return secondsSinceLastAccess;
    }

    public long getCas() {
        return cas;
    }

    public boolean isHasBeenFetchedAfterWrite() {
        return hasBeenFetchedAfterWrite;
    }

    public int getSlabClass() {
        return slabClass;
    }

    public int getSizeInBytes() {
        return sizeInBytes;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (cas ^ (cas >>> 32));
        result = prime * result + (hasBeenFetchedAfterWrite ? 1231 : 1237);
        result = prime * result + (int) (secondsLeftToExpire ^ (secondsLeftToExpire >>> 32));
        result = prime * result + (int) (secondsSinceLastAccess ^ (secondsSinceLastAccess >>> 32));
        result = prime * result + sizeInBytes;
        result = prime * result + slabClass;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EVCacheItemMetaData other = (EVCacheItemMetaData) obj;
        if (cas != other.cas)
            return false;
        if (hasBeenFetchedAfterWrite != other.hasBeenFetchedAfterWrite)
            return false;
        if (secondsLeftToExpire != other.secondsLeftToExpire)
            return false;
        if (secondsSinceLastAccess != other.secondsSinceLastAccess)
            return false;
        if (sizeInBytes != other.sizeInBytes)
            return false;
        if (slabClass != other.slabClass)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "EVCacheItemMetaData [secondsLeftToExpire=" + secondsLeftToExpire + ", secondsSinceLastAccess="
                + secondsSinceLastAccess + ", cas=" + cas + ", hasBeenFetchedAfterWrite=" + hasBeenFetchedAfterWrite
                + ", slabClass=" + slabClass + ", sizeInBytes=" + sizeInBytes + "]";
    }

}
