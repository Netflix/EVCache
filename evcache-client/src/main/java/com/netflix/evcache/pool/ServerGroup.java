package com.netflix.evcache.pool;

public class ServerGroup implements Comparable<ServerGroup> {
    private final String zone;
    private final String name;

    public ServerGroup(String zone, String name) {
        super();
        this.zone = zone;
        this.name = name;
    }

    public String getZone() {
        return zone;
    }

    public String getName() {
        return name;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((zone == null) ? 0 : zone.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof ServerGroup)) return false;
        ServerGroup other = (ServerGroup) obj;
        if (name == null) {
            if (other.name != null)
                return false;
        } else if (!name.equals(other.name))
            return false;
        if (zone == null) {
            if (other.zone != null)
                return false;
        } else if (!zone.equals(other.zone))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "Server Group [zone=" + zone + (name.equals(zone) ? "" : ", name=" + name) + "]";
    }

    @Override
    public int compareTo(ServerGroup o) {
        return toString().compareTo(o.toString());
    }

}
