/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.evcache.pool.standalone;

import java.util.Map;

import javax.management.DescriptorKey;

public interface ZoneClusteredEVCacheClientPoolImplMBean {

    /**
     * Returns the number of {@link com.netflix.evcache.EVCache} instances in this pool.
     */
    @DescriptorKey("The number of EVCache instances(hosts) in this pool")
    int getInstanceCount();

    /**
     * The Map of instances by availability zone, where the key is zone and value is the comma separated list of instances in that zone.
     */
    @DescriptorKey("The Map of instances by availability zone, where the key is zone and value is the comma separated list of instances in that zone")
    Map<String, String> getInstancesByZone();

    /**
     * The Map of instance count by availability zone, where the key is zone and value is the number of instances in that zone.
     */
    @DescriptorKey("The Map of instance count by availability zone, where the key is zone and value is the number of instances in that zone")
    Map<String, Integer> getInstanceCountByZone();

    /**
     * The Map of instance by availability zone that can perform Read operations, where the key is zone
     * and value is the comma separated list of instances in that zone.
     */
    @DescriptorKey("The Map of instances by availability zone that can do read operations, where the key is zone and value is the comma"
                + "separated list of instances in that zone")
    Map<String, String> getReadZones();

    /**
     * The Map of instance count by availability zone that can perform Read operations, where the key is zone
     * and value is the count of instances in that zone.
     */
    @DescriptorKey("The Map of instances count by availability zone that can do read operations, where the key is zone and value "
            + "is the count of instances in that zone")
    Map<String, Integer> getReadInstanceCountByZone();

    /**
     * The Map of instance by availability zone that can perform write operations, where the key is zone
     * and value is the comma separated list of instances in that zone.
     */
    @DescriptorKey("The Map of instances by availability zone that can do write operations, where the key is zone and value is the comma"
                + "separated list of instances in that zone")
    Map<String, String> getWriteZones();

    /**
     * The Map of instance count by availability zone that can perform write operations, where the key is zone
     * and value is the count of instances in that zone.
     */
    @DescriptorKey("The Map of instances count by availability zone that can do write operations, where the key is zone and value "
            + "is the count of instances in that zone")
    Map<String, Integer> getWriteInstanceCountByZone();

    /**
     * Force refresh on the pool.
     */
    @DescriptorKey("Force refresh on the pool")
    void refreshPool();
}
