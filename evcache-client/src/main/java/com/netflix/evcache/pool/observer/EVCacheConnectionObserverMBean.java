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

package com.netflix.evcache.pool.observer;

import java.net.SocketAddress;
import java.util.Set;


public interface EVCacheConnectionObserverMBean {

    /**
     * Returns the number of Active Servers associated with this Observer.
     */
    int getActiveServerCount();

    /**
     * Returns the Set of address of Active Servers associated with this Observer.
     */
    Set<SocketAddress> getActiveServerNames();

    /**
     * Returns the number of InActive Servers associated with this Observer.
     */
    int getInActiveServerCount();

    /**
     * Returns the Set of address of InActive Servers associated with this Observer.
     */
    Set<SocketAddress> getInActiveServerNames();
}
