/**
 * Copyright 2016 Netflix, Inc.
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
package com.netflix.evcache.pool;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertSame;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.mockito.Answers;
import org.testng.annotations.Test;

import com.netflix.archaius.api.PropertyRepository;
import com.netflix.evcache.util.EVCacheConfig;

/**
 * @author Scott Mansfield
 */
public class EVCacheClientPoolTest {

    @Test
    public void selectClient_hugeNumOfModOps_noException() throws Exception {
        // Arrange
    	PropertyRepository propertyRepository = mock(PropertyRepository.class, Answers.RETURNS_DEEP_STUBS);
    	Method setPropertyRepositoryMethod = EVCacheConfig.class.getDeclaredMethod("setPropertyRepository", PropertyRepository.class);
    	setPropertyRepositoryMethod.setAccessible(true);
    	setPropertyRepositoryMethod.invoke(null, propertyRepository);
    	
        // set up the object under test
        EVCacheNodeList evCacheNodeList = mock(EVCacheNodeList.class);
        EVCacheClientPoolManager evCacheClientPoolManager = mock(EVCacheClientPoolManager.class);
        
        EVCacheClientPool evCacheClientPool = new EVCacheClientPool("in a unit test", evCacheNodeList, (ThreadPoolExecutor)Executors.newFixedThreadPool(1), evCacheClientPoolManager);
        FieldUtils.writeField(evCacheClientPool, "numberOfModOps", new AtomicLong(0xFFFF_FFFF_FFFF_FFFFL), true);

        // Set up the method arguments
        EVCacheClient client1 = mock(EVCacheClient.class);
        EVCacheClient client2 = mock(EVCacheClient.class);
        List<EVCacheClient> clientsList = new ArrayList<>();
        clientsList.add(client1);
        clientsList.add(client2);

        // Ensure it's accessible
        // Yes it's private but this is a real bug we fixed.
        Method method = evCacheClientPool.getClass().getDeclaredMethod("selectClient", List.class);
        method.setAccessible(true);

        // Act
        Object ret = method.invoke(evCacheClientPool, clientsList);

        // Assert
        // The number set in numOfModOps should roll over to 0x1_0000_0000_0000_0000
        // so we should get client1 back
        EVCacheClient selected = (EVCacheClient) ret;
        assertSame(selected, client1);
    }
}
