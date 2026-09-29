package com.netflix.evcache;

import com.netflix.evcache.pool.EVCacheClient;
import com.netflix.evcache.pool.ServerGroup;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class EVCacheInternalImplTest {

    private static List<EVCacheClient> clients(int poolSize) {
        List<EVCacheClient> clients = new ArrayList<>();
        for (int i = 0; i < poolSize; i++) {
            clients.add(mock(EVCacheClient.class));
        }
        return clients;
    }

    @Test
    public void picksOneClientForTargetServerGroupRegardlessOfPoolSize() {
        List<EVCacheClient> target = clients(6);
        Map<ServerGroup, List<EVCacheClient>> clientsByServerGroup = new LinkedHashMap<>();
        clientsByServerGroup.put(new ServerGroup("us-east-1c", "app-useast1c-v000"), clients(6));
        clientsByServerGroup.put(new ServerGroup("us-east-1d", "app-useast1d-v001"), target);

        for (int i = 0; i < 100; i++) {
            List<EVCacheClient> selected = EVCacheInternalImpl.selectOneClientPerServerGroup(
                    clientsByServerGroup, Collections.singletonList("app-useast1d-v001"));
            assertEquals(selected.size(), 1);
            assertTrue(target.contains(selected.get(0)));
        }
    }

    @Test
    public void picksOneClientPerTargetServerGroup() {
        List<EVCacheClient> groupC = clients(3);
        List<EVCacheClient> groupD = clients(3);
        Map<ServerGroup, List<EVCacheClient>> clientsByServerGroup = new LinkedHashMap<>();
        clientsByServerGroup.put(new ServerGroup("us-east-1c", "app-useast1c-v000"), groupC);
        clientsByServerGroup.put(new ServerGroup("us-east-1d", "app-useast1d-v001"), groupD);
        clientsByServerGroup.put(new ServerGroup("us-east-1e", "app-useast1e-v002"), clients(3));

        List<EVCacheClient> selected = EVCacheInternalImpl.selectOneClientPerServerGroup(
                clientsByServerGroup, Arrays.asList("app-useast1c-v000", "app-useast1d-v001"));

        assertEquals(selected.size(), 2);
        assertTrue(groupC.contains(selected.get(0)));
        assertTrue(groupD.contains(selected.get(1)));
    }

    @Test
    public void returnsEmptyWhenNoServerGroupMatchesOrHasClients() {
        Map<ServerGroup, List<EVCacheClient>> clientsByServerGroup = new LinkedHashMap<>();
        clientsByServerGroup.put(new ServerGroup("us-east-1c", "app-useast1c-v000"), clients(3));
        clientsByServerGroup.put(new ServerGroup("us-east-1d", "app-useast1d-v001"), Collections.emptyList());

        assertTrue(EVCacheInternalImpl.selectOneClientPerServerGroup(
                clientsByServerGroup, Collections.singletonList("app-useast1e-v002")).isEmpty());
        assertTrue(EVCacheInternalImpl.selectOneClientPerServerGroup(
                clientsByServerGroup, Collections.singletonList("app-useast1d-v001")).isEmpty());
    }
}
