**EVC**ache is a distributed Data Management service that stores data in-memory. EVCache vends a client library which is used by various applications (Web services, micro service, standalone apps, SPARK & FLINK apps, etc) and it internally uses spymemcached to talk to cluster of memcached servers. EVCache clusters (a.k.a ServerGroups) are provisioned in each zone. EVCache writes each key-value to all zones (I.e. three writes for each application level write). Reads should primarily/only happen from the local zone to meet the SLA. Write-failures or WAW hazards are accepted as cases that lead to data inconsistencies across zones. That is acceptable because:

- It is rare
- Most items have a short TTL (minutes) so that the duration for inconsistencies are constrained
- If a particular cache needs high consistency then write failures can be configured to written to a kafka topic and EVCache consistency checker can fix such inconsistencies
- EVCache client can notify application of such failures and the application can chose to retry such failures

Read failures are retried on other copies thus masking any transient failures. 

## Workload characteristics

- Typically a Read heavy load.
- At Peak over 125K ops/sec for data size of 1KB on an m4.xlarge (1 Gbps)
- Around 12GB RAM per instance (when using m4.xlarge)
- Key sizes < 255 bytes (Includes cache prifix).
- Key cannot have spaces or new line characters in them.
- Value sizes vary heavily and change over time. Value size larger than 1MB will be chunked on the server. 
	- Larger values take longer to retrieve
- Typical TTL few mins to few hours (some have multiple days, but we don't recommend long TTL)
- A cache hit rates in the 90% range.
- memcached is configured to use 90% of RAM on an EC2 instance, and not provided any swap
- For a value size of 1KB within a zone latency is < 1ms, across zones > 2ms
	- Target SLA: Avg Latency <1ms; 99% < 5ms
	- 99% is heavily influenced by JVM Pauses


**Defaults**

- Read timeout set to 20 milliseconds, Goal is to answer fast, if not possible then fail fast and retry.
- Max Read queue size from evcache client to each evcache node is 5. This will ensure we fail fast if a client tries to overload a server i.e. DOS attack
- Write timeout is 2500 milliseconds. 
- Write queue size is 16K. i.e. we will try really hard to ensure we don't drop writes. Failed writes are written to a kafka queue and such failures are then fixed by EVCache Consistency checker app.