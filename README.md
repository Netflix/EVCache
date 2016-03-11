EVCache
=======

EVCache is a memcached & spymemcached based caching solution that is mainly used for AWS EC2 infrastructure for caching frequently used data. 

EVCache is an abbreviation for:
* Ephemeral  - The data stored is for a short duration as specified by its TTL (Time To Live).
* Volatile  - The data can disappear any time (Evicted).
* Cache - An in-memory key-value store.

## Features
* Distributed Key-Value store,  i.e., the cache is spread across multiple instances
* AWS Zone-Aware - Data can be replicated across zones.
* Registers and works with [Eureka] (https://github.com/Netflix/eureka/) for automatic discovery of new nodes/services.

<iframe src="//www.slideshare.net/slideshow/embed_code/key/AqBz3zvrrVktEA" width="595" height="485" frameborder="0" marginwidth="0" marginheight="0" scrolling="no" style="border:1px solid #CCC; border-width:1px; margin-bottom:5px; max-width: 100%;" allowfullscreen> </iframe> <div style="margin-bottom:5px"> <strong> <a href="//www.slideshare.net/secret/AqBz3zvrrVktEA" title="EVCache at Netflix" target="_blank">EVCache at Netflix</a> </strong> from <strong><a target="_blank" href="//www.slideshare.net/ShashiShekarMadappa">Shashi Shekar Madappa</a></strong> </div>
