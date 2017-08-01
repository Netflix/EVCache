package com.netflix.evcache.server.metrics;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Singleton;
import com.netflix.config.DynamicBooleanProperty;
import com.netflix.config.DynamicStringProperty;
import com.netflix.config.DynamicStringSetProperty;
import com.netflix.evcache.server.metrics.clients.LocalHttpMetricsClient;
import com.netflix.evcache.server.metrics.clients.LocalMemcachedMetricsClient;
import com.netflix.evcache.server.metrics.clients.LocalMemcachedSlabMetricsClient;
import com.netflix.evcache.server.metrics.filters.BlacklistMetricsFilter;
import com.netflix.evcache.server.metrics.filters.GateMetricsFilter;
import com.netflix.evcache.server.metrics.filters.MergedMetricsClient;
import com.netflix.evcache.server.metrics.filters.NumberFormatMetricsFilter;
import com.netflix.evcache.server.metrics.filters.TagCheckMetricsFilter;
import com.netflix.evcache.server.metrics.filters.WhitelistMetricsFilter;
import com.netflix.evcache.server.metrics.filters.ZeroCounterMetricsFilter;
import com.netflix.evcache.util.Config;
import com.netflix.servo.DefaultMonitorRegistry;
import com.netflix.servo.annotations.DataSourceType;
import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.CompositeMonitor;
import com.netflix.servo.monitor.DoubleGauge;
import com.netflix.servo.monitor.LongGauge;
import com.netflix.servo.monitor.Monitor;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.util.Pair;

import net.spy.memcached.MemcachedClient;

@Singleton
public class EVCacheAtlasPlugin implements CompositeMonitor<Long> {

	private static final Logger LOGGER = LoggerFactory.getLogger(EVCacheAtlasPlugin.class);

	//    private static final DynamicStringProperty REND_ENDPOINT = new DynamicStringProperty("evcache.stats.rend.endpoint","http://localhost:11299/metrics");
	private static final DynamicStringProperty MNEMONIC_ENDPOINT = new DynamicStringProperty("evcache.stats.mnemonic.endpoint", "http://localhost:11299/metrics");

	public static DynamicBooleanProperty memcachedStatsGate = new DynamicBooleanProperty("evcache.metrics.memcached.send.stats", true);

	private static DynamicBooleanProperty memcachedStatsBlacklistEnabled = new DynamicBooleanProperty("evcache.stats.memcached.blacklist.enabled", false);
	private static DynamicStringSetProperty memcachedStatsBlacklist = new DynamicStringSetProperty("evcache.stats.memcached.blacklist", "");
	private static DynamicBooleanProperty memcachedStatsWhitelistEnabled = new DynamicBooleanProperty("evcache.stats.memcached.whitelist.enabled", true);
	private static DynamicStringSetProperty memcachedStatsWhitelist = new DynamicStringSetProperty("evcache.stats.memcached.whitelist",
			"cmd_get," +
					"cmd_touch," +
					"cmd_set," +
					"get_hits," +
					"get_misses," +
					"delete_hits," +
					"delete_misses," +
					"bytes_read," +
					"bytes_written," +
					"limit_maxbytes," +
					"curr_connections," +
					"conn_yields," +
					"slabs_moved," +
					"bytes," +
					"curr_items," +
					"expired_unfetched," +
					"evicted_unfetched," +
					"evictions," +
					"reclaimed," +
					"conn_yields," +
					"touch_hits," +
					"touch_misses," +
					"total_connections," +
					"connection_structures," +
					"lru_crawler_starts," +
					"lru_maintainer_juggles," +
					"malloc_fails," +
					"crawler_reclaimed," +
					"crawler_items_checked," +
					"total_items," +
					"lrutail_reflocked," +
					"moves_to_cold," +
					"moves_to_warm," +
					"moves_within_lru," +
					"direct_reclaims," +
					"slab_reassign_rescues," +
					"slab_reassign_evictions_samepage," +
					"slab_reassign_evictions_nomem," +
					"slab_reassign_busy_items," +
					"slab_global_page_pool," +
			"slab_reassign_inline_reclaim");


	public static DynamicBooleanProperty memcachedSlabStatsGate = new DynamicBooleanProperty("evcache.metrics.memcached.slab.send.stats", true);

	private static DynamicBooleanProperty memcachedSlabStatsBlacklistEnabled = new DynamicBooleanProperty("evcache.stats.memcached.slab.blacklist.enabled", false);
	private static DynamicStringSetProperty memcachedSlabStatsBlacklist = new DynamicStringSetProperty("evcache.stats.memcached.slab.blacklist", "");
	private static DynamicBooleanProperty memcachedSlabStatsWhitelistEnabled = new DynamicBooleanProperty("evcache.stats.memcached.slab.whitelist.enabled", true);
	private static DynamicStringSetProperty memcachedSlabStatsWhitelist = new DynamicStringSetProperty("evcache.stats.memcached.slab.whitelist",
			"chunk_size," +
					"cmd_set," +
					"delete_hits," +
					"free_chunks," +
					"free_chunks_end," +
					"get_hits," +
					"mem_requested," +
					"total_chunks," +
					"total_pages," +
					"touch_hits," +
					"used_chunks," +
					"active_slabs," +
			"total_malloced");

	public static DynamicBooleanProperty rendStatsGate = new DynamicBooleanProperty("evcache.metrics.rend.send.stats", false);

	/*
    private static DynamicBooleanProperty rendStatsBlacklistEnabled = new DynamicBooleanProperty("evcache.stats.rend.blacklist.enabled", false);
    private static DynamicStringSetProperty rendStatsBlacklist = new DynamicStringSetProperty("evcache.stats.rend.blacklist", "");
    private static DynamicBooleanProperty rendStatsWhitelistEnabled = new DynamicBooleanProperty("evcache.stats.rend.whitelist.enabled", false);
    private static DynamicStringSetProperty rendStatsWhitelist = new DynamicStringSetProperty("evcache.stats.rend.whitelist", "");
	 */

	public static DynamicBooleanProperty mnemonicStatsGate = new DynamicBooleanProperty("evcache.metrics.mnemonic.send.stats", true);

	private static DynamicBooleanProperty mnemonicStatsBlacklistEnabled = new DynamicBooleanProperty("evcache.stats.mnemonic.blacklist.enabled", true);
	private static DynamicStringSetProperty mnemonicStatsBlacklist = new DynamicStringSetProperty("evcache.stats.mnemonic.blacklist",
			"mnemonic_num_files_at_level1," +
					"mnemonic_num_files_at_level2," +
					"mnemonic_num_files_at_level3," +
					"mnemonic_num_files_at_level4," +
					"mnemonic_num_files_at_level5," +
					"mnemonic_num_files_at_level6," +
					"mnemonic_stall_l0_slowdown_count," +
					"mnemonic_write_raw_block_micros," +
					"mnemonic_read_block_get_micros," +
					"mnemonic_filter_operation_total_time," +
					"mnemonic_compact_read_bytes," +
					"mnemonic_compact_write_bytes," +
					"mnemonic_get_hit_l0," +
					"mnemonic_get_hit_l1," +
			"mnemonic_get_hit_l2_and_up");
	private static DynamicBooleanProperty mnemonicStatsWhitelistEnabled = new DynamicBooleanProperty("evcache.stats.mnemonic.whitelist.enabled", false);
	private static DynamicStringSetProperty mnemonicStatsWhitelist = new DynamicStringSetProperty("evcache.stats.mnemonic.whitelist", "");

	private static final Map<String, MonitorConfig> monitorConfigMap = new HashMap<>();

	private static final MonitorConfig baseConfig = MonitorConfig.builder("EVCacheMetrics").build();

	private MetricsClient metricsClient;

	public void init() {

		LOGGER.info("Atlas Plugin inited ");

		// This whole block sets up a flow from metrics sources to the final metrics source of filtered, concatenated metrics
		try {
			MemcachedClient memcachedclient = new MemcachedClient(new InetSocketAddress("localhost", Config.getMemcachedPort()));

			MetricsClient localMemcachedMetricsClient;
			MetricsClient localMemcachedSlabMetricsClient;
			MetricsClient localMnemonicMetricsClient;

			localMemcachedMetricsClient = new LocalMemcachedMetricsClient(memcachedclient);
			localMemcachedMetricsClient = new GateMetricsFilter(localMemcachedMetricsClient, memcachedStatsGate);
			localMemcachedMetricsClient = new BlacklistMetricsFilter(localMemcachedMetricsClient, memcachedStatsBlacklist, memcachedStatsBlacklistEnabled);
			localMemcachedMetricsClient = new WhitelistMetricsFilter(localMemcachedMetricsClient, memcachedStatsWhitelist, memcachedStatsWhitelistEnabled);

			localMemcachedSlabMetricsClient = new LocalMemcachedSlabMetricsClient(memcachedclient);
			localMemcachedSlabMetricsClient = new GateMetricsFilter(localMemcachedSlabMetricsClient, memcachedSlabStatsGate);
			localMemcachedSlabMetricsClient = new BlacklistMetricsFilter(localMemcachedSlabMetricsClient, memcachedSlabStatsBlacklist, memcachedSlabStatsBlacklistEnabled);
			localMemcachedSlabMetricsClient = new WhitelistMetricsFilter(localMemcachedSlabMetricsClient, memcachedSlabStatsWhitelist, memcachedSlabStatsWhitelistEnabled);

			if (Config.isMoneta()) {
				LOGGER.info("moneta metrics");

				localMnemonicMetricsClient = new LocalHttpMetricsClient(MNEMONIC_ENDPOINT.get());
				localMnemonicMetricsClient = new GateMetricsFilter(localMnemonicMetricsClient, mnemonicStatsGate);
				localMnemonicMetricsClient = new BlacklistMetricsFilter(localMnemonicMetricsClient, mnemonicStatsBlacklist, mnemonicStatsBlacklistEnabled);
				localMnemonicMetricsClient = new WhitelistMetricsFilter(localMnemonicMetricsClient, mnemonicStatsWhitelist, mnemonicStatsWhitelistEnabled);
				localMnemonicMetricsClient = new ZeroCounterMetricsFilter(localMnemonicMetricsClient);

				metricsClient = new MergedMetricsClient(Arrays.asList(
						localMemcachedMetricsClient,
						localMemcachedSlabMetricsClient,
						localMnemonicMetricsClient
						));
			} else {
				LOGGER.info("classic evcache metrics");
				metricsClient = new MergedMetricsClient(Arrays.asList(
						localMemcachedMetricsClient,
						localMemcachedSlabMetricsClient
						));
			}
			metricsClient = new TagCheckMetricsFilter(metricsClient);
			metricsClient = new NumberFormatMetricsFilter(metricsClient);

		} catch (Throwable ex) {
			LOGGER.error("Error initializing metrics clients", ex);
			throw new RuntimeException("Error initializing metrics clients", ex);
		}
		LOGGER.info("Registering AtlasPlugin");
		DefaultMonitorRegistry.getInstance().register(this);
		LOGGER.info("Initialized AtlasPlugin");
	}

	private MonitorConfig getMonitorConfig(final EVCacheMetric metric) {

		// First check if this config already exists. If so, return it.
		final String metricKey = metric.mapKey();
		MonitorConfig config = monitorConfigMap.get(metricKey);
		if (config != null) {
			return config;
		}

		// If it doesn't exist, create it, insert it for next time, and return it.
		// Notice it uses the Atlas-specific name which includes a prefix
		MonitorConfig.Builder builder = MonitorConfig.builder(metric.getAtlasName());

		// Make a copy of the tags map
		Map<String, String> tags = metric.getTags().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

		// Remove the type tags and translate into the proper tag on the builder
		String type = tags.remove(EVCacheMetric.TAG_TYPE);
		if (StringUtils.equals(type, EVCacheMetric.TYPE_COUNTER)) {
			builder = builder.withTag(DataSourceType.COUNTER);
		} else {
			builder = builder.withTag(DataSourceType.GAUGE);
		}

		// remove the data type tag, as it's only used in this code and not by atlas.
		// It is used elsewhere to select the type of Gauge or Counter to report
		tags.remove(EVCacheMetric.TAG_DATA_TYPE);

		// Apply all other tags left, including
		for (Map.Entry<String, String> entry : tags.entrySet()) {
			builder = builder.withTag(entry.getKey(), entry.getValue());
		}

		final MonitorConfig mc = builder.build();
		monitorConfigMap.put(metricKey, mc);
		return mc;
	}

	private Optional<Monitor<?>> toMonitor(Pair<EVCacheMetric, MonitorConfig> p) {
		try {
			EVCacheMetric metric = p.first();
			MonitorConfig config = p.second();

			// The metric can be either an int counter, an int gauge or a float gauge.
			switch (metric.getTags().get(EVCacheMetric.TAG_TYPE)) {
			case EVCacheMetric.TYPE_COUNTER:
				final Long longCounterValue = Long.parseLong(metric.getVal());
				final BasicCounter counter = new BasicCounter(config);
				counter.increment(longCounterValue);
				return Optional.of(counter);

			case EVCacheMetric.TYPE_GAUGE:
				switch (metric.getTags().get(EVCacheMetric.TAG_DATA_TYPE)) {
				case EVCacheMetric.DATA_TYPE_UINT64:
					final Long longGaugeValue = Long.parseLong(metric.getVal());
					final LongGauge longGauge = new LongGauge(config);
					longGauge.set(longGaugeValue);
					return Optional.of(longGauge);

				case EVCacheMetric.DATA_TYPE_FLOAT64:
					final Double doubleGaugeValue = Double.parseDouble(metric.getVal());
					final DoubleGauge doubleGauge = new DoubleGauge(config);
					doubleGauge.set(doubleGaugeValue);
					return Optional.of(doubleGauge);

				default:
					LOGGER.error("Metric of unrecognized data type: {}", metric);
				}

			default:
				LOGGER.error("Metric of unrecognized type: {}", metric);
			}
		} catch (Exception ex) {
			// This really, REALLY should never happen since there's a filter that checks number formats
			LOGGER.error("Exception while creating monitors.", ex);
		}

		return Optional.empty();
	}

	private Stream<Monitor<?>> statsToMonitors(Stream<EVCacheMetric> stats) {
		// first decorate with monitor config as pair, then send through a modified "addMonitors" to actually create the monitors
		return stats
				// pair up the metric with its config
				.map(metric -> new Pair<>(metric, getMonitorConfig(metric)))
				// and change them into monitors
				.map(this::toMonitor)
				// remove the monitors that failed for some reason
				.flatMap(o -> o.map(Stream::of).orElseGet(Stream::empty));
	}


	@Override
	public Long getValue() {
		return getValue(0);
	}

	@Override
	public Long getValue(int pollerIndex) {
		return Long.valueOf(monitorConfigMap.size());
	}

	@Override
	public MonitorConfig getConfig() {
		return baseConfig;
	}

	@Override
	public List<Monitor<?>> getMonitors() {
		LOGGER.info("Metrics poller called");
		return statsToMonitors(metricsClient.getStats()).collect(Collectors.toList());
	}
}
