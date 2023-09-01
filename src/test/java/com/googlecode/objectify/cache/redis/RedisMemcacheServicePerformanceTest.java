package com.googlecode.objectify.cache.redis;

import com.googlecode.objectify.cache.MemcacheService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Disabled
class RedisMemcacheServicePerformanceTest {

	private JedisPool pool;
	private RedisMemcacheService service;

	private static final int NR_ENTRIES = 100000;

	@BeforeEach
	void setUp() {
		pool = new JedisPool();
		service = new RedisMemcacheService(pool);

		try (final Jedis jedis = pool.getResource()) {
			jedis.flushDB();
		}
	}

	@AfterEach
	void tearDown() {
		pool.close();
	}

	@Test
	void getIdentifiables() {
		Set<String> keys = IntStream.range(0, NR_ENTRIES).mapToObj(Integer::toString).collect(Collectors.toSet());
		long start = System.currentTimeMillis();
		service.getIdentifiables(keys);
		System.out.println(System.currentTimeMillis() - start);
	}

	@Test
	void deleteAll() {
		Set<String> keys = IntStream.range(0, NR_ENTRIES).mapToObj(Integer::toString).collect(Collectors.toSet());
		long start = System.currentTimeMillis();
		service.deleteAll(keys);
		System.out.println(System.currentTimeMillis() - start);
	}

	@Test
	void getAll() {
		Set<String> keys = IntStream.range(0, NR_ENTRIES).mapToObj(Integer::toString).collect(Collectors.toSet());

		service.getIdentifiables(keys);

		long start = System.currentTimeMillis();
		service.getAll(keys);
		System.out.println(System.currentTimeMillis() - start);
	}

	@Test
	void putIfUntocuhed() {
		Map<String, MemcacheService.CasPut> data = IntStream.range(0, NR_ENTRIES).mapToObj(i -> {
			RedisIdentifiableValue iv = new RedisIdentifiableValue(i);
			return new MemcacheService.CasPut(iv, "next", 0);
		}).collect(Collectors.toMap(v -> Integer.toString((Integer) v.getIv().getValue()), v -> v));

		service.getIdentifiables(IntStream.range(0, NR_ENTRIES).mapToObj(Integer::toString).collect(Collectors.toSet()));

		long start = System.currentTimeMillis();
		service.putIfUntouched(data);
		System.out.println(System.currentTimeMillis() - start);
	}

}
