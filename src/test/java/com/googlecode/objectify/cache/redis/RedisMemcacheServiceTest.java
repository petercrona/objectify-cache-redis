package com.googlecode.objectify.cache.redis;

import com.google.common.collect.ImmutableMap;
import com.googlecode.objectify.cache.IdentifiableValue;
import com.googlecode.objectify.cache.MemcacheService.CasPut;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.truth.Truth.assertThat;

class RedisMemcacheServiceTest {

	private JedisPool pool;
	private RedisMemcacheService service;

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
	void getNothing() {
		final Object result = service.get("asdf");
		assertThat(result).isNull();
	}

	@Test
	void getAllMissing() {
		final Map<String, Object> result = service.getAll(Collections.singletonList("asdf"));
		assertThat(result.get("asdf")).isNull();
	}

	@Test
	void getAllExistingAndNullIfMissing() {
		service.put("asdf", "foobar");
		Collection<String> keys = new ArrayList<>();
		keys.add("asdf");
		keys.add("kalle");

		final Map<String, Object> result = service.getAll(keys);

		assertThat(result.get("asdf")).isEqualTo("foobar");
		assertThat(result.get("kalle")).isNull();
	}

	@Test
	void setAndGetNull() {
		service.put("asdf", null);
		final Object result = service.get("asdf");
		assertThat(result).isNull();
	}

	@Test
	void setAndGetSimpleValue() {
		service.put("asdf", "value");
		final Object result = service.get("asdf");
		assertThat(result).isEqualTo("value");
	}

	@Test
	void deleteWorks() {
		service.put("asdf", "value");
		service.deleteAll(Collections.singletonList("asdf"));
		final Object result = service.get("asdf");
		assertThat(result).isNull();
	}

	@Test
	void deleteManyWorks() {
		service.put("asdf", "value");
		service.put("kalle", "value");

		Collection<String> toDelete = new ArrayList<>();
		toDelete.add("asdf");
		toDelete.add("kalle");
		toDelete.add("missing");

		service.deleteAll(toDelete);

		assertThat(service.get("asdf")).isNull();
		assertThat(service.get("kalle")).isNull();
		assertThat(service.get("missing")).isNull();
	}

	@Test
	void simpleGetAndPutIdentifiable() {
		final IdentifiableValue iv = this.getIdentifiable("asdf");
		assertThat(iv.getValue()).isNull();

		final boolean success = this.putIfUntouched("asdf", new CasPut(iv, "next", 0));
		assertThat(success).isTrue();

		final Object result = service.get("asdf");
		assertThat(result).isEqualTo("next");
	}

	@Test
	void simpleMultiGetAndPutIdentifiable() {
		List<IdentifiableValue> ivs = IntStream.range(0, 10)
				.mapToObj(i -> this.getIdentifiable(Integer.toString(i))).collect(Collectors.toList());
		ivs.forEach(iv -> assertThat(iv.getValue()).isNull());

		Map<String, CasPut> data = IntStream.range(0, 10).boxed().collect(Collectors.toMap(
				i -> Integer.toString(i),
				i -> new CasPut(ivs.get(i), "next" + i, 0)
		));

		Set<String> successes = service.putIfUntouched(data);

		IntStream.range(0, 10).mapToObj(Integer::toString).forEach(i -> {
			assertThat(successes.contains(i)).isTrue();
			assertThat(service.get(i)).isEqualTo("next" + i);
		});
	}

	@Test
	void multiGetAndPutIdentifiableWithExpiration() {
		List<IdentifiableValue> ivs = IntStream.range(0, 10)
				.mapToObj(i -> this.getIdentifiable(Integer.toString(i))).collect(Collectors.toList());
		ivs.forEach(iv -> assertThat(iv.getValue()).isNull());

		Map<String, CasPut> data = IntStream.range(0, 10).boxed().collect(Collectors.toMap(
				i -> Integer.toString(i),
				i -> new CasPut(ivs.get(i), "next" + i, 1)
		));

		Set<String> successes = service.putIfUntouched(data);

		IntStream.range(0, 10).mapToObj(Integer::toString).forEach(i -> {
			assertThat(successes.contains(i)).isTrue();
			assertThat(service.get(i)).isEqualTo("next" + i);
		});
	}

	@Test
	void getAndPutIdentifiableWithExpiration() throws Exception {
		final IdentifiableValue iv = this.getIdentifiable("asdf");
		assertThat(iv.getValue()).isNull();

		final boolean success = this.putIfUntouched("asdf", new CasPut(iv, "next", 1));
		assertThat(success).isTrue();

		final Object result1 = service.get("asdf");
		assertThat(result1).isEqualTo("next");

		Thread.sleep(2_000);

		final Object result2 = service.get("asdf");
		assertThat(result2).isNull();
	}

	@Test
	void getAndPutIdentifiableInterrupted() {
		final IdentifiableValue iv = this.getIdentifiable("asdf");
		assertThat(iv.getValue()).isNull();

		service.put("asdf", "somethingelse");

		final boolean success = this.putIfUntouched("asdf", new CasPut(iv, "next", 0));
		assertThat(success).isFalse();

		final Object result = service.get("asdf");
		assertThat(result).isEqualTo("somethingelse");
	}

	private IdentifiableValue getIdentifiable(final String key) {
		final Map<String, IdentifiableValue> map = service.getIdentifiables(Collections.singletonList(key));
		final IdentifiableValue iv = map.get(key);
		assertThat(iv).isNotNull();
		return iv;
	}

	private boolean putIfUntouched(final String key, final CasPut casPut) {
		final Set<String> result = service.putIfUntouched(ImmutableMap.of(key, casPut));
		return result.contains(key);
	}

//	@Test
	void experiment() {
		final byte[] script = "local value = redis.call('get', KEYS[1]); if (value:sub(1, 16) == KEYS[2]) then return 'OK' else return 'NOTOK' end".getBytes(StandardCharsets.UTF_8);

		final String key = "asdf";
		final byte[] binKey = key.getBytes(StandardCharsets.UTF_8);

		final RedisIdentifiableValue oldIv = (RedisIdentifiableValue)getIdentifiable(key);
		final byte[] oldVersion = oldIv.getVersionRedisString();

		try (final Jedis jedis = pool.getResource()) {
			final Object response = jedis.eval(script, 2, binKey, oldVersion);
			final String responseStr = new String((byte[])response, StandardCharsets.UTF_8);
			assertThat(responseStr).isEqualTo("OK");
		}
	}

}
