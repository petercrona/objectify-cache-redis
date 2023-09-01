package com.googlecode.objectify.cache.redis;

import com.googlecode.objectify.cache.IdentifiableValue;
import com.googlecode.objectify.cache.MemcacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * We store everything using java serialization. In theory we could be more efficient with some sort of custom
 * translator, but we really just store Entity objects, which are pretty complex, so let's just go with serialization.
 *
 * Storage format: We prefix all values with a 16-byte UUID version number, which is randomized on every put.
 */
@RequiredArgsConstructor
@Slf4j
public class RedisMemcacheService implements MemcacheService {
	/**
	 * If we give Jedis bytes, we get back bytes
	 */
	private static final byte[] OK_BYTES = "OK".getBytes(StandardCharsets.UTF_8);

	/** */
	private final JedisPool jedisPool;

	public RedisMemcacheService(final String host) {
		this(new JedisPool(host));
	}

	public RedisMemcacheService(final String host, final int port) {
		this(new JedisPool(host, port));
	}

	/** */
	private RedisIdentifiableValue fromCacheValue(final byte[] thing) {
		if (thing == null)
			return null;

		if (thing.length == 0)
			return null;

		try {
			return RedisIdentifiableValue.fromRedisString(thing);
		} catch (final Exception e) {
			log.error("Error deserializing from redis", e);
			return null;
		}
	}

	/** */
	private RedisIdentifiableValue getValue(final String key) {
		final byte[] binKey = key.getBytes(StandardCharsets.UTF_8);

		try (final Jedis jedis = jedisPool.getResource()) {
			final byte[] bytes = jedis.get(binKey);
			return fromCacheValue(bytes);
		}
	}

	/** */
	private void putValue(final String key, final RedisIdentifiableValue value) {
		final byte[] binKey = key.getBytes(StandardCharsets.UTF_8);

		try (final Jedis jedis = jedisPool.getResource()) {
			jedis.set(binKey, value.toRedisString());
		}
	}

	private <T> void putValues(Function<T, RedisIdentifiableValue> mapFn, Set<Map.Entry<String, T>> entrySet) {
		int n = entrySet.size();

		if (n == 0) {
			return;
		}

		byte[][] keyValuePairs = new byte[n * 2][];

		int i = 0;
		for (Map.Entry<String, T> entry : entrySet) {
			keyValuePairs[i++] = entry.getKey().getBytes(StandardCharsets.UTF_8);
			keyValuePairs[i++] = mapFn.apply(entry.getValue()).toRedisString();
		}

		try (final Jedis jedis = jedisPool.getResource()) {
			jedis.mset(keyValuePairs);
		}
	}

	@Override
	public Object get(final String key) {
		final RedisIdentifiableValue iv = getValue(key);
		return iv == null ? null : iv.getValue();
	}

	@Override
	public Map<String, IdentifiableValue> getIdentifiables(final Collection<String> keys) {
		Map<String, IdentifiableValue> ivs = getAll(
				x -> x == null
						? new RedisIdentifiableValue(null)
						: x,
				keys
		);

		putValues(
				e -> (RedisIdentifiableValue) e,
				ivs.entrySet().stream()
						.filter(e -> e.getValue().getValue() == null)
						.collect(Collectors.toSet())
		);

		return ivs;
	}

	@Override
	public Map<String, Object> getAll(final Collection<String> keys) {
		return getAll(x -> x == null ? null : x.getValue(), keys);
	}

	private <T> Map<String, T> getAll(Function<RedisIdentifiableValue, T> mapFn, final Collection<String> keys) {
		final byte[][] binKeys = keys.stream().map(key -> key.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);

		final List<byte[]> fetched;
		try (final Jedis jedis = jedisPool.getResource()) {
			fetched = jedis.mget(binKeys);
		}

		final Map<String, T> result = new LinkedHashMap<>();

		final Iterator<String> keysIt = keys.iterator();
		int index = 0;
		while (keysIt.hasNext()) {
			final String key = keysIt.next();
			final byte[] value = fetched.get(index);

			final RedisIdentifiableValue iv = value == null ? null : RedisIdentifiableValue.fromRedisString(value);

			result.put(key, mapFn.apply(iv));

			index++;
		}

		return result;
	}

	@Override
	public void put(final String key, final Object value) {
		putValue(key, new RedisIdentifiableValue(value));
	}

	public Set<String> putIfUntouched(final Map<String, CasPut> values) {
		final Set<String> successes = new HashSet<>();
		final Map<String, Response<Object>> responses = new HashMap<>();

		try (final Jedis jedis = jedisPool.getResource()) {
			Pipeline pipeline = jedis.pipelined();

			values.forEach((key, cput) -> {
				final RedisIdentifiableValue iv = (RedisIdentifiableValue)cput.getIv();
				final byte[] lastVersion = iv.getVersionRedisString();

				final byte[] binKey = key.getBytes(StandardCharsets.UTF_8);
				final RedisIdentifiableValue nextIv = new RedisIdentifiableValue(cput.getNextToStore());

				responses.put(key, executePutScript(pipeline, binKey, lastVersion, nextIv.toRedisString(), cput.getExpirationSeconds()));
			});

			pipeline.close();

			responses.forEach((key, val) -> {
				if (Arrays.equals(OK_BYTES, (byte[]) val.get())) {
					successes.add(key);
				}
			});
		}

		return successes;
	}

	private static final byte[] PUT_SCRIPT_WITH_EXPIRATION = "local value = redis.call('get', KEYS[1]); if (value:sub(1, 16) == KEYS[2]) then return redis.call('set', KEYS[1], KEYS[3], 'EX', KEYS[4]) end".getBytes(StandardCharsets.UTF_8);

	private static final byte[] PUT_SCRIPT_WITHOUT_EXPIRATION = "local value = redis.call('get', KEYS[1]); if (value:sub(1, 16) == KEYS[2]) then return redis.call('set', KEYS[1], KEYS[3]) end".getBytes(StandardCharsets.UTF_8);

	private Response<Object> executePutScript(final Pipeline pipeline, final byte[] binKey, final byte[] lastVersion, final byte[] value, final int expiration) {
		if (expiration > 0) {
			// Redis expects the number as a string
			final byte[] exp = Integer.toString(expiration).getBytes(StandardCharsets.UTF_8);
			return pipeline.eval(PUT_SCRIPT_WITH_EXPIRATION, 4, binKey, lastVersion, value, exp);
		} else {
			return pipeline.eval(PUT_SCRIPT_WITHOUT_EXPIRATION, 3, binKey, lastVersion, value);
		}
	}

	@Override
	public void putAll(final Map<String, Object> values) {
		putValues(RedisIdentifiableValue::new, values.entrySet());
	}

	@Override
	public void deleteAll(final Collection<String> keys) {
		final byte[][] binKeys = keys.stream().map(key -> key.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
		try (final Jedis jedis = jedisPool.getResource()) {
			jedis.del(binKeys);
		}
	}
}
