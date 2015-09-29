package com.lufax.asset.Lock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.lufax.kernel.cache.CacheException;
import com.lufax.kernel.cache.redis.NXXX;
import com.lufax.kernel.cache.redis.RedisCacheConfig;
import com.lufax.kernel.cache.redis.RedisCacheStore;
import com.lufax.asset.Lock.DistributeLock.DataSource;

@SuppressWarnings({"unchecked","rawtypes"})
public class RedisLock extends DistributeLock implements DataSource<String>{
	
	private String name ;
	
	public static RedisCacheStore cache;
	static{
		//测试环境的redis服务器，需要配置线上的。
		RedisCacheConfig c = new RedisCacheConfig("172.19.15.230:6601","p2p-cache","MY");
		c.setPassword("redis1");
		cache = new RedisCacheStore(c, String.class, null, null);
	}
	
	
	public RedisLock(String name){
		this.name = name;
	}

	@Override
	protected DataSource<String> dataSource() {
		return this;
	}
	
	@Override
	public String key() {
		return name;
	}

	@Override
	public int lockLife() {
		return 60 * 60 *24;
	}
	@Override
	public boolean compareAndSet(String key, String value, long lockLife) {
		try {
			return cache.set(key, value, NXXX.NX, (int)lockLife);
		} catch (CacheException e) {
			return false;
		}
	}
	@Override
	public String get(String key) {
		try {
			return cache.get(key).toString();
		} catch (CacheException e) {
			return null;
		}
		
	}
	@Override
	public boolean remove(String key) {
		try {
			cache.remove(key);
		} catch (CacheException e) {
			
		}
		return true;
	}
	@Override
	public String[] keys(String lockName, String conditionName) {
		try {
			Object[] fakeKeys = cache.keys(String.format("%s_%s_*", lockName, conditionName)).toArray();
			if(fakeKeys.length != 0){
				String[] keys = new String[fakeKeys.length];
				for(int i=0 ; i<fakeKeys.length; i++){
					keys[i] = fakeKeys[i].toString().split(":")[1];
				}
				return keys;
			}
		} catch (CacheException e) {
			
		}
		return new String[0];
	}
	@Override
	public Map<String, String> batchGet(String[] keys) {
		Map<String, String> res = new HashMap<String, String>();
		try {
			
			if(keys.length != 0){
				List<Object> values = cache.mget(keys);
				for(int i=0; i< keys.length; i++){
					res.put(keys[i], values.get(i).toString().substring(1,values.get(i).toString().length() - 1));
				}
			}
		} catch (CacheException e) {
			
		}
		return res;
	}
	
	@Override
	public boolean put(String key, String value, long lockLife) {
		try {
			cache.put(key, value, (int)lockLife);
		} catch (CacheException e) {
		}
		return true;
	}
}