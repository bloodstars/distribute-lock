package com.lufax.asset.Lock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.lufax.kernel.cache.CacheException;
import com.lufax.kernel.cache.redis.NXXX;
import com.lufax.kernel.cache.redis.RedisCacheConfig;
import com.lufax.kernel.cache.redis.RedisCacheStore;
import com.lufax.asset.Lock.DistributeLock.DataSource;

public class DBLock extends DistributeLock implements DataSource<String>{
	/*
	 * 利用数据库做分布式锁，伪代码先弄上，以后在写吧。
	 */
	private String name ;
	
	public DBLock(String name){
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
		//insert lock_table set (key,value) value (#key#,#value#)
		//if(insertnum > 0) return true;
		//else   update lock_table set value = #value# where key = #key# and value = #originValue#;
		//if(updatenum > 0)
		return true;
	}
	@Override
	public String get(String key) {
		//select value from lock_table where key = #key#
		return "value";
	}
	@Override
	public boolean remove(String key) {
		//delete from lock_table where key = #key#
		//if(returnnum > 0)
		return true;
	}
	@Override
	public String[] keys(String lockName, String conditionName) {
		String patern = String.format("%s_%s_%", lockName, conditionName);
		//select key from lock_table where key like #patern#
		//if(keys.length > 0)
		//return keys
		return new String[0];
	}
	@Override
	public Map<String, String> batchGet(String[] keys) {
		//select value from lock_table where key in (keys)
		Map<String,String> map = new HashMap<String,String>();
		//for(all values)
		//		map.put(key,value)
		return map;
	}
	
	@Override
	public boolean put(String key, String value, long lockLife) {
		//insert lock_table set (key,value) value (#key#,#value#)
		//if(insertnum > 0) return true;
		//else   update lock_table set value = #value# where key=#key#;
		//if(updatenum > 0)
		return true;
	}
}