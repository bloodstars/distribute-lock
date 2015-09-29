package com.lufax.asset.Lock;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.locks.Condition;

import com.lufax.kernel.cache.CacheException;
import com.lufax.kernel.cache.redis.RedisCacheConfig;
import com.lufax.kernel.cache.redis.RedisCacheStore;

public class Z_Consumer2{

	static final RedisLock l = new RedisLock("ASSET");
	static final Condition notFull = l.newCondition().initCondition("full");
	static final Condition notEmpty = l.newCondition().initCondition("empty");
	static volatile int total = 0;
	static RedisCacheStore<String> c = l.cache;
	static volatile int interruped = 0;
	public static void main(String s[]) throws Exception{

		List<Thread> lt = new ArrayList<Thread>();
		Thread.sleep(1000);
		int to = 10;
		for(int i=0;i<to ;i ++){
			Thread t = new Thread(new A(i));
			lt.add(t);
			t.start();
		}
	}
	
	static class A implements Runnable{
		int index ;
		public A(int i){
			index = i;
		}
		@Override
		public void run() {
			for(;;){
				try {
					l.lock();
					int count = Integer.parseInt(c.get("mycount"));
					while(count <= 0){
						try {
							notEmpty.await();
						} catch (InterruptedException e) {
							interruped ++;
							throw e;
						}
						count = Integer.parseInt(c.get("mycount"));
					}
					
					total ++;
					c.put("mycount", String.valueOf(count - 1));
					//if(total % 1000 == 0)
					{
						System.out.println(String.format("Thread (%d) 干活，消费后剩余%d,一共干了%d,打断了%d次", index, count - 1,total,interruped));
					}
					
					
					notFull.signalAll();
				} catch (Exception e) {
					//e.printStackTrace();
				}finally{
					l.unlock();
				}
			
			}
			
		}
		
	}
}