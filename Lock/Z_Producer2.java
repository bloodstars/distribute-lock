package com.lufax.asset.Lock;

import java.util.Random;
import java.util.concurrent.locks.Condition;

import com.lufax.kernel.cache.CacheException;
import com.lufax.kernel.cache.redis.RedisCacheConfig;
import com.lufax.kernel.cache.redis.RedisCacheStore;

public class Z_Producer2{
	static final RedisLock l = new RedisLock("ASSET");
	static final Condition notFull = l.newCondition().initCondition("full");
	static final Condition notEmpty = l.newCondition().initCondition("empty");
	static final RedisCacheStore<String> c = l.cache;
	static volatile int total = 0;
	static volatile int interruped = 0;
	public static void main(String s[]) throws Exception{
		
		try{
			for(int j=0;j<10;j++){
				new Thread(new Runnable(){
					@Override
					public void run() {
						for(;;){
							try {
								l.lock();
								int count = Integer.parseInt(c.get("mycount"));
								while(count > 10){
									try {
										notFull.await();
									} catch (InterruptedException e) {
										interruped ++;
										throw e;
									}
									count = Integer.parseInt(c.get("mycount"));
								}
								
								int countt = new Random().nextInt(5) + 1;
								total += countt;
								c.put("mycount", String.valueOf(count + countt));
								System.out.println(String.format("线程(%s)生产，生产%d，剩余 %d", Thread.currentThread().getName(),countt, count + countt));
								notEmpty.signalAll();
							} catch (Exception e) {
								e.printStackTrace();
							}finally{
								l.unlock();
								if(total > 10000){
									System.out.println(String.format("线程(%s)生产完毕, 总共已经生产了%d", Thread.currentThread().getName(), total));
									return ;
								}
							}
						}
						
					}
					
				}).start();
				
			}
		}catch(Exception e){
			//e.printStackTrace();
		}
		
		
	}
}