package com.lufax.asset.Lock;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sun.misc.Unsafe;
/*
 * 基于远端CAS操作的分布式锁，以key()为锁关键字来进行；
 * 
 * 主要解决无论是数据库锁还是zk锁还是redis锁都是低并发的可以保证每个线程自旋，遇到高并发就完蛋了，db和zk都顶不住
 * 本锁只有head线程自旋，其他的sleep，支持interrupt和trylock，支持Condition
 * 
 * 该锁为非公平锁，极低概率会有饿死情况不必担心。
 * 
 * 各个方法遵守java.util.concurrent.locks.Lock的规范.
 * 使用例子：
	
	public class Test{
		static final RedisLock l = new RedisLock("ASSET");//相同参数在各个server被视为同一个锁
		static final Condition full = l.newCondition().initCondition("full");//相同参数在各个server被视为同一个condition
		static final Condition empty = l.newCondition().initCondition("empty");
		
		public static void main(String s[]) throws Exception{
			new Thread(new Runnable(){//消费者线程
					@Override
					public void run() {
						while(true){
							l.lock();
							int count = getCount();
							try{
								while(count <= 0){
									empty.await();//此处会释放锁，所以之前获取的count已经不准确了
									count = getCount();
								} 
							}catch (InterruptedException e) {
								//处理中断异常
							}
							consumeObject();//已经获取了锁，消费吧
							full.signalAll();
							l.unlock();
						}
					}
			}).start();
			
			new Thread(new Runnable(){//生产者线程
					@Override
					public void run() {
						while(true){
							l.lock();
							int count = getCount();
							try{
								while(count > 10){
									full.await();//此处会释放锁，所以之前获取的count已经不准确了
									count = getCount();
								} 
							}catch (InterruptedException e) {
								//处理中断异常
							}
							produceObject();//已经获取了锁，生产吧
							empty.signalAll();
							l.unlock();
						}
					}
			}).start();
		}
	}
 * 20150918，今天进行了测试，
 * 用例1： 3个server，每server有100线程，每线程100次对缓存中int对象执行i++操作，结果正确无误。
 * 用例2:  3个server，每server有2线程，每线程10000次对缓存中int对象执行i++操作，结果正确无误。
 * 
 * 锁效率跟DataSource实现有关。
 * 
 * 20150921 今天抽空测试了Condition
 * 典型的生产消费者模式：
 * final Lock lock = new Lock();//锁对象  
   final Condition notFull  = lock.newCondition().initCondition("full");//写线程条件   
   final Condition notEmpty = lock.newCondition().initCondition("empty");//读线程条件   
   
   用例1：生产者在server1上，单线程，循环200次，每次生产6个object到容量为15的池子中，如果超过15个则wait
   		 消费者在server2上，10线程，无限循环，每次消费1个object，如果池子没有object则wait。
   		 
   用例2：上述条件，生产者改为随机数量的生产，结果正确。
   
   测试20次，尚未发现卡死或数量对不上的情况，改天测试三台机器的。
 * 
 * 20150922 今天加大并发和总量测试Condition
   用例1：2个server生产者（各10线程），2个server消费者（各15线程），总共生产和消费30万，结果正确无误。
   
 * 20150924 今天实现了condition的可interrupt的wait，接下来的接口也不打算实现了，然后进行了测试：
 * 
 * 用例1：生产者2个server共20线程，消费者2个server共20线程，每个server的主线程随机interrupt某个工作线程，运行了10万次，共执行打断4000次，
 * 		 生产消费结果数据无误。
 * 
 * 此类已经定型，不再继续开发。
 */
public abstract class DistributeLock implements Lock{
	
	public static interface DataSource<T>{
		/**
		 * 远端可以实现cas操作的接口，是锁的核心，因为不在乎originvalue，只需要desValue
		 * @param key 关键字
		 * @param value 目标value
		 * @param lockLife 远端值的声明周期，单位秒
		 * @return
		 */
		public boolean compareAndSet(String key, T value, long lockLife);
		
		/**
		 * 将key和value都push到远端
		 * @param key
		 * @param value
		 * @param lockLife
		 * @return
		 */
		public boolean put(String key, T value, long lockLife);
		
		/**
		 * 获取远端key对应的value
		 * @param key
		 * @return
		 */
		public T get(String key);
		
		/**
		 * 移除远端的key和value
		 * @param key
		 * @return
		 */
		public boolean remove(String key);
		
		/**
		 * 根据lock名称和condition名称 获取所有远端的key
		 * 举例：
		 * 		
		 * @param lockName  锁名称
		 * @param conditionName  condition名称
		 * @return
		 */
		public String[] keys(String lockName, String conditionName);
		
		/**
		 * 批量获取key和value
		 * @param keys
		 * @return
		 */
		public Map<String, T> batchGet(String[] keys);
		
		/**
		 * 返回这个lock的key，相同key在各个server被算作同一个lock
		 * @return
		 */
		public String key();
		/**
		 * 当前thread获取到lock之后所能持续的时间，超过时间之后，其他server的线程就可以获得锁。
		 * （单位秒）
		 * @return
		 */
		public int lockLife();
	}
	
	/**
	 * 返回数据源结构，可以是远端的redis或者db或者zk，也可以是AtomicInteger
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	protected abstract DataSource dataSource();
	
	@Override
	public void lock() {
		Thread currentThread = Thread.currentThread();
		if(headThread == currentThread){
			return ;
		}
		try{
			for(;;){
				if(compareAndSetHeadThread(null, currentThread)){
					while(!tryAcquire()){
						LockSupport.parkNanos(timepart());
						Thread.interrupted();//lock函数不支持中断，所以清除标志位。
					}
					return ;
				}else{
					waitingQueue.add(currentThread);
					LockSupport.park();
					while(Thread.interrupted()){
						LockSupport.park();//lock函数不支持中断，所以清除标志位。
					}
				}
			}
		}catch(RuntimeException ex){
			cancelAcquire();
			throw ex;
		}
	}
	
	@Override
	public void lockInterruptibly() throws InterruptedException {
		if (Thread.interrupted())
            throw new InterruptedException();
		
		Thread currentThread = Thread.currentThread();
		
		if(headThread == currentThread){
			return ;
		}
		try{
			for(;;){
				if(compareAndSetHeadThread(null, currentThread)){
					while(!tryAcquire()){
						LockSupport.parkNanos(timepart());
						if(Thread.interrupted()){
							cancelAcquire();
							throw new InterruptedException();
						}
					}
					return;
				}else{
					waitingQueue.add(currentThread);
					LockSupport.park();
					if(Thread.interrupted()){
						cancelAcquire();
						throw new InterruptedException();
					}
				}
			}
		}catch(RuntimeException ex){
			cancelAcquire();
			throw ex;
		}
	}
	
	@Override
	public boolean tryLock() {
		
		Thread currentThread = Thread.currentThread();
		if(headThread == currentThread){
			return true;
		}
		try{
			if(compareAndSetHeadThread(null, currentThread)){
				if(tryAcquire()){
					return true;
				}else{
					cancelAcquire();
					return false;
				}
			}else{
				return false;
			}
		}catch(RuntimeException ex){
			cancelAcquire();
			throw ex;
		}
	}
	
	@Override
	public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
		if(tryLock()){
			return true;
		}
		long nanos = unit.toNanos(timeout);
		long partna = timepart();
		for(int i=0; i < nanos / partna ; i++){
			LockSupport.parkNanos(partna);
			if(Thread.interrupted()){
				throw new InterruptedException();
			}
			if(tryLock()){
				return true;
			}
		}
		
		LockSupport.parkNanos(nanos % partna + 1);
		if(Thread.interrupted()){
			throw new InterruptedException();
		}
		return tryLock();
	}
	@Override
	public void unlock() {	
		if(Thread.currentThread() != headThread){
			throw new IllegalMonitorStateException();
		}
		
		while(!tryRelease()){
			LockSupport.parkNanos(timepart());
			Thread.interrupted();//lock函数不支持中断，所以清除标志位。
		}
		
		cancelAcquire();
	}
	
	@Override
	public ConditionObject newCondition() {
		return new ConditionObject();
	}
	
	@SuppressWarnings("unchecked")
	private boolean tryAcquire(){
		return dataSource().compareAndSet(dataSource().key(), currentThreadName(), dataSource().lockLife());
	}
	
	private boolean tryRelease() {
		try {
			if(currentThreadName().equals(dataSource().get(dataSource().key()).toString())){
				return dataSource().remove(dataSource().key());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
    }
	
	private void cancelAcquire() {
		Thread currectThread = Thread.currentThread();
		if(currectThread == headThread){
			compareAndSetHeadThread(currectThread, null);
			Thread next = waitingQueue.poll();
			while(next != null){
				LockSupport.unpark(next);
				if(headThread == null){
					next = waitingQueue.poll();
				}else{
					break;
				}
			}
		}else{
			waitingQueue.remove(currectThread);
		}
	}
    private final boolean compareAndSetHeadThread(Thread src , Thread des) {
        return unsafe.compareAndSwapObject(this, headOffset, src, des);
    }
    private static final String currentThreadName(){
    	Thread currectThread = Thread.currentThread();
    	return String.format("%s-%d", currectThread.getName(), System.identityHashCode(currectThread));
    } 
	private long timepart() {
		return timepart / 2 + System.currentTimeMillis() % (timepart / 2);
	}
	private static Unsafe unsafe;
	private static long headOffset;
	private final ConcurrentLinkedQueue<Thread> waitingQueue = new ConcurrentLinkedQueue<Thread>();
	private volatile Thread headThread;
	protected long timepart =  50000000L;//自旋操作间隔时间，可以被继承修改。50毫秒
	private final static String serverSeed;
	
	static{
		try{
			Field theUnsafeInstance = Unsafe.class.getDeclaredField("theUnsafe");
			theUnsafeInstance.setAccessible(true);
			unsafe = (Unsafe) theUnsafeInstance.get(Unsafe.class);
			headOffset = unsafe.objectFieldOffset(DistributeLock.class.getDeclaredField("headThread"));
		}catch(Exception e){
			e.printStackTrace();
		}
		
		Thread lockDeamonThread = new Thread(new Runnable(){
			@Override
			public void run() {
				Log logL = LogFactory.getLog(DistributeLock.class.getName());
				Log logC = LogFactory.getLog(ConditionObject.class.getName());
				for(;;){
					try{
						LockSupport.parkNanos(60000000000L);//60秒
						
						Iterator<WeakReference<DistributeLock>>  it = deamonList.iterator();
						while(it.hasNext()){
							WeakReference<DistributeLock> ref = it.next();
							if(ref.get() == null){
								it.remove();
								continue;
							}
							
							DistributeLock lock = ref.get();
							
							logL.info(String.format("DLock (%s), HeadThread(%s), (%d) are waiting. ", lock.dataSource().key(), lock.headThread, lock.waitingQueue.size()));
						}
						
						Iterator<WeakReference<ConditionObject>>  itc = deamonConditionList.iterator();
						while(itc.hasNext()){
							WeakReference<ConditionObject> ref = itc.next();
							if(ref.get() == null){
								itc.remove();
								continue;
							}
							
							ConditionObject condition = ref.get();
							
							logC.info(String.format("DLock (%s)'s Condition(%s), Head is (%s) , (%d) are waiting. ", condition.lockWhoHoldMe.dataSource().key(), condition.conditionName, condition.conditionHeadThread, condition.conditionQueue.size()));
						}
					}catch(Exception e){
						e.printStackTrace();
					}
				}
			}
		});
		
		lockDeamonThread.setDaemon(true);
		lockDeamonThread.start();
		
		//redis的模糊查询被支持了，所以随机一个种子就行了
		serverSeed = String.format("%s-%d-%d", RandomStringUtils.random(10) ,System.currentTimeMillis() ,System.identityHashCode(lockDeamonThread));
	}
	
	private static final LinkedBlockingQueue<WeakReference<DistributeLock>> deamonList = new LinkedBlockingQueue<WeakReference<DistributeLock>>();
	private static final LinkedBlockingQueue<WeakReference<ConditionObject>> deamonConditionList = new LinkedBlockingQueue<WeakReference<ConditionObject>>();
	{
		deamonList.offer(new WeakReference<DistributeLock>(this));
	}
	
	public class ConditionObject implements Condition{
		
		private String conditionName ;
		
		private volatile Thread conditionHeadThread;
		
		private final ConcurrentLinkedQueue<Thread> conditionQueue = new ConcurrentLinkedQueue<Thread>();
		
		protected final DistributeLock lockWhoHoldMe = DistributeLock.this;
		/**
		 * 设置condition的name，相同的key的lock产生的相同name的condition被算作同一个
		 * @param name
		 * @return Condition
		 */
		public Condition initCondition(String name){
			deamonConditionList.add(new WeakReference<ConditionObject>(this));
			conditionName = new String(name);
			return this;
		}
		
		@Override
		public long awaitNanos(long nanosTimeout) throws InterruptedException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean await(long time, TimeUnit unit) throws InterruptedException {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean awaitUntil(Date deadline) throws InterruptedException {
			throw new UnsupportedOperationException();
		}

		@Override
		public void awaitUninterruptibly() {
			if (!isHeldExclusively())
                throw new IllegalMonitorStateException();
			
			Thread currentThread = Thread.currentThread();
			
			if(conditionHeadThread == null){
				setNotifyInfo(WAIT);
				conditionHeadThread = currentThread;
				unlock();
				conditionHeadWork();
			}else{
				conditionQueue.add(currentThread);
				unlock();
				LockSupport.park();
				if(conditionHeadThread == currentThread){
					conditionHeadWork();
				}else{
					lock();
				}
			}
		}
		
		@Override
		public void await() throws InterruptedException {
			if (!isHeldExclusively())
                throw new IllegalMonitorStateException();
			
			Thread currentThread = Thread.currentThread();
			
			if(conditionHeadThread == null){
				setNotifyInfo(WAIT);
				conditionHeadThread = currentThread;
				unlock();
				conditionHeadWorkInterruptily();
			}else{
				conditionQueue.add(currentThread);
				unlock();
				LockSupport.park();
				if(conditionHeadThread == currentThread){
					conditionHeadWorkInterruptily();
				}else{
					lock();
					if(Thread.interrupted()){
						throw new InterruptedException();
					}
				}
			}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void signalAll() {
			if (!isHeldExclusively())
                throw new IllegalMonitorStateException();
			
				String[] keys = dataSource().keys(dataSource().key(), conditionName);
				if(keys.length != 0){
					Map<String, Object> keysValue = dataSource().batchGet(keys);
					for(String key : keys){
						if(WAIT.equals(keysValue.get(key))){
							setNotifyInfo(ALL ,key);
						}
					}
				}
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void signal() {
			if (!isHeldExclusively())
                throw new IllegalMonitorStateException();
			
			String[] keys = dataSource().keys(dataSource().key(), conditionName);
			if(keys.length != 0){
				Map<String, Object> keysValue = dataSource().batchGet(keys);
				
				LinkedList<String> optionList = new LinkedList<String>();
				
				for(String key : keys){
					if(WAIT.equals(keysValue.get(key))){
						optionList.add(key);
					}
				}
				
				if(!optionList.isEmpty()){
					setNotifyInfo(ONE, optionList.get(new Random().nextInt(optionList.size())));
				}
			}
		}
		
		private boolean isHeldExclusively() {
			if(conditionName == null){
				throw new RuntimeException("you must initCondition() first.");
			}
			if(Thread.currentThread() == headThread){
				if(currentThreadName().equals(dataSource().get(dataSource().key()).toString())){
					return true;
				}
			}
			return false;
	    }
		
		private void conditionHeadWork(){
			for(String info = getNotifyInfo(); ; info = getNotifyInfo()){
				if(NONE.equals(info) || WAIT.equals(info) || info == null){
					LockSupport.parkNanos(timepart());
				}else{
					lock();
					if(info.equals(ONE)){
						conditionHeadThread = conditionQueue.poll();
						if(conditionHeadThread != null){
							setNotifyInfo(WAIT);
							LockSupport.unpark(conditionHeadThread);
						}else{
							setNotifyInfo(NONE);
						}
					}else if(info.equals(ALL)){
						Thread next = conditionQueue.poll();
						while(next != null){
							LockSupport.unpark(next);
							next = conditionQueue.poll();
						}
						conditionHeadThread = null;
						setNotifyInfo(NONE);
					}
					break;
				}
			}
		}
		
		private void conditionHeadWorkInterruptily() throws InterruptedException {
			for(String info = getNotifyInfo() ; ; info = getNotifyInfo()){
				if(NONE.equals(info) || WAIT.equals(info) || info == null){
					LockSupport.parkNanos(timepart());
					if(Thread.interrupted()){
						lock();
						conditionHeadThread = conditionQueue.poll();
						if(conditionHeadThread != null){
							LockSupport.unpark(conditionHeadThread);
						}
						throw new InterruptedException();
					}
				}else{
					lock();
					if(info.equals(ONE)){
						conditionHeadThread = conditionQueue.poll();
						if(conditionHeadThread != null){
							setNotifyInfo(WAIT);
							LockSupport.unpark(conditionHeadThread);
						}else{
							setNotifyInfo(NONE);
						}
					}else if(info.equals(ALL)){
						Thread next = conditionQueue.poll();
						while(next != null){
							LockSupport.unpark(next);
							next = conditionQueue.poll();
						}
						conditionHeadThread = null;
						setNotifyInfo(NONE);
					}
					break;
				}
			}
		}
		
		private String getNotifyInfo(){
			return (String) dataSource().get(String.format("%s_%s_%s", dataSource().key(), conditionName, serverSeed));
		}
		
		private void setNotifyInfo(String info){
			setNotifyInfo(info, String.format("%s_%s_%s", dataSource().key(), conditionName, serverSeed));
		}
		
		@SuppressWarnings("unchecked")
		private void setNotifyInfo(String info, String key){
			dataSource().put(key, info, 24 * 60 * 60);
		}
		
		
		private static final String NONE = "NONE";
		private static final String WAIT = "WAIT";
		private static final String ONE = "ONE";
		private static final String ALL = "ALL";
	}
}
