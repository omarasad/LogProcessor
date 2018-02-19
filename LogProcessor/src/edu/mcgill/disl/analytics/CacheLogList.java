package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.log.StatisticsManager;


public class CacheLogList implements Cloneable {
	
	public ArrayList<CacheLogSegment> cacheLogList;
	public CacheLogSegment currentLogInterval;
	public int NUMBER_INTERVALS;
	long segBegTimeStamp = -1;
	long segTimeInterval;
	
	StatisticsManager sm = null;
	
	public CacheLogList(StatisticsManager manager){
		
		sm = manager;
		cacheLogList = new ArrayList<CacheLogSegment>();
		segTimeInterval = Long.parseLong(sm.props.getProperty(StatisticsManager.SEGMENT_INTERVAL));
		NUMBER_INTERVALS = Integer.parseInt(sm.props.getProperty(StatisticsManager.NO_INTERVALS));
//		System.out.println("cacheLogList created in constructor");
		
//		addNewCacheInterval();
	}
	
	public HashMap<String, CacheObject> aggregateSegments(){
		HashMap<String, CacheObject> hc = new HashMap<String, CacheObject>();
		
		int no_intervals = cacheLogList.size();
		
		if(no_intervals==0){
			System.out.println("No intervals yet. Returning null");
			
			return null;
		}
			
		
		//for(int i=0; i<no_intervals && no_intervals > 0; i++)
		for(int i=0; i<NUMBER_INTERVALS && no_intervals > 0; i++)
		{
			System.out.println("No# intervals" +no_intervals );
				
				CacheLogSegment cls = cacheLogList.get(no_intervals-1);
				if (cls==null)
					continue;
				
				System.out.println("cls.cacheMap.size()" +cls.cacheMap.size());
				
				Collection<CacheObject> c = cls.cacheMap.values();
				Iterator<CacheObject> it = c.iterator();
				
				while(it.hasNext()){
					CacheObject cot = it.next();
					CacheObject cachObj = hc.get(cot.cacheKey);
					
				//	System.out.println("cot.cacheKey" +cot.cacheKey);
				//	System.out.println("cot.cacheKey" +cot.getCount);
					if(cachObj == null){
						hc.put(cot.cacheKey, cot);
					}
					else{
					//	System.out.println("Else" + cot.cacheKey);
					//	System.out.println("cachObj.getCount" + cachObj.getCount);
					//	System.out.println("cot.getCount" + cot.getCount);
						cachObj.getCount = cachObj.getCount + cot.getCount;
						if (cachObj.size==1 && cot.size==1)
						{
							if (cachObj.last_put_time<cot.last_put_time)
								cachObj.last_put_time=cot.last_put_time;
						}
						else if (cot.size==1)
						{
							cachObj.last_put_time=cot.last_put_time;
							cachObj.size=1;
						}
					//	System.out.println("Else" +cachObj.getCount);
					}
					
				}
				
			no_intervals--;
		}
		
		return hc;
	}
	
	public void addNewCacheSegment(){
		System.out.println("addNewCacheInterval ");
		currentLogInterval = new CacheLogSegment();
		cacheLogList.add(currentLogInterval);
		if(cacheLogList.size()>NUMBER_INTERVALS){
			cacheLogList.remove(0);
		}
		segBegTimeStamp = System.currentTimeMillis();
	}
	
	public void addToCacheLogInterval(CacheObject co, String type, long accTime, int logType){
		
		//System.out.println("addToCacheLogInterval called by:" + co.cacheKey +"co.updateCount="+type);
		
		
		
		if(segBegTimeStamp == -1 || (System.currentTimeMillis()-segBegTimeStamp>segTimeInterval))
		{
			//System.out.println("Inside CacheLogList of CacheLogProcessor: New Segment Interval added");
			addNewCacheSegment();
		} 
		
		CacheObject ct;
		if((ct = sm.globalCacheMap.get(co.cacheKey))!= null)
		{
//        	if(type.contains("get"))
//        	{
//        		ct.last_get_time = co.first_get_time;
//        		ct.getCount++;
//        	}
//        	else if(type.contains("put"))
//        	{
//        		ct.last_put_time = co.first_put_time;
//        		ct.putCount++;
//        	}
			/*
			ct.getCount++;
			if (co.size==1)
			{
				ct.size=co.size;
			}
			*/
			if (co.size!=1)
			{
				ct.getCount++;
			}
			else
			{
				ct.size=1;
			}
			ct.accessTimes.add(accTime);
			
			/*
			if (type.contains("update") || logType>=2 )
			{
				if (type.contains("update"))
					ct.updateCount++;
				
				ct.totalExecTimeUupdate+=co.totalExecTime;
				//System.out.println("ct.cacheKey:" + ct.cacheKey +"co.updateCount="+ct.updateCount);
			}
			*/
			
			/*
			if (type.contains("UpdatNew"))
			{
				
				ct.updateCount++;
				ct.totalExecTimeUupdate+=co.totalExecTime;
				System.out.println("ct.1cacheKey:" + ct.cacheKey +"co.updateCount="+ct.updateCount);
			}
			*/
			if (logType==0)
				ct.local_hit++;
			else if (logType==1)
				ct.remote_hit++;
			else if (logType==2)
			{
				//System.out.println("ct.cacheKey:" + ct.cacheKey +"co.updateCount="+ct.updateCount);
				ct.updateCount++;
				ct.totalExecTimeUupdate+=co.totalExecTime;
			}
			
			else if (logType==3)
				ct.locLock++;
			else if (logType==4)
				ct.remLock++;
			else if (logType==5)
				ct.updtLock++;
			else if (logType==6)
				ct.cacheUpdt++;
					
			
			
			
			
			
		if (logType<3)
		{
				ct.totalExecTime+=co.totalExecTime;
				ct.totalCount++;
				
		}
			
			
			
			//System.out.println("co.getCount" + ct.getCount);
			//System.out.println("ct.size" + ct.size);
			
		}
		else
		{
			co.putCount=1;
			co.getCount=1;
			co.totalCount++;
			
			if (logType==0)
				co.local_hit++;
			else if (logType==1)
				co.remote_hit++;
			else if (logType==2)
			{
				co.updateCount++;
				co.totalExecTimeUupdate+=co.totalExecTime;
			}
			
			else if (logType==3)
				co.locLock++;
			else if (logType==4)
				co.remLock++;
			else if (logType==5)
				co.updtLock++;
			else if (logType==6)
				co.cacheUpdt++;
			//currentLogInterval.cacheMap.put(co.cacheKey, co);
			
			sm.globalCacheMap.put(co.cacheKey, co);
			
			
			if (!sm.analyserClassString.contains("Object"))
			{
				//Analyser.log.info("sm.ANALYSER_CLASSES =" +sm.ANALYSER_CLASSES);
			sm.gloabCacheObjectIndexing.put(sm.gloabalCacheKeyCounter, co.cacheKey);
			co.index=sm.gloabalCacheKeyCounter;
			sm.gloabalCacheKeyCounter++;
			}
			
			//////////// 	NEW DS //////////////
		//	co.accessTimes.add(accTime);
			
		//	System.out.println("co.size" + co.size);
			//System.out.println("co.cacheKey" + co.cacheKey);
			//System.out.println("co.size" + co.size);
			//Analyser.log.info("co.toString()" + co.toString());
		}
		
		//if (sm.counter==0) // here we need to fill out the system parameters
		if (sm.counter>=0) // here we need to fill out the system parameters
		{
			if (logType==0)
			{
				
				sm.readLocalTimeTotal+=co.totalExecTime;
				sm.readLocalCount++;
			}
			else if (logType==1)
			{
				if (co.totalExecTime<10000)
				{
				co.totalExecTime=1000000;
				}
				sm.readRemoteTimeTotal+=co.totalExecTime;
				sm.readRemotecount++;
				//System.out.println(co.cacheKey +" === "+co.totalExecTime);
				
			}
			else if (logType==2)
			{
				sm.updateRemoteTimeTotal+=co.totalExecTime;
				sm.updateRemoteCount++;
				
			}
			else if (logType==7)
			{
				
				sm.putLocalTimeTotal+=co.totalExecTime;
				sm.putLocalCount++;
			}
			else if (logType==8)
			{
			
				sm.putRemoteTimeTotal+=co.totalExecTime;
				sm.putRemoteCount++;
			}
		}
		
		
	}
	
	
public void addToCacheLogIntervalOri(CacheObject co, String type, long accTime){
		
//		System.out.println("addToCacheLogInterval called by:" + co.cacheKey);
		
		if(segBegTimeStamp == -1 || (System.currentTimeMillis()-segBegTimeStamp>segTimeInterval))
		{
			//System.out.println("Inside CacheLogList of CacheLogProcessor: New Segment Interval added");
			addNewCacheSegment();
		} 
		
		CacheObject ct;
		if((ct = currentLogInterval.cacheMap.get(co.cacheKey))!= null)
		{
//        	if(type.contains("get"))
//        	{
//        		ct.last_get_time = co.first_get_time;
//        		ct.getCount++;
//        	}
//        	else if(type.contains("put"))
//        	{
//        		ct.last_put_time = co.first_put_time;
//        		ct.putCount++;
//        	}
			/*
			ct.getCount++;
			if (co.size==1)
			{
				ct.size=co.size;
			}
			*/
			if (co.size!=1)
			{
				ct.getCount++;
			}
			else
			{
				ct.size=1;
			}
			ct.accessTimes.add(accTime);
			//System.out.println("co.getCount" + ct.getCount);
			//System.out.println("ct.size" + ct.size);
			
		}
		else
		{
			co.putCount=1;
			co.getCount=1;
			currentLogInterval.cacheMap.put(co.cacheKey, co);
			co.accessTimes.add(accTime);
			
		//	System.out.println("co.size" + co.size);
			//System.out.println("co.cacheKey" + co.cacheKey);
			//System.out.println("co.size" + co.size);
			//Analyser.log.info("co.toString()" + co.toString());
		}
		
		
	}
	
	
	public static class CacheLogSegment implements Cloneable {
		//public HashMap<String, CacheObject> cacheMap;
		public ConcurrentHashMap<String, CacheObject> cacheMap;
		int[] cacheObjAllocatedLast;
		int[] reqAllocatedLast;
		
		public CacheLogSegment()
		{
			//cacheMap = new HashMap<String, CacheObject>(); 
			cacheMap = new ConcurrentHashMap<String, CacheObject>();
		}
		/*
		public boolean containsObj(String s)
		{
			
			for (CacheObject value : this.cacheMap.values()) 
			{
				if (value.cacheKey.equals(s))
				{
					return true;
				}
				
			}	
			return false;
		}
	*/
		
		

		@SuppressWarnings("null")
		public CacheLogSegment cloneCLS() {
			try
			{
				
				CacheLogSegment cls = new CacheLogSegment();
				if (this.cacheMap!=null)
				{
					
					ConcurrentHashMap<String, CacheObject> newHM = new ConcurrentHashMap<String, CacheObject>(); ;
					
					Iterator ir=this.cacheMap.entrySet().iterator();
					while (ir.hasNext())
					{	 Map.Entry pairs= (Entry) ir.next();
						 CacheObject co=(CacheObject) pairs.getValue();
						 CacheObject coc=co.cloneCO();
						 newHM.put((String) pairs.getKey(), coc);	
					}
				cls.cacheMap=newHM;
			return cls;
				}
				else
				{
					System.out.print("CacheLogSegment is null");
					return null;
				}
			}
			catch(Exception e){System.out.print("CacheLogSegment ERROR" + e.toString()); }
			return null;
			}
		
		
		
	}
	
	public static ArrayList<CacheLogSegment> cloneList(List<CacheLogSegment> list) {
		ArrayList<CacheLogSegment> clone = new ArrayList<CacheLogSegment>(list.size());
	    for(CacheLogSegment item: list) 
	    	{
	    	clone.add((CacheLogSegment) item.cloneCLS());
	    	}
	    return clone;
	}
	
	@SuppressWarnings("null")
	public CacheLogList clone() {
		try
		{
			sm.acceptLogs=false;
			CacheLogList cl = new CacheLogList(sm);
			//cl=(CacheLogList) super.clone();
			if (this.cacheLogList!=null)
			{
			cl.cacheLogList=cloneList(this.cacheLogList);
		//	cl.currentLogInterval=currentLogInterval.cloneCLS();
		//	sm.acceptLogs=true;
			return cl;
			}
			else
			{
				System.out.print("cacheLogList is null");
			//	sm.acceptLogs=true;
				return null;
			}
			
		
		}
		catch(Exception e){ System.out.print("CacheLogList ERROR"+e.toString()); }
	//	sm.acceptLogs=true;
		return null;
		}
	
	
	
	
}
