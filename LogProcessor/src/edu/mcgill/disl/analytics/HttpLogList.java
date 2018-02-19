package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import edu.mcgill.disl.analytics.CacheLogList.CacheLogSegment;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.analytics.HttpRequestObject;

public class HttpLogList {
	
	ArrayList<HttpLogSegment> httpLogList;
	public HttpLogSegment currentLogInterval;
	long segBegTimeStamp = -1;
	long segTimeInterval;// = 5 * 60 * 1000;
	int NUMBER_INTERVALS;// = 50;
	StatisticsManager smgr = null;
	
	

	public HttpLogList(StatisticsManager sm) {
		httpLogList = new ArrayList<HttpLogSegment>();
		smgr=sm;
		
		segTimeInterval = Long.parseLong(sm.props.getProperty(StatisticsManager.SEGMENT_INTERVAL));
		NUMBER_INTERVALS = Integer.parseInt(sm.props.getProperty(StatisticsManager.NO_INTERVALS));
		
		// addNewCacheInterval();
	}

	public void addNewHttpInterval() {
		if(currentLogInterval!=null){
			Analyser.log.info("AVGTIMENEW\t" + currentLogInterval.avgResp + "\tREQS\t" + currentLogInterval.reqCount);
			
		}
		
		System.out.println("addNewHttpInterval ");
		currentLogInterval = new HttpLogSegment();
		httpLogList.add(currentLogInterval);
		if(httpLogList.size()>NUMBER_INTERVALS){   // by Omar
			httpLogList.remove(0);
			System.out.println("httpLogList.remove(0) ");
		}
		segBegTimeStamp = System.currentTimeMillis();
	}

	
	public void addtoHttpLogInterval(HttpRequestObject hr, long timetaken) 
	{
		if (segBegTimeStamp == -1
				|| (System.currentTimeMillis() - segBegTimeStamp > segTimeInterval))
		{
			Analyser.log.info("Success_count\t" + hr.success_count + "\tFailure_Count\t" + hr.failure_count);
			addNewHttpInterval();
		}

		HttpRequestObject ht;
		
		if ((ht =smgr.globalRequestMap.get(hr.url)) != null)
		//if ((ht = currentLogInterval.reqMap.get(hr.url)) != null) 
		{
			
			////////// NEW DS ////////////
		//	ht.accessTimes.add(hr.first_date_time); // by Omar
			ht.last_date_time = hr.first_date_time;
			ht.resp_code = hr.resp_code;
			if (hr.resp_code == 200 || hr.resp_code == 304) {
				ht.success_count++;
			} else {
				ht.failure_count++;
			}
			ht.counter++;
			ht.execution_time = ((ht.execution_time * (ht.counter - 1)) + timetaken)/ ht.counter;			
			
		} 
		else 
		{
			// NEW DS ////////////
			//hr.accessTimes.add(hr.first_date_time);// by Omar 
			smgr.globalRequestMap.put(hr.url, hr);
			
		}
		//hr.accessTimes.add(hr.first_date_time);
	}
	
	public void addtoHttpLogIntervalOri(HttpRequestObject hr, long timetaken) {
		if (segBegTimeStamp == -1
				|| (System.currentTimeMillis() - segBegTimeStamp > segTimeInterval)) {
			Analyser.log.info("Success_count\t" + hr.success_count + "\tFailure_Count\t" + hr.failure_count);
			addNewHttpInterval();
		}

		HttpRequestObject ht;
		

		if ((ht = currentLogInterval.reqMap.get(hr.url)) != null) 
		{
			ht.accessTimes.add(hr.first_date_time); // by Omar
			ht.last_date_time = hr.first_date_time;
			ht.resp_code = hr.resp_code;
			if (hr.resp_code == 200 || hr.resp_code == 304) {
				ht.success_count++;
			} else {
				ht.failure_count++;
			}
			ht.counter++;
			ht.execution_time = ((ht.execution_time * (ht.counter - 1)) + timetaken)/ ht.counter;
			
			currentLogInterval.success_acs++;
			//for avg total response time in a seg
			currentLogInterval.updateRespTime(timetaken);
		//	System.out.println("ht.accessTimes.size() "+ht.accessTimes.size());
		//	System.out.println("ht.counter "+ht.counter);
			
			
		} 
		else 
		{
			
			hr.accessTimes.add(hr.first_date_time);// by Omar 
			currentLogInterval.reqMap.put(hr.url, hr);
		//	System.out.println("hr.accessTimes.size() "+hr.accessTimes.size());
		//	System.out.println("hr.counter "+hr.counter);
			currentLogInterval.failure_acs++;
		}
		//hr.accessTimes.add(hr.first_date_time);
	}

	public class HttpLogSegment {
		HashMap<String, HttpRequestObject> reqMap;
		
		public int reqCount = 0;
		public double avgResp = 0;
		long success_acs=0; // added by Omar
		long failure_acs=0; // added by Omar
		
		public void updateRespTime(long time){
			avgResp = ((avgResp * reqCount) + time)/++reqCount;
		}

		public HttpLogSegment() {
			reqMap = new HashMap<String, HttpRequestObject>();
		}

		@SuppressWarnings("null")
		public HttpLogSegment cloneHLS() {
			try
			{
				HttpLogSegment hls = new HttpLogSegment();
				System.out.print("this.reqMap!=null" +this.reqMap!=null);
				if (this.reqMap!=null)
				{
					
					HashMap<String,HttpRequestObject > newHM = new HashMap<String, HttpRequestObject>(); ;
					
					Iterator ir= this.reqMap.entrySet().iterator();
					System.out.print("ir.hasNext()" +ir.hasNext());
					while (ir.hasNext())
					{	 Map.Entry pairs= (Entry) ir.next();
					HttpRequestObject co=(HttpRequestObject) pairs.getValue();
					HttpRequestObject coc=co.cloneCO();
						 newHM.put((String) pairs.getKey(), coc);	
					}
					System.out.print("after while");
					hls.reqMap=newHM;
					hls.avgResp=this.avgResp;
					hls.failure_acs=this.failure_acs;
					hls.reqCount=this.reqCount;
					hls.success_acs=this.success_acs;
			return hls;
				}
				else
				{
					System.out.print("HttpLogSegment is null");
					return null;
				}
			}
			catch(Exception e){System.out.print("HttpLogSegment ERROR" + e.toString()); }
			return null;
			}
		
		
		
	}
	

	public HashMap<String, HttpRequestObject> aggregateSegments() {
		HashMap<String, HttpRequestObject> hc = new HashMap<String, HttpRequestObject>();

		
		int no_intervals = httpLogList.size();
		System.out.println("no_intervals "+no_intervals);

		for (int i = 0; i < NUMBER_INTERVALS && no_intervals > 0; i++) {
		if (no_intervals > 0)
		{
		HttpLogSegment cls = httpLogList.get(no_intervals - 1);  
		System.out.println("cls.reqMap.size()= "+cls.reqMap.size());		
			Collection<HttpRequestObject> c = cls.reqMap.values();
			Iterator<HttpRequestObject> it = c.iterator();

			while (it.hasNext())
			{
				HttpRequestObject cot = it.next();
				if (!hc.containsKey(cot.url)) 
				{
					cot.assignedbefore=-1;
					hc.put(cot.url, cot);
					
				} 
				else 
				{
					HttpRequestObject cachObj = hc.get(cot.url);
					cachObj.counter +=  cot.counter;
					cachObj.assignedbefore=-1;
					//TODO update all other variables
				}
			}

			no_intervals--;
		}
		}
		return hc;
	}
	
	public HashMap<String, HttpRequestObject> getSegment(int segNo) {
		HashMap<String, HttpRequestObject> hc = new HashMap<String, HttpRequestObject>();

		
		int no_intervals = httpLogList.size();
		System.out.println("no_intervals "+no_intervals);

		
		HttpLogSegment cls = httpLogList.get(segNo);  
		System.out.println("cls.reqMap.size()= "+cls.reqMap.size());		
			Collection<HttpRequestObject> c = cls.reqMap.values();
			Iterator<HttpRequestObject> it = c.iterator();

			while (it.hasNext()) {
				HttpRequestObject cot = it.next();
				if (!hc.containsKey(cot.url)) 
				{
					cot.assignedbefore=-1;
					hc.put(cot.url, cot);
					
				} else {
					HttpRequestObject cachObj = hc.get(cot.url);
					
					if (!cachObj.objectDrift && cot.objectDrift)
						cachObj.objectDrift=true;	
					cachObj.counter +=  cot.counter;
					cachObj.assignedbefore=-1;
					//TODO update all other variables
				}
			

			no_intervals--;
		}
		return hc;
	}
	
	public static HashMap<String, HttpRequestObject> mergeHttpRequestObjects(List<HashMap<String, HttpRequestObject>> httpMaps) {
		if (httpMaps.size()==0)
			return null;
		HashMap<String, HttpRequestObject> hc = new HashMap<String, HttpRequestObject>();

		for (HashMap<String,HttpRequestObject> httpMap : httpMaps) { // loop on the different servers
			
			for(Entry<String, HttpRequestObject> entry : httpMap.entrySet()){ // loop on the different objects at each server
				HttpRequestObject obj = hc.get(entry.getKey());
				if(obj == null){
					obj = (HttpRequestObject) entry.getValue().clone();
					hc.put(entry.getKey(), obj);
				}else{
					HttpRequestObject reqObj = entry.getValue();
					//if (obj.counter !=0 && reqObj.counter !=0)
					//	obj.execution_time= ((obj.execution_time * obj.counter) + (reqObj.execution_time * reqObj.counter)) / (obj.counter+reqObj.counter);
					obj.counter += reqObj.counter;
					obj.accessTimes.addAll(reqObj.accessTimes);
					 
					
					//TODO update other variables too
				}
			}
		}

		return hc;
	}
	
	public static HashMap<String, HttpRequestObject> mergeHttpRequestObjectsLARD(List<HashMap<String, HttpRequestObject>> httpMaps) {
		HashMap<String, HttpRequestObject> hc = new HashMap<String, HttpRequestObject>();

		for (HashMap<String,HttpRequestObject> httpMap : httpMaps) { // loop on the different servers
			
			for(Entry<String, HttpRequestObject> entry : httpMap.entrySet()){ // loop on the different objects at each server
				HttpRequestObject obj = hc.get(entry.getKey());
				if(obj == null){
					obj = (HttpRequestObject) entry.getValue().clone();
					hc.put(entry.getKey(), obj);
				}else{
			
					HttpRequestObject reqObj = entry.getValue();
					if (reqObj.first_date_time<=obj.first_date_time)
						{
						System.out.println("reqObj.url" + reqObj.url);
						System.out.println("obj.candidate" + obj.candidate.serverId+ "time= "+ obj.first_date_time);
						System.out.println("reqObj.candidate" + reqObj.candidate.serverId + "time= "+ reqObj.first_date_time);
						obj.candidate=reqObj.candidate.clone();
						obj.first_date_time=reqObj.first_date_time;
						//obj=null;
						//obj=reqObj.cloneCO();
						}
					obj.counter += reqObj.counter;
					obj.accessTimes.addAll(reqObj.accessTimes);
					
					//TODO update other variables too
				}
			}
		}

		return hc;
	}
	
	
	public static List<HttpRequestObject> getHttpRequestObjectList(HashMap<String, HttpRequestObject> map,final boolean desc){
		
		List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map.values());
		
		Collections.sort(col, new Comparator<HttpRequestObject>(){

			@Override
			public int compare(HttpRequestObject o1, HttpRequestObject o2) {
				
				return desc? o2.counter - o1.counter : o1.counter - o2.counter;
			}
			
		});
		
		return col;
		
	}

	
public static List<HttpRequestObject> sortRequestList(List<HttpRequestObject> map,final boolean desc){
		
		//List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map.values());
		
		Collections.sort(map, new Comparator<HttpRequestObject>(){

			@Override
			public int compare(HttpRequestObject o1, HttpRequestObject o2) {
				
				
				return desc? (o2.weight) - o1.weight : o1.weight - o2.weight;
			}
			
		});
		
		return map;
		
	}

public static List<HttpRequestObject> getHttpRequestObjectListWeighted(HashMap<String, HttpRequestObject> map,final boolean desc){
		
		List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map.values());
		
		Collections.sort(col, new Comparator<HttpRequestObject>(){

			@Override
			public int compare(HttpRequestObject o1, HttpRequestObject o2) {
				
				
				return desc? (o2.weight) - o1.weight : o1.weight - o2.weight;
			}
			
		});
		
		return col;
		
	}
	

public static List<HttpRequestObject> getHttpRequestObjectListWeightedNew(List<HttpRequestObject> map,final boolean desc){
	
	
	List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map);
	
	Collections.sort(col, new Comparator<HttpRequestObject>(){

		@Override
		public int compare(HttpRequestObject o1, HttpRequestObject o2) {
			
			
			return desc? (o2.counter) - o1.counter : o1.counter - o2.counter;
		}
		
	});
	
	return col;
	
}

	public static ArrayList<HttpLogSegment> cloneList(ArrayList<HttpLogSegment> httpLogList2) {
		ArrayList<HttpLogSegment> clone = new ArrayList<HttpLogSegment>(httpLogList2.size());
		System.out.println("httpLogList2.size()= "+httpLogList2.size());
	    for(HttpLogSegment item: httpLogList2) 
	    	{
	    	clone.add((HttpLogSegment)item.cloneHLS());
	    	System.out.println("clone.size()="+clone.size());
	    	}
	    return clone;
	}
	
	@SuppressWarnings("null")
	public HttpLogList cloneHLL() {
		
		System.out.println("this.httpLogList111!=null "+this.httpLogList!=null);
		try
		{
			smgr.acceptLogs=false;
			HttpLogList cl = new HttpLogList(smgr);
			//cl=(CacheLogList) super.clone();
			System.out.println("this.httpLogList!=null "+this.httpLogList!=null);
			if (this.httpLogList!=null)
			{
				System.out.println("111111 ");
			cl.httpLogList=cloneList(this.httpLogList);
			System.out.println("22222 ");
		//	cl.currentLogInterval=currentLogInterval.cloneCLS();
		//	cl.currentLogInterval=this.currentLogInterval;
		//	cl.NUMBER_INTERVALS=this.NUMBER_INTERVALS;
		//	cl.segBegTimeStamp=this.segBegTimeStamp;
			smgr.acceptLogs=true;
			return cl;
			}
			else
			{
				System.out.print("HttpLogList is null");
				smgr.acceptLogs=true;
				return null;
				
				
			}
			
		
		}
		catch(Exception e){ System.out.print("HttpLogList ERROR"+e.toString()); }
		smgr.acceptLogs=true;
		return null;
		}

}
