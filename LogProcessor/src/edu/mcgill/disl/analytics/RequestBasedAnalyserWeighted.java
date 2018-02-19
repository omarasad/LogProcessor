package edu.mcgill.disl.analytics;

import java.util.*;
import java.util.Map.Entry;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.RestoreAction;

import edu.mcgill.disl.analytics.CacheLogList.CacheLogSegment;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;
import edu.mcgill.disl.log.processor.RequestResourceMappingLogProcessor;

public class RequestBasedAnalyserWeighted extends Analyser {
	
	public static final String RBA_STRATEGY_CLASS = "rba.strategy.class";
	public static final String RBA_STRATEGY_CONTINUELB = "rba.strategy.lbURLFreq";
	
	RequestBasedAnalyserWeightedStrategy strategy = null;
	
	//all variables that could be needed by strategy should be made public here or have public methods
	public int cacheMemorySize;	
	public int cacheSz;
	public int numServers;
	public int totalCapacity;
	public int totalMemoryCapacity;
	public int servCap=0;
	public int totalWeight=0;
	public int variation=0;
	
	public int lbURLFreq = 0;
	HashMap<Integer, ASServer> servIndex = new HashMap<Integer,ASServer>();
	
	public RequestToResourcesMap reqToResAll=null;
	public ResourceToRequestMap resToReqAll=null;
	public HashMap<String,HttpRequestObject> mergedHttpMapAll = null;
	public List<HttpRequestObject> httpListAll;
	public HashMap<ASServer, CacheLogList> servCacheLogIndex = new HashMap<ASServer, CacheLogList>();
	public HashMap<ASServer, HttpLogList> servHttpLogIndex = new HashMap<ASServer, HttpLogList>();
	public HashMap<ASServer, RequestToResourcesMap> servReqtoResIndex = new HashMap<ASServer, RequestToResourcesMap>();
	public static HashMap<HttpRequestObject, List<HttpRequestObject>> sharedReq = new HashMap<HttpRequestObject, List<HttpRequestObject>>();
	public HashMap<ASServer, ResourceToRequestMap> servRestoReqIndex = new HashMap<ASServer, ResourceToRequestMap>();
	public static HashMap<String, HashSet<String>> sharedReqNew = new HashMap<String, HashSet<String>>();


	
	
	//should ideally be kept in a load balancer class that contains lb specific properties and policy
	
	public LoadBalancerPolicy currentLBPolicy = null;
	//public LoadBalancerPolicy oldLB = null;
	
	public int getObjectKey(String s)
	{
		
		int start=s.indexOf("#");
		String r=(String) s.subSequence(start+1, s.length());
		return Integer.parseInt(r);
		
		
	}
	public boolean isShareqReq(HttpRequestObject req1, HttpRequestObject req2)
	{
		HashSet<String> objects = reqToResAll.map.get(req1.url);
		HashSet<String> objects2 = reqToResAll.map.get(req2.url);
		
		if(objects == null || objects2==null)
			return false;
		for(String obj : objects)
		{ 
			for(String obj2 : objects2)
			{ 
			if (obj.equals(obj2))
				return true;
			}
		}
		
		return false;
	}
	
	public void findSharedReq()
	{
		for (int i=0; i<httpListAll.size(); i++)
		{
			HttpRequestObject curReq =(HttpRequestObject) httpListAll.get(i);
			//HttpRequestObject curReq = (HttpRequestObject) ir.next();
			//System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
			//System.out.println("sharedReq.size() "+sharedReq.size());
		//	System.out.println("curReq1111 "+curReq.url);
			for (int j=0; j<httpListAll.size(); j++)
			{
				HttpRequestObject curReq2 =(HttpRequestObject) httpListAll.get(j);
			//	System.out.println("curReq2222 "+curReq2.url);
				if (!curReq.equals(curReq2))
				{
					if (isShareqReq(curReq, curReq2))
					{
						List<HttpRequestObject> k;
						k=sharedReq.get(curReq);
						if (k==null)
						{
							k=new ArrayList<HttpRequestObject>();
							
						}
						k.add(curReq2);
						sharedReq.put(curReq, k);
					//	System.out.println("shared");
					//	System.out.println("sharedReqwwww.size() "+sharedReq.size());
					}
					
				}
			}
			
			
		}
	}
	
	
	@SuppressWarnings("unchecked")
	public void findSharedReqObj()
	{
		//HashSet<String> requests;
		
		
		 
		
		
		for(Entry<String, HashSet<String>> entry : resToReqAll.map.entrySet())
		{
			HashSet<String> requests = null;
			requests=resToReqAll.map.get(entry.getKey());
			 
			if (requests !=null)
			{
				if(requests.size()>1)
				{
					Iterator it = requests.iterator();
					
					while (it.hasNext())
					{
						String req=(String) it.next();
					
						HashSet<String> subRequests=null;
						subRequests=sharedReqNew.get(req);
						
						if(subRequests == null)
						{
							subRequests = (HashSet<String>) requests.clone();
							
							sharedReqNew.put(req,  subRequests);
							 
						}
						
						subRequests.addAll(requests);
						
						
						
					}
				}
			}
		}	
	}
	
	
	public void printResToReq()
	{
		
		for(Entry<String, HashSet<String>> entry : resToReqAll.map.entrySet())
		{
			Analyser.log.info(" *********************************** "); 
			Analyser.log.info("Object Key "+  entry.getKey()); 
			Analyser.log.info("URL Values:"); 
			HashSet<String> requests = null;
			requests=resToReqAll.map.get(entry.getKey());
			if (requests!=null)
			{
			Iterator it = requests.iterator();
			while (it.hasNext())
			{
				String s=(String) it.next();
				Analyser.log.info("Req " +s);  
			}
			}
		}
	}
	
	public void printSharedNew()
	{
		
		for(Entry<String, HashSet<String>> entry : sharedReqNew.entrySet())
		{
			Analyser.log.info(" *********************************** "); 
			Analyser.log.info("URL Key "+  entry.getKey()); 
			Analyser.log.info("URL Values"); 
			HashSet<String> requests = sharedReqNew.get(entry.getKey());
			if (requests!=null)
			{
			Iterator<String> it = requests.iterator();
			while (it.hasNext())
			{
				Analyser.log.info((String) it.next());  
			}
			}
		}
	}
	
	public void findSharedReqBit()
	{
		for (int i=0; i<httpListAll.size(); i++)
		{
			HttpRequestObject curReq =(HttpRequestObject) httpListAll.get(i);
			BitSet b1= curReq.items;
			
			
			
			for (int j=i+1; j<httpListAll.size(); j++)
			{
				//b11=(BitSet) b1.clone();
				HttpRequestObject curReq2 =(HttpRequestObject) httpListAll.get(j);
			//	System.out.println("curReq2222 "+curReq2.url);
				//b1.intersects(b2)
				BitSet b2= curReq2.items;
			//	b11.and(b2);
				if (b1.intersects(b2))
				{
					List<HttpRequestObject> k;
					k=sharedReq.get(curReq);
					if (k==null)
					{
						k=new ArrayList<HttpRequestObject>();
						
					}
					k.add(curReq2);
					sharedReq.put(curReq, k);
				}
				
			}
			
			
		}
	}
	
	public HttpRequestObject reqPrev(String url)
	{
		Iterator it=sharedReq.entrySet().iterator();
		//Analyser.log.info("$$$$$$$$$$$$$$$ ");  
	//	Analyser.log.info("URL "+url);  
		//Analyser.log.info("sharedReq.size()"+sharedReq.size());  
		while (it.hasNext())
		{
			
			Map.Entry pairs = (Map.Entry)it.next();
			HttpRequestObject ro1=(HttpRequestObject)pairs.getKey();
		//	Analyser.log.info("ro1.url"+ro1.url); 
		//	Analyser.log.info("ro1.url.equals(url)"+ro1.url.equals(url)); 
			if(ro1.url.equals(url))
				return ro1;
		}
		
		return null;
	}
	
	/*
	public boolean hasPrevShared(String url)
	{
		boolean res=true;
		Iterator it=sharedReq.entrySet().iterator();
		
	        
		while (it.hasNext())
		{
			
			Map.Entry pairs = (Map.Entry)it.next();
			HttpRequestObject ro1=(HttpRequestObject)pairs.getKey();
			Analyser.log.info("ro1.url"+ro1.url); 
			Analyser.log.info("ro1.url.equals(url)"+ro1.url.equals(url)); 
			if(ro1.url.equals(url));
				res=true;
		}
		
		return res;
	}
	*/
	public void updateSharedReq(HttpRequestObject newReq)
	{
		
			//HttpRequestObject curReq = (HttpRequestObject) ir.next();
			//System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
			//System.out.println("sharedReq.size() "+sharedReq.size());
		//	System.out.println("curReq1111 "+curReq.url);
			for (int j=0; j<httpListAll.size(); j++)
			{
				HttpRequestObject curReq2 =(HttpRequestObject) httpListAll.get(j);
			//	System.out.println("curReq2222 "+curReq2.url);
				
				if (!curReq2.equals(reqPrev(newReq.url)))
				{
					if (isShareqReq(newReq, curReq2))
					{
						List<HttpRequestObject> k;
						k=sharedReq.get(newReq);
						if (k==null)
						{
							k=new ArrayList<HttpRequestObject>();
							
						}
						k.add(curReq2);
						sharedReq.put(newReq, k);
						
						List<HttpRequestObject> curReq2List;
						curReq2List=sharedReq.get(newReq);
						if (curReq2List==null)
						{
							curReq2List=new ArrayList<HttpRequestObject>();
							
						}
						curReq2List.add(newReq);
						sharedReq.put(curReq2, curReq2List);
					//	System.out.println("shared");
					//	System.out.println("sharedReqwwww.size() "+sharedReq.size());
					}
					
				}
			}
			
			
		
	}
	public RequestBasedAnalyserWeighted(StatisticsManager manager) {
		super(manager);
		
		setupStrategy();
		
	}
	
	//make it read a strategy from configuration
	
	/*
	public static void printMap() {
		System.out.println("start1");
	    Iterator it = sharedReq.entrySet().iterator();
	    System.out.println("start2");
	    while (it.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it.next();
	        System.out.println(pairs.getKey() + " %%%=%%% " + pairs.getValue());
	       // it.remove(); // avoids a ConcurrentModificationException
	    }
	}
	*/
	public void setupStrategy(){
		
		try{
		
			System.out.println("enter here");
		Class c = Class.forName(manager.props.getProperty(RBA_STRATEGY_CLASS), false, this.getClass()
				.getClassLoader());
		
		System.out.println("RBA_STRATEGY_CLASS\n"+manager.props.getProperty(RBA_STRATEGY_CLASS));
		
			
			
		strategy = (RequestBasedAnalyserWeightedStrategy) c.getConstructor(new Class[] { RequestBasedAnalyserWeighted.class }).newInstance(this);
		
		}catch(Exception ex){
			System.out.println("RBA_STRATEGY_CLASS\n"+manager.props.getProperty(RBA_STRATEGY_CLASS));
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Called by StatisticsManager periodically. Should create policy files for
	 * AS and LB.
	 *  
	 */
	
	@Override
	public void analyse()throws Exception {
		
		System.out.print("hiiiiiiiiiiiiiii");
		Analyser.log.info("Start Analyser");
		
		//GIVEN DATA
		
		/*
		 * 1. Individual Cache Capacity  
		 * 2. Derived: Total cache capacity
		 * 
		 * 
		 */
		
		long l1=System.currentTimeMillis();
		
		cacheSz = Integer.parseInt(manager.props.getProperty(StatisticsManager.AS_CACHE_SIZE));
		
		numServers = manager.getASServers().size();
		
		totalCapacity = cacheSz * numServers;
		
		lbURLFreq = Integer.parseInt(manager.props.getProperty(RBA_STRATEGY_CONTINUELB));
		
		// From Strucutres
		
		/*
		 * get req->objs map for each server and merge them. 
		 * 
		 * get req frequency list
		 * 
		 * sort req frequency
		 * 
		 * 
		 * 
		 */
		
		for(ASServer serv:manager.getASServers().values())
		{
			
			
			CacheLogList cl= (CacheLogList) serv.getStruct("cacheLogGroup");
			System.out.print("cl.cacheLogList.size() before= "+cl.cacheLogList.size());
			servCacheLogIndex.put(serv, (CacheLogList) cl.clone());	
			System.out.print("cl.cacheLogList.size() after= "+cl.cacheLogList.size());
			
			RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(
					RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
			servReqtoResIndex.put(serv, map.cloneReqResMap());
			
			ResourceToRequestMap map2 =	(ResourceToRequestMap)serv.getStruct(
					RequestResourceMappingLogProcessor.OBJ_REQ_MAP);
			
			servRestoReqIndex.put(serv, map2.cloneResReqMap());
			
			HttpLogList hll = (HttpLogList)serv.getStruct(HttpRequestProcessor.REQ_LOG_GROUP);
			servHttpLogIndex.put(serv, hll.cloneHLL());
			
			
			
			

		}
		 
		reqToResAll = mergeAllReqToRes();
		resToReqAll = mergeAllResToReq();
		
		System.out.println("Omar====reqToResAll.map.size()===================\n"+reqToResAll.map.size());
		
		mergedHttpMapAll = mergeHttpRequestObjectsFromServers();
		
		System.out.println("Omar======mergedHttpMapAll.size()===========\n"+ mergedHttpMapAll.size());
		
		
		
		
		
		//get HttpRequestObject list order by counter desc
		
		//System.out.println("Omar======httpListAll.size()==========\n" +httpListAll .size());
		
		
		
		int mergesize=mergedHttpMapAll.size();
		
		long sharedListBefore0=System.currentTimeMillis();
		
		/*
		if (mergesize>0)
		{
			Iterator it = mergedHttpMapAll.entrySet().iterator();
			//Iterator it =httpListAll.iterator();
		while (it.hasNext())
		{
			 Map.Entry pairs = (Map.Entry)it.next();
			 
			 HttpRequestObject ro=(HttpRequestObject) pairs.getValue();
			HashSet<String> objects = reqToResAll.map.get(ro.url);
			if (objects==null)
			{
				continue;
			}
			else
			{
				//if (objects.size() >25)
				//	ro.weight=ro.counter * 2;
				//else 
					ro.weight=ro.counter * objects.size();
					Iterator ob=objects.iterator();
					String s;
					while (ob.hasNext())
					{
						s=(String) ob.next();
						ro.items.set(getObjectKey(s));
					}
					
			//ro.weight=ro.counter * objects.size();
			}
		}
		}
		
		*/
		long sharedListAfter0=System.currentTimeMillis();
		long constructingBitSet=sharedListAfter0-sharedListBefore0;
		
		Analyser.log.info(" constructingBitSet=" + constructingBitSet);
		
		httpListAll = HttpLogList.getHttpRequestObjectListWeighted(mergedHttpMapAll, true);
		
		
		servCap=0;
		totalWeight=0;
		variation=0;
		
		if (mergesize>0)
		{
			Iterator itr = httpListAll.iterator();
			//Iterator it =httpListAll.iterator();
		while (itr.hasNext())
		{
			
			 HttpRequestObject hr= (HttpRequestObject) itr.next();
			 hr.assignedbefore=-1;
			
			 
			
			HashSet<String> objects = reqToResAll.map.get(hr.url);
			if (objects==null)
			{
				continue;
			}
			else
			{
		//	Analyser.log.info(" curReq.url" + hr.url);
			totalWeight+=hr.weight;
			}
		}
		}
		servCap=totalWeight/numServers;
		//variation=servCap/40;
		
		//variation=httpListAll.get(0).weight; //does not work
		variation=0;
		servCap=servCap+0;
		
		
		/*
		int reqq=0;
		int totalWeightWhileFFF=0;
		while (reqq< httpListAll.size())
		{
			
			Analyser.log.info(" reqq" + reqq);
			HttpRequestObject curReq = httpListAll.get(reqq);
				
			Analyser.log.info(" curReq.url" + curReq.url);
		//	Analyser.log.info("curReq.counter" + curReq.counter);
			Analyser.log.info("curReq.weight" + curReq.weight);
			totalWeightWhileFFF+=curReq.weight;
			Analyser.log.info("totalWeightWhileFFF " + totalWeightWhileFFF);
			reqq++;
		}
		*/
		System.out.println("Omar======totalWeight==========\n" +totalWeight);
		System.out.println("Omar======servCap==========\n" +servCap);
		
		
		
		Analyser.log.info("sharedReq.size()"+sharedReq.size());
		
		
		  Iterator it1 = sharedReq.entrySet().iterator();
		  //  System.out.println("start2");
		    
		    
		  //  System.out.println("sharedReq22.size() "+sharedReq.size());
		 //   System.out.println("start1");
		//    Iterator it2 = sharedReq.entrySet().iterator();
		//    System.out.println("start2");
		   // List l= new ArrayList<HttpRequestObject>();
		    
		    
		    /*
		    while(it1.hasNext())
		    {
		    	
		        Map.Entry pairs = (Map.Entry)it1.next();
		        HttpRequestObject ro1=(HttpRequestObject)pairs.getKey();
		        
		      //  l=(List)pairs.getValue();
		        
		       // System.out.println( ro1.url +" %%%=%%% ");
		       // it.remove(); // avoids a ConcurrentModificationException
		        
		    }
			*/
		    
		    //sharedReq.clear();
		   //  printResToReq();
		  	Analyser.log.info("resToReqAll.map.size()= " +resToReqAll.map.size());
		    long sharedListBefore=System.currentTimeMillis();
		   // findSharedReqBit();
		    findSharedReqObj();
		    long sharedListAfter=System.currentTimeMillis();
		    
		   // Analyser.log.info(" constructingBitSet=" + constructingBitSet);
		    long overlaptime=sharedListAfter - sharedListBefore;
		    Analyser.log.info("overlap constructing time "+ overlaptime);
		    Analyser.log.info(" sharedReqNew.size()=" + sharedReqNew.size());
		    
		   // Analyser.log.info("??????????????????????????printSharedNew()????????????????????????????" );
		    
		   // printSharedNew();
		   
		    
		    /*
		if (sharedReq.isEmpty())
			findSharedReq();
		else
		{
			for (int j=0; j<httpListAll.size(); j++)
			{
				HttpRequestObject curReq =(HttpRequestObject) httpListAll.get(j);
				//Analyser.log.info("Update Size"+sharedReq.get(curReq).size());
			//	Analyser.log.info("Update Size"+sharedReq.get(curReq).size());
				
			//	if (sharedReq.get(curReq) == null);
				if (!servIndex.get(0).oldLB.policyMap.containsKey(curReq.url))
				//if ((reqPrev(curReq.url)==null))
				{
			//	Analyser.log.info("Update"+curReq.url);
					updateSharedReq(curReq);
				}
			}		
					
		}
			
		*/
	    
	  //  System.out.println("sharedReq22.size() "+sharedReq.size());
	    
	    while (it1.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it1.next();
	        HttpRequestObject ro1=(HttpRequestObject)pairs.getKey();
	        List<HttpRequestObject> k=(List<HttpRequestObject>) pairs.getValue();
	        Analyser.log.info("Overlap %%%=%%% " + ro1.url);
	      //  Analyser.log.info(ro1.url+"" +ro2.url);
	        Iterator ir=k.iterator();
	        while (ir.hasNext())
	        {
	        	HttpRequestObject hr=(HttpRequestObject) ir.next();
	        	Analyser.log.info(hr.url);	
	        }
	        
	       // it.remove(); // avoids a ConcurrentModificationException
	        
	    }
		
		
		//Iterator irr= mergedHttpMapAll.iterator();
		
		
		
		
		//httpListAll.get(index)
		//Iterator itr = httpListAll.iterator();

		//System.out.println("Omar======itr.size==========\n" + httpListAll.size());
//		while(itr.hasNext())
//		{
//			System.out.println("Omar======itr.next().toString()==========\n" + itr.next().toString());
//		}
//		
		//call the strategy to generate policy 
		
		
	    Analyser.log.info("Omar======before==========:");
	    long l0=System.currentTimeMillis();
		boolean success = strategy.generatePolicies();
		
		long l2=System.currentTimeMillis();
		
		long consumedTime=l2-l1;
		
		Analyser.log.info("total time =" + consumedTime);
		
		
		System.out.println("Omar======Generated==========:" + success);
		Analyser.log.info("Omar======Generated==========:" + success);
		
		
		
		Analyser.log.info("BitSet Time:" + (sharedListAfter0 - sharedListBefore0));
		Analyser.log.info("Req Size:" + httpListAll.size());
		Analyser.log.info("Shared Time:" + (sharedListAfter - sharedListBefore));
		Analyser.log.info("Process Time:" + (l2-l0));
		Analyser.log.info("Total Time:" + consumedTime);
		
		if(success)
		{
			// send all AS policies
			
			String policyFor = manager.props.getProperty(StatisticsManager.POLICY_FOR);
			if(policyFor == null || policyFor.equals(""))
				policyFor = "both";
			
			if(policyFor.equalsIgnoreCase("as") || policyFor.equalsIgnoreCase("both"))
			{		
				for(ASServer serv : manager.getASServers().values())
				{
					//TODO update the port logic in base class
					sendASPolicy(serv.currentPolicy, serv.getServerId());
				//	System.out.println(serv.currentPolicy);
					
				//	Analyser.log.info("serv.getServerId()" + serv.getServerId());
				//	Analyser.log.info("serv.currentPolicy" + serv.currentPolicy);
					
				}
			}
			
			//send lb policy
		//	System.out.println("Omar======currentLBPolicy:" + currentLBPolicy);
			//System.out.println("Omar======LBSERVER:" +  manager.props.getProperty(StatisticsManager.LBSERVER));
			
			
			if(policyFor.equalsIgnoreCase("lb") || policyFor.equalsIgnoreCase("both")){
				sendLBPolicy(currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
			}
		}
	}
	
	/**
	 * this method will first do segment merge.. and the all server merge
	 */
	private HashMap<String, HttpRequestObject> mergeHttpRequestObjectsFromServers(){
		List<HashMap<String,HttpRequestObject>> list = new ArrayList<HashMap<String,HttpRequestObject>>();
		
		for(ASServer serv:manager.getASServers().values()){
			//HttpLogList hll = (HttpLogList)serv.getStruct(HttpRequestProcessor.REQ_LOG_GROUP);
			HttpLogList hll = servHttpLogIndex.get(serv);
			
			HashMap<String,HttpRequestObject> aggMap = hll.aggregateSegments();
			list.add(aggMap);	
		//	System.out.println("Omar======serv.serverId:" +serv.serverId );
		//	System.out.println("Omar======hll.httpLogList.size():" +hll.httpLogList.size());
		//	System.out.println("Omar======aggMap.size():" +aggMap.size());
		}
		
		return HttpLogList.mergeHttpRequestObjects(list);
		
	}
	
	
	
	private RequestToResourcesMap mergeAllReqToRes(){
		
		RequestToResourcesMap mapAll = new RequestToResourcesMap(manager);
		
		for(ASServer serv: manager.getASServers().values()){
		//	RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
			
			RequestToResourcesMap map =	servReqtoResIndex.get(serv);
			
			
			System.out.println("Omar======"+serv.serverId+" size= " +map.map.size());
			
			
			for(Entry<String, HashSet<String>> entry : map.map.entrySet()){
				
				HashSet<String> set = mapAll.map.get(entry.getKey());
				if(set == null){
					set = new HashSet<String>();
					mapAll.map.put(entry.getKey(), set);
				}
				set.addAll(entry.getValue());
			}
		}
		
		return mapAll;
	}
	
	
// new	
private ResourceToRequestMap mergeAllResToReq(){
		
	ResourceToRequestMap mapAll = new ResourceToRequestMap(manager);
		
		for(ASServer serv: manager.getASServers().values()){
		//	RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
			
			ResourceToRequestMap map =	servRestoReqIndex.get(serv);
			
			
			System.out.println("Omar======"+serv.serverId+" size= " +map.map.size());
			
			
			for(Entry<String, HashSet<String>> entry : map.map.entrySet()){
				
				HashSet<String> set = mapAll.map.get(entry.getKey());
				if(set == null){
					set = new HashSet<String>();
					mapAll.map.put(entry.getKey(), set);
				}
				set.addAll(entry.getValue());
			}
		}
		
		return mapAll;
	}
	
	
	public int getUniqueResourceCount(Collection<ASServer> asCon){
		
		HashSet<String> resources = new HashSet<String>();

		for(ASServer AStemp : asCon){
		//	CacheLogList cl = (CacheLogList) AStemp.getStruct("cacheLogGroup");
			CacheLogList cl =  servCacheLogIndex.get(AStemp);
			
			for(CacheLogSegment seg : cl.cacheLogList){
				for(String res : seg.cacheMap.keySet()){
					resources.add(res);
				}
			}
		}
		return resources.size();
	}
}
