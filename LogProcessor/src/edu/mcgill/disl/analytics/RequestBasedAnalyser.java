package edu.mcgill.disl.analytics;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.RestoreAction;

import edu.mcgill.disl.analytics.CacheLogList.CacheLogSegment;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.analytics.policy.ObjectKey;
import edu.mcgill.disl.analytics.policy.RuleList;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;
import edu.mcgill.disl.log.processor.RequestResourceMappingLogProcessor;

public class RequestBasedAnalyser extends Analyser {
	
	public static final String RBA_STRATEGY_CLASS = "rba.strategy.class";
	public static final String RBA_STRATEGY_CONTINUELB = "rba.strategy.lbURLFreq";
	
	RequestBasedAnalyserStrategy strategy = null;
	
	//all variables that could be needed by strategy should be made public here or have public methods
	public int cacheMemorySize;	
	public int cacheSz;
	public int numServers;
	public int totalCapacity;
	public int totalMemoryCapacity;
	
	public int lbURLFreq = 0;
	
	public RequestToResourcesMap reqToResAll=null;
	public HashMap<String,HttpRequestObject> mergedHttpMapAll = null;
	public List<HttpRequestObject> httpListAll;
	public HashMap<ASServer, CacheLogList> servCacheLogIndex = new HashMap<ASServer, CacheLogList>();
	public HashMap<ASServer, HttpLogList> servHttpLogIndex = new HashMap<ASServer, HttpLogList>();
	public HashMap<ASServer, RequestToResourcesMap> servReqtoResIndex = new HashMap<ASServer, RequestToResourcesMap>();
	public HashMap<HttpRequestObject, HttpRequestObject> sharedReq = new HashMap<HttpRequestObject, HttpRequestObject>();
	public List<Integer> sharedObj = new ArrayList<Integer>() ;
	int totalObj=0;
	int totalReq=0;
	int sharedReqTot=0;
	int SharedObj=0;
	
	//should ideally be kept in a load balancer class that contains lb specific properties and policy
	
	public LoadBalancerPolicy currentLBPolicy = null;
	//public LoadBalancerPolicy oldLB = null;
	
	// Analysis
	
	public int isShareqReq(HttpRequestObject req1, HttpRequestObject req2)
	{
		HashSet<String> objects = reqToResAll.map.get(req1.url);
		HashSet<String> objects2 = reqToResAll.map.get(req2.url);
		
		int sharedCount=0;
		
		
		if(objects == null || objects2==null)
			return 0;
		for(String obj : objects)
		{ 
			for(String obj2 : objects2)
			{ 
			if (obj.equals(obj2))
				sharedCount++;
				
			}
		}
		
		return sharedCount;
	}
	
	public void findSharedReq()
	{
		
		Analyser.log.info("Starting Shared Req..."); 
		Iterator ir= httpListAll.iterator();
		while (ir.hasNext())
		{
			int simpleCount=0;
			HttpRequestObject curReq = (HttpRequestObject) ir.next();
			//System.out.println("curReq1111 "+curReq.url);
			Iterator ir2=httpListAll.iterator();
			while (ir2.hasNext())
			{
				int sharedObjCount=0;
				HttpRequestObject curReq2 = (HttpRequestObject) ir2.next();
			//	System.out.println("curReq2222 "+curReq2.url);
				if (!curReq.equals(curReq2) && (!curReq.url.contains("BrowseCategories"))  && (!curReq2.url.contains("BrowseCategories") && (!curReq2.url.contains("browseRegions"))))
				{	 
					
					if (simpleCount ==0)
				   {
						HashSet<String> objects = reqToResAll.map.get(curReq.url);
						if(objects != null)
							totalObj+=objects.size();
							totalReq++;
			    	}
					simpleCount=1;
						
					sharedObjCount=isShareqReq(curReq, curReq2);
					
					if (sharedObjCount>0)
					{
						sharedReq.put(curReq, curReq2);
						sharedObj.add(sharedObjCount);
						Analyser.log.info(curReq.url +" SH "+ curReq2.url); 
						Analyser.log.info(" sharedObjCount "+ sharedObjCount);
						sharedReqTot++;
						SharedObj+=sharedObjCount;
					//	System.out.println("shared");
					}
					
				}
			}
			
			
		}
		Analyser.log.info(" totalReq *** totalReq "+ totalReq);
		Analyser.log.info(" sharedReqTot *** sharedReqTot "+ sharedReqTot);
		Analyser.log.info(" totalObj *** totalObj "+ totalObj);
		Analyser.log.info(" SharedObj *** SharedObj "+ SharedObj);
		Analyser.log.info(" sharedReq.size() *** sharedReq.size() "+ sharedReq.size());
	}
	
	
	public RequestBasedAnalyser(StatisticsManager manager) {
		super(manager);
		
		setupStrategy();
		
	}
	
	//make it read a strategy from configuration
	
	
	public void setupStrategy(){
		
		try{
		
		Class c = Class.forName(manager.props.getProperty(RBA_STRATEGY_CLASS), false, this.getClass()
				.getClassLoader());
		
		System.out.println("RBA_STRATEGY_CLASS\n"+manager.props.getProperty(RBA_STRATEGY_CLASS));
		strategy = (RequestBasedAnalyserStrategy) c.getConstructor(new Class[] { RequestBasedAnalyser.class }).newInstance(this);
		
		}catch(Exception ex){
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
		
		long l1=System.currentTimeMillis();
		
		//GIVEN DATA
		
		/*
		 * 1. Individual Cache Capacity  
		 * 2. Derived: Total cache capacity
		 * 
		 * 
		 */
		
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
			
			HttpLogList hll = (HttpLogList)serv.getStruct(HttpRequestProcessor.REQ_LOG_GROUP);
			servHttpLogIndex.put(serv, hll.cloneHLL());
			
			
			
			

		}
		 
		reqToResAll = mergeAllReqToRes();
		
		System.out.println("OmarNEWW====reqToResAll.map.size()===================\n"+reqToResAll.map.size());
		
		mergedHttpMapAll = mergeHttpRequestObjectsFromServers();
		
		System.out.println("OmarNEWW======mergedHttpMapAll.size()===========\n"+ mergedHttpMapAll.size());
		
		
		//get HttpRequestObject list order by counter desc
		httpListAll = HttpLogList.getHttpRequestObjectList(mergedHttpMapAll, true);
		System.out.println("OmarNEWW======httpListAll.size()==========\n" +httpListAll .size());
		
		   
		
		
		//httpListAll.get(index)
		//Iterator itr = httpListAll.iterator();

		//System.out.println("Omar======itr.size==========\n" + httpListAll.size());
//		while(itr.hasNext())
//		{
//			System.out.println("Omar======itr.next().toString()==========\n" + itr.next().toString());
//		}
//		
		//call the strategy to generate policy 
		
		
		// Analyser.log.info("STARTTTTT11111");
		//findSharedReq();
	//	Analyser.log.info("STARTTTTT2222");
		//System.out.println("start1");
	   // Iterator it1 = sharedReq.entrySet().iterator();
	   // System.out.println("start2");
	    
	    /*
	    while (it1.hasNext()) {
	        Map.Entry pairs = (Map.Entry)it1.next();
	        HttpRequestObject ro1=(HttpRequestObject)pairs.getKey();
	        HttpRequestObject ro2=(HttpRequestObject)pairs.getValue();
	        
	   //     System.out.println(ro1.url + " %%%=%%% " + ro2.url);
	       // it.remove(); // avoids a ConcurrentModificationException
	    }
		
		
		*/
		boolean success = strategy.generatePolicies();
		
		long l2=System.currentTimeMillis();
		

		long consumedTime=l2-l1;
		
		Analyser.log.info("total time =" + consumedTime);
		
		System.out.println("Omar======Generated==========:" + success);
		Analyser.log.info("Omar======Generated==========:" + success);
		
		if(success){
			// send all AS policies
			
			String policyFor = manager.props.getProperty(StatisticsManager.POLICY_FOR);
			if(policyFor == null || policyFor.equals(""))
				policyFor = "both";
			Map <ObjectKey,RuleList> mp = new HashMap<ObjectKey,RuleList>() ;
			if(policyFor.equalsIgnoreCase("as") || policyFor.equalsIgnoreCase("both")){
				ObjectKey k=new ObjectKey();
				RuleList rl= new RuleList();
				mp.put(k, rl);
				for(ASServer serv : manager.getASServers().values()){
					//TODO update the port logic in base class
					Analyser.log.info("serv.currentPolicy.policyMap!=null" + serv.currentPolicy.policyMap!=null);
					
					serv.currentPolicy.policyMap=mp;
					if (serv.currentPolicy.policyMap!=null)
					{
					sendASPolicy(serv.currentPolicy, serv.getServerId());
					//System.out.println(serv.currentPolicy);
					
					Analyser.log.info("serv.getServerId()" + serv.getServerId());
					Analyser.log.info("serv.currentPolicy" + serv.currentPolicy);
					}
					
				}
			}
			
			//send lb policy
			System.out.println("Omar======currentLBPolicy:" + currentLBPolicy);
			System.out.println("Omar======LBSERVER:" +  manager.props.getProperty(StatisticsManager.LBSERVER));
			
			
			if(policyFor.equalsIgnoreCase("lb") || policyFor.equalsIgnoreCase("both")){
				sendLBPolicy(currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
			}
		}
	}
	
	/**
	 * this method will first do segment merge.. and the all server merge
	 */
	private HashMap<String, HttpRequestObject> mergeHttpRequestObjectsFromServers(){
		/*
		 String filename1="";
		    filename1="/home/db/dislcluster/loadbalancer/rubisEJB3_client_jdbc/stats/item1.dat";
		    String filename11="";
		    filename11="/home/db/dislcluster/loadbalancer/rubisEJB3_client_jdbc/stats/item11.dat";
		    String filename111="";
		    filename111="/home/db/dislcluster/loadbalancer/rubisEJB3_client_jdbc/stats/item111.dat";
		    String filename0="";
		    filename0="/home/db/dislcluster/loadbalancer/rubisEJB3_client_jdbc/stats/item0.dat";
		    String filename2="";
		    filename2="/home/db/dislcluster/loadbalancer/rubisEJB3_client_jdbc/stats/user.dat";
		    String filename3="";
		    filename3="/home/db/dislcluster/loadbalancer/rubisEJB3_client_jdbc/stats/bid.dat";
		    String filename4="";
		    filename4="/home/db/dislcluster/loadbalancer/rubisEJB3_client_jdbc/stats/comment.dat";
		    */
		List<HashMap<String,HttpRequestObject>> list = new ArrayList<HashMap<String,HttpRequestObject>>();
		List<HashMap<String,CacheObject>> objList = new ArrayList<HashMap<String,CacheObject>>();
		
		for(ASServer serv:manager.getASServers().values()){
			//HttpLogList hll = (HttpLogList)serv.getStruct(HttpRequestProcessor.REQ_LOG_GROUP);
			HttpLogList hll = servHttpLogIndex.get(serv);
			CacheLogList cll= servCacheLogIndex.get(serv);
			
			HashMap<String,HttpRequestObject> aggMap = hll.aggregateSegments();
			HashMap<String,CacheObject> objAggMap = cll.aggregateSegments();
			
			list.add(aggMap);	
			objList.add(objAggMap);
		//	System.out.println("Omar======serv.serverId:" +serv.serverId );
		//	System.out.println("Omar======hll.httpLogList.size():" +hll.httpLogList.size());
		//	System.out.println("Omar======aggMap.size():" +aggMap.size());
		}
		
		/*
		File f0= new File(filename0);
		File f= new File(filename1);
		File f1= new File(filename11);
		PrintWriter out1 = null;
		if (!f0.exists())
		{
		 
			try {
				out1 = new PrintWriter(new BufferedWriter(new FileWriter(filename0, true)));
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else if (!f.exists())
		{
			
			try {
				out1 = new PrintWriter(new BufferedWriter(new FileWriter(filename1, true)));
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		else if (!f1.exists()) 
		{
			
			try {
				out1 = new PrintWriter(new BufferedWriter(new FileWriter(filename11, true)));
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
		else 
		{
			
			try {
				out1 = new PrintWriter(new BufferedWriter(new FileWriter(filename111, true)));
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		
			 PrintWriter out2 = null;
				try {
					out2 = new PrintWriter(new BufferedWriter(new FileWriter(filename2, true)));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				 PrintWriter out3 = null;
					try {
						out3 = new PrintWriter(new BufferedWriter(new FileWriter(filename3, true)));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
					 PrintWriter out4 = null;
						try {
							out4 = new PrintWriter(new BufferedWriter(new FileWriter(filename4, true)));
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						
		int objCounter=0;
		System.out.println("objList.size():" +objList.size() );
		for (int s=0;s< objList.size(); s++)
		{
			HashMap<String,CacheObject> newmap= new HashMap<String,CacheObject>();
			newmap=objList.get(s);
			if (newmap==null)
				continue;
			Set setObj = newmap.entrySet();
			System.out.println("setObj.size():" +setObj.size());
			
		      Iterator itrrObj = setObj.iterator();

			while (itrrObj.hasNext())
			{
				 Map.Entry meobj = (Map.Entry)itrrObj.next();
		        // System.out.print(me.getKey() + ": ");
				 CacheObject ht =(CacheObject) meobj.getValue();
				
				 if (ht.cacheKey.contains("Item"))
				 {
					 String outputdata=ht.cacheKey+" "+ ht.getCount;
			       		out1.println(outputdata);
				 }
				 else if (ht.cacheKey.contains("User"))
				 {
					 String outputdata=ht.cacheKey+" "+ ht.getCount;
			       		out2.println(outputdata);
				 }
				 else if (ht.cacheKey.contains("Bid"))
				 {
					 String outputdata=""+ ht.getCount;
			       		out3.println(outputdata);
				 }
				 else if (ht.cacheKey.contains("Comment"))
				 {
					// Analyser.log.info("COMMMENTTTTTTTTTTTTT");
					 
					 String outputdata=""+ ht.getCount;
			       		out4.println(outputdata);
				 }
				 
				 objCounter++;
				// ht.cacheKey
			//	 Analyser.log.info(ht.cacheKey +" obj "+ ht.getCount); 
				
			}
			out1.close();
			out2.close();
			out3.close();
			out4.close();
			
		}
		*/
		//return HttpLogList.mergeHttpRequestObjects(list);
		return HttpLogList.mergeHttpRequestObjectsLARD(list);
		
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
