package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.spi.LocaleNameProvider;

import org.apache.log4j.Priority;


import edu.mcgill.disl.analytics.policy.AppServerPolicy;
import edu.mcgill.disl.analytics.policy.ListKey;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.analytics.policy.ObjectKey;
import edu.mcgill.disl.analytics.policy.RuleList;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.log.processor.RequestResourceMappingLogProcessor;

//sizes:
//18:15:18,392 INFO  [STDOUT] Size of:edu.rice.rubis.hibernate.Categoryis:322
//18:15:18,392 INFO  [STDOUT] Size of:edu.rice.rubis.hibernate.Useris:0
//18:15:18,392 INFO  [STDOUT] Size of:edu.rice.rubis.hibernate.User.commentsTois:0
//18:15:18,393 INFO  [STDOUT] Size of:edu.rice.rubis.hibernate.Regionis:316
//18:15:18,393 INFO  [STDOUT] Size of:edu.rice.rubis.hibernate.Itemis:1395
//18:15:18,393 INFO  [STDOUT] Size of:edu.rice.rubis.hibernate.Bidis:491




public class ObjectBasedAnalyserRepOnly extends Analyser {
	
	
	//private static final org.apache.log4j.Logger mLogger = org.apache.log4j.Logger.getLogger("ObjectAnalyser");
	
	public LoadBalancerPolicy currentLBPolicy;
	public HashMap<String, CacheObject> globalCacheMap;
	
	public static int REPL_PERCENT;
	
	public List<CacheObject> cacheList;
	public List<CacheObject> candidateStableOnlyList;
	public List<CacheObject> candidateList;
	public List<CacheObject> candidateReplicationList;
	public List<String> urlList;
	public HashMap<String, String> replMap;
	public HashMap<String, Integer> serverList;
	public HashMap<String, HashMap<String, CacheObject>> individualMap;
	
	public HashMap<String, int[]> LBPCreatorMap;
	public HashMap<String, AppServerPolicy> ASPolicyMap;
	
	public HashMap<String,Integer> sizeMap;
	public HashMap<String, Integer> LBDMap;
	
	
	public static int NO_SERVERS;
	public static int EACH_CACHE_SIZE;
	
	public static int CACHE_SIZE;
	public static int REPL_SIZE;
	public static int STABLE_SIZE;
	public static int NO_CACHE_OBJECT;
	public static int OBJ_FOR_REQ;
	
	
	public ObjectBasedAnalyserRepOnly(StatisticsManager manager) {
		super(manager);
		
		
	}
	
	public void HouseKeeping(){
		currentLBPolicy = new LoadBalancerPolicy();
		globalCacheMap = new HashMap<String, CacheObject>();
		candidateStableOnlyList = new ArrayList<CacheObject>();
		candidateList = new ArrayList<CacheObject>();
		candidateReplicationList = new ArrayList<CacheObject>();
		urlList = new ArrayList<String>();
		replMap = new HashMap<String, String>();
		serverList = new HashMap<String, Integer>();
		individualMap = new HashMap<String, HashMap<String,CacheObject>>();
		LBPCreatorMap = new HashMap<String, int[]>();
		ASPolicyMap = new HashMap<String, AppServerPolicy>();
		sizeMap = new HashMap<String, Integer>();
		LBDMap = new HashMap<String,Integer>();
		
		
		NO_SERVERS = manager.getASServers().size();
		REPL_PERCENT = Integer.parseInt(manager.props.getProperty(StatisticsManager.REPL_PERCENT));
		EACH_CACHE_SIZE = Integer.parseInt(manager.props.getProperty(StatisticsManager.EACH_CACHE)) * 1024 * 1024;
		//CACHE_SIZE = (NO_SERVERS * EACH_CACHE_SIZE);
		REPL_SIZE = ((EACH_CACHE_SIZE*REPL_PERCENT)/100);
		STABLE_SIZE = (EACH_CACHE_SIZE - REPL_SIZE);
		NO_CACHE_OBJECT = Integer.parseInt(manager.props.getProperty(StatisticsManager.NO_CACHE_OBJECT));
		OBJ_FOR_REQ = Integer.parseInt(manager.props.getProperty(StatisticsManager.OBJ_FOR_REQ));
	}

	@Override
	public void analyse() throws Exception{

		HouseKeeping();
		
		//System.out.println("Inside ObjectBasedAnalyzer: analyse");
		
//		NO_SERVERS = manager.getASServers().values().size();
		Collection<ASServer> asCon = manager.getASServers().values();
		
		// store list of servers
		
		for(Object o: asCon){
			
			serverList.put(((ASServer) o).serverId, 0);
		}
		
		
		// merge and sort structures
		
		mergeASCacheLogs(asCon);
		
		// determine most frequent ones that fit within cache size
		
		listCandidateObjects();
		
		
		// decide objects that are to be replicated based on duplication
		
		listReplCandidateObjects();
		
		
		if(cacheList!=null){
			System.out.println("-------------Cache List start--------");
//			System.out.println(cacheList);
			System.out.println("CacheList Size:"+ cacheList.size());
			System.out.println("-------------Cache List end----------");			
		}
//		
		if(candidateList!= null){
			System.out.println("-------------Candidate List start--------");
//			System.out.println(candidateList);
			System.out.println("CandidateList Size:"+ candidateList.size());
			System.out.println("-------------Candidate List end----------");	
//			
		}
//		
		if(candidateStableOnlyList!=null){
			System.out.println("-------------Stable List start--------");
//			System.out.println(candidateStableOnlyList);
			System.out.println("CandidateStableOnly List Size:"+ candidateStableOnlyList.size());
			System.out.println("-------------Stable List end----------");
//			
		}
//		
		if(candidateReplicationList != null){
			System.out.println("-------------Replication List start--------");
//			System.out.println(candidateReplicationList);
			System.out.println("CandidateReplication List Size:"+ candidateReplicationList.size());
			System.out.println("-------------Replication List end--------");
		}
		
		
		
		
		// create AS policy
		
		createASPolicy();
		
		// create LB policy
		
//		createLBPolicy2();
		
//		createOnlyLBPolicy();
		
		// send policies
		
		sendPolicy();
		
	}
	
	public void mergeASCacheLogs(Collection<ASServer> asCon){
		
		//System.out.println("Inside ObjectBasedAnalyzer: mergeASCacheLogs");
		
		for(Object o : asCon){
			
			ASServer AStemp = (ASServer) o;
			CacheLogList cl = (CacheLogList) AStemp.getStruct("cacheLogGroup");
			 
			HashMap<String, CacheObject> tempMap = cl.aggregateSegments();
			
			if(tempMap == null){
				
				//System.out.println("NULL Returned from Aggregate Segments");
				continue;
			}
			
			individualMap.put(AStemp.serverId, tempMap);
			
			Collection<CacheObject> colCacObj = tempMap.values();
			
			for(CacheObject co: colCacObj){
				CacheObject cachObjTemp = globalCacheMap.get(co.cacheKey);
				
				if(cachObjTemp == null){
					co.sites.add(AStemp.getServerId());
					globalCacheMap.put(co.cacheKey, co);
				}
				else{
					cachObjTemp.getCount = cachObjTemp.getCount + co.getCount;
					cachObjTemp.no_sites++;
					cachObjTemp.sites.add(AStemp.getServerId());
				}
			}
		}
		
		Collection<CacheObject> cObjColl = globalCacheMap.values();
		cacheList = new ArrayList(cObjColl);
		Collections.sort(cacheList); // sort on getCount
		
//		if(mLogger.isLoggable(java.util.logging.Level.INFO)){
//			mLogger.log(java.util.logging.Level.INFO, "CacheList sorted on Frequency is:\n" + cacheList);
//		}
		/*
		if(mLogger.isInfoEnabled()){
			mLogger.log(Priority.INFO, "LOGGING BEGINS");
		}
		
		if(mLogger.isInfoEnabled()){
			mLogger.log(Priority.INFO, "CacheList sorted on Frequency is:\n" + cacheList);
		}
		*/
//		System.out.println("<<<<<<-------Sorted CacheList is--------------->>>>>>>>");
//		System.out.println(cacheList);
//		System.out.println("<<<<<<-------Sorted CacheList Ends--------------->>>>>>>>");
		
	}
	
	public void listCandidateObjects(){
		
		//System.out.println("Inside ObjectBasedAnalyzer: listCandidateObjects");
		
		int cur_size = 0;
		
		if(cacheList == null)
			return;
		
		Iterator<CacheObject> it = cacheList.iterator();
				
		System.out.println("Size to reach is:" + (STABLE_SIZE*NO_SERVERS + REPL_SIZE) );
		
		
		while( (cur_size < (STABLE_SIZE*NO_SERVERS + REPL_SIZE)) && (it.hasNext()) ){
			
			
			CacheObject temp = it.next();
			if(!sizeMap.containsKey(ResourceToRequestMap.getEntityType(temp.cacheKey))){
				sizeMap.put(ResourceToRequestMap.getEntityType(temp.cacheKey), temp.size);
			}
//			System.out.println("Being added to candidate List:" + temp.cacheKey);
//			if(mLogger.isInfoEnabled()){
//				mLogger.log(Priority.INFO, ":" + temp.cacheKey + ": being added to Candidate List");
//			}
			candidateList.add(temp);
			cur_size = cur_size + temp.size;
//			System.out.println("Current size is:" + cur_size);
		}
		
		System.out.println("<<<<<<<<<SIZES BEGIN>>>>>>>>>>");
		
		ArrayList<String> ks = new ArrayList<String>(sizeMap.keySet());
		
		Iterator<String> iks = ks.iterator();
		
		while(iks.hasNext()){
		
			String sk = iks.next();
			System.out.println("Size of:" + sk + " is:" + sizeMap.get(sk));
			
		}
		
		
		System.out.println("<<<<<<<<<SIZES END>>>>>>>>>>>>");
		
	}
	
	public void listReplCandidateObjects(){
		
		//System.out.println("Inside ObjectBasedAnalyzer: listReplCandidateObjects");
		
		Collections.sort(candidateList, new Comparator<CacheObject>() {

			@Override
			public int compare(CacheObject o1, CacheObject o2) {
				
				return (o2.no_sites - o1.no_sites);
			}
			
		});
		
//		System.out.println("<<<<<<-------Sorted CandidateList is--------------->>>>>>>>");
//		System.out.println(candidateList);
//		System.out.println("<<<<<<-------Sorted CandidateList Ends--------------->>>>>>>>");
		
		int cur_size = 0;
		
		Iterator<CacheObject> clt = candidateList.iterator();
		//System.out.println("Replication Size is:" + REPL_SIZE);
		
		while(clt.hasNext()){
			CacheObject temp = clt.next();
//			if(cur_size < (REPL_SIZE)){
//			System.out.println("No of objects from config file:" + NO_CACHE_OBJECT);
			if(cur_size < (NO_CACHE_OBJECT)){
				//System.out.println("Being Added to CandidateReplication List");
				candidateReplicationList.add(temp); //for now replicate everywhere
//				cur_size = cur_size + temp.size;
				cur_size++;
//				if(mLogger.isInfoEnabled()){
//					mLogger.log(Priority.INFO, ":" + temp.cacheKey + ": being added to CandidateReplication List");
//				}
			}
//			else
//			{
//				System.out.println("Being Added to CandidateStableOnly List");
//				if(mLogger.isInfoEnabled()){
//					mLogger.log(Priority.INFO, ":" + temp.cacheKey + ": being added to CandidateStableOnly List");
//				}
//				candidateStableOnlyList.add(temp);
//			}
		}
		
	}
	
	// This strategy for creating replicated object policy replicates the objects at all sites
	
	public void createReplicatedObjectsPolicy(){
		
		Iterator<String> it = serverList.keySet().iterator();
		while(it.hasNext()){
			String serverId = it.next();
			//System.out.println("Server ID: " + serverId);
			ASPolicyMap.put(serverId, new AppServerPolicy());
		}
		
		Iterator<CacheObject> cit = candidateReplicationList.iterator();
		
		while(cit.hasNext()){
			
			CacheObject ct = cit.next();
			Iterator<String> itemp = serverList.keySet().iterator();
			while(itemp.hasNext()){
				String serverId = itemp.next();
				RuleList rl = new RuleList(serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, new ArrayList<String>(serverList.keySet()));
				ListKey lk = new ListKey();
				lk.addtoListKey(ct.cacheKey);
				ASPolicyMap.get(serverId).addNewPolicy(lk, rl);
			}
			
		}
		
	}
	
	// The policy to create distributed objects policy sees for presence of objects
	
	public void createDistributedObjectsPolicy(){
		
		int size = 0;
		
		Iterator<CacheObject> cst = candidateStableOnlyList.iterator();
		
		while(cst.hasNext()){
			
			CacheObject ct = cst.next();
			
			Iterator<String> it1 = serverList.keySet().iterator();
			
			while(it1.hasNext()){
				
				String serverId = it1.next();
				HashMap<String, CacheObject> htemp = individualMap.get(serverId); 
				
				if(htemp.containsKey(ct.cacheKey)){
					
					Integer sz = serverList.get(serverId);
					
					if(sz.intValue() < STABLE_SIZE){
						ArrayList<String> lt = new ArrayList<String>();
						lt.add(serverId);
						RuleList rl = new RuleList(serverId, RuleList.MOVE , RuleList.LONG_TTL, lt);
						ListKey lk = new ListKey();
						lk.addtoListKey(ct.cacheKey);
						ASPolicyMap.get(serverId).addNewPolicy(lk, rl);
						sz = sz + ct.size;
						size += ct.size;
						serverList.put(serverId, sz);
						break;
					}
					else
					{
						Iterator<String> it2 = serverList.keySet().iterator();
						
						while(it2.hasNext()){
							String serverId2 = it2.next();
							Integer sz2 = serverList.get(serverId2);
							
							if(sz2.intValue() < STABLE_SIZE){
								ArrayList<String> lt = new ArrayList<String>();
								lt.add(serverId2);
								RuleList rl = new RuleList(serverId2, RuleList.MOVE , RuleList.LONG_TTL, lt);
								ListKey lk = new ListKey();
								lk.addtoListKey(ct.cacheKey);
								ASPolicyMap.get(serverId2).addNewPolicy(lk, rl);
								sz2 = sz2 + ct.size;
								size += ct.size;
								serverList.put(serverId2, sz2);
								break;
							}
						
						}
						
						break;
						
					}
				
				}
				
			}
			
			if(size >= (STABLE_SIZE*NO_SERVERS) ){
				break;
			}
			
		}
		
	}
	// This policy equally distributed the stable objects regardless of log history
	public void createOnlyDistributedObjectsPolicy(){
		
		int size = 0;
		
		Iterator<CacheObject> cst = candidateStableOnlyList.iterator();
		Collection<String> it1 = serverList.keySet();
		ArrayList<String> server = new ArrayList<String>(it1);
		int counter = 0;		
		
		while(cst.hasNext()){
			
			CacheObject ct = cst.next();
			if(size <= (STABLE_SIZE * NO_SERVERS)){
				
				ArrayList<String> lt = new ArrayList<String>();
				String serId = server.get( (counter++) % NO_SERVERS );
				lt.add(serId);
				RuleList rl = new RuleList(serId, RuleList.MOVE , RuleList.STABLE_TTL, lt);
				ListKey lk = new ListKey();
				lk.addtoListKey(ct.cacheKey);
				ASPolicyMap.get(serId).addNewPolicy(lk, rl);
			}
			else{
				break;
			}
			
					
		}
		
	}
	
	
	// Original AS Policy which is solely based on objects
	public void createASPolicy(){
		
		// First create for replicated objects
		//System.out.println("Inside ObjectBasedAnalyzer: createASPolicy");
		
		createReplicatedObjectsPolicy();
				
		// Now for rest of objects
		
//		createOnlyDistributedObjectsPolicy();
		
		Iterator<String> iap = ASPolicyMap.keySet().iterator();
		
		System.out.println("---------------ASPolicy Start------------------");
//		
		while(iap.hasNext()){
//			
//			
			String serId = iap.next();
			System.out.println("AS Policy for:" + serId);
			manager.getASServer(serId).currentPolicy = ASPolicyMap.get(serId);
			System.out.println("No of policy entries for this server are:" + manager.getASServer(serId).currentPolicy.policyMap.size());
		}
//		
		System.out.println("---------------ASPolicy End------------------");
						
	}
	
	
	// Request unaware LBPolicy
	public void createLBPolicy1(){
		
		//System.out.println("Inside ObjectBasedAnalyzer: createLBPolicy");
		
		Iterator<ASServer> itm = manager.getASServers().values().iterator();
		
		while(itm.hasNext()){
			
			ASServer as = itm.next();
			String serId = as.serverId;
			AppServerPolicy ap = as.currentPolicy;
			Iterator<ObjectKey> ik = ap.policyMap.keySet().iterator();
			
			while(ik.hasNext()){
				
				ObjectKey ot = ik.next();
				
				if(ot instanceof ListKey){
					ListKey tlk = (ListKey) ot;

					String t = tlk.key;
					//System.out.println("Checking for mapping of :" + t);
					
					if(((ResourceToRequestMap) as.getStruct((String)RequestResourceMappingLogProcessor.OBJ_REQ_MAP)).map.get(t) == null){
						//System.out.println("Mapping Map is NULL");
						continue;
					}
					
					ArrayList<String> test = new ArrayList<String>(((ResourceToRequestMap) as.getStruct((String)RequestResourceMappingLogProcessor.OBJ_REQ_MAP)).map.get(t));
					
					Iterator<String> itemp = test.iterator();
					
					
//						//System.out.println("For cache object:" + t + "these are the urls:");
					
					while(itemp.hasNext()){
					
						String url = itemp.next();
						
						this.currentLBPolicy.mapUrlToServers(url, Analyser.resolveHost(serId));
						
						
					}
					
				}
				
			}
			
			
		}
		
		System.out.println("------------LBPolicy start---------------");
//		System.out.println(currentLBPolicy);
//		
		System.out.println("Number of entries in LBPolicy are: " + currentLBPolicy.policyMap.size() );
//	
		System.out.println("------------LBPolicy end-----------------");
	}
	
	// Create LB Policy based on the urls corresponding to the most popular objects
	public void createOnlyLBPolicy(){
		
		Iterator<CacheObject> ican = cacheList.iterator();
		
		int objcounter = 0;
		
		while(ican.hasNext() && objcounter < (OBJ_FOR_REQ)){
			
			objcounter ++;
			CacheObject tempCacheObject = ican.next();
			Iterator<ASServer> itm = manager.getASServers().values().iterator();
			while(itm.hasNext()){
				
				ASServer as = itm.next();
				if(((ResourceToRequestMap) as.getStruct((String)RequestResourceMappingLogProcessor.OBJ_REQ_MAP)).map.get(tempCacheObject.cacheKey) == null){
					//System.out.println("Mapping Map is NULL");
					continue;
				}
				ArrayList<String> urllist = new ArrayList<String>(((ResourceToRequestMap) as.getStruct((String)RequestResourceMappingLogProcessor.OBJ_REQ_MAP)).map.get(tempCacheObject.cacheKey));
				Iterator<String> iurl = urllist.iterator();
				while(iurl.hasNext()){
					String url = iurl.next();
					if(!urlList.contains(url)){
						urlList.add(url);
					}
				}
			
			}
		}
		
		int urlcounter = 0;
		int urllistsize = urlList.size();
		Collection<String> it1 = serverList.keySet();
		ArrayList<String> server = new ArrayList<String>(it1);
		
		while(urlcounter < urllistsize){
			
			String serId = server.get( (urlcounter) % NO_SERVERS );
			this.currentLBPolicy.mapUrlToServers(urlList.get(urlcounter++), Analyser.resolveHost(serId));
			
		}
		
		System.out.println("------------LBPolicy start---------------");
//		System.out.println(currentLBPolicy);
//		
		System.out.println("Number of entries in LBPolicy are: " + currentLBPolicy.policyMap.size() );
//	
		System.out.println("------------LBPolicy end-----------------");
	/*	
		if(mLogger.isInfoEnabled()){
			mLogger.log(Priority.INFO, "LBPolicy being printed");
		mLogger.log(Priority.INFO, ":" + this.currentLBPolicy);
	}
	*/	
	}
	
	// Policy which assigns URL to a server based on which server has the maximum number of objects accessed by the URL
	public void createLBPolicy2(){

		Iterator<ASServer> itm = manager.getASServers().values().iterator();
		
		while(itm.hasNext()){
			
			ASServer as = itm.next();
			AppServerPolicy ap = as.currentPolicy;
			Iterator<ObjectKey> ik = ap.policyMap.keySet().iterator();
			
			while(ik.hasNext()){
				
				ObjectKey ot = ik.next();
				
				if(ot instanceof ListKey){
					ListKey tlk = (ListKey) ot;
					String t = tlk.key;
					//System.out.println("Checking for mapping of :" + t);
					
					if(((ResourceToRequestMap) as.getStruct((String)RequestResourceMappingLogProcessor.OBJ_REQ_MAP)).map.get(t) == null){
						//System.out.println("Mapping Map is NULL");
						continue;
					}
					
					ArrayList<String> test = new ArrayList<String>(((ResourceToRequestMap) as.getStruct((String)RequestResourceMappingLogProcessor.OBJ_REQ_MAP)).map.get(t));
					
					Iterator<String> itemp = test.iterator();
					
					while(itemp.hasNext()){
					
						String url = itemp.next();
						
						int[] sc = LBPCreatorMap.get(url);		
						
						if(sc == null){								
							
							sc = new int[NO_SERVERS];
							sc[as.serverNo]++;
							LBPCreatorMap.put(url, sc);
						}
						else{
							sc[as.serverNo]++;
						}
						
					}
				}
				
			}
			
			
		}
		
		Iterator<String> iturl = LBPCreatorMap.keySet().iterator();
		
		while(iturl.hasNext()){
			
			String url = iturl.next();
			
			int[] sc = LBPCreatorMap.get(url);
			
			int size = sc.length;
			int maxtemp=0;
			int maxindex=0;
			int val;
			
			
			for(int i=0; i< size; i++){
				
				val = sc[i];
				if(val > maxtemp)
				{
					maxtemp = val;
					maxindex = i;
				}
				
			}
			
			
			String xyz = manager.getASServer(maxindex).serverId;
			this.currentLBPolicy.mapUrlToServers(url, Analyser.resolveHost(xyz));
			
			
//			if(mLogger.isInfoEnabled()){
//				mLogger.log(Priority.INFO, ":" + url + ": being assigned to: " + xyz);
//			}
			
			Integer sId = LBDMap.get(xyz);
			
			if(sId == null){
				sId=new Integer(1);
				LBDMap.put(xyz, sId);
			}
			else{
				sId++;
				LBDMap.put(xyz, sId);
			}
			
			
		}
		
//		System.out.println("------------URL ServerCounter start---------------");
//		
//		Iterator<String> ilbp = LBPCreatorMap.keySet().iterator();
//		
//		while(ilbp.hasNext()){
//			
//			String url = ilbp.next();
//			int[] pi = LBPCreatorMap.get(url);
//			System.out.println("url is:" + url);
//			for(int i=0; i< pi.length; i++){
//				System.out.print("Site:"+ i + ":" + pi[i]);
//			}
//		}
//		
////		//System.out.println("Number of entries corresponding to LBPolicy for localhost are: " + currentLBPolicy.policyMap.values().size() );
//	
//		System.out.println("------------URL ServerCounter end-----------------");
		
		System.out.println("------------LBPolicy start---------------");
//		System.out.println(currentLBPolicy);
//		
//		System.out.println("Number of entries in LBPolicy are: " + currentLBPolicy.policyMap.size() );
		
		Iterator<String> itlb = LBDMap.keySet().iterator();
		
		while(itlb.hasNext()){
			String serid = itlb.next();
			
			System.out.println("Server:" + serid + " has been allocated: " + LBDMap.get(serid) + " objects");
		}
		
//	
		System.out.println("------------LBPolicy end-----------------");
	

		
	}
	
	public void sendPolicy() throws Exception{
		
		//System.out.println("Inside ObjectBasedAnalyzer: sendPolicy");
		
		// send AS policies
		Iterator<ASServer> iam = manager.getASServers().values().iterator();
		
		while(iam.hasNext()){
			
			ASServer serId = iam.next();
			sendASPolicy(serId.currentPolicy, serId.serverId);
			
		}
		
		// send LB policies
		
//		sendLBPolicy(currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
		
	}

}