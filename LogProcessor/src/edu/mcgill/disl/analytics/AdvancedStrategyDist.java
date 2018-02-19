package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.mcgill.disl.analytics.CacheLogList.CacheLogSegment;
import edu.mcgill.disl.analytics.policy.AppServerPolicy;
import edu.mcgill.disl.analytics.policy.ListKey;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy.ServerInfo;
import edu.mcgill.disl.analytics.policy.ObjectKey;
import edu.mcgill.disl.analytics.policy.RuleList;
import edu.mcgill.disl.log.processor.CacheLogProcessor;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;



/**
 * 
 * For this request based strategy we are not replicating objects.
 * 
 * how to come up with replication factor:
 * 
 * to = total no of objects logged/accessed
 * 
 * tc = total/aggregate cache capacity
 * 
 * to/tc = ops = objects per slot = ratio of objects to capacity => (repl 1 > tpc > 1 dist)
 * 
 * drp = desired replication (given) 
 * 
 * erp = effective replication (computed) = drp/ops = erp { 1.0 erp>1.0 }
 * 
 * Now we have the percentage of replication in our system.. This is the % of object not request that needs replication.
 * But we can approximate it for requests too if we assume the number of objects accessed per request is equal. OR
 * 
 * we replicate requests until we reach erp factor.
 * 
 * Now, we can replicate in all servers (n) or k servers (k<=n). We need to compute k here which should be based on erp also.
 * 
 * k = n
 *   
 * 
 * @author dislcluster
 *
 */

public class AdvancedStrategyDist extends RequestBasedAnalyserStrategy {
	
	double STABLE_PERCENT = .2;
	
	double REPLICATE_PERCENT = .1;
	
	int MAX_ALLOC_DIVISOR = 3; // 1 is all cache, 2 is half, 3 is 1/3, 4 is quarter
	
	int foundBefore=0;
	int foundBeforedist=0;
	
	Map<HashSet<String>,Integer> objCounter= new HashMap<HashSet<String>, Integer>();
	
	public AdvancedStrategyDist(RequestBasedAnalyser analyser) {
		super(analyser);
	}

	
	public int getServIdRev(String s,  HashMap<ASServer, Integer> servIndexReverse )
	{//Analyser.log.info("ssss "+ s);
		for(ASServer serv:manager.getASServers().values())
		{ //Analyser.log.info("serv.getServerId() "+serv.getServerId());
			if (serv.getServerId().equals(s))
			{
				//Analyser.log.info("serv.getServerId()==s "+serv.getServerId());
				return servIndexReverse.get(serv);
			}
		}
		return -1;
	}
	
	public boolean containsReqObj(ListKey lk,  ASServer serv)
	{
		return serv.currentPolicy.policyMap.containsKey(lk); 
	}
	
	
	public Integer containsObj(String s, CacheLogSegment rrm)
	{
		int putget=-1;
		for (CacheObject value :rrm.cacheMap.values()) 
		{
			if (value.cacheKey.equals(s))
			{ 
				if (value.size==1)
				{
					return 1;
				}
				else 
				{
					return 0;
				}
			}
			
		}	
		return putget;
	}

	
	/*
public void createReplicatedObjectsPolicy(CacheObject co, HashMap<Integer, ASServer> servInd ){
		
	    Iterator<Integer> it = servInd.keySet().iterator();
		//Iterator<String> it = serverList.keySet().iterator();
		while(it.hasNext()){
			String serverId = it.next();
			//System.out.println("Server ID: " + serverId);
			ASPolicyMap.put(serverId, new AppServerPolicy());
		}
		
	
			Iterator<Integer> itemp = servInd.keySet().iterator();
			while(itemp.hasNext()){
				String serverId = servInd.get(itemp.next()).serverId;
				RuleList rl = new RuleList(serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, new ArrayList<String>(serverList.keySet()));
				ListKey lk = new ListKey();
				lk.addtoListKey(co.cacheKey);
				ASPolicyMap.get(serverId).addNewPolicy(lk, rl);
			}		
	}
	
	*/
	

	@Override	
	public boolean generatePolicies()throws Exception {
		
		Analyser.log.info("AdvancedStrategyDynamic");
		
		
		//get factors from config
		STABLE_PERCENT = Double.parseDouble(manager.props.getProperty("rba.stable.percent"));
		
		REPLICATE_PERCENT = Double.parseDouble(manager.props.getProperty("rba.replicate.percent"));
		
		MAX_ALLOC_DIVISOR = Integer.parseInt(manager.props.getProperty("rba.max.alloc.divisor"));
		
				
		// we should have all required data structures ready by now..
		
		int totalReqAllocated = 0;
		int totalObjAllocated = 0;
		
		int[] cacheObjAllocated = new int[analyser.numServers];
		Arrays.fill(cacheObjAllocated, 0);
		
		int[] reqAllocated = new int[analyser.numServers];
		Arrays.fill(reqAllocated, 0);
		
		final int[] cacheObjMax = new int[analyser.numServers];
		Arrays.fill(cacheObjMax, analyser.cacheSz/MAX_ALLOC_DIVISOR + ((analyser.cacheSz*20)/100)); //20% extra buffer
		
		final int[] stableMax = new int[analyser.numServers];
		Arrays.fill(stableMax, (int) ((double)analyser.cacheSz * STABLE_PERCENT));
		
		int totalOverlap=0;		
		//total number of object accessed in last interval
		
		int totalObj = analyser.getUniqueResourceCount(analyser.manager.getASServers().values());
		
		Analyser.log.info("total Obj:" + totalObj);
		
		// to/tc = ops = objects per slot = ratio of objects to capacity => (repl 1 > tpc > 1 dist)

		double ops = (double)totalObj/(double)analyser.totalCapacity;
		
		Analyser.log.info("Objects Per Slot (ops):" + ops);
		
		double drp = REPLICATE_PERCENT;
		
		//erp = effective replication (computed) = drp/ops = erp { 1.0 erp>1.0 }
		double erp = drp/ops;
		
		Analyser.log.info("Effective Replication (erp) ratio:" + erp);
		
		 HashMap<Integer, ASServer> servIndex = new HashMap<Integer,ASServer>();
		
		 ArrayList<String> toServers = new ArrayList<String>(analyser.numServers);
		 String[] toServersLB= new String[analyser.numServers];
		 
		
		 HashMap<ASServer, Integer> servIndexReverse = new HashMap<ASServer,Integer>();
		int weakcouter=0;
		 
		for(ASServer serv:manager.getASServers().values()){
			System.out.print("serv= "+serv.getServerId());
			servIndex.put(serv.serverNo, serv);
			servIndexReverse.put(serv, serv.serverNo);
			toServers.add(serv.getServerId()); // for replicatoin
			toServersLB[weakcouter]=Analyser.resolveHost(serv.getServerId());
			weakcouter++;
				
		}
		
		// create lbPolicy and ASPolicy objects
		analyser.currentLBPolicy = new LoadBalancerPolicy();
		
		for(ASServer serv:manager.getASServers().values()){
			serv.currentPolicy = new AppServerPolicy();
//			
//			if (serv.currentPolicy!=null)
//			{
////				if (serv.lastPolicy!=null)
////				{
////					Analyser.log.info("serv.lasttPolicy size: FIRST " + serv.lastPolicy.policyMap.size());
////				}
//			//	Analyser.log.info("serv.currentPolicy size before:" + serv.currentPolicy.policyMap.size());
//				serv.lastPolicy= serv.currentPolicy.clone();
//		//		Analyser.log.info("--------------------");
//		//		Analyser.log.info("serv.lasttPolicy size: after " + serv.lastPolicy.policyMap.size());
//			}
//			
//			
////			if (serv.lastPolicy!=null)
////			{
////				Analyser.log.info("serv.currentPolicy size After:" + serv.currentPolicy.policyMap.size());
////				Analyser.log.info("serv.lastPolicy size After:" + serv.lastPolicy.policyMap.size());
////			}
		}
		Analyser.log.info("Effective Replication (erp) ratio:" + erp);
		Analyser.log.info("Effective Replication (erp) ratio:" + erp);
		Analyser.log.info("========================");
					
		int maxReqIdx = 0;
		int maxReqCnt=0;
		
		
		int reqIdx = 0;
		int allocFailCount = 0;
		boolean repMode = erp > 0.0;
		int repServers = analyser.numServers;
		int oldPolicy=-1;
	
		
		
		 
		HttpRequestObject maxAccess=new HttpRequestObject();
		
		//Analyser.log.info("CacheLogProcessor.oldLB==null" + servIndex.get(0).oldLB==null);
		if (servIndex.get(0).oldLB!=null)
		{
			oldPolicy=1;
		}
		
		
	//	Analyser.log.info("analyser.totalCapacity/MAX_ALLOC_DIVISOR=" + analyser.totalCapacity/MAX_ALLOC_DIVISOR);
		//Analyser.log.info("analyser.httpListAll.size()=" + analyser.httpListAll.size());
		Analyser.log.info("httpListAllNew.size()" + analyser.httpListAll.size());
		
		int repFlag=1;
		
		//while(totalObjAllocated < analyser.totalCapacity/MAX_ALLOC_DIVISOR && reqIdx < 1500){	
		while(totalObjAllocated < analyser.totalCapacity/MAX_ALLOC_DIVISOR && reqIdx < analyser.httpListAll.size()){	
			//pick current object 
			int currentServer = -1;
			int objSze=0;
			int objCtr=0;
			int urlInOldPolicy=-1;
			foundBeforedist=0;
			
			
			HttpRequestObject curReq = analyser.httpListAll.get(reqIdx);
			
			
			/*
	
			if (curReq.counter==1)
				break;
			
		if (curReq.counter<=5)
				repFlag=-1;
			
			*/
			////System.out.println("\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			////System.out.println("curReq= "+curReq.url.toString());	
					
			//get corresponding objects for this req
			HashSet<String> objects = analyser.reqToResAll.map.get(curReq.url);
										
			
			
			//////////System.out.println(">>>>>>>>>> " + curReq.url + " = " + objects);
			
			if(objects == null){
				reqIdx++;
				//Analyser.log.info("curReq.url" + curReq.url);
				continue;
			}
			
			Analyser.log.info("reqIdx" + reqIdx);
			Analyser.log.info("curReq.toString()" + curReq.url.toString());
			Analyser.log.info("curReq.counter" + curReq.counter);
			//					analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));

			//Analyser.log.info("curReq.url" + curReq.url);
			//Analyser.log.info("oldPolicy" + oldPolicy);
			boolean candidateFound = false;
			if (oldPolicy==1)
			{
				if (servIndex.get(0).oldLB.policyMap.containsKey(curReq.url))
				{
					urlInOldPolicy=1;
					ArrayList<ServerInfo> si = servIndex.get(0).oldLB.policyMap.get(curReq.url); 
					String s=si.get(0).host;
					int maxServer=getServIdRev( Analyser.resolveHost(s), servIndexReverse);
					//int maxServer=servIndexReverse.get(s);
					currentServer=maxServer;
					foundBefore=1;
					 candidateFound = true;
				}
			}
			
		
			//Analyser.log.info("curReq.url" + curReq.url);
			
			
			objCounter.put(objects,curReq.counter);
					
//			if (curReq.counter>maxAccess.counter)
//			{
//				maxAccess=(HttpRequestObject) curReq.clone();
//				tempMax=maxAccess.counter;
//				Analyser.log.info("curReq.url " + curReq.url);
//				Analyser.log.info("curReq.counter " + curReq.counter);			
//			}
//			
			
			if (curReq.counter>maxReqCnt)
			{
				maxReqIdx=reqIdx;
				maxReqCnt=curReq.counter;
				//Analyser.log.info("maxReqCnt= " + maxReqCnt);
				//Analyser.log.info("curReq.accessTimes.size()= " + curReq.accessTimes.size());
				//Analyser.log.info("reqIdx= " + reqIdx);
				
			}
				
			
			

		
		//	Analyser.log.info("objects.size()" + objects.size());
		//	Analyser.log.info("curReq.counter" + curReq.counter);
			
			HashSet<String> objMatched = new HashSet<String>();
			
			//------------------ Another approach to find Current Server
			
			//Only used in Non-Rep Mode
			
		//	Analyser.log.info("urlInOldPolicy=" +urlInOldPolicy);
			if(!repMode && (urlInOldPolicy==-1)){
				int matches[] = new int[servIndex.size()];
				Arrays.fill(matches, 0);
				
				int matchesCheck[] = new int[servIndex.size()];
				Arrays.fill(matchesCheck, 0);
				
				int matchesmove[] = new int[servIndex.size()];
				Arrays.fill(matchesmove, 0);
				
				int candidateInd = -1;
				
				int max = -1;
				int maxdist = -1;
				int maxServer = -1;
				
				
				for(int i=0;i<matches.length;i++){ // to loop through different servers
					ASServer serv = servIndex.get(i);
					//Analyser.log.info("serv.serverId:" + serv.serverId);
					//Analyser.log.info("serv.serverId:" + serv.currentPolicy);
					////System.out.println("serv.serverId:" + serv.serverId);
					foundBefore=0;
					
					
					//Analyser.log.info("serv.serverId:" + serv.serverId);
					for(String obj : objects)
					{ 
						ListKey lk = new ListKey();
						lk.addtoListKey(obj);
			
					 if (foundBefore!=1 && (oldPolicy!=1))
							{ 
						 Analyser.log.info("first for");
						 for(int k=0;k<servIndex.size();k++)
								{
									if (servIndex.get(k).currentPolicy.policyMap.containsKey(lk))
									{
										Analyser.log.info("contains key");
										matchesmove[k]++;
										foundBeforedist++;
										if (matchesmove[k]>maxdist)
												maxdist=matchesmove[k];
									}
									
								}	
							}
						//CacheLogList cl = (CacheLogList) serv.getStruct("cacheLogGroup");
						 CacheLogList cl=analyser.servCacheLogIndex.get(serv);
						 
						
						//Analyser.log.info("cl.cacheLogList.size() " +cl.cacheLogList.size());
						 int objectInanyCacheLog=-1;
						if (cl.cacheLogList.size()>0)
						   { 
							Analyser.log.info("serv.serverId " +serv.serverId);
							Iterator ir=cl.cacheLogList.iterator();
							
							int checkCounter=0;
							
							outerwhile:
							while (ir.hasNext())
							{
								checkCounter++;
								CacheLogList.CacheLogSegment cls= (CacheLogSegment) ir.next();
								//System.out.print("cl.cacheLogList.size()= "+cl.cacheLogList.size());
								
								//Analyser.log.info("cls.cacheMap.toString()" + cls.cacheMap.toString());
								//Analyser.log.info("lk.key.toString()" + lk.key.toString());
								//Analyser.log.info("cls.containsObj(lk.key)" + containsObj(lk.key,cls));
								//Analyser.log.info("cls.containsObj(lk.toString())" + containsObj(lk.key,cls));
								CacheObject co= new CacheObject();
								co.cacheKey=lk.key;
								
								//if (cls.cacheMap.cget(key)containsKey(lk))
								Analyser.log.info("lk.key.toString()" + lk.key.toString());
								Analyser.log.info("containsObj(lk.key,cls)" + containsObj(lk.key,cls));
								if (containsObj(lk.key,cls)==1)
								{       ////System.out.println("2nd if ");
									    //Analyser.log.info("2nd if candidateInd:" + candidateInd);
										////System.out.println(" matches[i]++;" +  matches[i]++);
										////System.out.println(" max" +  max);
									        matches[i]++;
									    	objectInanyCacheLog=1;
									    	maxServer=i;
									    	objMatched.add(obj);
									   
								}
								else if (containsObj(lk.key,cls)==-1)
								{
									objectInanyCacheLog=0;
								}
								
							
							 
							}		
							 if (objectInanyCacheLog==-1)
								{
								 matchesCheck[i]=matchesCheck[i]-100;
								}
							 
							 Analyser.log.info("matches[i] = "+matches[i]);
							 Analyser.log.info("matchesCheck[i] = "+matchesCheck[i]);
						   }
						
						   }
			
					
					//now maintain the server with max overlap
					//also check if the current server has free allocation space
					
					}
				
				
				
				//safety check. If no candidate server found.. then we dont have any more assignment to do in this phase
				Analyser.log.info("maxServer before = "+maxServer);
				
				if (foundBeforedist>0)
				{
					int minimumServerObj=200000;
					for(int k=0;k<matchesmove.length;k++){ 
					{
						if (matchesmove[k]==maxdist)
						{
								maxServer=k;
								minimumServerObj=servIndex.get(k).currentPolicy.policyMap.size();
						
						}
					}
				}
				}
				else
				{				
				for(int k=0;k<matches.length;k++){ 
				{
					Analyser.log.info("matches["+k+"] = "+matches[k]);
					if (matches[k]>0 && matchesCheck[k]>=0)
					{
						        maxServer=k;
							    candidateFound = true;
						
						
					   }
				  }
				}
				}	
					
				
				Analyser.log.info("maxServer after = "+maxServer);
				//Analyser.log.info("maxServer = "+maxServer);
				
				if(candidateInd != -1 && foundBefore==1){
					candidateFound = true;
				//	currentServer = candidateInd;
					
				}else if (maxServer!=-1){
					//find the least request-allocated
					////////System.out.println("Not found before \n");
					candidateFound = true;
					
					currentServer = maxServer;
					////System.out.println("maxserver= "+maxServer);		
					}
									
			}
			
			//Analyser.log.info("currentServer = "+currentServer);
			
			totalOverlap+=objMatched.size();
		
			//-----------------------------------------------------------
			//get current server
			
			Analyser.log.info("currentServer = "+currentServer);
			
			if (currentServer==-1)
			{
				reqIdx++;
				continue;
				
			}
			
			ASServer cServer = servIndex.get(currentServer);
			
		
			////System.out.println("finishedd Req = "+curReq.url.toString());
			////System.out.println("getServerId = "+Analyser.resolveHost(cServer.getServerId()));
			//check if the current server has the capacity to cache all objects
			
				Analyser.log.info("urlInOldPolicy = "+urlInOldPolicy);
				Analyser.log.info("cServer.getServerId()= "+cServer.getServerId());
				Analyser.log.info("getServerId = "+Analyser.resolveHost(cServer.getServerId()));


			if(candidateFound || ( cacheObjAllocated[currentServer] + objects.size() < cacheObjMax[currentServer])){
				//make an allocation
				//Analyser.log.info("RepMode:" + repMode + " \t" + curReq.url + ", \t" + curReq.counter + ", \t" + objects.size());
				//********* create an lb policy
				//if (urlInOldPolicy!=1)
				
				
				analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
				
				int objInServerBef = cServer.currentPolicy.policyMap.size();
				
				//********* create a cache policy with each ojbect having its own rule
				Analyser.log.info("repFlag = "+repFlag);
				for(String cacheKey : objects){
					
				//	if(objMatched.contains(cacheKey))
					//	continue;
					
					ListKey lk = new ListKey();
					lk.addtoListKey(cacheKey);
					
					if (repFlag!=1)
					{
						if(!cServer.currentPolicy.policyMap.containsKey(lk))
						{	
							ArrayList<String> toServer = new ArrayList<String>(analyser.numServers);
							toServer.add(cServer.getServerId());
							RuleList rl = new RuleList(cServer.getServerId(), RuleList.MOVE, repMode? RuleList.STABLE_TTL : RuleList.LONG_TTL , toServer);
							cServer.currentPolicy.addNewPolicy(lk, rl);
						}
					}	
					
					else if (repFlag==1)
					{
						
							toServers.clear();
							toServers.add(cServer.getServerId());
							//RuleList rl = new RuleList(cServer.getServerId(), RuleList.MOVE, repMode? RuleList.STABLE_TTL : RuleList.LONG_TTL , toServer);
							RuleList rl = new RuleList(cServer.serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
							cServer.currentPolicy.addNewPolicy(lk, rl);
							Analyser.log.info("serverId.getServerId() = "+cServer.getServerId());
							Analyser.log.info("rl.toString() = "+rl.toString());				
						
					}
				}
								
				int objInServerAft = cServer.currentPolicy.policyMap.size();
				int objAlloc = objInServerAft - objInServerBef;				
				//update all counters
				cacheObjAllocated[currentServer] += objAlloc;
				//cacheObjAllocated[currentServer] += objects.size();
				totalObjAllocated += objAlloc;				cacheObjAllocated[currentServer] += objAlloc;

				//totalObjAllocated += objects.size();
				
				reqAllocated[currentServer]++;
				
				totalReqAllocated++;
				
				allocFailCount=0; //reset fail counter
											
				//move to the next server 
				currentServer = (currentServer+1) % analyser.numServers;
				
				/*
				 * update reqIdx based on repMode and repServers
				 */
				
				if(repMode){
					double repNow = (double)totalObjAllocated/(double)analyser.totalCapacity;
					//if we have reached erp then stop replicating
					if(repNow >= erp){
						repMode = false;
						reqIdx++;
					}else if(--repServers==0){
						reqIdx++;
						repServers = analyser.numServers;
					}
				}else{
					//not a rep mode.. 
					reqIdx++;
				}
				
			}else{
				
				//choose another server
				currentServer = (currentServer+1) % analyser.numServers;
				
				if(++allocFailCount == analyser.numServers){
					//cant allocate this request on any server.. so ignoring this req
					reqIdx++;
					allocFailCount=0; //reset fail counter
				}
			}
		}
		
		Analyser.log.info("---------- AFTER MAIN ASSIGNMENT---------");
		Analyser.log.info("totalOverlap:" + totalOverlap);
		for(int i=0;i<servIndex.size();i++){
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", obj:" + cacheObjAllocated[i]);
		}
		
		
		
		//for the requests not assigned any policy..generate lb only policies
		// without looking for overlap
		
	//	CacheLogProcessor.oldLB= new LoadBalancerPolicy();
		
		servIndex.get(0).oldLB=analyser.currentLBPolicy.clone();
		Analyser.log.info("---------- AFTER LB ONLY POLICY---------");
		for(int i=0;i<servIndex.size();i++){
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", obj:" + cacheObjAllocated[i]);
		}
		
		
		 Iterator<HashSet<String>> itr = objCounter.keySet().iterator();
			while (itr.hasNext())
			{
				HashSet<String> key=itr.next();
				int c=key.size();
				while (c>0)
				{
				//	Analyser.log.info( key.size()+ "	" +objCounter.get(key));
					c--;
				}
			}
			
			Analyser.log.info( "==================================="); 
			Analyser.log.info( "Access Counts");
			Analyser.log.info( "===================================");
			
			
			
		

//		if (!(httpListAllNew.size()==0))
//		{
//			HttpRequestObject maxReq = httpListAllNew.get(maxReqIdx);
//		//	Analyser.log.info("maxReqCnt After= " + maxReqCnt);
//		//	Analyser.log.info("maxReq.accessTimes.size()= " + maxReq.accessTimes.size());
//		//	Analyser.log.info("maxReqIdx= " + maxReqIdx);
//			
//	//		Iterator<Long> itrMaxAccess = maxReq.accessTimes.iterator();
//		//	while (itrMaxAccess.hasNext())
//		//	{
//			//t	Analyser.log.info(itrMaxAccess.next());
//		//	}
//		}		
		return true;
	
	}
}

