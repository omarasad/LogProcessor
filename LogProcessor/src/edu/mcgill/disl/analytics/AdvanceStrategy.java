package edu.mcgill.disl.analytics;

import java.util.*;

import edu.mcgill.disl.analytics.policy.AppServerPolicy;
import edu.mcgill.disl.analytics.policy.ListKey;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.analytics.policy.RuleList;



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

public class AdvanceStrategy extends RequestBasedAnalyserStrategy {
	
	double STABLE_PERCENT = .2;
	
	double REPLICATE_PERCENT = .1;
	
	int MAX_ALLOC_DIVISOR = 3; // 1 is all cache, 2 is half, 3 is 1/3, 4 is quarter
	
	Map<HashSet<String>,Integer> objCounter= new HashMap<HashSet<String>, Integer>();
	
	public AdvanceStrategy(RequestBasedAnalyser analyser) {
		super(analyser);
	}

	@Override
	public boolean generatePolicies()throws Exception {
		
		
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
		
		for(ASServer serv:manager.getASServers().values()){
			servIndex.put(serv.serverNo, serv);
		//	Analyser.log.info("serv"+serv+" policy map=" + serv.currentPolicy.policyMap.toString());
			
		}
		
		// create lbPolicy and ASPolicy objects
		analyser.currentLBPolicy = new LoadBalancerPolicy();
		
		for(ASServer serv:manager.getASServers().values()){
			serv.currentPolicy = new AppServerPolicy();
		}
		
					
		int maxReqIdx = 0;
		int maxReqCnt=0;
		
		int currentServer = 0;
		int reqIdx = 0;
		int allocFailCount = 0;
		boolean repMode = erp > 0.0;
		int repServers = analyser.numServers;
		
		List<HttpRequestObject> httpListAllNew;
		 RequestToResourcesMap reqToResAllNew=null;
		
		
		 httpListAllNew=analyser.httpListAll;
		 reqToResAllNew=analyser.reqToResAll;
		 
		HttpRequestObject maxAccess=new HttpRequestObject();
		
	//	Analyser.log.info("analyser.totalCapacity/MAX_ALLOC_DIVISOR=" + analyser.totalCapacity/MAX_ALLOC_DIVISOR);
	//	Analyser.log.info("analyser.httpListAll.size()=" + analyser.httpListAll.size());
		Analyser.log.info("True of False=" + (totalObjAllocated < analyser.totalCapacity/MAX_ALLOC_DIVISOR && reqIdx < httpListAllNew.size()));
		while(totalObjAllocated < analyser.totalCapacity/MAX_ALLOC_DIVISOR && reqIdx < httpListAllNew.size()){
			
			//pick current object 
			
			int objSze=0;
			int objCtr=0;
			
			HttpRequestObject curReq = httpListAllNew.get(reqIdx);
					
			//get corresponding objects for this req
			HashSet<String> objects = reqToResAllNew.map.get(curReq.url);
			
	//		Analyser.log.info("reqIdx" + reqIdx);
			//System.out.println(">>>>>>>>>> " + curReq.url + " = " + objects);
			
			if(objects == null){
				reqIdx++;
				//Analyser.log.info("curReq.url" + curReq.url);
				continue;
			}
		
		
			
		
			
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
			//	Analyser.log.info("maxReqCnt= " + maxReqCnt);
			//	Analyser.log.info("curReq.accessTimes.size()= " + curReq.accessTimes.size());
			//	Analyser.log.info("reqIdx= " + reqIdx);
				
			}
				
			
			

		
			Analyser.log.info("objects.size()" + objects.size());
			Analyser.log.info("curReq.counter" + curReq.counter);
			
			HashSet<String> objMatched = new HashSet<String>();
			
			//------------------ Another approach to find Current Server
			boolean candidateFound = false;
			//Only used in Non-Rep Mode
			if(!repMode){
				int matches[] = new int[servIndex.size()];
				Arrays.fill(matches, 0);
				
				int candidateInd = -1;
				
				int max = -1;
				
				for(int i=0;i<matches.length;i++){ // to loop through different servers
					ASServer serv = servIndex.get(i);
				//	Analyser.log.info("serv.serverId:" + serv.serverId);
					//Analyser.log.info("serv.serverId:" + serv.currentPolicy.p);
					
					for(String obj : objects){
						ListKey lk = new ListKey();
						lk.addtoListKey(obj);
						
						if(serv.currentPolicy.policyMap.containsKey(lk)){
							objMatched.add(obj);
							matches[i]++;
						}
						
					//	Analyser.log.info("serv.currentPolicy.policyMap.size()" +serv.currentPolicy.policyMap.size());
					//	Analyser.log.info("matches["+i+"]" + matches[i]);
						
					}
					
					//now maintain the server with max overlap
					//also check if the current server has free allocation space
					if(matches[i]>max && (cacheObjAllocated[i]+objects.size()-matches[i] < cacheObjMax[i])){
						max = matches[i];
						candidateInd = i;
						//Analyser.log.info("if candidateInd:" + candidateInd);
					}else if(matches[i]==max){
						// here we break ties by selecting the one with less objects assignment so far
					//	Analyser.log.info("else if");
						if(cacheObjAllocated[i] < cacheObjAllocated[candidateInd]){
							candidateInd = i;
						//	Analyser.log.info("2nd if candidateInd:" + candidateInd);
						}
					}
					
				}
				
				//safety check. If no candidate server found.. then we dont have any more assignment to do in this phase
				
				if(candidateInd != -1){
					candidateFound = true;
					currentServer = candidateInd;
					
				}else{
					
					//find the least request-allocated
					candidateInd = 0;
					for(int j=0;j<reqAllocated.length;j++){
						if(reqAllocated[j]<reqAllocated[candidateInd]){
							candidateInd = j;
						}
					}
					currentServer = candidateInd;
				}
									
			}
			
			totalOverlap+=objMatched.size();
			
			//-----------------------------------------------------------
			//get current server
			ASServer cServer = servIndex.get(currentServer);
		
			
			//check if the current server has the capacity to cache all objects
			if(candidateFound || ( cacheObjAllocated[currentServer] + objects.size() < cacheObjMax[currentServer])){
				//make an allocation
				
				//Analyser.log.info("RepMode:" + repMode + " \t" + curReq.url + ", \t" + curReq.counter + ", \t" + objects.size());
				//********* create an lb policy
				analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
				
				
				int objInServerBef = cServer.currentPolicy.policyMap.size();
				
				//********* create a cache policy with each ojbect having its own rule
				
				for(String cacheKey : objects){
					
					if(objMatched.contains(cacheKey))
						continue;
					
					ListKey lk = new ListKey();
					lk.addtoListKey(cacheKey);
					if(!cServer.currentPolicy.policyMap.containsKey(lk)){	
						ArrayList<String> toServer = new ArrayList<String>(analyser.numServers);
						toServer.add(cServer.getServerId());
						RuleList rl = new RuleList(cServer.getServerId(), RuleList.MOVE, repMode? RuleList.STABLE_TTL : RuleList.LONG_TTL , toServer);
						
						cServer.currentPolicy.addNewPolicy(lk, rl);
					}
				}
								
				int objInServerAft = cServer.currentPolicy.policyMap.size();
				int objAlloc = objInServerAft - objInServerBef;				
				//update all counters
				cacheObjAllocated[currentServer] += objAlloc;
				//cacheObjAllocated[currentServer] += objects.size();
				totalObjAllocated += objAlloc;
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
		
		if(analyser.lbURLFreq > 0){
			while(reqIdx < httpListAllNew.size()){
				//pick current object
				HttpRequestObject curReq = httpListAllNew.get(reqIdx);
						
				//get least request-allocated server
				//find the least request-allocated
				int candidateInd = 0;
				for(int j=0;j<reqAllocated.length;j++){
					if(reqAllocated[j]<reqAllocated[candidateInd]){
						candidateInd = j;
					}
				}
				currentServer = candidateInd;
				
				ASServer cServer = servIndex.get(currentServer);
				
				//only allocate if url frequency is more than lbURLFreq
				if(curReq.counter >= analyser.lbURLFreq){
					//make an allocation
					
					//Analyser.log.info("LBOnly \t" + curReq.url + ", \t" + curReq.counter);
					
					//********* create an lb policy
					analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
					
					reqAllocated[currentServer]++;
					
					reqIdx++;
					
				}else{
					break;
				}
			}
		}
		
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
					Analyser.log.info( key.size()+ "	" +objCounter.get(key));
					c--;
				}
			}
			
			Analyser.log.info( "==================================="); 
			Analyser.log.info( "Access Counts");
			Analyser.log.info( "===================================");
			
			
			
		

		if (!(httpListAllNew.size()==0))
		{
			HttpRequestObject maxReq = httpListAllNew.get(maxReqIdx);
			Analyser.log.info("maxReqCnt After= " + maxReqCnt);
			Analyser.log.info("maxReq.accessTimes.size()= " + maxReq.accessTimes.size());
			Analyser.log.info("maxReqIdx= " + maxReqIdx);
			
			Iterator<Long> itrMaxAccess = maxReq.accessTimes.iterator();
		//	while (itrMaxAccess.hasNext())
		//	{
		//		Analyser.log.info(itrMaxAccess.next());
		//	}
		}		
		return true;
	
	}
}

