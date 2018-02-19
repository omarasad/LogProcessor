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
 * Requests are assigned to each server exclusively in order of their frequency until there
 * is enough capacity in server cache.
 * 
 * The loadbalancer rule is simply a map from request to exactly one server corresponding to 
 * the accessed object locality.
 * 
 * 
 * @author dislcluster
 *
 */

public class DistributionOnlyStrategy extends RequestBasedAnalyserStrategy {
	
	double STABLE_PERCENT = .2;
	
	int MAX_ALLOC_DIVISOR = 3; // 1 is all cache, 2 is half, 3 is 1/3, 4 is quarter
	
	public DistributionOnlyStrategy(RequestBasedAnalyser analyser) {
		super(analyser);
	}

	@Override
	public boolean generatePolicies()throws Exception {
		
		// we should have all required data structures ready by now..
		STABLE_PERCENT = Double.parseDouble(manager.props.getProperty("rba.stable.percent"));
		
		MAX_ALLOC_DIVISOR = Integer.parseInt(manager.props.getProperty("rba.max.alloc.divisor"));
		
		int totalReqAllocated = 0;
		int totalObjAllocated = 0;
		
		int[] cacheObjAllocated = new int[analyser.numServers];
		Arrays.fill(cacheObjAllocated, 0);
		
		final int[] cacheObjMax = new int[analyser.numServers];
		Arrays.fill(cacheObjMax, analyser.cacheSz);
		
		final int[] stableMax = new int[analyser.numServers];
		Arrays.fill(stableMax, (int) ((double)analyser.cacheSz * STABLE_PERCENT));
		
		HashMap<Integer, ASServer> servIndex = new HashMap<Integer,ASServer>(); 
		for(ASServer serv:manager.getASServers().values()){
			servIndex.put(serv.serverNo, serv);
		}
		
		
		// create lbPolicy and ASPolicy objects
		analyser.currentLBPolicy = new LoadBalancerPolicy();
		
		//TODO see if there is a policy dont overwrite it
		for(ASServer serv:manager.getASServers().values()){
			serv.currentPolicy = new AppServerPolicy();
		}
		
		
				
		int currentServer = 0;
		int reqIdx = 0;
		int allocFailCount = 0;
		
		while(totalObjAllocated < analyser.totalCapacity/MAX_ALLOC_DIVISOR && reqIdx < analyser.httpListAll.size()){
			
			//pick current object
			HttpRequestObject curReq = analyser.httpListAll.get(reqIdx);
					
			//get corresponding objects for this req
			HashSet<String> objects = analyser.reqToResAll.map.get(curReq.url);
			
			//System.out.println(">>>>>>>>>> " + curReq.url + " = " + objects);
			
			if(objects == null){
				reqIdx++;
				continue;
			}
			
			//get current server
			ASServer cServer = servIndex.get(currentServer);
			
			//check if the current server has the capacity to cache all objects
			if(cacheObjAllocated[currentServer] + objects.size() < cacheObjMax[currentServer]){
				//make an allocation
				
				//Analyser.log.info(">>>" + curReq.url + ", \t" + curReq.counter + ", \t" + objects.size());
				
				//********* create an lb policy
				analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
				
				//********* create a cache policy
				//create a list key with all objects mapped to one rule
//				ListKey lk = new ListKey();
//				for(String cacheKey : objects){
//					lk.addtoListKey(cacheKey);
//				}
//				//create the rule
//				ArrayList<String> toServer = new ArrayList(analyser.numServers);
//				toServer.add(cServer.getServerId());
//				RuleList rl = new RuleList(cServer.getServerId(), RuleList.MOVE, RuleList.LONG_TTL , toServer);
//				
//				cServer.currentPolicy.addNewPolicy(lk, rl);
				
				int objInServerBef = cServer.currentPolicy.policyMap.size();
				
				//********* create a cache policy with each ojbect having its own rule
				
				for(String cacheKey : objects){
					
					ListKey lk = new ListKey();
					lk.addtoListKey(cacheKey);
					if(!cServer.currentPolicy.policyMap.containsKey(lk)){
						ArrayList<String> toServer = new ArrayList(analyser.numServers);
						toServer.add(cServer.getServerId());
						RuleList rl = new RuleList(cServer.getServerId(), RuleList.MOVE, RuleList.LONG_TTL , toServer);
					
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
				
				totalReqAllocated++;
				
				//update current server and
				currentServer = (currentServer+1) % analyser.numServers;
				reqIdx++;
				allocFailCount=0; //reset fail counter
			}else{
				//choose another server
				currentServer = (currentServer+1) % analyser.numServers;
				if(++allocFailCount == analyser.numServers){
					//cant allocate this request on any server.. so ignoring this one..
					reqIdx++;
					allocFailCount=0; //reset fail counter
				}
			}
		}
		
		//for the requests not assigned any policy..generate lb only policies 
		if(analyser.lbURLFreq > 0){
			while(reqIdx < analyser.httpListAll.size()){
				//pick current object
				HttpRequestObject curReq = analyser.httpListAll.get(reqIdx);
						
				//get current server
				ASServer cServer = servIndex.get(currentServer);
				
				//only allocate if url frequency is more than lbURLFreq
				if(curReq.counter >= analyser.lbURLFreq){
					//make an allocation
					
					//Analyser.log.info("LBOnly \t" + curReq.url + ", \t" + curReq.counter);
					
					//********* create an lb policy
					analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
					
					currentServer = (currentServer+1) % analyser.numServers;
					reqIdx++;
					
				}else{
					break;
				}
			}
		}
		
		return true;
	}

}
