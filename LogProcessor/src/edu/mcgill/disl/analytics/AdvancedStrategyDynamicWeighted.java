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

public class AdvancedStrategyDynamicWeighted extends RequestBasedAnalyserWeightedStrategy {
	
	double STABLE_PERCENT = .2;
	
	double REPLICATE_PERCENT = .1;
	
	int MAX_ALLOC_DIVISOR = 3; // 1 is all cache, 2 is half, 3 is 1/3, 4 is quarter
	
	int foundBefore=0;
	
	Map<HashSet<String>,Integer> objCounter= new HashMap<HashSet<String>, Integer>();

	
	
	public AdvancedStrategyDynamicWeighted(RequestBasedAnalyserWeighted analyser) {
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
	public boolean containsObj(String s, CacheLogSegment rrm)
	{
		
		for (CacheObject value :rrm.cacheMap.values()) 
		{
			if (value.cacheKey.equals(s))
			{ 
				if (value.size==1)
				{
					return true;
				}
			}
			
		}	
		return false;
	}
	@Override
	
	
	public boolean generatePolicies()throws Exception {
		
		Analyser.log.info("AdvancedStrategyDynamicWeighted");
		
		
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
		
		int[] serverWeights = new int[analyser.numServers];
		 Arrays.fill(serverWeights, 0);
		
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
		 
		
		 
		 HashMap<ASServer, Integer> servIndexReverse = new HashMap<ASServer,Integer>();
		
		for(ASServer serv:manager.getASServers().values()){
			System.out.print("serv= "+serv.getServerId());
			servIndex.put(serv.serverNo, serv);
			servIndexReverse.put(serv, serv.serverNo);
			
			
			
			
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
		
		//while(totalObjAllocated < analyser.totalCapacity/MAX_ALLOC_DIVISOR && reqIdx < 1500){	
		int counterroundrobin=0;
		while(reqIdx < analyser.httpListAll.size()){	
			//pick current object 
			int currentServer = -1;
			int objSze=0;
			int objCtr=0;
			int urlInOldPolicy=-1;
			
			HttpRequestObject curReq = analyser.httpListAll.get(reqIdx);
			
		//	if (curReq.counter==1)
		//		break;

			HashSet<String> objects = analyser.reqToResAll.map.get(curReq.url);
			
			
			
			if(objects == null){
				reqIdx++;
				//Analyser.log.info("curReq.url" + curReq.url);
				continue;
			}
			
			Analyser.log.info("reqIdx" + reqIdx);
			Analyser.log.info("curReq.url" + curReq.url);
			Analyser.log.info("curReq.counter" + curReq.counter);
			Analyser.log.info("curReq.weight" + curReq.weight);
			objCounter.put(objects,curReq.counter);
			
				int matches[] = new int[servIndex.size()];
				Arrays.fill(matches, 0);
				
				
				int maxServer = -1;
				
				
				int minimumServerWeighted=9000000;
				for(int k=0;k<analyser.numServers;k++)
				{
					if (serverWeights[k]<=minimumServerWeighted)
					{
						
							maxServer=k;
							minimumServerWeighted=serverWeights[k];
							
						}
						
					}
				
					
				

				serverWeights[maxServer]+=curReq.weight;
				reqAllocated[maxServer]++;
				totalOverlap++;

				
				currentServer = maxServer;
				ASServer cServer = servIndex.get(currentServer);
				analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
				
				Analyser.log.info("cServer.serverId" + cServer.serverId);
				
				reqIdx++;
				
						
			}
			

		
	
		
		
		Analyser.log.info("---------- AFTER LB ONLY POLICY---------");
		Analyser.log.info("total Requests Assigned:" + totalOverlap);
		for(int i=0;i<servIndex.size();i++){
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", weight:" + serverWeights[i]);
		}
			
			Analyser.log.info( "==================================="); 
			Analyser.log.info( "Access Counts");
			Analyser.log.info( "===================================");
			
		
		return true;
		}
	}


