package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.swing.text.html.MinimalHTMLWriter;

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

public class AdvancedStrategyDynamicRep extends RequestBasedAnalyserWeightedStrategy {
	
	double STABLE_PERCENT = .2;
	
	double REPLICATE_PERCENT = .1;
	
	int MAX_ALLOC_DIVISOR = 1; // 1 is all cache, 2 is half, 3 is 1/3, 4 is quarter
	
	int foundBefore=0;
	
	Map<HashSet<String>,Integer> objCounter= new HashMap<HashSet<String>, Integer>();
	
	public AdvancedStrategyDynamicRep(RequestBasedAnalyserWeighted analyser) {
		super(analyser);
	}

	
	public int leastWeightedServer(int []serv, int size)
	{
		int minimumServ=0;
		for (int c=0;c<size;c++)
		{
			if (serv[c]<=serv[minimumServ])
					minimumServ=c;
			
		}
		return minimumServ;
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
		
		Analyser.log.info("AdvancedStrategyDynamicDist");
		
		
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
		 String[] toServersLB= new String[analyser.numServers];
		 
		 ArrayList<String> toServers = new ArrayList<String>(analyser.numServers);
		 HashMap<ASServer, Integer> servIndexReverse = new HashMap<ASServer,Integer>();
		 int weakcouter=0;
		 
		for(ASServer serv:manager.getASServers().values()){
			System.out.print("serv= "+serv.getServerId());
			servIndex.put(serv.serverNo, serv);
			servIndexReverse.put(serv, serv.serverNo);
			toServers.add(serv.getServerId());
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
		
		
		int maxReqIdx = 0;
		int maxReqCnt=0;
		
		
		int reqIdx = 0;
		int allocFailCount = 0;
		boolean repMode = erp > 0.0;
		int repServers = analyser.numServers;
		int oldPolicy=-1;
	
		
		
		int repTresh= analyser.totalWeight / analyser.httpListAll.size();
		HttpRequestObject maxAccess=new HttpRequestObject();
		
		//Analyser.log.info("CacheLogProcessor.oldLB==null" + servIndex.get(0).oldLB==null);
		if (servIndex.get(0).oldLB!=null)
		{
			oldPolicy=1;
		}
		Analyser.log.info("=====analyser.totalWeight======"+analyser.totalWeight);
		Analyser.log.info("=====analyser.servCap======"+analyser.servCap);
		Analyser.log.info("=====analyser.variation======"+analyser.variation);
		Analyser.log.info("=====repTresh======"+ repTresh);
		
		
		
	//	Analyser.log.info("analyser.totalCapacity/MAX_ALLOC_DIVISOR=" + analyser.totalCapacity/MAX_ALLOC_DIVISOR);
		//Analyser.log.info("analyser.httpListAll.size()=" + analyser.httpListAll.size());
		Analyser.log.info("httpListAllNew.size()" + analyser.httpListAll.size());
		
		int totalWeightWhile=0;
		int repFlag=1;
		//while(totalObjAllocated < analyser.totalCapacity/MAX_ALLOC_DIVISOR && reqIdx < 1500){	
		
		while(reqIdx < analyser.httpListAll.size()){	
			//pick current object 
			int currentServer = -1;
			int objSze=0;
			int objCtr=0;
			int urlInOldPolicy=-1;
			int flagMulitpleLocation=-1;
			HttpRequestObject curReq = analyser.httpListAll.get(reqIdx);
			
			//System.out.println("curReq.url"+curReq.url);
			
		//	Analyser.log.info(" reqIdx" + reqIdx);
			Analyser.log.info(" curReq.url" + curReq.url);
			Analyser.log.info("curReq.counter" + curReq.counter);
			Analyser.log.info("curReq.weight" + curReq.weight);
			if (analyser.currentLBPolicy.policyMap.containsKey(curReq.url))
			{
				reqIdx++;
				Analyser.log.info("curr req in policy" + curReq.url);
				continue;
	
			}
			
			if (curReq.weight<= repTresh)
			{
				repFlag=-1;
				Analyser.log.info("repFlag=-1");
				
			}
			else
			{
				repFlag=1;
				Analyser.log.info("repFlag=1");
			}

			
			//Analyser.log.info("curReq:" + curReq.url);
			
			/*
			if (curReq.counter==1)
			{
				reqIdx++;
				continue;
			}	
				*/
			////System.out.println("\n%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
			////System.out.println("curReq= "+curReq.url.toString());	
					
			//get corresponding objects for this req
			HashSet<String> objects = analyser.reqToResAll.map.get(curReq.url);
			
			
			//////////System.out.println(">>>>>>>>>> " + curReq.url + " = " + objects);
			
			if(objects == null){
				reqIdx++;
				//Analyser.log.info("curReq objects is nul");
				continue;
			}
			
			totalWeightWhile=totalWeightWhile+curReq.weight;
			Analyser.log.info("totalWeightWhile:" + totalWeightWhile);
			
			/*
			for(int i=0;i<servIndex.size();i++){
				
				Analyser.log.info("Before serverWeights["+ servIndex.get(i).serverId.toString()+"]:" + serverWeights[i]);
			}
			
			*/
			Analyser.log.info("Check for redundant key ?" +analyser.currentLBPolicy.policyMap.get(curReq.url));
			if (analyser.currentLBPolicy.policyMap.containsKey(curReq.url))
			{
				Analyser.log.info("analyser.currentLBPolicy.policyMap.containsKey("+curReq.url+")");
			}
			
		//	Analyser.log.info("reqIdx" + reqIdx);
		//	Analyser.log.info("curReq.toString()" + curReq.url.toString());
		//	Analyser.log.info("curReq.counter" + curReq.counter);
		//	Analyser.log.info("curReq.weight" + curReq.weight);
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
				
				int matchesInPolicy[] = new int[servIndex.size()];
				Arrays.fill(matchesInPolicy, 0);
				
				int candidateInd = -1;
				int max = -1;
				int maxServer = -1;
				
				int foundInPolicy=-1;
				int maxPolicy=-1;
				int maxServerPolicy = -1;
				
				for(int i=0;i<matches.length;i++){ // to loop through different servers
					ASServer serv = servIndex.get(i);
				//	Analyser.log.info("serv.serverId:" + serv.serverId);
				//	Analyser.log.info("serv.currentPolicy.policyMap.size():" + serv.currentPolicy.policyMap.size());
					////System.out.println("serv.serverId:" + serv.serverId);
					foundBefore=0;
					//Analyser.log.info("serv.serverId:" + serv.serverId);
					for(String obj : objects)
					{ 
						//Analyser.log.info("obj" +obj.toString());
						ListKey lk = new ListKey();
						lk.addtoListKey(obj);
						
					
					 if (foundBefore!=1)
							{ 
				//		 Analyser.log.info("serv.currentPolicy.policyMap.containsKey(obj)" +serv.currentPolicy.policyMap.containsKey(lk));
						//CacheLogList cl = (CacheLogList) serv.getStruct("cacheLogGroup");
						 CacheLogList cl=analyser.servCacheLogIndex.get(serv);
						 
						
						 if (serv.currentPolicy.policyMap.containsKey(lk))
						 {
							 
							 matchesInPolicy[i]++;
							 foundInPolicy=1;
							 if (matchesInPolicy[i]>=maxPolicy)
							    {
								 
								    maxPolicy=matchesInPolicy[i];
								    maxServerPolicy=i;
							    	objMatched.add(obj);
							    	//Analyser.log.info("maxServerPolicy" +maxServerPolicy);
							    }
						 }
						
						//Analyser.log.info("cl.cacheLogList.size() " +cl.cacheLogList.size());
						 else if (cl.cacheLogList.size()>0)
						   { //Analyser.log.info("first if ");
							Iterator ir=cl.cacheLogList.iterator();
							outerwhile:
							while (ir.hasNext())
							{
								CacheLogList.CacheLogSegment cls= (CacheLogSegment) ir.next();
								//System.out.print("cl.cacheLogList.size()= "+cl.cacheLogList.size());
								
								//Analyser.log.info("cls.cacheMap.toString()" + cls.cacheMap.toString());
								//Analyser.log.info("lk.key.toString()" + lk.key.toString());
								//Analyser.log.info("cls.containsObj(lk.key)" + containsObj(lk.key,cls));
								//Analyser.log.info("cls.containsObj(lk.toString())" + containsObj(lk.key,cls));
								CacheObject co= new CacheObject();
								co.cacheKey=lk.key;
								
								//if (cls.cacheMap.cget(key)containsKey(lk))
								if (containsObj(lk.key,cls))
								{       ////System.out.println("2nd if ");
									    //Analyser.log.info("2nd if candidateInd:" + candidateInd);
										////System.out.println(" matches[i]++;" +  matches[i]++);
										////System.out.println(" max" +  max);
									    matches[i]++;
									    if (matches[i]>=max)
									    {
									    	
									    	max=matches[i];
									    	maxServer=i;
									    	objMatched.add(obj);
									    	break outerwhile;
									    }
									    	
								
								}
							}
										
						   }			//	Analyser.log.info("foundInPolicy = "+foundInPolicy);

						   }
			
					
					//now maintain the server with max overlap				Analyser.log.info("foundInPolicy = "+foundInPolicy);

					//also check if the current server has free allocation space
					
					}
					
				}
				
				//safety check. If no candidate server found.. then we dont have any more assignment to do in this phase
				
				Analyser.log.info("foundInPolicy = "+foundInPolicy);
				
				int minimumServerObjPolicy=200000;
				if (foundInPolicy==1)
				{
					int MultipleLocationsPolicy=0;
					for(int k=0;k<matchesInPolicy.length;k++)
					{ 
						if (matchesInPolicy[k]>0)
						{
							MultipleLocationsPolicy++;
						}
						
					}
					
					 if (MultipleLocationsPolicy>1)
					{
						 flagMulitpleLocation=1;
						 Analyser.log.info("MultipleLocationsPolicy>1 = ");
						for(int k=0;k<matchesInPolicy.length;k++)
						{ 
							if (matchesInPolicy[k]==maxPolicy)
							{
								
									maxServer=k;
									// Analyser.log.info("serverWeights[k]<=analyser.servCap ");
								
							}
							
						}
					}
					 else
					 {
						 Analyser.log.info("Else NEW");
						 
						
							 maxServer=maxServerPolicy;
								// Analyser.log.info("serverWeights[k]<=analyser.servCap ");
							
						
						 
						 
						 
					 }
				//	serverWeights[maxServer]=curReq.counter * objects.size();
					
					/*for(int k=0;k<matchesInPolicy.length;k++){ 
						{
							if (matchesInPolicy[k]==maxPolicy)
							{
								int leastAllocatedServer=-1;
								int currentServerObj=-1;
								if (serverWeights[k]<=serverWeights[maxServer])
								{
									maxServer=k;
									
									
								}
								
							}
						}
				}*/
				}
				
				else if (foundInPolicy==-1)
				{
				int minimumServerObj=200000;
				int MultipleLocations=0;
				for(int k=0;k<matches.length;k++) 
					{
						if (matches[k]>0)
						{
							MultipleLocations++;
						}
					}
				
		
				//	Analyser.log.info("Not Multiple Locations");
					 minimumServerObj=200000;
					int minimumWeighted= leastWeightedServer(serverWeights, serverWeights.length);
				//	Analyser.log.info(" minimumWeighted ----" + minimumWeighted);
				for(int k=0;k<matches.length;k++){ 
				{
					Analyser.log.info(" server ----" + k);
					if (matches[k]>0)
					{
						Analyser.log.info("matches[k]>0 server");
						if (serverWeights[minimumWeighted]==serverWeights[k])
						{
							maxServer=k;
						//	serverWeights[maxServer]=curReq.counter * objects.size();
						//	Analyser.log.info("serverWeights[minimumWeighted]==serverWeights[k]");
						}
						else
						{
							maxServer=minimumWeighted;
						//	serverWeights[maxServer]=curReq.counter * objects.size();
						//	Analyser.log.info("else --------");
						}
					//	Analyser.log.info("maxServer not Multiple maxServer" + maxServer);
						
					}
				}
				}
				}
				
				
				Analyser.log.info("maxServer = "+maxServer);
				
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
				currentServer=leastWeightedServer(serverWeights, serverWeights.length);
				candidateFound=true;
				//reqIdx++;
				//continue;
				
				
			}
			
			ASServer cServer = servIndex.get(currentServer);
			
		
			////System.out.println("finishedd Req = "+curReq.url.toString());
			////System.out.println("getServerId = "+Analyser.resolveHost(cServer.getServerId()));
			//check if the current server has the capacity to cache all objects
			
				Analyser.log.info("urlInOldPolicy = "+urlInOldPolicy);
			//	Analyser.log.info("cServer.getServerId()= "+cServer.getServerId());
				Analyser.log.info("getServerId = "+Analyser.resolveHost(cServer.getServerId()));


			if(candidateFound || ( cacheObjAllocated[currentServer] + objects.size() < cacheObjMax[currentServer])){
				//make an allocation
				//Analyser.log.info("RepMode:" + repMode + " \t" + curReq.url + ", \t" + curReq.counter + ", \t" + objects.size());
				//********* create an lb policy
				//if (urlInOldPolicy!=1)
				
				
				
				
				if (!analyser.sharedReq.isEmpty())
				{
					Iterator it1 = analyser.sharedReq.entrySet().iterator();
					 while (it1.hasNext())
					 {
					        Map.Entry pairs = (Map.Entry)it1.next();
					        HttpRequestObject ro1=(HttpRequestObject)pairs.getKey();
					        HttpRequestObject ro2=(HttpRequestObject)pairs.getValue();
					        if (ro1.equals(curReq))
					        {
					        	if (serverWeights[currentServer]<=analyser.servCap)
					        	{
					    			HashSet<String> objects2 = analyser.reqToResAll.map.get(ro2.url);
					    			if (objects2!=null)
					    			{
					    				for(String cacheKey : objects2)
										{					
											ListKey lk = new ListKey();
											lk.addtoListKey(cacheKey);
											
											toServers.clear();
											toServers.add(cServer.getServerId());
											RuleList rl = new RuleList(cServer.serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
											cServer.currentPolicy.addNewPolicy(lk, rl);
											//Analyser.log.info("lk.toString() = "+lk.toString());
										}
					    				
					    				Analyser.log.info("ro2.url = "+ro2.url);
					    				analyser.currentLBPolicy.mapUrlToServers(ro2.url, Analyser.resolveHost(cServer.getServerId()));
					    				serverWeights[currentServer]=serverWeights[currentServer]+(ro2.counter*objects2.size());
					    				reqAllocated[currentServer]++;
					    				cacheObjAllocated[currentServer]=cacheObjAllocated[currentServer]+objects2.size();
					    			//	analyser.sharedReq.remove(ro1);
					    			//	analyser.reqToResAll.map.remove(ro1);
					    			}

					        	}
					        }
					    }
				}
				
				
				
				if (repFlag!=1)
				{
					analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
			
				}
				else 
				{
					//analyser.currentLBPolicy.mapUrlToServers(curReq.url, toServersLB);
					Analyser.log.info("no requests to assign -replicate");
				}
				//analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
				//serverWeights[currentServer]=serverWeights[currentServer]+curReq.weight;	
				
				
				
				int objInServerBef = cServer.currentPolicy.policyMap.size();
				
				//********* create a cache policy with each ojbect having its own rule
				
				
					for(String cacheKey : objects)
					{					
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
								serverWeights[cServer.serverNo]=serverWeights[cServer.serverNo]+curReq.weight;
							}
						}	
						
						else if (repFlag==1)
						{
							int servcounter=0;
							while (servcounter<servIndex.size())
							{
								ASServer serverId=servIndex.get(servcounter);
								//RuleList rl = new RuleList(cServer.getServerId(), RuleList.MOVE, repMode? RuleList.STABLE_TTL : RuleList.LONG_TTL , toServer);
								RuleList rl = new RuleList(serverId.getServerId(), RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
								serverId.currentPolicy.addNewPolicy(lk, rl);
							//	Analyser.log.info("serverId.getServerId() = "+serverId.getServerId());
								//Analyser.log.info("rl.toString() = "+rl.toString());
								serverWeights[servcounter]=serverWeights[servcounter]+curReq.weight;
							    servcounter++;
							    
								}
							
							
							
							
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
		
		int totalWeightAfter=0;
		servIndex.get(0).oldLB=analyser.currentLBPolicy.clone();
		Analyser.log.info("---------- AFTER LB ONLY POLICY---------");
		for(int i=0;i<servIndex.size();i++){
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", obj:" + cacheObjAllocated[i]);
			Analyser.log.info("serverWeights["+ servIndex.get(i).serverId.toString()+"]:" + serverWeights[i]);
			totalWeightAfter=totalWeightAfter+serverWeights[i];
			
		}
		Analyser.log.info("totalWeightBefore:" +analyser.totalWeight);
		Analyser.log.info("totalWeightAfter:" +totalWeightAfter);
		Analyser.log.info("analyser.httpListAll:" +analyser.httpListAll.size());
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

