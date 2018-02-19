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

///////////////// THIS IS SHUFFLING ALGORITHM

/**
 * 
 * For this request based strategy we are not replicating objects.
 * 
 * how to come up with replication factor:
 * 
 * to = total no of objects logged/accenew filessed
 * final int[] cacheObjMax = new int[analyser.numServers];
		Arrays.fill(cacheObjMax, analyser.cacheSz/MAX_ALLOC_DIVISOR + ((analyser.cacheSz*20)/100)); //20% extra buffer
		
		final int[] stableMax = new int[analyser.numServers];
		Arrays.fill(stableMax, (int) ((double)analyser.cacheSz * STABLE_PERCENT));
		
		int totalOverlap=0;		
		//total number of object accessed in last interval
		
		int totalObj = analyser.getUniqueResourceCount(analyser.manager.getASServers().values());
 * tc = total/aggregate cache capacity
 * 
 * to/tc = ops = objects per slot = rserver1Reqatio of objects to capacity => (repl 1 > tpc > 1 dist)
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

public class AdvancedStrategyDynamicDist extends RequestBasedAnalyserWeightedStrategy {
	
	public AdvancedStrategyDynamicDist(RequestBasedAnalyserWeighted analyser) {
		super(analyser);
	}
	
	public double STABLE_PERCENT = .2;
	
	public double REPLICATE_PERCENT = .1;
	
	public int MAX_ALLOC_DIVISOR = 1; // 1 is all cache, 2 is half, 3 is 1/3, 4 is quarter
	
	public int totalReqAllocated = 0;
	public int totalObjAllocated = 0;
	public int[] cacheObjAllocated ;
	public int[] serverWeights ;
	public int[] cacheObjMax ;
	
	public int[] stableMax;
	public int[] reqAllocated;
	public List<HttpRequestObject> recursieveRec = new ArrayList<HttpRequestObject>();
	
	public int totalOverlap=0;		
	//total number of object accessed in last interval
	
	
	//ArrayList<String> toServers = new ArrayList<String>(analyser.numServers);
	public ArrayList<String> toServers;
	 HashMap<ASServer, Integer> servIndexReverse = new HashMap<ASServer,Integer>();

	
	
	
	 public int foundBefore=0;
	public int [] server0Req= new int [5];
	public int [] server1Req= new int [5];
	public int [] server2Req= new int [5];
	public int [] server3Req= new int [5];
	
	
	public void recursiveShared(HttpRequestObject curReq, int currentServer, ASServer cServer, HttpRequestObject curReq1 ) 
	{
		
		List<HttpRequestObject> recursieveRec = new ArrayList<HttpRequestObject>();
		Analyser.log.info("REC ********************** ");
		Analyser.log.info("REC curReq = "+curReq.url);
		//Analyser.log.info("REC curReq1 = "+curReq1.url);
		 
		List<HttpRequestObject> k;
		k= analyser.sharedReq.get(curReq);
		Iterator it1=k.iterator();
		//Iterator it1 = analyser.sharedReq.entrySet().iterator();
		 while (it1.hasNext())
		 {
			 	
		    //    Map.Entry pairs = (Map.Entry)it1.next();
		        HttpRequestObject ro2=(HttpRequestObject)it1.next();
		      //  HttpRequestObject ro2=(HttpRequestObject)pairs.getValue();
		     //   Analyser.log.info("REC2 ro1 = "+ro1.url);
		     //  Analyser.log.info("REC2 ro2 = "+ro2.url);
	    		//Analyser.log.info("REC2 curReq1 = "+ curReq1.url);
		  //      Analyser.log.info("ro2.equals(curReq) = "+ ro2.equals(curReq));
		   //     Analyser.log.info("ro2.assignedbefore "+ ro2.assignedbefore);
		        
	    		 if (ro2.equals(curReq) || ro2.assignedbefore==1)
			        {
	    			 
			      //  	Analyser.log.info("REC3 ro2 = "+ro2.url);
			     //   	Analyser.log.info("REC2 CONT. ");
			    	//	Analyser.log.info("REC3 curReq1 = "+ curReq1.url);
			         continue;
			         
			        }
		       
		        	
		        //	Analyser.log.info("REC serverWeights ["+currentServer+"] = "+serverWeights[currentServer]);
		        	//Analyser.log.info("REC ro2 = "+ro2.url);
		        	if (serverWeights[currentServer]>=analyser.servCap)
		        		return;
		        	else //(serverWeights[currentServer]<analyser.servCap)
		        	{
		    			HashSet<String> objects2 = analyser.reqToResAll.map.get(ro2.url);
		    			if (objects2!=null)
		    			{
		    				for(String cacheKey : objects2)
							{					
								ListKey lk = new ListKey();
								lk.addtoListKey(cacheKey);
								
								if (!objAssignedBefore(lk))
								{
									toServers.clear();
									toServers.add(cServer.getServerId());
									RuleList rl = new RuleList(cServer.serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
									cServer.currentPolicy.addNewPolicy(lk, rl);
									cacheObjAllocated[currentServer]++;
									
									
								}
								
								//Analyser.log.info("lk.toString() = "+lk.toString());
							}
		    				
		    			//	Analyser.log.info("ro2.url = "+ro2.url);
		    			//	 Analyser.log.info("REC before Size = "+recursieveRec.size());
		    				analyser.currentLBPolicy.mapUrlToServers(ro2.url, Analyser.resolveHost(cServer.getServerId()));
		    				serverWeights[currentServer]=serverWeights[currentServer]+(ro2.weight);
		    				reqAllocated[currentServer]++;
		    				//cacheObjAllocated[currentServer]=cacheObjAllocated[currentServer]+objects2.size();
		    			//	Analyser.log.info("REC cServer.serverId inside shared = "+cServer.serverId);
		    				ro2.assignedbefore=1;
		    			//	analyzeUrl(ro2.url, cServer.serverNo);
		    				recursieveRec.add(ro2);
		    			//	analyser.sharedReq.remove(ro1);
		    			//	analyser.reqToResAll.map.remove(ro1);
		    			}

		        	}
		        	
		        	
		        }
		 
		 int i=0;
		 int size=recursieveRec.size();
		 Analyser.log.info("***REC Size*** = "+size);
		 if (recursieveRec.size()>0 )
		 {
			// HttpRequestObject max=recursieveRec.get(0);
			 while (i<recursieveRec.size())
			 {
				// if (max.weight<recursieveRec.get(i).weight)
				//	 max=recursieveRec.get(i);
				 recursiveShared(recursieveRec.get(i),currentServer,cServer,curReq);
				 
				 i++;
			 }
			 recursieveRec.clear();
		 }
		// Analyser.log.info("0000000000000000000 ");

		    
	}
	
	
	

	public void printServReq(int [] serverReq)
	{
		int total=0;
		for (int i=0;i<serverReq.length; i++)
		{
			//Analyser.log.info("part"+ i +" "+ serverReq[i]);
			total+=serverReq[i];
		}
		double tot= total;
		double d=0.0;
		double s=0.0;
		
		String servDist="";
		for (int i=0;i<serverReq.length; i++)
		{
			
			d=((double)serverReq[i]/ tot) *100.0;
			servDist=servDist+ " " +d;
			Analyser.log.info("part"+ i +" "+ serverReq[i] +"per " +d);
			//total+=serverReq[i];
		}
		Analyser.log.info("servDist = " + servDist);
		Analyser.log.info("total req = " + total);
	}
	
	public void analyzeUrl(String url, int serverNo)
	{
		int  [] serverReq= new int [5];
		if (serverNo==0)
			serverReq=server0Req;
		else if (serverNo==1)
			serverReq=server1Req;
		else if (serverNo==2)
			serverReq=server2Req;
		else if (serverNo==3)
			serverReq=server3Req;


			
		String r=null;
		int cat=0;
		int start=0;
		int end =0;
		int size=4000;
		int partSize=size/5;
		
		start=url.indexOf("category=");
		end=url.indexOf("&categoryName=");
		
		r=(String) url.subSequence(start+9, end);
		cat=Integer.parseInt(r);
		
		if (cat >0 && cat <= 160)
			serverReq[0]++;
		else if (cat >160 && cat <= 400)
			serverReq[1]++;
		else if (cat >400 && cat <= 700)
			serverReq[2]++;
		else if (cat >700 && cat <= 1140)
			serverReq[3]++;
		else if (cat >1140 && cat <= 2000)
			serverReq[4]++;
		
		/*
	
		if (cat >0 && cat <= partSize)
			serverReq[0]++;
		else if (cat >partSize && cat <= 2*partSize)
			serverReq[1]++;
		else if (cat >2*partSize && cat <= 3*partSize)
			serverReq[2]++;
		else if (cat >3*partSize && cat <= 4*partSize)
			serverReq[3]++;
		else if (4*cat >partSize && cat <= 5*partSize)
			serverReq[4]++;
*/
		//r=s.split(s.substring(4).toString());
		System.out.println(r);
		
	}
	
	Map<HashSet<String>,Integer> objCounter= new HashMap<HashSet<String>, Integer>();
	
	

	
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
	
	
	public boolean objAssignedBefore(Object k)
	{
	for(ASServer serv:manager.getASServers().values()){
		
		
		//Analyser.log.info(serv.getServerId());
		//Analyser.log.info(serv.currentPolicy.policyMap.toString());
		//Analyser.log.info(serv.currentPolicy.policyMap.containsKey(k));
		if (serv.currentPolicy.policyMap.containsKey(k))
			return true;
		
	}
	return false;
	}
	
	@Override
	
	public boolean generatePolicies()throws Exception {
		
		
		 cacheObjAllocated = new int [analyser.numServers];
		 serverWeights =new int [analyser.numServers];
		 cacheObjMax =new int [analyser.numServers];
		
		stableMax=new int [analyser.numServers];
		 reqAllocated=new int [analyser.numServers];
		 toServers = new ArrayList<String>(analyser.numServers);
		 
		Analyser.log.info("AdvancedStrategyDynamicDist RRR");
		 Arrays.fill(server0Req, 0);
			Arrays.fill(server1Req, 0);
			Arrays.fill(server2Req, 0);
		
		//get factors from config
		STABLE_PERCENT = Double.parseDouble(manager.props.getProperty("rba.stable.percent"));
		
		REPLICATE_PERCENT = Double.parseDouble(manager.props.getProperty("rba.replicate.percent"));
		
		MAX_ALLOC_DIVISOR = Integer.parseInt(manager.props.getProperty("rba.max.alloc.divisor"));
		
				
		// we should have all required data structures ready by now..
		
		
		
		
		Arrays.fill(cacheObjAllocated, 0);
		
		Arrays.fill(reqAllocated, 0);
		
	//	 int[] serverWeights = new int[analyser.numServers];
		 Arrays.fill(serverWeights, 0);
		
	//	final int[] cacheObjMax = new int[analyser.numServers];
		Arrays.fill(cacheObjMax, analyser.cacheSz/MAX_ALLOC_DIVISOR + ((analyser.cacheSz*20)/100)); //20% extra buffer
		
	//	final int[] stableMax = new int[analyser.numServers];
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
		
		 
		 
		
		 
		 
		
		 		
		for(ASServer serv:manager.getASServers().values()){
			System.out.print("serv= "+serv.getServerId());
			analyser.servIndex.put(serv.serverNo, serv);
			servIndexReverse.put(serv, serv.serverNo);
			toServers.add(serv.getServerId());
		
		}
		
		// create lbPolicy and ASPolicy objects040
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
//		//		Analyser.lanalyzeUrlog.info("serv.lasttPolicy size: after " + serv.lastPolicy.policyMap.size());
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
	
		
		
		 
		HttpRequestObject maxAccess=new HttpRequestObject();
		
		//Analyser.log.info("CacheLogProcessor.oldLB==null" + servIndex.get(0).oldLB==null);
		if (analyser.servIndex.get(0).oldLB!=null)
		{
			oldPolicy=1;
		}
		Analyser.log.info("=====analyser.totalWeight======"+analyser.totalWeight);
		Analyser.log.info("=====analyser.servCap======"+analyser.servCap);
		Analyser.log.info("=====analyser.variation======"+analyser.variation);
		
		
	//	Analyser.log.info("analyser.totalCapacity/MAX_ALLOC_DIVISOR=" + analyser.totalCapacity/MAX_ALLOC_DIVISOR);
		//Analyser.log.info("analyser.httpListAll.size()=" + analyser.httpListAll.size());
		Analyser.log.info("httpListAllNew.size()" + analyser.httpListAll.size());
		
		int totalWeightWhile=0;
		//while(totalObjAllocated < analyser.totalCapacity/MAX_ALLOC_DIVISOR && reqIdx < 1500){	
		
		while(reqIdx < analyser.httpListAll.size()){	
			//pick current object 
			int currentServer = -1;
			int objSze=0;
			int objCtr=0;
			int urlInOldPolicy=-1;
			int flagMulitpleLocation=-1;
			HttpRequestObject curReq = analyser.httpListAll.get(reqIdx);
			if (curReq.assignedbefore==1)
			{
			//	Analyser.log.info("Req assigned before");
				
				reqIdx++;
				continue;
			}
			
			List<HttpRequestObject> recursieveRec = new ArrayList<HttpRequestObject>();
			//System.out.println("curReq.url"+curReq.url);
			Analyser.log.info(" ===============================================");
			
			Analyser.log.info(" reqIdx" + reqIdx);
			Analyser.log.info(" curReq.url" + curReq.url);
			Analyser.log.info("curReq.counter" + curReq.counter);
		//	Analyser.log.info("curReq.weight" + curReq.weight);
			
			
			if (analyser.currentLBPolicy.policyMap.containsKey(curReq.url))
			{
				reqIdx++;
			//	Analyser.log.info("curr req in policy" + curReq.url);
				continue;
	
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
				Analyser.log.info("curReq objects is nul");
				continue;
			}
			
			totalWeightWhile=totalWeightWhile+curReq.weight;
			Analyser.log.info("totalWeightWhile:" + totalWeightWhile);
			
			/*
			for(int i=0;i<servIndex.size();i++){
				
				Analyser.log.info("Before serverWeights["+ servIndex.get(i).serverId.toString()+"]:" + serverWeights[i]);
			}
			
			*/
		//	Analyser.log.info("Check for redundant key ?" +analyser.currentLBPolicy.policyMap.get(curReq.url));
		//	if (analyser.currentLBPolicy.policyMap.containsKey(curReq.url))
		//	{
				//Analyser.log.info("analyser.currentLBPolicy.policyMap.containsKey("+curReq.url+")");
		//	}
			
		//	Analyser.log.info("reqIdx" + reqIdx);
		//	Analyser.log.info("curReq.toString()" + curReq.url.toString());
		//	Analyser.log.info("curReq.counter" + curReq.counter);
		//	Analyser.log.info("curReq.weight" + curReq.weight);
			//					analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));

			//Analyser.log.info("curReq.url" + curReq.url);
			//Analyser.log.info("oldPolicy" + oldPolicy);
			boolean candidateFound = false;
		//	Analyser.log.info("serverWeights lengt 0 = " + serverWeights.length);
		//	Analyser.log.info("cacheObjAllocated lengt 0 = " + cacheObjAllocated.length);
			
			/*
			if (oldPolicy==1)
			{
				if (analyser.servIndex.get(0).oldLB.policyMap.containsKey(curReq.url))
				{
					urlInOldPolicy=1;
					ArrayList<ServerInfo> si = analyser.servIndex.get(0).oldLB.policyMap.get(curReq.url); 
					//Analyser.log.info("si=" + si);
					String s=si.get(0).host;
				//	Analyser.log.info("s=" + s);
					int maxServer=getServIdRev( Analyser.resolveHost(s), servIndexReverse);
					
					Analyser.log.info("maxServer=" + maxServer);
					
					//int maxServer=servIndexReverse.get(s);
					
					if (serverWeights[maxServer]< analyser.servCap)
					{
					currentServer=maxServer;
					foundBefore=1;
					 candidateFound = true;
					 Analyser.log.info("Old policy server = " + currentServer);
					}
					else
					{
						currentServer=leastWeightedServer(serverWeights, serverWeights.length);
						foundBefore=1;
						 candidateFound = true;
						 Analyser.log.info("Old policy server Full = " + currentServer);
					}
					 
				}
			}
			*/
		
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
				int matches[] = new int[analyser.servIndex.size()];
				Arrays.fill(matches, 0);
				
				int matchesInPolicy[] = new int[analyser.servIndex.size()];
				Arrays.fill(matchesInPolicy, 0);
				
				int candidateInd = -1;
				int max = -1;
				int maxServer = -1;
				
				int foundInPolicy=-1;
				int maxPolicy=-1;
				int maxServerPolicy = -1;
				
				for(int i=0;i<matches.length;i++){ // to loop through different servers
					ASServer serv = analyser.servIndex.get(i);
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
					//	Analyser.log.info("foundInPolicy = "+foundInPolicy);

						   }
			
					
					//now maintain the server with max overlap				Analyser.log.info("foundInPolicy = "+foundInPolicy);

					//also check if the current server has free allocation space
					
					}
					
				}
				
				//safety check. If no candidate server found.. then we dont have any more assignment to do in this phase
				
			//	Analyser.log.info("foundInPolicy = "+foundInPolicy);
				
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
						// Analyser.log.info("MultipleLocationsPolicy>1 = ");
						for(int k=0;k<matchesInPolicy.length;k++)
						{ 
							if (matchesInPolicy[k]==maxPolicy)
							{
								if (serverWeights[k]<analyser.servCap)
								{
									maxServer=k;
									// Analyser.log.info("serverWeights[k]<=analyser.servCap ");
								}
								else
								{
									maxServer= leastWeightedServer(serverWeights, serverWeights.length);
								//	 Analyser.log.info("else serverWeights[k]<=analyser.servCap ");
								}
							}
							
						}
					}
					 else
					 {
						// Analyser.log.info("Else NEW");
						 
						 if (serverWeights[maxServerPolicy]<analyser.servCap)
							{
							 maxServer=maxServerPolicy;
								// Analyser.log.info("serverWeights[k]<=analyser.servCap ");
							}
						 else
							{
							// Analyser.log.info("Else Else NEW");
								maxServer= leastWeightedServer(serverWeights, serverWeights.length);
								// Analyser.log.info("else serverWeights[k]<=analyser.servCap ");
							}
						 
						 
						 
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
					
					Analyser.log.info("serverWeights lengt 1 = " + serverWeights.length);
				for(int k=0;k<matches.length;k++){ 
				{
					//Analyser.log.info(" server ----" + k);
					if (matches[k]>0)
					{
						//Analyser.log.info("matches[k]>0 server");
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
				
				
			//	Analyser.log.info("maxServer = "+maxServer);
				
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
			
		//	Analyser.log.info("serverWeights lengt 2 = " + serverWeights.length);
		//	Analyser.log.info("currentServer Before= "+currentServer);
			if (currentServer==-1)
			{
				currentServer=leastWeightedServer(serverWeights, serverWeights.length);
				candidateFound=true;
				//reqIdx++;
				//continue;
				
				
			}
			
			ASServer cServer = analyser.servIndex.get(currentServer);
			
		
			////System.out.println("finishedd Req = "+curReq.url.toString());
			////System.out.println("getServerId = "+Analyser.resolveHost(cServer.getServerId()));
			//check if the current server has the capacity to cache all objects
			
				Analyser.log.info("currentServer After= "+currentServer);
				Analyser.log.info("cServer.getServerId()= "+cServer.getServerId());
				//analyzeUrl(curReq.url, cServer.serverNo);
			//	Analyser.log.info("getServerId = "+Analyser.resolveHost(cServer.getServerId()));


			if(candidateFound || ( cacheObjAllocated[currentServer] + objects.size() < cacheObjMax[currentServer])){
				//make an allocation
				//Analyser.log.info("RepMode:" + repMode + " \t" + curReq.url + ", \t" + curReq.counter + ", \t" + objects.size());
				//********* create an lb policy
				//if (urlInOldPolicy!=1)
				curReq.assignedbefore=1;
				serverWeights[currentServer]=serverWeights[currentServer]+curReq.weight;
				cacheObjAllocated[currentServer]=cacheObjAllocated[currentServer]+objects.size();
				
		//		Analyser.log.info("currentServer After22= "+currentServer);
		//		Analyser.log.info("serverWeights lengt last = " + serverWeights.length);
		//		Analyser.log.info("serverWeights["+currentServer+"] = " +serverWeights[currentServer]);
		//		Analyser.log.info("serverWeights size2222 = " +serverWeights.length);
				if (!analyser.sharedReq.isEmpty())
				{
				//	Iterator it1 = analyser.sharedReq.entrySet().iterator();
					List<HttpRequestObject> k;
					k= analyser.sharedReq.get(curReq);
					if (k!=null)
					{
					Iterator it1=k.iterator();
					 while (it1.hasNext())
					 {
					      //  Map.Entry pairs = (Map.Entry)it1.next();
					       // HttpRequestObject ro1=(HttpRequestObject)pairs.getKey();
					     //   HttpRequestObject ro2=(HttpRequestObject)pairs.getValue();
					        HttpRequestObject ro2= (HttpRequestObject) it1.next();
					        if (ro2.assignedbefore==1)
					        {
					        	continue;
					        }
					        
					        if (curReq.equals(curReq) && (!analyser.currentLBPolicy.policyMap.containsKey(ro2.url)) )
					        {
					        	Analyser.log.info("ro2.url = "+ro2.url);
					        	if (serverWeights[currentServer]<analyser.servCap)
					        	{
					    			HashSet<String> objects2 = analyser.reqToResAll.map.get(ro2.url);
					    			if (objects2!=null)
					    			{
					    				for(String cacheKey : objects2)
										{					
											ListKey lk = new ListKey();
											lk.addtoListKey(cacheKey);
											
											if (!objAssignedBefore(lk))
											{
												toServers.clear();
												toServers.add(cServer.getServerId());
												RuleList rl = new RuleList(cServer.serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
												cServer.currentPolicy.addNewPolicy(lk, rl);
												cacheObjAllocated[currentServer]++;
												
												
											}
											
											//Analyser.log.info("lk.toString() = "+lk.toString());
										}
					    				
					    			//	Analyser.log.info("ro2.url = "+ro2.url);
					    				analyser.currentLBPolicy.mapUrlToServers(ro2.url, Analyser.resolveHost(cServer.getServerId()));
					    				serverWeights[currentServer]=serverWeights[currentServer]+(ro2.weight);
					    				reqAllocated[currentServer]++;
					    				
					    				Analyser.log.info("cServer.serverId inside shared = "+cServer.serverId);
					    				ro2.assignedbefore=1;
					    				analyzeUrl(ro2.url, cServer.serverNo);
					    				recursieveRec.add(ro2);
					    			//	analyser.sharedReq.remove(ro1);
					    				//analyser.reqToResAll.map.remove(ro1);
					    			}

					        	}
					        	 
					      // 	recursiveShared(ro2,currentServer,cServer,ro1);
					        	
					        }
					    }
					}
					 int i=0;
				//	 Analyser.log.info("size outside = " +recursieveRec.size());
					 int size=recursieveRec.size();
					 Analyser.log.info("ERERER serverWeights ["+currentServer+"] = "+serverWeights[currentServer]);
					 if (recursieveRec.size()>0 && serverWeights[currentServer]<analyser.servCap)
					 {
						 while (i<size)
						 {
							 recursiveShared(recursieveRec.get(i),currentServer,cServer,curReq);
							// Analyser.log.info("11111111111 ");
							 
							 i++;
						 }
				//		 Analyser.log.info("2222222222222222 ");
						 recursieveRec.clear();
					 }
				//	 Analyser.log.info("33333333333333333 ");
				}
				
			//	Analyser.log.info("44444444444444444444444444 ");
				analyser.currentLBPolicy.mapUrlToServers(curReq.url, Analyser.resolveHost(cServer.getServerId()));
					
				
				
				
				int objInServerBef = cServer.currentPolicy.policyMap.size();
				
				//********* create a cache policy with each ojbect having its own rule
				
				if (flagMulitpleLocation==-1)
				{
				//	Analyser.log.info("55555555555555555555 ");
					for(String cacheKey : objects)
					{					
						ListKey lk = new ListKey();
						lk.addtoListKey(cacheKey);
					//	Analyser.log.info("lk.toString() = "+lk.toString());
						
						if (!objAssignedBefore(lk))
						{
					//		Analyser.log.info("6666666666666666 ");

							toServers.clear();
							toServers.add(cServer.getServerId());
							RuleList rl = new RuleList(cServer.serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
							cServer.currentPolicy.addNewPolicy(lk, rl);
							
						}
					}
				}			
			//	Analyser.log.info("7777777777777777777 ");

				int objInServerAft = cServer.currentPolicy.policyMap.size();
				int objAlloc = objInServerAft - objInServerBef;				
				//update all counters
			//	cacheObjAllocated[currentServer] += objAlloc;
				//cacheObjAllocated[currentServer] += objects.size();
				totalObjAllocated += objAlloc;		
			//	cacheObjAllocated[currentServer] += objAlloc;

				//totalObjAllocated += objects.size();
				
				reqAllocated[currentServer]++;
				
				totalReqAllocated++;
				
				allocFailCount=0; //reset fail counter
											
				//move to the next server 
				currentServer = (currentServer+1) % analyser.numServers;
				
				/*
				 * update reqIdx based on repMode and repServers
				 */
				
				if(repMode)
				{
					double repNow = (double)totalObjAllocated/(double)analyser.totalCapacity;
					//if we have reached erp then stop replicating
					if(repNow >= erp)
					{
						repMode = false;
						reqIdx++;
					}
					else if(--repServers==0)
					{
						reqIdx++;
						repServers = analyser.numServers;
					}
				}
				else
				{
					//not a rep mode.. 
					reqIdx++;
				}
				
			}
			else
			{
				
				//choose another server
				currentServer = (currentServer+1) % analyser.numServers;
				
				if(++allocFailCount == analyser.numServers)
				{
					//cant allocate this request on any server.. so ignoring this req
					reqIdx++;
					allocFailCount=0; //reset fail counter
				}
			}
		}
		
		Analyser.log.info("---------- AFTER MAIN ASSIGNMENT---------");
		Analyser.log.info("totalOverlap:" + totalOverlap);
		for(int i=0;i<analyser.servIndex.size();i++){
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", obj:" + cacheObjAllocated[i]);
		}
		
		
		
		//for the requests not assigned any policy..generate lb only policies
		// without looking for overlap
		
	//	CacheLogProcessor.oldLB= new LoadBalancerPolicy();
		
		int totalWeightAfter=0;
		analyser.servIndex.get(0).oldLB=analyser.currentLBPolicy.clone();
		Analyser.log.info("---------- AFTER LB ONLY POLICY---------");
		for(int i=0;i<analyser.servIndex.size();i++){
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", obj:" + cacheObjAllocated[i]);
			Analyser.log.info("serverWeights["+ analyser.servIndex.get(i).serverId.toString()+"]:" + serverWeights[i]);
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
			Analyser.log.info("Server0 Dist");
			printServReq(server0Req);
			Analyser.log.info("Server1 Dist");
			printServReq(server1Req);
			Analyser.log.info("Server2 Dist");
			printServReq(server2Req);
			Analyser.log.info("Server3 Dist");
			printServReq(server3Req);
			
			
			
		

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

