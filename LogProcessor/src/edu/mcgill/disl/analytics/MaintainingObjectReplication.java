package edu.mcgill.disl.analytics;

import java.awt.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.mcgill.disl.analytics.policy.ListKey;
import edu.mcgill.disl.analytics.policy.ObjectKey;
import edu.mcgill.disl.analytics.policy.RuleList;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;

// The goal of this class is to maintain object replication status whenever there is a detection of read/write ratio change
public class MaintainingObjectReplication {

	private StatisticsManager sm;
	private Analyser analyser;
	private boolean currentlyMainting;
	private int curMainintenceInterval;
	
	private boolean keepRunning=true;
	private int curProcessedObj=0;
	public final int noOfIntervalFixed=3;
	
	
	private int curLocalInterval;
	
	private Set<CacheObjectReplicationCandidate> curObjToReplicatetempSet= new HashSet<CacheObjectReplicationCandidate>(); // MAY BE WE NEED A MORE SPECIFIC DS THAT MAINTAINS TO WHICH REPLICAS TO REPLICATE OR TO WHICH REPLICS TO UNREPLICATE
	private Set<CacheObjectReplicationCandidate> curObjToUnReplicatetempSet= new HashSet<CacheObjectReplicationCandidate>();
	
	
	
	

	
	
	public MaintainingObjectReplication(StatisticsManager manager, Analyser ans, boolean currentlyMainting, int curMainintenceInterval)
	{
		sm=manager;
		analyser=ans;
		currentlyMainting=currentlyMainting;
		curMainintenceInterval=curMainintenceInterval;
		curLocalInterval=curLocalInterval;
		
	}
	
	public void checkKeepRunning()
	{
		
		
		/*
		if (curProcessedObj>=sm.distributedObject.size())
		{
			keepRunning=false;
			analyser.log.info("checkKeepRunning()=false FIRST CONDITION");
		}
		// here to check linear regression slope
		*/
		int sizeOfObservation=sm.readWriteRatioMap.size();
		if (sizeOfObservation>=noOfIntervalFixed)
		{
			
			double x[] = new double [noOfIntervalFixed];
			double y[]= new double [noOfIntervalFixed];
			
			int counter =0;
			int startFrom=sizeOfObservation-noOfIntervalFixed;
			System.out.println("sizeOfObservation"+sizeOfObservation+ "startFrom="+startFrom +"noOfInterval"+noOfIntervalFixed);
			while (counter<noOfIntervalFixed)
			{
				
				x[counter]=counter;
				y[counter]=sm.readWriteRatioMap.get(startFrom);
				System.out.println("counter="+counter +"x[counter]"+x[counter] +"y[counter]"+y[counter]);
				startFrom++;
				counter++;
			}
				
				
			LinearRegression lr= new LinearRegression(x, y);
			analyser.log.info("lr.slope()"+lr.slope());
			if (Math.abs(lr.slope())<0.35)
			{
				keepRunning=false;
				
				analyser.log.info("checkKeepRunning()=false SECOND CONDITION");
			}
		}	
	}
	
	
	public void montiroAndChangeObjectReplication(long sleepSeconds) throws Exception
	{
		while (keepRunning) // here we need stopping conditions
		{
			try {
				Thread.sleep(sleepSeconds);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Analyser.log.info("==============================");
			Analyser.log.info("curLocalInterval="+curLocalInterval);
			// here we call another function that check each object replication ....
			
			if (curLocalInterval>noOfIntervalFixed) // the reason for this condition is to start the process after 3 intervals to guarantee that workload change is permanent
			{
				checkCurrentObjectsRepNew();
				if (curObjToReplicatetempSet.size()>0)
				{
					analyser.log.info("curObjToReplicatetempSet()="+curObjToReplicatetempSet.size());
					System.out.println("curObjToReplicatetempSet()="+curObjToReplicatetempSet.size());
					doReplicateNew();
					//doSendASPolicyNewTEMP();
					// Just one send policy for both
					//doSendASPolicyNew();
				}
				if (curObjToUnReplicatetempSet.size()>0)
				{
					analyser.log.info("curObjToUnReplicatetempSet()="+curObjToUnReplicatetempSet.size());
					System.out.println("curObjToUnReplicatetempSet()="+curObjToUnReplicatetempSet.size());
					doUnReplicateNew();
					
					// Just one send policy for both
					//doSendASPolicyNew();
				}
				
				
				doSendASPolicyNew();
				
				if (curObjToReplicatetempSet!=null || curObjToUnReplicatetempSet!=null)
				{
					//doSendASPolicyNew();
					
					curObjToReplicatetempSet.clear();
					curObjToUnReplicatetempSet.clear();
				}
 
				// checkKeepRunning();  if it is here then it will continue into an infinite loop
			}
			
			
			
			
			double totalHits =sm.readLocalCount+sm.readRemotecount;
			double totalRreadWriteHits =sm.updateRemoteCount+totalHits;
			//double curWriteRatio=(sm.updateRemoteCount/totalRreadWriteHits)*100;
			double curWriteRatio=(sm.updateRemoteCount/(totalRreadWriteHits- sm.updateRemoteCount))*100.0;
			sm.readWriteRatioMap.put(curLocalInterval,curWriteRatio);
			sm.lastReadWriteRatio=curWriteRatio;
			
			Analyser.log.info("INSIDE MAINTAIN"+sm.readLocalCount + "/"+ sm.readRemotecount+ "/"+totalHits + "/"+sm.updateRemoteCount+"/"+totalRreadWriteHits+ "curWriteRatio"+curWriteRatio);
			
			if (curLocalInterval>noOfIntervalFixed)
				checkKeepRunning(); // this is to check whether to continue monitoring phase or just stop
			
			curLocalInterval++;
			
		}
		
		
		
		
	}
	
	// this method finds for every partition, the new number of respective requests frequency for this object
	public HashMap<String,Double> getCurrentRemoteAccessCountNew(CacheObjectReplicationCandidate cor)
	{
		double totalRemoteAccess=0;	
		//analyser.log.info("sm.globalRequestMap="+sm.globalRequestMap.size());
		HashMap<String,Double> toReturn= new HashMap<String, Double>();
		
		for (String req:cor.distributedRequests.keySet())
		{
			if (!sm.globalRequestMap.containsKey(req))
				continue;
			
			String serv=cor.distributedRequests.get(req);
			if (toReturn.containsKey(serv))
			{
			Double curReplicaAccess= toReturn.get(serv);
			curReplicaAccess+=sm.globalRequestMap.get(req).counter;
			toReturn.put(serv, curReplicaAccess);
			
			}
			else
			{
			toReturn.put(serv, (double) sm.globalRequestMap.get(req).counter);
			}
			
			
			totalRemoteAccess+=sm.globalRequestMap.get(req).counter;
		}
		
		return toReturn;
	}
	
	
	/*
	public double getCurrentRemoteAccessCount(CacheObjectReplicationCandidate cor)
	{
		double totalRemoteAccess=0;	
		//analyser.log.info("sm.globalRequestMap="+sm.globalRequestMap.size());
		
		for (String req:cor.distributedRequests)
		{
			if (!sm.globalRequestMap.containsKey(req))
				continue;
			
			totalRemoteAccess+=sm.globalRequestMap.get(req).counter;
		}
		
		return totalRemoteAccess;
	}
	*/
	
	
	
	public void doSendASPolicyDELTE() throws Exception
	{
		
		
		for(ASServer serv : sm.servers.values())
		{
			serv.currentPolicy=serv.tmpPolicy.clone();
			analyser.sendASPolicy(serv.currentPolicy, serv.getServerId());
			
			
		//	System.out.println(serv.currentPolicy);
			
			Analyser.log.info("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
			Analyser.log.info("serv.getServerId()" + serv.getServerId());
			Analyser.log.info("serv.currentPolicy" + serv.currentPolicy.policyMap.size());
			Analyser.log.info("serv.currentPolicy" + serv.currentPolicy.policyMap.toString());
				serv.currentPolicy.policyMap.clear();
				serv.tmpPolicy.policyMap.clear();
			//if (serv.serverNo==0)
				
			
			
		}
	}
	
	public void doSendASPolicyNew() throws Exception
	{
		
		
		for(ASServer serv : sm.servers.values())
		{
			//serv.currentPolicy=serv.tmpPolicy.clone();
			analyser.sendASPolicy(serv.currentPolicy, serv.getServerId());
			
			
		//	System.out.println(serv.currentPolicy);
			
			System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"+sm.counter);
			System.out.println("serv.getServerId()" + serv.getServerId());
			System.out.println("serv.currentPolicy" + serv.currentPolicy.policyMap.size());
			System.out.println("serv.currentPolicy" + serv.currentPolicy.policyMap.toString());
			//	serv.currentPolicy.policyMap.clear();
				//serv.tmpPolicy.policyMap.clear();
			//if (serv.serverNo==0)
				
			
			
		}
	}
	
	public void doSendASPolicyNewTEMP() throws Exception
	{
		
		
		for(ASServer serv : sm.servers.values())
		{
			//serv.currentPolicy=serv.tmpPolicy.clone();
			analyser.sendASPolicy(serv.tempPolicytoDelete, serv.getServerId());
			
			
		//	System.out.println(serv.currentPolicy);
			
			System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX:D:D:D:D:DXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"+sm.counter);
			System.out.println("serv.getServerId()" + serv.getServerId());
			System.out.println("serv.currentPolicy" + serv.tempPolicytoDelete.policyMap.size());
			System.out.println("serv.currentPolicy" + serv.tempPolicytoDelete.policyMap.toString());
			//	serv.currentPolicy.policyMap.clear();
				//serv.tmpPolicy.policyMap.clear();
			//if (serv.serverNo==0)
				
			
			
		}
	}
	
	@SuppressWarnings("null")
	public void doReplicate()
	{
		Analyser.log.info("NNNdoReplicate=" + curObjToReplicatetempSet.size());
		 ArrayList<String> toServers= new ArrayList<>();
		for (CacheObjectReplicationCandidate cor:curObjToReplicatetempSet)
		{
			ListKey lk = new ListKey();
			lk.addtoListKey(cor.cacheKey);
			if (toServers!=null)
					toServers.clear();
			for (String server:cor.toReplicateCurrent)
			{
				//ASServer as= sm.servers.get(server);
				Analyser.log.info("NNN1"+server);
				ASServer as= sm.getASServer(server);
				
				toServers.add(as.serverId);
				RuleList rl;
				rl = new RuleList(as.serverId, RuleList.MOVE,	RuleList.STABLE_TTL, toServers);
				as.tmpPolicy.addNewPolicy(lk, rl);
			}
			
			//Here we need also to replicate default
			
			toServers.add(cor.defaultPartitionLocation.serverId);
			RuleList rl;
			rl = new RuleList(cor.defaultPartitionLocation.serverId, RuleList.MOVE,	RuleList.STABLE_TTL, toServers);
			cor.defaultPartitionLocation.tmpPolicy.addNewPolicy(lk, rl);
			
			cor.lastReplicatedUnreplicatedInterval=curMainintenceInterval;

			
		}
		
	}
	
	@SuppressWarnings("null")
	public void doReplicateNew()
	{
		Analyser.log.info("NNNewdoReplicate=" + curObjToReplicatetempSet.size());
		 ArrayList<String> toServers= new ArrayList<>();
		for (CacheObjectReplicationCandidate cor:curObjToReplicatetempSet)
		{
			unReplicateAllFirst(cor); // here we need to unreplicate this object from everywhere first
			
			ListKey lk = new ListKey();
			lk.addtoListKey(cor.cacheKey);
			
			for (String server:cor.toReplicateCurrent)
			{
				//ASServer as= sm.servers.get(server);
				Analyser.log.info("NNN1"+server);
				ASServer as= sm.getASServer(server);
				as.currentPolicy.policyMap.remove(cor.cacheKey);
				
				
				
				if (toServers!=null)
					toServers.clear();
				toServers.add(as.serverId);
				RuleList rl;
				rl = new RuleList(as.serverId, RuleList.MOVE,	RuleList.STABLE_TTL, toServers);
				as.currentPolicy.addNewPolicy(lk, rl);
				
			}
			
			cor.toReplicateCurrent.clear(); // to reset
			
			//Here we need also to replicate default
			
			/*
			ASServer as= sm.getASServer(cor.defaultPartitionLocation.serverId);
			as.currentPolicy.policyMap.remove(cor.cacheKey);
			
			
			
			//Analyser.log.info("doReplicateNew2"+as.serverId +" toServers="+toServers.toString());
			toServers.add(as.serverId);
			RuleList rl;
			rl = new RuleList(as.serverId, RuleList.MOVE,	RuleList.STABLE_TTL, toServers);
			as.currentPolicy.addNewPolicy(lk, rl);
			*/
			/*
			toServers.add(cor.defaultPartitionLocation.serverId);
			RuleList rl;
			rl = new RuleList(cor.defaultPartitionLocation.serverId, RuleList.MOVE,	RuleList.STABLE_TTL, toServers);
			cor.defaultPartitionLocation.tmpPolicy.addNewPolicy(lk, rl);
			*/
			cor.lastReplicatedUnreplicatedInterval=curMainintenceInterval;
			Analyser.log.info("doReplicateNew"+cor.cacheKey+" toServers="+toServers.toString());

			
		}
		
	}
	
	@SuppressWarnings("null")
	public void doUnReplicate()
	{
		Analyser.log.info("NNNdoUnReplicate=" + curObjToUnReplicatetempSet.size());
		 ArrayList<String> toServers = new ArrayList<String>();
		for (CacheObjectReplicationCandidate cor:curObjToUnReplicatetempSet)
		{
			ListKey lk = new ListKey();
			lk.addtoListKey(cor.cacheKey);
			if (toServers!=null)
				toServers.clear();
			//String server=cor.defaultPartitionLocation;
			
		///	Analyser.log.info("server=" + cor.defaultPartitionLocation.serverId);
			//ASServer as= sm.servers.get(server);
			toServers.add(cor.defaultPartitionLocation.serverId);
			RuleList rl;
			rl = new RuleList(cor.defaultPartitionLocation.serverId, RuleList.REPLICATE,	RuleList.STABLE_TTL, toServers);
			cor.defaultPartitionLocation.tmpPolicy.addNewPolicy(lk, rl);
			
			cor.lastReplicatedUnreplicatedInterval=curMainintenceInterval;
	
		}
		
	}
	
	public void unReplicateAllFirst(CacheObjectReplicationCandidate cor)
	{
		
		
		for(ASServer serv : sm.servers.values())
		{
			ListKey lk = new ListKey();
			lk.addtoListKey(cor.cacheKey);
			
			Analyser.log.info("policyMap Before=" + serv.currentPolicy.policyMap.containsKey(cor.cacheKey)+ "=="+cor.cacheKey+" =="+serv.serverId);
			ObjectKey ok=serv.currentPolicy.getKey(cor.cacheKey);
			if (ok!=null)
				serv.currentPolicy.policyMap.remove(ok);
			Analyser.log.info("policyMap After=" + serv.currentPolicy.policyMap.containsKey(cor.cacheKey) +"=="+serv.currentPolicy.policyMap.size()+"ok="+ok );
		}
		
		cor.replicatedPartitions.clear();
		
		/*/
		for (String serv:cor.replicatedPartitions)
		{
			
			ASServer as= sm.servers.get(serv);
			
			Analyser.log.info("policyMap Before=" + as.currentPolicy.policyMap.size()+ "=="+cor.cacheKey+" =="+as.serverId);
			as.currentPolicy.policyMap.remove(cor.cacheKey);
			Analyser.log.info("policyMap After=" + as.currentPolicy.policyMap.size() );
			
		}
		*/
		
	}
	
	
	@SuppressWarnings("null")
	public void doUnReplicateNew()
	{
		Analyser.log.info("NNNdoUnReplicate=" + curObjToUnReplicatetempSet.size());
		 ArrayList<String> toServers = new ArrayList<String>();
		 
		 
		for (CacheObjectReplicationCandidate cor:curObjToUnReplicatetempSet)
		{
			unReplicateAllFirst(cor); // here we need to unreplicate this object from everywhere first
			
			ListKey lk = new ListKey();
			lk.addtoListKey(cor.cacheKey);
			if (toServers!=null)
				toServers.clear();
			
			ASServer as=sm.servers.get(cor.defaultPartitionLocation.serverId);
			//ASServer as= sm.getASServer(cor.defaultPartitionLocation.serverId);
			if (as.currentPolicy.policyMap.containsKey(cor.cacheKey))
				Analyser.log.info("ERERERER as.currentPolicy.policyMap.containsKey(cor.cacheKey)=" + cor.cacheKey);
			
			as.currentPolicy.policyMap.remove(cor.cacheKey);
			
			//cor.replicatedPartitions.add(cor.cacheKey);
			
			
			toServers.add(as.serverId);
			RuleList rl;
			rl = new RuleList(as.serverId, RuleList.REPLICATE,	RuleList.STABLE_TTL, toServers);
			as.currentPolicy.addNewPolicy(lk, rl);
			
			cor.lastReplicatedUnreplicatedInterval=curMainintenceInterval;

			Analyser.log.info("doUnReplicateNew"+cor.cacheKey+"  toServers="+toServers.toString());
		}
		
	}
	
	/*
	public void checkCurrentObjectsRep()
	{
		Analyser.log.info("checkCurrentObjectsRep="+sm.distributedObject.size());
		Analyser.log.info("curMainintenceInterval="+curMainintenceInterval);
		
		for (CacheObjectReplicationCandidate cor:sm.distributedObject)
		{
			/*
			if (cor.lastReplicatedUnreplicatedInterval==curMainintenceInterval) 
			{
				Analyser.log.info("object has been maintained this interval" + cor.cacheKey);
				continue;// it means that this object has been maintained this interval
			}
			*/
			/*
			// here for each object we need to calculate the new replication factor and if it is > 0 then replicate if it has not been replicated or unreplicated 
			
			int replicaNo=1;
			
			
			if (sm.globalCacheMap==null)
			{
				Analyser.log.info("sm.globalCacheMap==null");
				continue;
			}
			
			if (!sm.globalCacheMap.containsKey(cor.cacheKey))
			{
				Analyser.log.info("!sm.globalCacheMap.containsKey(cor.cacheKey)"+cor.cacheKey);
				continue;
			}
			
			CacheObject co=sm.globalCacheMap.get(cor.cacheKey);
			
			double remoteAccess=getCurrentRemoteAccessCountNew(cor);
			if (cor.distributedPartitions.size()>0)
				replicaNo=cor.distributedPartitions.size();

			replicaNo++; // the reason for this is to add up the original partition that contains the object to the replication cost
			
			Analyser.log.info( "replicaNo="+replicaNo +"UC(1)="+sm.replicateObjectCost.get(1)+" UC(N)="+sm.replicateObjectCost.get(replicaNo) +"remoteAccess="+remoteAccess +"co.updateCount="+co.updateCount);
			
			double ObjectReplicationFactorModel= remoteAccess*(sm.readRemoteCost-sm.readLocalCost)- co.updateCount*(sm.replicateObjectCost.get(replicaNo)-sm.replicateObjectCost.get(1));
			
			Analyser.log.info("cor.cacheKey" + cor.cacheKey +"ObjectReplicationFactorModel="+ObjectReplicationFactorModel);
			
			if (ObjectReplicationFactorModel>0 && cor.lastRepFactor<=0)
			{
				//replicate
				//Analyser.log.info("replicate"+cor.cacheKey + " = "+cor.lastReplicatedUnreplicatedInterval);
				curObjToReplicatetempSet.add(cor);
				curProcessedObj++;
			}
			
			else if (ObjectReplicationFactorModel<0 && cor.lastRepFactor>=0)
			{
				//unreplicate
				//Analyser.log.info("Unreplicate"+cor.cacheKey + " = "+cor.lastReplicatedUnreplicatedInterval);
				curObjToUnReplicatetempSet.add(cor);
				curProcessedObj++;
			}
		}
	}
	
	*/
	public void checkCurrentObjectsRepNew()
	{
		Analyser.log.info("checkCurrentObjectsRep="+sm.distributedObject.size()+"sm.globalCacheMap="+sm.globalCacheMap.size()) ;
		Analyser.log.info("curMainintenceInterval="+curMainintenceInterval);
		
		for (CacheObjectReplicationCandidate cor:sm.distributedObject)
		{
			
			Analyser.log.info("****************************************");
			/*
			if (cor.lastReplicatedUnreplicatedInterval==curMainintenceInterval) 
			{
				Analyser.log.info("object has been maintained this interval" + cor.cacheKey);
				continue;// it means that this object has been maintained this interval
			}
			*/
			// here for each object we need to calculate the new replication factor and if it is > 0 then replicate if it has not been replicated or unreplicated 
			
			int replicaNo=1;
			
			
			if (sm.globalCacheMap==null)
			{
				Analyser.log.info("sm.globalCacheMap==null");
				continue;
			}
			
			if (!sm.globalCacheMap.containsKey(cor.cacheKey))
			{
				Analyser.log.info("!sm.globalCacheMap.containsKey(cor.cacheKey)"+cor.cacheKey);
				continue;
			}
			
			CacheObject co=sm.globalCacheMap.get(cor.cacheKey);
			
			HashMap <String,Double> servToAccess= getCurrentRemoteAccessCountNew(cor);
			//double remoteAccess=getCurrentRemoteAccessCount(cor);
			if (cor.distributedPartitions.size()>0)
				replicaNo=cor.distributedPartitions.size();

			replicaNo++; // the reason for this is to add up the original partition that contains the object to the replication cost
			
		//	Analyser.log.info( "replicaNo="+replicaNo +"UC(1)="+sm.replicateObjectCost.get(1)+" UC(N)="+sm.replicateObjectCost.get(replicaNo) +"remoteAccess="+remoteAccess +"co.updateCount="+co.updateCount);
			
		//	double ObjectReplicationFactorModel= remoteAccess*(sm.readRemoteCost-sm.readLocalCost)- co.updateCount*(sm.replicateObjectCost.get(replicaNo)-sm.replicateObjectCost.get(1));
			
			Analyser.log.info("cor.cacheKey" + cor.cacheKey +"cor.distributedPartitions="+cor.distributedPartitions);
			
			
			if  (cor.distributedPartitions.size()>=2)
			{
				// here we need to use ReplicaPermutation
				
				Analyser.log.info("partitionsRemoteNew.size()========="+cor.distributedPartitions.size() +" def="+cor.defaultPartitionLocation.serverId+"co.updateCount="+co.updateCount);
				
				
				
				for (String s:cor.distributedPartitions)
				{
					Analyser.log.info("sss="+s);
				}
				
				ReplicaPermutation rp= new ReplicaPermutation(co.updateCount,servToAccess,sm,cor.defaultPartitionLocation.serverId);
				rp.getBestReplicaSetNew();
				
				/*
				rp.bestRepPermGlobal=co.sites.get(0);
				rp.maxPermGlobal=partitionsRemoteNew.get(rp.bestRepPermGlobal);
				
				rp.generateReplicasOptions();
				
				System.out.println(rp.getBestRep() +""+rp.getmaxPermGlobal());
				*/
				
				
				
				
				String []s=rp.getBestRep().split("/");
				ArrayList <String> newReplicatedPartitions= new ArrayList<String>();
				ArrayList <String> tempOldReplicatedPartitions= new ArrayList<String>();
				
				tempOldReplicatedPartitions.addAll(cor.replicatedPartitions);
				
				
				double sum=0;
				
				Analyser.log.info("s.length"+s.length);
				
				for (int j=0;j<s.length;j++) // s is the current set of object to replicate
				{
					newReplicatedPartitions.add(s[j]);
					Analyser.log.info("newReplicatedPartitions"+s[j]+" " +!cor.replicatedPartitions.contains(s[j]) +" " + !cor.tempSetCurReplicas.contains(s[j]));
					if (!cor.replicatedPartitions.contains(s[j]) && !cor.tempSetCurReplicas.contains(s[j]))
					//if (!cor.tempSetCurReplicas.contains(s[j])) // Actually we need also here to replicate to def paritions to ask the cache to keep it at the def .
					{
						// here we need to replicate at this partition because the object was not replicated before and we need to replicate it  at this partition
						
						curObjToReplicatetempSet.add(cor);
						curProcessedObj++;
						cor.tempSetCurReplicas.add(s[j]);
						cor.toReplicateCurrent.add(s[j]);
						Analyser.log.info("replicateNEW ... "+cor.cacheKey +"at partition="+s[j] );
					}
					else 
					{
						// do nothinng: already replicated before
						
					}
				}
				
				for (int j=0;j<tempOldReplicatedPartitions.size();j++)
				{
					Analyser.log.info("tempOldReplicatedPartitions"+tempOldReplicatedPartitions.get(j));
					if (!newReplicatedPartitions.contains(tempOldReplicatedPartitions.get(j)) && !cor.tempSetCurReplicas.contains(tempOldReplicatedPartitions.get(j)))
					{
						// here we need to UNreplicate at this partition because the object was not replicated before and we need to replicate it  at this partition
						curObjToUnReplicatetempSet.add(cor);
						curProcessedObj++;
						cor.tempSetCurReplicas.add(tempOldReplicatedPartitions.get(j));
						
						cor.toUnReplicateCurrent.add(tempOldReplicatedPartitions.get(j));
						cor.replicatedPartitions.remove(tempOldReplicatedPartitions.get(j));
						Analyser.log.info("UnreplicateNEW ... "+cor.cacheKey +"at partition="+tempOldReplicatedPartitions.get(j) );
						
						
					}
					else 
					{
						// do nothinng: already replicated before
					}
					
					if (cor.toUnReplicateCurrent==null)
					{
						Analyser.log.info("***CHCHCHCHCHCH***");
					}
						
				}
		
				
				
				
			}
			
		}
	}
	
	public void startNewMonitoringInterval(int newInteveral)
	{
		Analyser.log.info("startNewMonitoringInterval");
		keepRunning=true;
		sm.globalCacheMap.clear();
		sm.globalRequestMap.clear();
		
		sm.acceptLogs=true;
		currentlyMainting=true;
		int curInt=newInteveral;
		

		sm.readLocalCount=0;
		sm.readRemotecount=0;
		sm.updateRemoteCount=0;
		
		for (CacheObjectReplicationCandidate cor:sm.distributedObject) // this is to empty the current temp replicated Object
			cor.tempSetCurReplicas.clear();
		
		
		
	}
	
	public void stopCurrentMonitoringInterval()
	{
		Analyser.log.info("stopCurrentMonitoringInterval");
		currentlyMainting=false;
		curLocalInterval=0;
		curObjToReplicatetempSet.clear();
		curObjToUnReplicatetempSet.clear();
		curProcessedObj=0;
		
		sm.readLocalCount=0;
		sm.readRemotecount=0;
		sm.updateRemoteCount=0;
		
		for (CacheObjectReplicationCandidate cor:sm.distributedObject) // this is to empty the current temp replicated Object
			cor.tempSetCurReplicas.clear();
		
		
		
	}
	
	
	
	

}
