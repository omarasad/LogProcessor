package edu.mcgill.disl.analytics;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import edu.mcgill.disl.log.StatisticsManager;

public class CacheObjectReplicationCandidate {
	
	public ASServer defaultPartitionLocation; // this is the default partition location of the object that is resulted of the parititoning...
	public Set <String> requests= new HashSet<String>(); // this is a set of all requests accessing this object
	public Set <String> replicatedPartitions =new HashSet<String>();  // this is a set of partitions that this object is being replicated at currently
	
	
	public HashMap <String,String> distributedRequests= new HashMap<String,String>(); // this is a map of all DISTRIBUTED requests accessing this object to their partitions
	public Set <String> distributedPartitions =new HashSet<String>();  // this is a set of partitions that this object is being accessed from
	
	public int lastReplicatedUnreplicatedInterval=-1; // this is to keep track of the last interval count this object has been replicated/unreplicated ... 
	// so that we might wait x number of intervals before deciding to update/unupdate again
	
	public double lastRepFactor=0.0;
	
	public Set<String> tempSetCurReplicas=new HashSet<String>(); // this is to keep track of which partitions this object has been replicated at this temp wrkld change/ so any replica
	// exists here we cannot replicate/unreplicate the object to the same replica again at this phase of wrkld change. 
	
	
	public Set<String> toReplicateCurrent=new HashSet<String>();
	public Set<String> toUnReplicateCurrent=new HashSet<String>();
	
	public String cacheKey="";
	
	public void updateLocations(int [] newConfiguration, StatisticsManager manager)
	{
		int curS=defaultPartitionLocation.serverNo;
		ASServer tempServer=defaultPartitionLocation;
		defaultPartitionLocation=manager.getASServer(newConfiguration[curS]);
		//Analyser.log.info("object" + cacheKey +"old=" +tempServer.serverId + "new="+defaultPartitionLocation.serverId);
		
		
		Set <String> temp =new HashSet<String>();
		
		for (String s:replicatedPartitions)
		{
			int curServer=manager.servers.get(s).serverNo;
			temp.add(manager.getASServer(newConfiguration[curServer]).serverId);
		}
		replicatedPartitions.clear();
		replicatedPartitions.addAll(temp);
		temp.clear();
		
		for (String s:distributedPartitions)
		{
			int curServer=manager.servers.get(s).serverNo;
			temp.add(manager.getASServer(newConfiguration[curServer]).serverId);
		}
		distributedPartitions.clear();
		distributedPartitions.addAll(temp);
		temp.clear();
		
		
		
		for (String s:tempSetCurReplicas)
		{
			int curServer=manager.servers.get(s).serverNo;
			temp.add(manager.getASServer(newConfiguration[curServer]).serverId);
		}
		tempSetCurReplicas.clear();
		tempSetCurReplicas.addAll(temp);
		temp.clear();
		
		
		HashMap <String,String> distributedRequeststemp= new HashMap<String,String>(); 
		for (Entry<String , String> s:distributedRequests.entrySet())
		{
			String serv=s.getValue();
			int curServer=manager.servers.get(serv).serverNo;
			distributedRequeststemp.put(s.getKey(),manager.getASServer(newConfiguration[curServer]).serverId);
			
		}
		distributedRequests.clear();
		distributedRequests.putAll(distributedRequeststemp);
		
		
		
	}
	

}
