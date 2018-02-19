package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import edu.mcgill.disl.log.StatisticsManager;

public class CacheObject implements Comparable<CacheObject>, Cloneable{

	public String cacheKey;
	public long first_get_time;
	public long last_get_time;
	public long first_put_time;
	public long last_put_time;
	
	public int getCount;
	public int putCount;
	public int updateCount;
	
	public int totalCount;
	
	public double replicationFactor; // This is for the dynamic replication strategy
	
	
	
	
	public float updateFactor;
	
	public long totalExecTime;
	
	
	public long totalExecTimeUupdate;
	
	public int locLock;
	public int remLock;
	public int updtLock;
	public int cacheUpdt;
	
	
	
	public int dist_freq; // this is for regression model for replication score
	
	public int local_hit;
	public int remote_hit;
	
	public String loggingLevel;
	public int size;
	public int no_sites;
	public List<String> sites;
	public int assignedbefore;
	public int index;
	public List<Integer> locations;
	public boolean replicate;
	
	public double tempreture;
	
	public List <Long> accessTimes;
	
	public String last_put_server;
	public long last_put_time_global;
	public float toConsider;
	public int candidate;
	public int tempPopulariyt; // this is for managing cache size
	
	
	public HashMap<String, Double> serverSpanCandidateHashObj ; // this is a list of all servers accessing this object with their freq.
	
	public CacheObject()
	{
		cacheKey = new String();
		first_get_time = 0;
		last_get_time = 0;
		first_put_time =0;
		last_put_time = 0;
		getCount = 0;
		putCount = 0;
		updateCount=0;
		updateFactor=0;
		size = 0;
		no_sites = 1;
		loggingLevel = new String();
		sites = new ArrayList<String>();
		locations= new ArrayList<Integer>();
		assignedbefore=-1;
		index=-1;
		last_put_server="";
		last_put_time_global=0;
		toConsider= -1;
		candidate=-1;
		accessTimes= new ArrayList<Long>();
		tempreture=-1;
		replicate=false;
		
		dist_freq=0; 
		local_hit=0;
		remote_hit=0;
		
		totalExecTime=0;
		
		totalCount=0;
		totalExecTimeUupdate=0;
		
		locLock=0;
		remLock=0;
		updtLock=0;
		cacheUpdt=0;
		replicationFactor=0.0;
		tempPopulariyt=0;
		
		//candidate=null;
		
		serverSpanCandidateHashObj= new  HashMap<String,Double>();
		
	}
	
	public String toString()
	{
		String result = new String();
		result = cacheKey + ":: getCount=" + getCount + "::NoSites=" + no_sites + "\n";
		return result;
	}

	@Override
	public int compareTo(CacheObject o) {
		// TODO Auto-generated method stub
		return (o.getCount - this.getCount);
	}
	
	public CacheObject cloneCO() {
		try
		{
		return (CacheObject) super.clone();
		}
		catch(Exception e){ return null; }
		}
	
	
	public void updateLocations(int [] newConfiguration, StatisticsManager manager)
	{
		HashMap <String,Double> distributedRequeststemp= new HashMap<String,Double>(); 
		for (Entry<String , Double> s:serverSpanCandidateHashObj.entrySet())
		{
			String serv=s.getKey();
			int curServer=manager.servers.get(serv).serverNo;
			distributedRequeststemp.put(manager.getASServer(newConfiguration[curServer]).serverId,s.getValue());
			
		}
		serverSpanCandidateHashObj.clear();
		serverSpanCandidateHashObj.putAll(distributedRequeststemp);
		
		
		
	}
	

}
