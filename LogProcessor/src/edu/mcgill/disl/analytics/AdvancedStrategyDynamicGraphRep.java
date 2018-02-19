package edu.mcgill.disl.analytics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.Map.Entry;

import javax.swing.text.html.MinimalHTMLWriter;

import org.jgrapht.graph.DefaultEdge;

import edu.mcgill.disl.analytics.CacheLogList.CacheLogSegment;
import edu.mcgill.disl.analytics.policy.AppServerPolicy;
import edu.mcgill.disl.analytics.policy.ListKey;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy.ServerInfo;
import edu.mcgill.disl.analytics.policy.ObjectKey;
import edu.mcgill.disl.analytics.policy.RuleList;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.log.replicationRegressionInfo;
import edu.mcgill.disl.log.processor.CacheLogProcessor;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;

///////////////// THIS IS Request-based GRAPH ALGORITHM

/**
 * 
 * For this request based strategy we are not replicating objects.
 * 
 * how to come up with replication factor:
 * 
 * to = total no of objects logged/accenew filessed
 * final int[] cacheObjMax = new int[manager.servers.size()];
		Arrays.fill(cacheObjMax, analyser.cacheSz/MAX_ALLOC_DIVISOR + ((analyser.cacheSz*20)/100)); //20% extra buffer
		
		final int[] stableMax = new int[manager.servers.size()];
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

public class AdvancedStrategyDynamicGraphRep extends RequestBasedAnalyserGraphMetisStrategyRep {
	
	public AdvancedStrategyDynamicGraphRep(RequestBasedAnalyserGraphMetisRep analyser) {
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
	
	public int[][] combinations ;
	
	public int[] serverSpanCandidate ;
	
	public int[] stableMax;
	public int[] reqAllocated;
	public List<HttpRequestObject> recursieveRec = new ArrayList<HttpRequestObject>();
	public List<HttpRequestObject> overLoadedRec = new ArrayList<HttpRequestObject>();
	public List<HttpRequestObject> sortedOverLoadedRec = new ArrayList<HttpRequestObject>();
	public List<Integer> overLoadedPartitions = new ArrayList<Integer>();
	public List<Integer> underLoadedPartitions = new ArrayList<Integer>();
	public List<String> permPartServ=new ArrayList<String>();	
	public HashMap<Integer, Integer> serverSpanCandidateHash = new  HashMap<Integer,Integer>();
	
	public int totalSearchReq=0;
	public int totalOtherReq=0;
	
	
	
	public int totalOverlap=0;		
	//total number of object accessed in last interval
	
	
	//ArrayList<String> toServers = new ArrayList<String>(manager.servers.size());
	public ArrayList<String> toServers;
	 HashMap<ASServer, Integer> servIndexReverse = new HashMap<ASServer,Integer>();
	 public Set<String> ObjSpanList = new HashSet<String>();
	
	
	
	 public int foundBefore=0;
	public int [] server0Req= new int [5];
	public int [] server1Req= new int [5];
	public int [] server2Req= new int [5];
	public int [] server3Req= new int [5];
	public List<Integer> servers=new ArrayList<Integer>();
	
	
	
	
	
	

	public void printServReq(int [] serverReq)
	{
		int total=0;
		for (int i=0;i<serverReq.length; i++)
		{
			//Analyser.log.info("part"+ i +" "+ serverReq[i]);
			total+=serverReq[i];
		}
		double tot= total;
		double d=0.10100;
		double s=0.0;
		
		String servDist="";
		for (int i=0;i<serverReq.length; i++)
		{
			 String numServers=Integer.toString(manager.servers.size());
			 String filePath=analyser.filePath;
			 String partioner=analyser.partioner;
			 String inputGraphFile=analyser.inputGraphFile;
			 String resultGraphFile=analyser.inputGraphFile+".part.";
			d=((double)serverReq[i]/ tot) *100.0;
			servDist=servDist+ " " +d;
			Analyser.log.info("part"+ i +" "+ serverReq[i] +"per " +d);
			//total+=serverReq[i];
		}
		Analyser.log.info("servDist = " + servDist);
		Analyser.log.info("total req = " + total);
	}
	
	public void initializePartitionsStats(ASServer serv )
	{		
		serv.partReq.put("searchRequests", 0);
		serv.partReq.put("viewItem", 0);
		serv.partReq.put("viewBidHistory", 0);
		serv.partReq.put("viewUserInformation", 0);
		
	}
	
public void analyzeUrlPartition(String url, ASServer serv) {	
		
		if (url.contains("browseItemsInRegion") || url.contains("browseItemsInCategory") || url.contains("browseCategoriesInRegion") || url.contains("browseRegions") || url.contains("browseCategories") )
		{
			int freq= serv.partReq.get("searchRequests");
			freq++;
			serv.partReq.put("searchRequests", freq);
			totalSearchReq++;
			//Analyser.log.info("searchRequests freq++"); 
		}
		else 
		{
			int freq= serv.partReq.get("viewItem");
			freq++;
			serv.partReq.put("viewItem", freq);	
			totalOtherReq++;
			//Analyser.log.info("viewItem freq++");
		}
		/*
		else if (url.contains("Others") )
		{
			int freq= serv.partReq.get("viewItem");
			freq++;
			serv.partReq.put("viewItem", freq);	
			//Analyser.log.info("viewItem freq++");
		}
		else if (url.contains("viewBidHistory") )
		{
			int freq= serv.partReq.get("viewBidHistory");
			freq++;
			serv.partReq.put("viewBidHistory", freq);
		//	Analyser.log.info("viewBidHistory freq++");
		}
		else if (url.contains("viewUserInformation") )
		{
			int freq= serv.partReq.get("viewUserInformation");
			freq++;
			serv.partReq.put("viewUserInformation", freq);
			//Analyser.log.info("viewUserInformation freq++");
		}
		
		*/

		/*
		 * 
		 * if (cat >0 && cat <= partSize) serverReq[0]++; else if (cat >partSize
		 * && cat <= 2*partSize) serverReq[1]++; else if (cat >2*partSize && cat
		 * <= 3*partSize) serverReq[2]++; else if (cat >3*partSize && cat <=
		 * 4*partSize) serverReq[3]++; else if (4*cat >partSize && cat <=
		 * 5*partSize) serverReq[4]++;
		 */
		// r=s.split(s.substring(4).toString());
		

	}
	


public void printPartReq(ASServer serv)
{
	Iterator<String> ir=serv.partReq.keySet().iterator();
	while (ir.hasNext())
	{
		String key= ir.next();
		if (key.contains("search"))
		{
			double v1=Math.round((((double)serv.partReq.get(key)/(double)totalSearchReq))*1000.0)/1000.0;
			//Analyser.log.info(key +" == " + serv.partReq.get(key));
			Analyser.log.info(key +" == " + (v1*100) +"%");
		}
		else if (key.contains("viewItem"))
		{
			double v1=Math.round((((double)serv.partReq.get(key)/(double)totalOtherReq))*1000.0)/1000.0;
		//	Analyser.log.info(key +" = " + serv.partReq.get(key));
			Analyser.log.info(key +" == " + (v1*100) +"%");
		}
		//Analyser.log.info(key +" = " + serv.partReq.get(key));
		
	}
	
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
	
	// CALL THE HMETIS LIBRARY PARTIONER FROM WITHIN JAVA
	public void doGraphPartition() throws IOException, InterruptedException
	{

		
		Analyser.log.info("START PARTIONING ");
		
		 String numServers=Integer.toString(manager.servers.size());
		// String numServers=Integer.toString(1);
		 String filePath=analyser.filePath;
		 String partioner=analyser.partioner;
		 String partionerOld=analyser.partionerOld;
		 String inputGraphFile=analyser.inputGraphFile;
		// String resultGraphFile=analyser.inputGraphFile+".part.";
		 
		 String filePathNewMetis=analyser.filePathNewMetis;
		 String partionerNewMetis=analyser.partionerNewMetis;
		 String[] params = null;
		 Process proc=null;
		// directly from command line 
		 //./khmetis req.hgr 2 6 1 1 1 1 1
		
		 String[] paramsOld = new String []{
				 partioner,
				 inputGraphFile,
				 numServers,
					"6",
					"1",
					"1",
					"1",
					"1",
					"1"
					};
		 
		 if (analyser.graphType.equals("hg"))
		    { 
		 params = new String []{
				 partioner,
				 inputGraphFile,
				 numServers,
				  "6"
					};
		  proc = Runtime.getRuntime().exec(params, null,new File (filePath));
		    }
		 
		 if (analyser.graphType.equals("g"))
		    {
			 	params = new String []{
				 partionerNewMetis,
				 "-ptype=rb",
				 inputGraphFile,
				 numServers
					};
			 	 proc = Runtime.getRuntime().exec(params, null,new File (filePathNewMetis)); // gPMETIS
		    }
		 
		 
		// Process proc = Runtime.getRuntime().exec(params, null,new File (filePath)); // hMETIS
		 
		//  proc = Runtime.getRuntime().exec(paramsNewMetis, null,new File (filePathNewMetis)); // gPMETIS
		 
		 
		 BufferedReader input = new BufferedReader(new InputStreamReader(proc.getInputStream()));

	        String line=input.readLine();
	        
	        /*
	        while (line== null) 
	        {
	        	Analyser.log.info("line null sleep 2s ");
	        	Thread.sleep(500);
	        	 proc = Runtime.getRuntime().exec(params, null,new File (filePath));
	   		  input = new BufferedReader(new InputStreamReader(proc.getInputStream()));
	   		line=input.readLine();
	        }
	        
	        */
	        
	        
	        
	        while ((line = input.readLine()) != null) 
	        {
	        	Analyser.log.info("line "+line);
	        }

	        input.close();
	        
	        Analyser.log.info("END PARTIONING ");
	    }
		
	
	// THIS METHOD TO CHECK IF THE CURRENT REQUEST SHARED REQUESTS THAT HAVE BEEN ASSIGNED TO DIFFERENT SERVER!
	
	public boolean hasSharedReqWithAnotherPartNew(String request, ASServer reqServ)
	{
		
		
		Set<DefaultEdge> edges= analyser.currentAnalysisPhase.requestsGraph.edgesOf(request);
		if (edges==null)
			return false;
		else
		{
			
			for (DefaultEdge edge:edges)
				{
				String SubV=analyser.currentAnalysisPhase.requestsGraph.getEdgeTarget(edge);
		
				HttpRequestObject ro=manager.globalRequestMap.get(SubV);
				if (!ro.candidate.equals(reqServ))
					return true;
				
				
				}
		}
		return false;
	}
	
	
	
	
	// this is to read Giant vertices
	public void doReadPreviousGiantVertices()
	{
		for (String httpReqObj:manager.globalRequestMap.keySet())
		{
			if (!manager.ReqOldLoc.containsKey(httpReqObj))
				continue;
    	
        	
        	HttpRequestObject ro=manager.globalRequestMap.get(httpReqObj);		        	
        	if (ro==null)
        	{
                Analyser.log.info("ro==null"+ro);
        		continue;
        	}
    	
        	HashSet<String> objects2 = manager.globalRequestToObjectMap.get(ro.url); 
        	
        	if (objects2!=null)
        	{
        		String serv=manager.ReqOldLoc.get(httpReqObj);
        		
        		
        		manager.getASServer(serv).curReqListString.add(httpReqObj);
        		ro.candidate=manager.getASServer(serv);
        		manager.getASServer(serv).tmpObjList.addAll(objects2);
        		
        		manager.ReqCurLoc.put(httpReqObj, manager.getASServer(serv).serverId);

        	}
			
		}
		
	}
	
	public boolean giantVertex(String vertex)
	{
		for(String as:manager.servers.keySet())
		{
			if (as.contains(vertex))
					return true;
		}
		return false;
	}
	
	
	//READ THE PARTITOIN RESULTS FROM THE TEXT FILE AND STORE THE REQUESTS IN (curReqListString) 
	
	
	
	//READ THE PARTITOIN RESULTS FROM THE TEXT FILE AND STORE THE REQUESTS IN (curReqListString) 
	private void doReadPartitionResults() throws IOException
	{
		
		Analyser.log.info("doReadPartitionResults ");
		
		String filePath="";
		 if (analyser.graphType.equals("hg"))
		    { 
		  filePath=analyser.filePath;
		    }
		 else if (analyser.graphType.equals("g"))
		 {
			  filePath=analyser.filePathNewMetis;
		 }
		
		
		Analyser.log.info("analyser.currentAnalysisPhase.requestIdIndex.size() " +analyser.currentAnalysisPhase.requestIdIndex.size());
		
		 
		
	
		String numServers=Integer.toString(manager.servers.size());
		
		//this is for hMETIS
		 //String resultGraphFile=filePath+analyser.inputGraphFile+".part."+numServers;
		 //BufferedReader br = new BufferedReader(new FileReader(resultGraphFile));
		 
		// this is for gpMETIS
		 String resultGraphFileNewMetis=filePath+analyser.inputGraphFile+".part."+numServers;
		BufferedReader br = new BufferedReader(new FileReader(resultGraphFileNewMetis));
		
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();
	        int counter=1;
	        //Analyser.log.info("analyser.currentAnalysisPhase.mergedHttpMapAll " +analyser.currentAnalysisPhase.mergedHttpMapAll.size());
	        while (line != null)
	        {
	        	//Analyser.log.info("counter "+counter);
	        	//Analyser.log.info("request "+analyser.requestIdIndex.get(counter));
	        	//Analyser.log.info("line " +line);
	        	//Analyser.log.info("server"+ analyser.currentAnalysisPhase.servIndex.get((Integer.parseInt(line))).serverId );
	        	
	        	
	        	//String httpReqObj=analyser.currentAnalysisPhase.requestIdIndex.get(counter);
	        	
	        	String httpReqObj=manager.gloablRequestIndexing.get(counter);
	        	
	        	
	        	
	        	HttpRequestObject ro=manager.globalRequestMap.get(httpReqObj);
	        	
	        	
	        	
	        	if (ro==null)
	        	{
	        		 line = br.readLine();
	                 counter++;
	                 Analyser.log.info("ro==null"+ro);
	        		continue;
	        	}
	        		
	        		
	        	
	        	HashSet<String> objects2 = manager.globalRequestToObjectMap.get(ro.url); 
	        	
	        	
	        	
	        //	HashSet<String> objects2 = analyser.reqToResAll.map.get(curReq);
	        	//Analyser.log.info("req = "+ro.url);
	        	
	        	if (objects2!=null)
	        	{
	        		// in case we use replicate popular object is used and the request is toReplicate
	        		if (analyser.replicationStrategy.equals("popular") && (ro.toReplicate))
	        		{
	        			for (ASServer serv:manager.servers.values())
	        			{
	        				serv.curReqListString.add(httpReqObj);
	        				ro.candidate=null;
	        				manager.ReqCurLoc.put(httpReqObj,null);
	        				
	        				
	        				
	        			}
	        		}
	        		
	        		if (analyser.replicationStrategy.equals("regression") && (ro.toReplicate))
	        		{
	        			for (ASServer serv:manager.servers.values())
	        			{
	        				serv.curReqListString.add(httpReqObj);
	        				ro.candidate=null;
	        				manager.ReqCurLoc.put(httpReqObj,null);
	        				
	        				
	        				
	        			}
	        		}
	        		
	        		else
	        		{
	        		manager.getASServer((Integer.parseInt(line))).curReqListString.add(httpReqObj);
	        		ro.candidate=manager.getASServer((Integer.parseInt(line)));
	       // 		manager.getASServer((Integer.parseInt(line))).tmpObjList.addAll(objects2);
	        		
	        		manager.ReqCurLoc.put(httpReqObj, manager.getASServer((Integer.parseInt(line))).serverId);
	        		}
	        		
	        		
	        		
	        		//analyser.currentAnalysisPhase.servIndex.get((Integer.parseInt(line))).curReqListString.add(httpReqObj);
	        	//	ro.candidate=analyser.currentAnalysisPhase.servIndex.get((Integer.parseInt(line)));
	        		//analyser.currentAnalysisPhase.servIndex.get((Integer.parseInt(line))).tmpObjList.addAll(objects2);
	            sb.append(line);
	        	}
	        	else
	        	{
	        		Analyser.log.info("objects is null ");
	        	}
	           // sb.append(System.lineSeparator());
	            line = br.readLine();
	            counter++;
	            
	        }
	        Analyser.log.info("doReadPartitionResults 2"+ counter);
	   
	}
	
	// for overloaded partition, sort requests ascendingly
	// get rid of requests starting from the least weighted one
	// add them to a temp list
	// after done with all overloaded partitions then take the list and for each request check for the underloaded partitions that has the max object overlap 
	// put the request objects excluding the ones that have been assigned before to this partition 
	// update the partition weights
	//continue
	
	
	public void fillHashMap()
	{
		for (ASServer serv:manager.servers.values())
		{
			if (serverSpanCandidateHash.containsKey(serv.serverNo))
				serverSpanCandidateHash.put(serv.serverNo,0);
			
			
			if (cacheObjAllocated[serv.serverNo]>analyser.currentAnalysisPhase.servCapObj)
				serverSpanCandidateHash.remove(serv.serverNo);
				
		}
	}
	
	public void fillHashMap(boolean replicate)
	{
		for (ASServer serv:manager.servers.values())
		{
			if (serverSpanCandidateHash.containsKey(serv.serverNo))
				serverSpanCandidateHash.put(serv.serverNo,0);
			
			
			if (!replicate)
				if (cacheObjAllocated[serv.serverNo]>analyser.servCapObj)
					serverSpanCandidateHash.remove(serv.serverNo);
				
		}
	}
	
	public void fillHashMapReplicatePopular()
	{
		for (ASServer serv:manager.servers.values())
		{
			if (serverSpanCandidateHash.containsKey(serv.serverNo))
				serverSpanCandidateHash.put(serv.serverNo,0);
			
			
			
				if (serverSpanCandidateHash.containsKey(serv.serverNo)&& cacheObjAllocated[serv.serverNo]>serv.totalObjectCapacity)
				{
					Analyser.log.info("now remove server = "+serv.serverNo +"cacheAlloc="+cacheObjAllocated[serv.serverNo]);
					serverSpanCandidateHash.remove(serv.serverNo);
				}
				
		}
	}
	
	public List<CacheObject> sortObjectList(List<String> objectList,final boolean desc)
	{
		
		//List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map.values());
		
		List<CacheObject> map=new ArrayList<CacheObject>();
		Iterator<String> ir=objectList.iterator();
		while (ir.hasNext())
		{
			String key=ir.next();
			CacheObject co=manager.globalCacheMap.get(key);
			map.add(co);
		}
		
		Collections.sort(map, new Comparator<CacheObject>(){

			@Override
			public int compare(CacheObject o1, CacheObject o2) 
			{
				return  desc? (int)o2.getCount - (int)o1.getCount : (int)o1.getCount - (int)o2.getCount;
			}
			
		});
		
	return map;
		
	}
	
	
	public void fillReplicationRegressionOutput(int index)
	{
		for (Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet()) 
		{
			String cacheKey = entry.getKey(); // cacheKey is the object
			CacheObject co=manager.globalCacheMap.get(cacheKey);
			
			System.out.println("co.local_hit=" + co.local_hit +"co.remote_hit="+co.remote_hit);
			
			//analyser.manager.replRegInfoInstance.addOutput(cacheKey, index, (double)co.local_hit);
		//	analyser.manager.replRegInfoInstance.addOutputRemote(cacheKey, index, (double)co.remote_hit);
			
			
		//	long avgEecTime=co.totalExecTime/Long.valueOf(co.getCount);
			long avgEecTime=co.totalExecTime/Long.valueOf(co.totalCount);
			
			analyser.manager.replRegInfoInstance.addOutput(cacheKey, index, (double) avgEecTime);
		}
	}
	
	
	public void printUpdateInfoTemp() throws IOException
	{
		StringBuilder sb = new StringBuilder();
		Analyser.log.info("fillReplicationRegressionFeatures...");
		int totalObjectDistFreq=0;
		for (Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet()) 
		{
			String newString="";
			int objectDistFreq=0;
			HashSet<String> requests = null;
			String cacheKey = entry.getKey(); // cacheKey is the object
			CacheObject co=manager.globalCacheMap.get(cacheKey);
			newString=newString+"obj="+co.cacheKey;
			newString+=" totFreq="+co.totalCount;
			newString+=" localFreq="+ String.valueOf((double)co.local_hit/(double)co.totalCount);
			newString+=" disFreq="+ String.valueOf((double)co.remote_hit/(double) co.totalCount);
			newString+=" updateFreq="+ String.valueOf((double)co.updateCount/(double)co.totalCount);
			newString+=" locLock="+ String.valueOf(co.locLock);
			newString+=" remLock="+ String.valueOf(co.remLock);
			newString+=" updtLock="+ String.valueOf(co.updtLock);
			newString+=" cacheUpdt="+ String.valueOf(co.cacheUpdt);
			newString+=" updtCount="+ String.valueOf(co.updateCount);
			if (co.updateCount>0)
				newString+=" avgUptTime="+ (co.totalExecTimeUupdate/(long)co.updateCount)/1000000.0;
			newString+=" avgTime="+ (co.totalExecTime/(long)co.totalCount)/1000000.0;
			
			
			sb.append((double) co.totalCount +","); // 
			sb.append((double)co.local_hit/(double)co.totalCount+",");
			sb.append((double)co.remote_hit/(double)co.totalCount+",");
			sb.append((double)co.updateCount/(double)co.totalCount+",");
			if (co.updateCount>0)
				sb.append((co.totalExecTimeUupdate/(long)co.updateCount)/1000000.0+",");
			else 
				sb.append("0.0,");
			sb.append((co.totalExecTime/(long)co.totalCount)/1000000.0);
			sb.append("\n");
			Analyser.log.info(newString);			
		}
		
		PrintWriter out1 = null;
		 String  filename="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/training.csv";
		 out1 = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
		out1.println(sb.toString());
		out1.close();
		
		
	}
	
	public void fillReplicationRegressionFeatures()
	{
		Analyser.log.info("fillReplicationRegressionFeatures...");
		int totalObjectDistFreq=0;
		for (Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet()) 
		{
			
			int objectDistFreq=0;
			HashSet<String> requests = null;
			String cacheKey = entry.getKey(); // cacheKey is the object
			CacheObject co=manager.globalCacheMap.get(cacheKey);
			
			//Analyser.log.info("fillReplicationRegressionFeatures..."+cacheKey);
			
			analyser.manager.replRegInfoInstance.addFeature(cacheKey, replicationRegressionInfo.readFreq, (double)co.getCount);
			
			analyser.manager.replRegInfoInstance.addFeature(cacheKey, replicationRegressionInfo.updateFreq, (double)co.updateCount);
			
			long avgEecTime=co.totalExecTime/Long.valueOf(co.totalCount);
			
			analyser.manager.replRegInfoInstance.addFeature(cacheKey, replicationRegressionInfo.resTime, (double) avgEecTime);
			
			//analyser.manager.replRegInfoInstance.addFeature(cacheKey, replicationRegressionInfo.writeFreq, (double)co.updateCount);
			
			
			requests = entry.getValue();
			if (requests == null) 
			{
				Analyser.log.info(" doFindSpanObjsnew requests==null="+ cacheKey);
				analyser.manager.replRegInfoInstance.addFeature(cacheKey, replicationRegressionInfo.distFreq, objectDistFreq);
				continue;
			}
			
			Iterator<String> it=requests.iterator();
			
			while (it.hasNext()) 
			{
				String req=it.next();
				if (!manager.globalRequestMap.containsKey(req))
				{
					analyser.manager.replRegInfoInstance.addFeature(cacheKey, replicationRegressionInfo.distFreq, objectDistFreq);
					continue;
				}
				HttpRequestObject ro = manager.globalRequestMap.get(req);
				
				if (co.sites.contains(ro.candidate.serverId))
				{
					
					//analyser.manager.replRegInfoInstance.addFeature(cacheKey, replicationRegressionInfo.writeFreq, objectDistFreq);
				}
				
					//ro.candidate.localAccess+=ro.counter;
				else
				{
					Analyser.log.info(" "+ cacheKey);
					objectDistFreq+=ro.counter;
					Analyser.log.info(" Distributed Objects"+ cacheKey + "ro.counte"+ro.counter);
					//ro.candidate.remoteAccess+=ro.counter;
					//manager.servers.get(ro.candidate.serverId).remoteAccess+=ro.counter;
					/*
					Iterator<String> ir=co.sites.iterator();
					while (ir.hasNext())
					{
						objectDistFreq+=ro.counter;
					}
					*/
				}
				
			}
			analyser.manager.replRegInfoInstance.addFeature(cacheKey, replicationRegressionInfo.distFreq, objectDistFreq);
			totalObjectDistFreq+=objectDistFreq;
		}
		
		
		
		int totalObjectFreqs=0;
		for (CacheObject co:manager.globalCacheMap.values())
			totalObjectFreqs+=co.getCount;
		
		Analyser.log.info("totalObjectDistFreq="+totalObjectDistFreq +"totalObjFreqs="+totalObjectFreqs +"analyser.totalFreqs="+analyser.totalFreqs);
		
		analyser.manager.replRegInfoInstance.totalObjectDistFreq=totalObjectDistFreq;
		analyser.manager.replRegInfoInstance.totalObjectFreq=totalObjectFreqs;
		
	}
	
	@SuppressWarnings("unchecked")
	private void doAssignAllObjFirstLowUpdate()
	{
		
		Analyser.log.info(" doAssignAllObjFirstTemp analyser.servCapObj="+analyser.servCapObj);
		List sortedKeys=new  ArrayList<CacheObject>();
		
		
		//List KeysTemp=new ArrayList(manager.globalObjectToRequestMap.keySet());
		
		List KeysTemp=new ArrayList(manager.globalCacheMap.keySet());
		
		
		//Collections.sort(sortedKeys, Collections.reverseOrder());
		
		sortedKeys=sortObjectList(KeysTemp, true);
		
		//Iterator<String> iterator = sortedKeysTemp.iterator();
		Iterator<CacheObject> iterator = sortedKeys.iterator();
		
		int tempNoSites=0;
		
		int objToReplicateCount=0;
		
		objectWhileLoop:
		while (iterator.hasNext())
		{
			//objCounter++;
			HashSet<String> requests = null;
			CacheObject co=iterator.next();
			String cacheKey =co.cacheKey; // cacheKey is the object
			CacheObject co1=manager.globalCacheMap.get(cacheKey);
			
			requests = manager.globalObjectToRequestMap.get(cacheKey);
		
			if (requests == null) {

				Analyser.log.info(" doFindSanObjsnew requests==null="
						+ cacheKey);
				continue;
			}
		//	if (manager.globalCacheMap.get(cacheKey).getCount<2)
		//		continue;
				
		//	Analyser.log.info("----------------------------------- ");
		//	Analyser.log.info("cacheKey = "+cacheKey + "counter="+objCounter);
			

			Iterator<String> it = requests.iterator();
			
			// serverSpanCandidate is an array initialized for each object and
			// it contains the key as a server and a value equal how many times a particular server is accessed by 
			// all requests accessing this particular object
			Arrays.fill(serverSpanCandidate, 0);
			
			boolean replicate=true;
			
			fillHashMapReplicatePopular();
			
			while (it.hasNext()) 
			{
				
				String reqString=it.next();
				if (!manager.globalRequestToObjectMap.containsKey(reqString))
				{
					//Analyser.log.info("continue1");
					continue;
				}
				if (!manager.globalRequestMap.containsKey(reqString))
				{
				//	Analyser.log.info("continue2");
					continue;
				}
			
				
				
				if (manager.globalRequestMap.containsKey(reqString) && co1!=null && manager.globalRequestMap.get(reqString).toReplicate && co1.updateCount<=analyser.MaxUpdateCount )
				{
					for (ASServer serv:manager.servers.values())
        			{				
						serv.curObjList.add(cacheKey);
						cacheObjAllocated[serv.serverNo]++;
						co1.sites.add(serv.serverId);
						co1.replicate=true;
						serv.totalObjectCapacity++;
					//	Analyser.log.info("max Server = "+serv.serverId +"cacheObjAllocated"+cacheObjAllocated[serv.serverNo]);

        			}
					
					objToReplicateCount++;
					//Analyser.log.info("to replicate"+objToReplicateCount);
					continue objectWhileLoop;
				}

				
				//Analyser.log.info("reqString = "+reqString);
				
				
				HttpRequestObject ro = manager.globalRequestMap.get(reqString);
				
				//Analyser.log.info("reqString="+reqString);
				//Analyser.log.info(" ro.candidate.serverNo=" +ro.candidate.serverNo);
				
				serverSpanCandidate[ro.candidate.serverNo] += ro.counter;
				
				// new omar
				if (serverSpanCandidateHash.containsKey(ro.candidate.serverNo))
				{
					//int temp=0;
				int temp=serverSpanCandidateHash.get(ro.candidate.serverNo);
				temp+=ro.counter;
				serverSpanCandidateHash.put(ro.candidate.serverNo, temp);
				//Analyser.log.info("temp = "+temp);
				}
				//noOfSuccessfulTimes++;
				
				
			//	Analyser.log.info(requests.size()+"ro.candidate.serverNo = "+ro.candidate.serverNo + "size ="+serverSpanCandidateHash.get(ro.candidate.serverNo));

			}
			//if (noOfSuccessfulTimes==0)
				//continue;
			int maxServ = 0;

			
			/*
			for (int i = 0; i < manager.servers.size(); i++) {
				if (serverSpanCandidate[i] > serverSpanCandidate[maxServ])
					maxServ = i;
			}
			*/
			// new omar
			
			
			//HERE TO REPLICATE
			
			
			 
		
			
			
			
			Iterator irr = serverSpanCandidateHash.keySet().iterator();
			maxServ=(int) irr.next();
			
			Iterator ir = serverSpanCandidateHash.keySet().iterator();
			while (ir.hasNext())
			{
				int x=(int) ir.next();
				if (serverSpanCandidateHash.get(x) > serverSpanCandidateHash.get(maxServ))
					maxServ = x;
				
			}
			
			// /////
			
		//	if (serverSpanCandidateHash.get(maxServ)==0)
		//		continue;
				
					
					
			ASServer as = manager.getASServer(maxServ);
			as.curObjList.add(cacheKey);
			
			//by omar
			cacheObjAllocated[as.serverNo]++;
			//Analyser.log.info("max Server = "+as.serverId +"cacheObjAllocated"+cacheObjAllocated[as.serverNo]);
			
			if (manager.globalCacheMap.containsKey(cacheKey))
				manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
			//	Analyser.log.info("max Server = "+as.serverId);

		
			
		}
		Analyser.log.info("No. of object to replicate= "+objToReplicateCount);
	}
	
	
	
	@SuppressWarnings("unchecked")
	private void doAssignAllObjFirstReplicatePopular()
	{
		
		Analyser.log.info(" doAssignAllObjFirstTemp analyser.servCapObj="+analyser.servCapObj);
		List sortedKeys=new  ArrayList<CacheObject>();
		
		
		//List KeysTemp=new ArrayList(manager.globalObjectToRequestMap.keySet());
		
		List KeysTemp=new ArrayList(manager.globalCacheMap.keySet());
		
		
		//Collections.sort(sortedKeys, Collections.reverseOrder());
		
		sortedKeys=sortObjectList(KeysTemp, true);
		
		//Iterator<String> iterator = sortedKeysTemp.iterator();
		Iterator<CacheObject> iterator = sortedKeys.iterator();
		
		int tempNoSites=0;
		
		int objToReplicateCount=0;
		
		objectWhileLoop:
		while (iterator.hasNext())
		{
			//objCounter++;
			HashSet<String> requests = null;
			CacheObject co=iterator.next();
			String cacheKey =co.cacheKey; // cacheKey is the object
			CacheObject co1=manager.globalCacheMap.get(cacheKey);
			
			requests = manager.globalObjectToRequestMap.get(cacheKey);
		
			if (requests == null) {

				Analyser.log.info(" doFindSpغanObjsnew requests==null="
						+ cacheKey);
				continue;
			}
		//	if (manager.globalCacheMap.get(cacheKey).getCount<2)
		//		continue;
				
		//	Analyser.log.info("----------------------------------- ");
		//	Analyser.log.info("cacheKey = "+cacheKey + "counter="+objCounter);
			

			Iterator<String> it = requests.iterator();
			
			// serverSpanCandidate is an array initialized for each object and
			// it contains the key as a server and a value equal how many times a particular server is accessed by 
			// all requests accessing this particular object
			Arrays.fill(serverSpanCandidate, 0);
			
			boolean replicate=true;
			
			fillHashMapReplicatePopular();
			
			while (it.hasNext()) 
			{
				
				String reqString=it.next();
				if (!manager.globalRequestToObjectMap.containsKey(reqString))
				{
					//Analyser.log.info("continue1");
					continue;
				}
				if (!manager.globalRequestMap.containsKey(reqString))
				{
				//	Analyser.log.info("continue2");
					continue;
				}
			
				
				
				if (manager.globalRequestMap.containsKey(reqString) && co1!=null && manager.globalRequestMap.get(reqString).toReplicate)
				{
					for (ASServer serv:manager.servers.values())
        			{				
						serv.curObjList.add(cacheKey);
						cacheObjAllocated[serv.serverNo]++;
						co1.sites.add(serv.serverId);
						co1.replicate=true;
						serv.totalObjectCapacity++;
					//	Analyser.log.info("max Server = "+serv.serverId +"cacheObjAllocated"+cacheObjAllocated[serv.serverNo]);

        			}
					
					objToReplicateCount++;
					//Analyser.log.info("to replicate"+objToReplicateCount);
					continue objectWhileLoop;
				}

				
				//Analyser.log.info("reqString = "+reqString);
				
				
				HttpRequestObject ro = manager.globalRequestMap.get(reqString);
				
				//Analyser.log.info("reqString="+reqString);
				//Analyser.log.info(" ro.candidate.serverNo=" +ro.candidate.serverNo);
				
				serverSpanCandidate[ro.candidate.serverNo] += ro.counter;
				
				// new omaraa
				if (serverSpanCandidateHash.containsKey(ro.candidate.serverNo))
				{
					//int temp=0;
				int temp=serverSpanCandidateHash.get(ro.candidate.serverNo);
				temp+=ro.counter;
				serverSpanCandidateHash.put(ro.candidate.serverNo, temp);
				//Analyser.log.info("temp = "+temp);
				}
				//noOfSuccessfulTimes++;
				
				
			//	Analyser.log.info(requests.size()+"ro.candidate.serverNo = "+ro.candidate.serverNo + "size ="+serverSpanCandidateHash.get(ro.candidate.serverNo));

			}
			//if (noOfSuccessfulTimes==0)
				//continue;
			int maxServ = 0;

			
			/*
			for (int i = 0; i < manager.servers.size(); i++) {
				if (serverSpanCandidate[i] > serverSpanCandidate[maxServ])
					maxServ = i;
			}
			*/
			// new omar
			
			
			//HERE TO REPLICATE
			
			
			 
		
			
			
			
			Iterator irr = serverSpanCandidateHash.keySet().iterator();
			maxServ=(int) irr.next();
			
			Iterator ir = serverSpanCandidateHash.keySet().iterator();
			while (ir.hasNext())
			{
				int x=(int) ir.next();
				if (serverSpanCandidateHash.get(x) > serverSpanCandidateHash.get(maxServ))
					maxServ = x;
				
			}
			
			// /////
			
		//	if (serverSpanCandidateHash.get(maxServ)==0)
		//		continue;
				
					
					
			ASServer as = manager.getASServer(maxServ);
			as.curObjList.add(cacheKey);
			
			//by omar
			cacheObjAllocated[as.serverNo]++;
			//Analyser.log.info("max Server = "+as.serverId +"cacheObjAllocated"+cacheObjAllocated[as.serverNo]);
			
			if (manager.globalCacheMap.containsKey(cacheKey))
				manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
			//	Analyser.log.info("max Server = "+as.serverId);

		
			
		}
		Analyser.log.info("No. of object to replicate= "+objToReplicateCount);
	}
	
	
	
	@SuppressWarnings("unchecked")
	private void doAssignAllObjFirstReplicateDistributed()
	{
		
		int totalNumberOfPhysicalObject=0;
		
		Analyser.log.info("NEWWWWW OMARRR doAssignAllObjFirstTemp analyser.servCapObj="+analyser.servCapObj);
		List sortedKeys=new  ArrayList<CacheObject>();
		
		
		//List KeysTemp=new ArrayList(manager.globalObjectToRequestMap.keySet());
		
		List KeysTemp=new ArrayList(manager.globalCacheMap.keySet());
		
		
		//Collections.sort(sortedKeys, Collections.reverseOrder());
		
		sortedKeys=sortObjectList(KeysTemp, true);
		
		//Iterator<String> iterator = sortedKeysTemp.iterator();
		Iterator<CacheObject> iterator = sortedKeys.iterator();
		
		int tempNoSites=0;
		while (iterator.hasNext())
		{
			HashSet<String> requests = null;
			CacheObject co=iterator.next();
			String cacheKey =co.cacheKey; // cacheKey is the object
			requests = manager.globalObjectToRequestMap.get(cacheKey);
		
			if (requests == null) {

				Analyser.log.info(" doFindSpanObjsnew requests==null="
						+ cacheKey);
				continue;
			}
		//	if (manager.globalCacheMap.get(cacheKey).getCount<2)
		//		continue;
				
		//	Analyser.log.info("----------------------------------- ");
		//	Analyser.log.info("cacheKey = "+cacheKey);
			

			Iterator<String> it = requests.iterator();
			
			// serverSpanCandidate is an array initialized for each object and
			// it contains the key as a server and a value equal how many times a particular server is accessed by 
			// all requests accessing this particular object
			Arrays.fill(serverSpanCandidate, 0);
			
			boolean replicate=true;
			
			fillHashMap(replicate);
			
			while (it.hasNext()) 
			{
				String reqString=it.next();
				//Analyser.log.info("reqString = "+reqString);
				if (!manager.globalRequestToObjectMap.containsKey(reqString))
				{
					//Analyser.log.info("continue1");
					continue;
				}
				if (!manager.globalRequestMap.containsKey(reqString))
				{
				//	Analyser.log.info("continue2");
					continue;
				}
				
				HttpRequestObject ro = manager.globalRequestMap.get(reqString);
				
				
				
				serverSpanCandidate[ro.candidate.serverNo] += ro.counter;
				
				// new omar
				if (serverSpanCandidateHash.containsKey(ro.candidate.serverNo))
				{
					//int temp=0;
				int temp=serverSpanCandidateHash.get(ro.candidate.serverNo);
				temp+=ro.counter;
				serverSpanCandidateHash.put(ro.candidate.serverNo, temp);
				//Analyser.log.info("accessed by  = "+ro.candidate.serverNo);
				}
				//noOfSuccessfulTimes++;
				
				
			//	Analyser.log.info(requests.size()+"ro.candidate.serverNo = "+ro.candidate.serverNo + "size ="+serverSpanCandidateHash.get(ro.candidate.serverNo));

			}
			//if (noOfSuccessfulTimes==0)
				//continue;
			int maxServ = 0;

			
			/*
			for (int i = 0; i < manager.servers.size(); i++) {
				if (serverSpanCandidate[i] > serverSpanCandidate[maxServ])
					maxServ = i;
			}
			*/
			// new omar
			
			
			//HERE TO REPLICATE
			
			
			 
		
			if (analyser.replicationStrategy.equals("distributed"))
			{
							
				// this is to know if the current obj is accessed by mutliple partitions ==> replicate
				Iterator irrTemp = serverSpanCandidateHash.keySet().iterator();
				int spanServer=(int)irrTemp.next();
				
				spanServer=-1;
				
				//Analyser.log.info("to replicate ... co.cacheKey="+co.cacheKey +"x="+co.replicationFactor);
				Set <Integer> tempSet= new HashSet<>();
				
				
					Iterator ir = serverSpanCandidateHash.keySet().iterator();
					
					
					
					
					boolean toReplicateObject=false;
					startWhile:
					while (ir.hasNext())
					{
						
						int x=(int) ir.next();
						
						
						if (serverSpanCandidateHash.get(x)==0)
						{
							
						//	Analyser.log.info("continue");
							continue startWhile;
						}
						
						if (spanServer==-1)
						{
							//Analyser.log.info("spanServer==-1 Now "+x);
							spanServer=x;
						}
						else if (spanServer!=x)
						{
						//	Analyser.log.info("x!=spanServer=" +x);
							toReplicateObject=true;
							Analyser.log.info("------------------------------------");
							
						}
						
						//Analyser.log.info("spanServer="+spanServer +"x="+x+"toReplicateObject="+toReplicateObject);
							
					
						
						
						ASServer as = manager.getASServer(x);
						as.curObjList.add(cacheKey);
						
						
						
						//Analyser.log.info("as = "+as.serverId  +"cache size= "+as.curObjList.size());
						//by omar
						cacheObjAllocated[as.serverNo]++;
						
						if (manager.globalCacheMap.containsKey(cacheKey) )
						{
							manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
							
						}
						
					}
					
					if(toReplicateObject)
					{
						tempNoSites ++;
						Analyser.log.info("-------------------------"+manager.globalCacheMap.get(cacheKey).sites.size());
						Analyser.log.info("toReplicateObject"+cacheKey);
						manager.globalCacheMap.get(cacheKey).replicate=true;
						toReplicateObject=false;
						
						for (String serv:manager.globalCacheMap.get(cacheKey).sites)
						{
							Analyser.log.info("to server="+serv);
						}
						totalNumberOfPhysicalObject+=manager.globalCacheMap.get(cacheKey).sites.size();
							
						
					}
					else
						totalNumberOfPhysicalObject++;
					
					
					
					
				
				
			}
			
			
			
			else
			{
			Iterator irr = serverSpanCandidateHash.keySet().iterator();
			maxServ=(int) irr.next();
			
			Iterator ir = serverSpanCandidateHash.keySet().iterator();
			while (ir.hasNext())
			{
				int x=(int) ir.next();
				if (serverSpanCandidateHash.get(x) > serverSpanCandidateHash.get(maxServ))
					maxServ = x;
				
			}
			
			// /////
			
		//	if (serverSpanCandidateHash.get(maxServ)==0)
		//		continue
				
					
					
																																																																																																																																									ASServer as = manager.getASServer(maxServ);
			as.curObjList.add(cacheKey);
			
			//by omar
			cacheObjAllocated[as.serverNo]++;
			
			if (manager.globalCacheMap.containsKey(cacheKey))
				manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
			//	Analyser.log.info("max Server = "+as.serverId);

		}
			
		}
		Analyser.log.info("New No. of object to replicate="+tempNoSites);
		Analyser.log.info("No. of total physical object ="+totalNumberOfPhysicalObject);
		double repFactor= (double)totalNumberOfPhysicalObject/(double)manager.globalCacheMap.size();
		Analyser.log.info("Replication Factor ="+repFactor);
		
	}
	
	
	
	
	private void doAssignSomeObjectToReplicate()
	{
		
		for (int x=analyser.numServers;x>0;x--)
		{
			String cacheKey=analyser.manager.updatedObjects.get(x);
			CacheObject co1=analyser.manager.globalCacheMap.get(cacheKey);
			Analyser.log.info("cacheKey="+cacheKey +"no of replicas="+x);
			int c=0;
			while (c<x)
			{
				Analyser.log.info("cc2c="+c);
				ASServer serv=analyser.currentAnalysisPhase.servIndex.get(c);
				serv.curObjList.add(cacheKey);
				cacheObjAllocated[serv.serverNo]++;
				co1.sites.add(serv.serverId);
				co1.replicate=true;
				serv.totalObjectCapacity++;
				c++;
				
			}
			
		}
		
		
	}
	@SuppressWarnings("unchecked")
	private void doAssignAllObjFirstTempOri()
	{
		
		Analyser.log.info(" doAssignAllObjFirstTemp analyser.servCapObj="+analyser.servCapObj);
		List sortedKeys=new  ArrayList<CacheObject>();
		
		//replaced
		//globalCacheMap
	//	List KeysTemp=new ArrayList(manager.globalObjectToRequestMap.keySet());
		
		List KeysTemp=new ArrayList(manager.globalCacheMap.keySet());
		
		
		//Collections.sort(sortedKeys, Collections.reverseOrder());
		
		sortedKeys=sortObjectList(KeysTemp, true);
		
		//Iterator<String> iterator = sortedKeysTemp.iterator();
		Iterator<CacheObject> iterator = sortedKeys.iterator();
		
		while (iterator.hasNext())
		{
			HashSet<String> requests = null;
			CacheObject co=iterator.next();
			String cacheKey =co.cacheKey; // cacheKey is the object
			requests = manager.globalObjectToRequestMap.get(cacheKey);
		
			if (requests == null) {

			//	Analyser.log.info(" doFindSpanObjsnew requests==null="	+ cacheKey);
				continue;
			}
		//	if (manager.globalCacheMap.get(cacheKey).getCount<2)
		//		continue;
				
		//	Analyser.log.info("cacheKey = "+cacheKey);
			
			System.out.println("cacheKey="+cacheKey);
			
			Iterator<String> it = requests.iterator();
			Arrays.fill(serverSpanCandidate, 0);
			
			//fillHashMap();
			
			fillHashMap(true);
			
			
			
			while (it.hasNext()) 
			{
				String reqString=it.next();
				//Analyser.log.info("reqString = "+reqString);
				if (!manager.globalRequestToObjectMap.containsKey(reqString))
					continue;
				if (!manager.globalRequestMap.containsKey(reqString))
					continue;
				HttpRequestObject ro = manager.globalRequestMap.get(reqString);
				serverSpanCandidate[ro.candidate.serverNo] += ro.counter;
				
				// new omar
				if (serverSpanCandidateHash.containsKey(ro.candidate.serverNo))
				{
				int temp=serverSpanCandidateHash.get(ro.candidate.serverNo);
				temp+=ro.counter;
				serverSpanCandidateHash.put(ro.candidate.serverNo, temp);
				//Analyser.log.info("temp = "+temp);
				}
				//noOfSuccessfulTimes++;
				
				
			//	Analyser.log.info("ro.candidate.serverNo = "+ro.candidate.serverNo);

			}
			//if (noOfSuccessfulTimes==0)
				//continue;
			int maxServ = 0;

			
			/*
			for (int i = 0; i < manager.servers.size(); i++) {
				if (serverSpanCandidate[i] > serverSpanCandidate[maxServ])
					maxServ = i;
			}
			*/
			// new omar
			
			
			
			//here do something
			
			Iterator irr = serverSpanCandidateHash.keySet().iterator();
			maxServ=(int) irr.next();
			
			Iterator ir = serverSpanCandidateHash.keySet().iterator();
			while (ir.hasNext())
			{
				int x=(int) ir.next();
				if (serverSpanCandidateHash.get(x) > serverSpanCandidateHash.get(maxServ))
					maxServ = x;
				
			}
			
			
			
			Iterator ir1=serverSpanCandidateHash.entrySet().iterator();
			
			while (ir1.hasNext())
			{
				Entry <Integer,Integer> p=(Entry<Integer, Integer>) ir1.next();
				if (p.getValue()>0)
				{
					co.serverSpanCandidateHashObj.put(manager.getASServer(p.getKey()).serverId, (double)(p.getValue()));
					
				}
				System.out.println("key="+manager.getASServer(p.getKey()).serverId +"value="+p.getValue());
			}
			
			// /////
			
		//	if (serverSpanCandidateHash.get(maxServ)==0)
		//		continue;
				
					
					
			ASServer as = manager.getASServer(maxServ);
			as.curObjList.add(cacheKey);
			
			//by omar
			cacheObjAllocated[as.serverNo]++;
			
			if (manager.globalCacheMap.containsKey(cacheKey))
				manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
			//	Analyser.log.info("max Server = "+as.serverId);

		}
	}
	
	private void doAssignAllObjOld()
	{
		for(Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet())
		{
			
			HashSet<String> requests = null;
			String cacheKey=entry.getKey(); // cacheKey is the object
			requests=entry.getValue();
			if (requests==null)
				{
					
					Analyser.log.info(" doFindSpanObjsnew requests==null=" +cacheKey);
					continue;
				}
			
			ListKey lk = new ListKey();
			lk.addtoListKey(cacheKey);
			
			Iterator<String> it = requests.iterator();
			Arrays.fill(serverSpanCandidate, 0);
			while (it.hasNext())
			{
				HttpRequestObject ro=manager.globalRequestMap.get(it.next());
				serverSpanCandidate[ro.candidate.serverNo]+=ro.counter;
				
			}
			
			int maxServ=0;
			
			for (int i=0;i<manager.servers.size();i++)
			{
				if (serverSpanCandidate[i]>serverSpanCandidate[maxServ])
					maxServ=i;
			}
			///////
			ASServer as=manager.getASServer(maxServ);
			toServers.clear();
			toServers.add(as.getServerId());
			RuleList rl = new RuleList(as.serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
		//	as.currentPolicy.addNewPolicy(lk, rl);
			as.tmpPolicy.addNewPolicy(lk, rl);
			cacheObjAllocated[as.serverNo]++;
			as.curObjList.add(cacheKey);
			manager.globalCacheMap.get(cacheKey).assignedbefore=1;
			
			
		}
	}
	
	private void doRandomizeCombinations()
	{
		
		int servNo=manager.servers.size();
		int c=0;
		while (c<=(servNo-1))
		 {			
			
			Analyser.log.info("c=" + c +"servNo-1-c =" + (servNo-1-c));
			 manager.getASServer(c).tmpObjList.addAll(manager.getASServer(servNo-1-c).curObjList); // add all objects
			 manager.getASServer(c).tmpReqListString.addAll(manager.getASServer(servNo-1-c).curReqListString);
			 c++;
		 }
		
		for (ASServer as : manager.servers.values())
		{
			as.curObjList.clear();
			as.curReqListString.clear();
			
			as.curObjList.addAll(as.tmpObjList);
			as.curReqListString.addAll(as.tmpReqListString);
			
			as.tmpObjList.clear();
			as.tmpReqListString.clear();
		}

	}
	
	
	private void calculateLoadModel()
	{	
		for (Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet()) 
		{

			HashSet<String> requests = null;
			String cacheKey = entry.getKey(); // cacheKey is the object
			CacheObject co=manager.globalCacheMap.get(cacheKey);
			requests = entry.getValue();
			if (requests == null) 
			{
				Analyser.log.info(" doFindSpanObjsnew requests==null="+ cacheKey);
				continue;
			}
			
			Iterator<String> it=requests.iterator();
			
			while (it.hasNext()) 
			{
				String req=it.next();
				if (!manager.globalRequestMap.containsKey(req))
					continue;
				HttpRequestObject ro = manager.globalRequestMap.get(req);
				if (co.sites.contains(ro.candidate.serverId))
					manager.servers.get(ro.candidate.serverId).localAccess+=ro.counter;
					//ro.candidate.localAccess+=ro.counter;
				else
				{
					//ro.candidate.remoteAccess+=ro.counter;
					manager.servers.get(ro.candidate.serverId).remoteAccess+=ro.counter;
					Iterator<String> ir=co.sites.iterator();
					while (ir.hasNext())
					{
						manager.servers.get(ir.next()).fromRemoteAccess+=ro.counter;
					}
				}
				
			}

			
			
		}
	}
	
	
	
	private void generateReplicationFactor()
	{	
		int numberOfObjectToReplicate=0,totalNoOfObjectInTheCut=0;
		int numberOfPhysicalReplicatedObject=0;
		int totalNumberOfPhysicalObject=0;
		for (Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet()) 
		{

			int localAccess=0;
			int remoteAccess=0;
			int remoteSites=1;
			Set <String> remoteServers= new HashSet<String>();
			//HashMap <String, Double> partitionsRemoteNew= new HashMap<String, Double>();
			
			HashSet<String> requests = null;
			String cacheKey = entry.getKey(); // cacheKey is the object
			CacheObject co=manager.globalCacheMap.get(cacheKey);
			
			// object downcasting problem
	
			CacheObjectReplicationCandidate cor=  new CacheObjectReplicationCandidate();
			cor.cacheKey=co.cacheKey;
			cor.defaultPartitionLocation=manager.servers.get(co.sites.get(0));
			
			//	cor= (CacheObjectReplicationCandidate)co;
			
			requests = entry.getValue();
			if (requests == null) 
			{
				Analyser.log.info(" doFindSpanObjsnew requests==null="+ cacheKey);
				continue;
			}
			Analyser.log.info("=================="+co.cacheKey);
			
			Iterator<String> it=requests.iterator();
			
			while (it.hasNext()) 
			{
				String req=it.next();
				if (!manager.globalRequestMap.containsKey(req))
					continue;
				
				HttpRequestObject ro = manager.globalRequestMap.get(req);
				//Analyser.log.info();
				
				cor.distributedRequests.put(ro.url,ro.candidate.serverId);
				
				if (co.sites.contains(ro.candidate.serverId))
					localAccess+=ro.counter;
					//ro.candidate.localAccess+=ro.counter;
				else
				{
					//ro.candidate.remoteAccess+=ro.counter;
					remoteAccess+=ro.counter;
					remoteServers.add(ro.candidate.serverId);
					//cor.distributedRequests.add(ro.url);
					
					/*
					if (partitionsRemoteNew!=null && partitionsRemoteNew.get(ro.candidate.serverId)!=null)
					{
						double x=partitionsRemoteNew.get(ro.candidate.serverId)+ro.counter;
						partitionsRemoteNew.put(ro.candidate.serverId, x);
						
						
					}
					else
					{
						partitionsRemoteNew.put(ro.candidate.serverId, (double) ro.counter);
					}
					
					Analyser.log.info("=================="+partitionsRemoteNew.size());
					/*
					Iterator<String> ir=co.sites.iterator();
					while (ir.hasNext())
					{
						String s=ir.next();
						Analyser.log.info("s=" +s);
						//manager.servers.get(ir.next()).fromRemoteAccess+=ro.counter;
						remoteSites++;
					}
					*/
				}
				
			}

			
			int totalCall=localAccess+remoteAccess+co.updateCount;
			double rar=(double)remoteAccess/(double)totalCall;
			
			
			// ori
			//double ObjectReplicationFactorModel2= (remoteAccess/(double)totalCall)*(manager.readRemoteCost-manager.readLocalCost)-(((double)co.updateCount/(double)totalCall)*(double)manager.updateRemoteCost*(double)remoteServers.size());
			int replicaNo=0;
			double ObjectReplicationFactorModel2=0;  // this is the final model we using for generating replication factor
			boolean ReplicaPermutation=false;
			int NoOfReplicas=0;
			
			if  (remoteServers.size()>=1)
			{
				totalNoOfObjectInTheCut++;
				// here we need to use ReplicaPermutation
				
				Analyser.log.info("partitionsRemoteNew.size()=================="+co.serverSpanCandidateHashObj.size());
				
				
				
				for (String s:co.serverSpanCandidateHashObj.keySet())
				{
					System.out.println("sss="+s);
				}
				
				ReplicaPermutation rp= new ReplicaPermutation(co.updateCount,co.serverSpanCandidateHashObj,manager,cor.defaultPartitionLocation.serverId);
				String []replicatedPartitions=rp.getBestReplicaSetNew().split("/");
				
				/*
				rp.bestRepPermGlobal=co.sites.get(0);
				rp.maxPermGlobal=partitionsRemoteNew.get(rp.bestRepPermGlobal);
				
				rp.generateReplicasOptions();
				
				System.out.println(rp.getBestRep() +""+rp.getmaxPermGlobal());
				*/
				
				
				
				
				//String []s=rp.getBestRep().split("/");
				double sum=0;
				Analyser.log.info("replicatedPartitions.length"+replicatedPartitions.length);
				
				
				
				if (replicatedPartitions.length>1)
				{
					ReplicaPermutation=true;
					numberOfObjectToReplicate++;
					for (int j=0;j<replicatedPartitions.length;j++)
					{
						if (replicatedPartitions[j].contains("/") || replicatedPartitions[j]=="" || replicatedPartitions[j]==null )
							continue;
						Analyser.log.info("replicatedPartitions[j]"+replicatedPartitions[j]);
						ASServer as= manager.getASServer(replicatedPartitions[j]);
						replicateObject(co.cacheKey,as);
						cor.replicatedPartitions.add(replicatedPartitions[j]);
						cor.distributedPartitions.add(replicatedPartitions[j]);
						NoOfReplicas++;
						numberOfPhysicalReplicatedObject++;
						totalNumberOfPhysicalObject++;
					}
					
		
				}
				
				else
				{
					totalNumberOfPhysicalObject++;
				}
				
				
				//if (replicatedPartitions.length>1)
					//numberOfObjectToReplicate++;
				
				
				
				
				manager.distributedObject.add(cor);
				
			//	cor.distributedPartitions.addAll(co.serverSpanCandidateHashObj.keySet());
				
			}
			
			else if (remoteServers.size()==0)
			{
				totalNumberOfPhysicalObject++;
				cor.distributedPartitions.addAll(remoteServers); // this is to keep track of all partitions accessing this object
				
				
				replicaNo=remoteServers.size();
				replicaNo++; // this is because we need to measure update costs including the current location which means +1
				Analyser.log.info("replicaNo="+replicaNo +"UC(1)="+manager.replicateObjectCost.get(1)+" UC(N)="+manager.replicateObjectCost.get(replicaNo));
				Analyser.log.info(" manager.replicateObjectCost.get(replicaNo)="+ manager.replicateObjectCost.get(replicaNo));
				ObjectReplicationFactorModel2= remoteAccess*(manager.readRemoteCost-manager.readLocalCost)- co.updateCount*(manager.replicateObjectCost.get(replicaNo)-manager.replicateObjectCost.get(1));
				
				
				
				if (co.sites.size()>0)
				{

					cor.defaultPartitionLocation=manager.servers.get(co.sites.get(0));// add the default partition
				}
				cor.requests.addAll(requests); // add the set of requests accessing this object
				
				
				 
			}
			
				
				
			
			
			//ObjectReplicationFactorModel2=ObjectReplicationFactorModel2*(double)totalCall;
			
			//co.replicationFactor=(localAccess*manager.readLocalCost) + (remoteAccess*manager.readRemoteCost) - (co.updateCount*manager.updateRemoteCost*remoteServers.size());
			
			/*
			co.replicationFactor=ObjectReplicationFactorModel2;
			double resTimeDis=(localAccess*manager.readLocalCost) + (remoteAccess*manager.readRemoteCost) + (co.updateCount*manager.updateRemoteCost*remoteServers.size());
			double resTimeRep=(localAccess*manager.readLocalCost) + (remoteAccess*manager.readRemoteCost) + (co.updateCount*manager.updateRemoteCost*remoteServers.size());
			if ((ObjectReplicationFactorModel2>=0) && (co.updateCount>0) && (remoteAccess>0))
				Analyser.log.info("HERE IS BIGGER THAN ZERO");
			if (remoteAccess>0)
				Analyser.log.info(cacheKey+" Model2="+co.replicationFactor +   "La="+localAccess +"Ra="+remoteAccess +"Ua="+co.updateCount+"totalCall="+totalCall);
			
			
			*/
			//if ((ObjectReplicationFactorModel2>=0) && (remoteAccess>0) && !ReplicaPermutation)
			if ( NoOfReplicas>1 && (remoteAccess>0) && !ReplicaPermutation)
			{
				
			//	numberOfObjectToReplicate++;
				Iterator<String> ir=remoteServers.iterator();
				Analyser.log.info("replicate Object====="+co.cacheKey +"repFactor="+co.replicationFactor);
				while (ir.hasNext())
				{
					String server=ir.next();
					ASServer as= manager.getASServer(server);
					replicateObject(co.cacheKey,as);
					Analyser.log.info(server);
					cor.replicatedPartitions.add(server);
					
					
				}
				
			}
			
		}
		Analyser.log.info("NUMBER OF OBJECT TO REPLICATE====="+numberOfObjectToReplicate +"totalNoOfObjectInTheCut="+totalNoOfObjectInTheCut+" numberOfPhysicalReplicatedObject==="+numberOfPhysicalReplicatedObject);
		
		
		Analyser.log.info("totalNumberOfPhysicalObject ="+totalNumberOfPhysicalObject);
		Analyser.log.info("numberOfPhysicalReplicatedObject ="+numberOfPhysicalReplicatedObject);
		
		
		double repFactor= (double)totalNumberOfPhysicalObject/(double)manager.globalCacheMap.size();
		Analyser.log.info("Replication Factor ="+repFactor);
		
	}
	
	public void replicateObject(String cacheKey, ASServer as)
	{
	//	ASServer as = manager.getASServer(server);
		as.curObjList.add(cacheKey);
		
		//Analyser.log.info("as = "+as.serverId  +"cache size= "+as.curObjList.size());
		//by omar
		cacheObjAllocated[as.serverNo]++;
		
		if (manager.globalCacheMap.containsKey(cacheKey) )
		{
			manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
			
		}
		Analyser.log.info("toReplicateObject"+cacheKey +"as="+as.serverId);
		manager.globalCacheMap.get(cacheKey).replicate=true;
		
	
	
	}
	
	public void PRINTcalcLocalAccess()
	{
		for (ASServer serv: manager.servers.values())
		{
			Iterator<String> ir=serv.curReqListString.iterator();
			int wgt=0;
			while (ir.hasNext())
			{
				String req=ir.next();
				HttpRequestObject hr=manager.globalRequestMap.get(req);
				wgt=wgt+hr.weight;
			}
			
			Analyser.log.info(" server==="+ serv.serverId +"LocalAccess===" +wgt);
		}
	}
	
	private void doAssignAllObjNew() {
		for (ASServer as:manager.servers.values())
		{
			Iterator<String> ir=as.curObjList.iterator();
			while (ir.hasNext())
			{
				String cacheKey= ir.next();
				ListKey lk = new ListKey();
				lk.addtoListKey(cacheKey);
				toServers.clear();
				toServers.add(as.getServerId());
				RuleList rl = new RuleList(as.serverId, RuleList.REPLICATE,
						RuleList.STABLE_TTL, toServers);
				// as.currentPolicy.addNewPolicy(lk, rl);
				as.tmpPolicy.addNewPolicy(lk, rl);
				cacheObjAllocated[as.serverNo]++;
				as.curObjList.add(cacheKey);
				manager.globalCacheMap.get(cacheKey).assignedbefore = 1;				
			}
			
		}
	}
	
	private void doAssignAllObjNewTempReplicateDistributed() {
		for (ASServer as:manager.servers.values())
		{
			Iterator<String> ir=as.curObjList.iterator();
			while (ir.hasNext())
			{
				String cacheKey= ir.next();
				//Analyser.log.info(cacheKey);
				ListKey lk = new ListKey();
				lk.addtoListKey(cacheKey);
				toServers.clear();
				toServers.add(as.getServerId());
				RuleList rl;
				
				if (manager.globalCacheMap.containsKey(cacheKey) && manager.globalCacheMap.get(cacheKey).replicate)
				{
						 rl = new RuleList(as.serverId, RuleList.MOVE,	RuleList.STABLE_TTL, toServers);
					// Analyser.log.info(cacheKey+"replicate"+as.serverId);
				}
				else
				{
					    rl = new RuleList(as.serverId, RuleList.REPLICATE,	RuleList.STABLE_TTL, toServers);
					 //   Analyser.log.info(cacheKey+"not to replicate"+as.serverId);
				}
				// as.currentPolicy.addNewPolicy(lk, rl);
				as.tmpPolicy.addNewPolicy(lk, rl);
				cacheObjAllocated[as.serverNo]++;
				as.curObjList.add(cacheKey);
				if (manager.globalCacheMap.containsKey(cacheKey))
					manager.globalCacheMap.get(cacheKey).assignedbefore = 1;				
			}
			
		}
	}
	
	
	private void doAssignAllObjNewTempReplicatePopular() {
		for (ASServer as:manager.servers.values())
		{
			Iterator<String> ir=as.curObjList.iterator();
			while (ir.hasNext())
			{
				String cacheKey= ir.next();
				//Analyser.log.info(cacheKey);
				ListKey lk = new ListKey();
				lk.addtoListKey(cacheKey);
				toServers.clear();
				toServers.add(as.getServerId());
				RuleList rl;
				
				CacheObject co=manager.globalCacheMap.get(cacheKey);
				if (co==null)
					continue;
				
				if (co.replicate)
				{
						 rl = new RuleList(as.serverId, RuleList.REPLICATE_ALL,	RuleList.STABLE_TTL, toServers);
					 //Analyser.log.info(cacheKey+"replicate"+as.serverId);
				}
				else
				{
					    rl = new RuleList(as.serverId, RuleList.REPLICATE,	RuleList.STABLE_TTL, toServers);
					 //   Analyser.log.info(cacheKey+"not to replicate"+as.serverId);
				}
				// as.currentPolicy.addNewPolicy(lk, rl);
				as.tmpPolicy.addNewPolicy(lk, rl);
				cacheObjAllocated[as.serverNo]++;
				as.curObjList.add(cacheKey);
				if (manager.globalCacheMap.containsKey(cacheKey))
					manager.globalCacheMap.get(cacheKey).assignedbefore = 1;				
			}
			
		}
	}
	
	private void doAssignReq()
	{
		Analyser.log.info("doAssignReq ");
		
		for (int curServ=0;curServ<manager.servers.size();curServ++)
		{
			ASServer as;
			as=manager.getASServer(curServ);
			
			
			
			
			for (int reqId=0;reqId<as.curReqListString.size();reqId++)
			{
				
			    String curReq=as.curReqListString.get(reqId);
			   // Analyser.log.info(curReq);
			    HttpRequestObject httpReqObj=manager.globalRequestMap.get(curReq);
			   // Analyser.log.info("curReq = "+curReq);
			//    Analyser.log.info("assignedbefore = "+httpReqObj.assignedbefore);
			    if (httpReqObj==null)
			    	continue;
			    if (httpReqObj.assignedbefore==1)
			    	continue;
			    
			    
			   
	    			//	Analyser.log.info("ro2.url = "+ro2.url);
	    			//	 Analyser.log.info("REC before Size = "+recursieveRec.size());
	    				//analyser.currentLBPolicy.mapUrlToServers(curReq, Analyser.resolveHost(as.serverId));
	    				analyser.currentAnalysisPhase.currentLBPolicy.mapUrlToServers(curReq, Analyser.resolveHost(as.serverId));
	    				serverWeights[curServ]=serverWeights[curServ]+httpReqObj.weight;
	    				reqAllocated[curServ]++;
	    				//cacheObjAllocated[currentServer]=cacheObjAllocated[currentServer]+objects2.size();
	    			//	Analyser.log.info("REC cServer.serverId inside shared = "+cServer.serverId);
	    				//httpReqObj.assignedbefore=1;
	    			//	analyzeUrl(curReq, curServ);
	    				analyzeUrlPartition(curReq, as);
	    			
	        	}
			//Analyser.log.info("as.curReqListString.size() = "+as.curReqListString.size());
			
			Analyser.log.info("ServWeight= "+serverWeights[curServ]);
			Analyser.log.info("Difference Weight= "+ (serverWeights[curServ] - analyser.currentAnalysisPhase.servCap));
		}
		}
	
	
	private void doAssignReqBestMatch(String maxCombination)
	{
		Scanner sr = new Scanner(maxCombination).useDelimiter("/");
		
		Analyser.log.info("doAssignReq ");
		
		for (int curServ=0;curServ<manager.servers.size();curServ++)
		{
			
			
			ASServer as,asTemp = null;
			as=manager.getASServer(curServ);
			if (sr.hasNext())
				asTemp=manager.getASServer(Integer.parseInt(sr.next()));
			//asTemp=analyser.currentAnalysisPhase.servIndex.get(Integer.parseInt(sr.next()));
			
			
			for (int reqId=0;reqId<asTemp.curReqListString.size();reqId++)
			{
				
			    String curReq=asTemp.curReqListString.get(reqId);
			    HttpRequestObject httpReqObj=manager.globalRequestMap.get(curReq);
			    //HttpRequestObject httpReqObj=analyser.currentAnalysisPhase.mergedHttpMapAll.get(curReq);
			    
			   // Analyser.log.info("curReq = "+curReq);
			//    Analyser.log.info("assignedbefore = "+httpReqObj.assignedbefore);
			    if (httpReqObj==null)
			    	continue;
			    
			 //   if (httpReqObj.assignedbefore==1) // REM BY OMAR
			 //   	continue;
			    
			  
	    			//	 Analyser.log.info("REC before Size = "+recursieveRec.size());
	    				//analyser.currentLBPolicy.mapUrlToServers(curReq, Analyser.resolveHost(as.serverId));
	    				analyser.currentAnalysisPhase.currentLBPolicy.mapUrlToServers(curReq, Analyser.resolveHost(as.serverId));
	    				serverWeights[curServ]=serverWeights[curServ]+httpReqObj.weight;
	    				reqAllocated[curServ]++;
	    				//cacheObjAllocated[currentServer]=cacheObjAllocated[currentServer]+objects2.size();
	    			//	Analyser.log.info("REC cServer.serverId inside shared = "+cServer.serverId);
	    				httpReqObj.assignedbefore=1;
	    			
	    				//analyzeUrl(curReq, curServ);
	    			
	        	}
			//Analyser.log.info("as.curReqListString.size() = "+as.curReqListString.size());
			Analyser.log.info("ServWeight= "+serverWeights[curServ]);
			Analyser.log.info("ServWeight= "+serverWeights[curServ]);
			Analyser.log.info("Difference Weight= "+ (serverWeights[curServ] - analyser.currentAnalysisPhase.servCap));
		}
		}
	
	public String arraytoString(List <Integer> list)
	{
		String s="";
		for (Integer i:list)
		{
			s=s+"/"+i;
		}
		return s;
	}
	
	
	
	
	public void permute(List<Integer> arr, int k)
	   {
        for(int i = k; i < arr.size(); i++){
            java.util.Collections.swap(arr, i, k);
            permute(arr, k+1);
            java.util.Collections.swap(arr, k, i);
        }
        if (k == arr.size() -1)
        {
            System.out.println(Arrays.toString(arr.toArray()));
          //  Analyser.log.info("permute =" + Arrays.toString(arr.toArray()));
            //permPartServ.add(Arrays.toString(arr.toArray()));
            permPartServ.add(arraytoString(arr));
           // permPartServ.add(arr.toArray()));
        }
    }
   
	
	
	
	
	public static void main (String []args)
	{
		int[] ser=new int [3] ;
		int[] NS=new int [3] ;
		NS[0]=0;
		NS[1]=1;
		NS[2]=2;
		
		//permute(java.util.Arrays.asList(3,4,6,2), 0);
	
		//printPermutations(ser, NS, 0);
		
	}
	
	
	
	
	
	private int doCalculateIntesections(ASServer as1, ASServer as2) 
	{
		int counter=0;
		for (String cacheKey : as1.curObjList) 
		{
			if (as2.oldObjList.contains(cacheKey))
				counter++;
				
		}
		Analyser.log.info("as1="+as1.serverId +"as2="+as2.serverId +"counter= "+counter);
		
		return counter;
	}	
	
	private void doFindAllCombiations(String migrate) {
		permPartServ.clear();
		permute(servers,0);
		// Analyser.log.info("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

		for (ASServer as : manager.getASServers().values()) {//
																// Analyser.log.info("as1 cur serv card.="
																// +as.curObjBits.cardinality());

			for (ASServer as2 : manager.getASServers().values()) {
				// Analyser.log.info("as2 temp serv card.="
				// +as2.tmpObjBits.cardinality());
				combinations[as.serverNo][as2.serverNo] = doCalculateIntesections(
						as, as2);
			}
		}
		String bestComb="";
		
		if (migrate.equals("dm") || migrate.equals("both") )
		{
			 bestComb=doGetBestCombination();
			 Analyser.log.info("doGetBestCombination" + bestComb);
			 
		}
		else 
		{
			 bestComb=doGetWorstCombination();
			 Analyser.log.info("doGetWorstCombination" + bestComb);
		}
		
			
		

	}
	
	
	private String doGetWorstCombination()
	{
		int minCounter=100000000;
		String minCombination="";
		for (String sr: permPartServ)
		{
			int tempSum=0;
			int tempCounter=0;
			//Analyser.log.info("sr" + sr +"tempSum" +tempSum);
			 Scanner s = new Scanner(sr).useDelimiter("/");
			 while (s.hasNext())
			 {				 
				 tempSum+=combinations[Integer.parseInt(s.next())][tempCounter];
				 tempCounter++;
			 }
			 Analyser.log.info("sr" + sr +"tempSum" +tempSum);
			 if (tempSum<minCounter)
			 {
				 minCounter=tempSum;
				 minCombination=sr;
			 }
			 
			 
		}
		
		int [] newConfiguration= new int[manager.servers.size()];
		
		int tempCounter = 0;
		Scanner sr = new Scanner(minCombination).useDelimiter("/");
		while (sr.hasNext())
		 {				
			 
			int servNo=Integer.parseInt(sr.next()); 
			 manager.getASServer(tempCounter).tmpObjList.addAll(manager.getASServer(servNo).curObjList); // add all objects
			 manager.getASServer(tempCounter).tmpReqListString.addAll(manager.getASServer(servNo).curReqListString);
			 Analyser.log.info("servNo#"+servNo +"Assigned to ==>"+tempCounter);
			 Analyser.log.info("manager.getASServer("+tempCounter+").tmpObjList.size()"+manager.getASServer(tempCounter).tmpObjList.size());
			// this is wronggggg newConfiguration[tempCounter]=servNo; // this is to keep track of the default partitoin of the replicated object
			 newConfiguration[servNo]=tempCounter; // this is to keep track of the default partitoin of the replicated object
			 tempCounter++;
			 
		 }
		
		for (ASServer as : manager.servers.values())
		{
			as.curObjList.clear();
			as.curReqListString.clear();		
			as.curObjList.addAll(as.tmpObjList);
			as.curReqListString.addAll(as.tmpReqListString);
			as.tmpObjList.clear();
			as.tmpReqListString.clear();
		}
		
		
		
//		for (CacheObjectReplicationCandidate cor:manager.distributedObject)
//		{
//			int curServer=cor.defaultPartitionLocation.serverNo;
//			cor.defaultPartitionLocation=manager.getASServer(newConfiguration[curServer]);
//			 Analyser.log.info("object" + cor.cacheKey +"old=" +manager.getASServer(curServer).serverId + "new="+cor.defaultPartitionLocation.serverId);
//		}
		
		
		for (CacheObjectReplicationCandidate cor:manager.distributedObject)
		{
			cor.updateLocations(newConfiguration, manager);
			
			CacheObject co=manager.globalCacheMap.get(cor.cacheKey);
			co.updateLocations(newConfiguration, manager);
		}
		
		return minCombination;
	}
	
	
	private String doGetBestCombination()
	{
		int maxCounter=-1;
		String maxCombination="";
		for (String sr: permPartServ)
		{
			int tempSum=0;
			int tempCounter=0;
			//Analyser.log.info("sr" + sr +"tempSum" +tempSum);
			 Scanner s = new Scanner(sr).useDelimiter("/");
			 while (s.hasNext())
			 {				 
				 tempSum+=combinations[Integer.parseInt(s.next())][tempCounter];
				 tempCounter++;
			 }
			 Analyser.log.info("sr" + sr +"tempSum" +tempSum);
			 if (tempSum>maxCounter)
			 {
				 maxCounter=tempSum;
				 maxCombination=sr;
			 }
			 
			 
		}
		
		
		int tempCounter = 0;
		Scanner sr = new Scanner(maxCombination).useDelimiter("/");
		while (sr.hasNext())
		 {				 
			int servNo=Integer.parseInt(sr.next()); 
			 manager.getASServer(tempCounter).tmpObjList.addAll(manager.getASServer(servNo).curObjList); // add all objects
			 manager.getASServer(tempCounter).tmpReqListString.addAll(manager.getASServer(servNo).curReqListString);
			 tempCounter++;
		 }
		
		for (ASServer as : manager.servers.values())
		{
			as.curObjList.clear();
			as.curReqListString.clear();
			
			as.curObjList.addAll(as.tmpObjList);
			as.curReqListString.addAll(as.tmpReqListString);
			
			as.tmpObjList.clear();
			as.tmpReqListString.clear();
		}
		return maxCombination;
	}
	
	
	
	
	
	private void printBits()
	{
		Analyser.log.info("&&&&&&&&&&&&&&&&&&&&&&&&");
		for(ASServer as:manager.getASServers().values()){
			Analyser.log.info("doAssignObjTemp" + as.getServerId());
			Analyser.log.info("tmp serv card.=" +as.tmpObjBits.cardinality());
			Analyser.log.info("cur serv card.=" +as.curObjBits.cardinality());
		}
		Analyser.log.info("&&&&&&&&&&&&&&&&&&&&&&&&");
	}
	
	private void doAssignObj()
	{
		for(ASServer as:manager.servers.values())
		//for(ASServer as:manager.getASServers().values())
		{
			int tmp=1;
			Analyser.log.info("XXXXXXXXXXXXXXXXXXX" + as.getServerId());
			Analyser.log.info("as.curObjList.size()" + as.curObjList.size());
			//as.tmpPolicy.policyMap.clear();
			for (String cacheKey:as.curObjList)
			{
				ListKey lk = new ListKey();
				lk.addtoListKey(cacheKey);
				
				
				if(manager.globalCacheMap.get(cacheKey)==null)
				{
					Analyser.log.info("cachekey=null" + cacheKey);
					HashSet<String> reqqq=manager.globalObjectToRequestMap.get(cacheKey);
					Iterator<String> ir=reqqq.iterator();
					while (ir.hasNext())
					{
						String rew=ir.next();
						Analyser.log.info("ir.next()" + rew);
						Analyser.log.info("t=" + manager.globalRequestMap.get(rew).url);
						
						
						
					}
					
					tmp++;
					continue;
				}
					
				
				if (manager.globalCacheMap.get(cacheKey).assignedbefore==1)
				{
					Analyser.log.info("continue");
						continue;
				}
				
				
					toServers.clear();
					toServers.add(as.getServerId());
					RuleList rl = new RuleList(as.serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
				//	as.currentPolicy.addNewPolicy(lk, rl);
					as.tmpPolicy.addNewPolicy(lk, rl);
					cacheObjAllocated[as.serverNo]++;
					manager.globalCacheMap.get(cacheKey).assignedbefore=1;	
					//Analyser.log.info(cacheKey);
			}
			
			Analyser.log.info("serv.currentPolicy" + as.tmpPolicy.policyMap.size());
			Analyser.log.info("tmp" + tmp);
		}
	}
	
	
	// THIS METHOD ASSIGN OBJECTS IN THE CUT TO THE APPROPRAITE SERVER, IT CALCULATES FOR EACH SERVER THE GAIN OF ASSIGNING THAT OBJECT 
	// TO THE SERVER BY SUMMING ALL REQUEST ACCESS FREQs BELONG TO THAT PARTITION . THEN FIND THE SEVER WITH MAX GAIN AND ASSIGN OBJ TO.
	private void doAssignObjSpanList()
	{
		
		Analyser.log.info("doAssignObjSpanList ");
		Analyser.log.info("ObjSpanList.size "+ ObjSpanList.size());
			for (String cacheKey:ObjSpanList)
			{
				ListKey lk = new ListKey();
				lk.addtoListKey(cacheKey);
				

				if (manager.globalCacheMap.get(cacheKey).assignedbefore==1)
						continue;
				
					//////
					
					
					HashSet<String> requests = manager.globalObjectToRequestMap.get(cacheKey);;
					
					if (requests==null)
						continue;
					 List<String> tempReq = new ArrayList<String>(); // tempReq contains all requests that access an object o					
					Iterator<String> it = requests.iterator();
					HttpRequestObject candidateReq=null;
					int maxCounter=-1;
					Arrays.fill(serverSpanCandidate, 0);
					while (it.hasNext())
					{
						HttpRequestObject ro=manager.globalRequestMap.get(it.next());
						serverSpanCandidate[ro.candidate.serverNo]+=ro.counter;
						
					}
					
					int maxServ=0;
					
					for (int i=0;i<manager.servers.size();i++)
					{
						if (serverSpanCandidate[i]>serverSpanCandidate[maxServ])
							maxServ=i;
					}
					///////
					ASServer as=manager.getASServer(maxServ);
					toServers.clear();
					toServers.add(as.getServerId());
					RuleList rl = new RuleList(as.serverId, RuleList.REPLICATE, RuleList.STABLE_TTL, toServers);
				//	as.currentPolicy.addNewPolicy(lk, rl);
					as.tmpPolicy.addNewPolicy(lk, rl);
					cacheObjAllocated[as.serverNo]++;
					as.curObjList.add(cacheKey);
					manager.globalCacheMap.get(cacheKey).assignedbefore=1;
					
					
				
				
	    			
	        	
			}
		
	}
	

	
	
	@Override
	
	
	
	public boolean generatePolicies()throws Exception {
		
		
		
		
		/*
		for (String cs:manager.gloabCacheObjectIndexing.values())
		{
			Analyser.log.info("Object=" +cs +" " +manager.globalCacheMap.get(cs).index);
		}
		
		*/
		
		int counter=0;
		for(ASServer as:manager.getASServers().values())
		{
			Analyser.log.info("####################");
			Analyser.log.info("server" + as.getServerId());
			Analyser.log.info("cur serv card.=" +as.curObjBits.cardinality());
			counter+=as.curObjBits.cardinality();
			serverSpanCandidateHash.put(as.serverNo,0);
		}
		
		Analyser.log.info("card counter."+ counter);
		Analyser.log.info("analyser.currentAnalysisPhase.tester"+ analyser.currentAnalysisPhase.tester);
		
		
		 cacheObjAllocated = new int [manager.servers.size()];
		 serverWeights =new int [manager.servers.size()];
		 cacheObjMax =new int [manager.servers.size()];
		 serverSpanCandidate=new int[manager.servers.size()];
		 
		 combinations= new  int[manager.servers.size()][manager.servers.size()];
		 
		stableMax=new int [manager.servers.size()];
		
		 reqAllocated=new int [manager.servers.size()];
		 toServers = new ArrayList<String>(manager.servers.size());
		 
		Analyser.log.info("AdvancedStrategyDynamicGRAPHRep");
		
		
		
	
		 Arrays.fill(server0Req, 0);
			Arrays.fill(server1Req, 0);
			Arrays.fill(server2Req, 0);
			Arrays.fill(serverSpanCandidate, 0);
			
			for (ASServer serv: manager.servers.values())
			{
				initializePartitionsStats(serv);
			}
			
			//for (int[] row: combinations)
			 //   Arrays.fill(row,0);
			
			
		
		//get factors from config
		STABLE_PERCENT = Double.parseDouble(manager.props.getProperty("rba.stable.percent"));
		
		REPLICATE_PERCENT = Double.parseDouble(manager.props.getProperty("rba.replicate.percent"));
		
		MAX_ALLOC_DIVISOR = Integer.parseInt(manager.props.getProperty("rba.max.alloc.divisor"));
		
				
		// we should have all re.anaquired data structures ready by now..
		
		
		
		
		Arrays.fill(cacheObjAllocated, 0);
		
		Arrays.fill(reqAllocated, 0);
		
	//	 int[] serverWeights = new int[manager.servers.size()];
		 Arrays.fill(serverWeights, 0);
		
	//	final int[] cacheObjMax = new int[manager.servers.size()];
		Arrays.fill(cacheObjMax, analyser.cacheSz/MAX_ALLOC_DIVISOR + ((analyser.cacheSz*20)/100)); //20% extra buffer
		
	//	final int[] stableMax = new int[manager.servers.size()];
		Arrays.fill(stableMax, (int) ((double)analyser.cacheSz * STABLE_PERCENT));
		
		int totalOverlap=0;		
		//total number of object accessed in last interval
		
		
		
		//int totalObj = analyser.getUniqueResourceCount(analyser.manager.getASServers().values());
		
	//	Analyser.log.info("total Obj:" + totalObj);
		
		// to/tc = ops = objects per slot = ratio of objects to capacity => (repl 1 > tpc > 1 dist)

	//	double ops = (double)totalObj/(double)analyser.totalCapacity;
		
	//	Analyser.log.info("Objects Per Slot (ops):" + ops);
		
		double drp = REPLICATE_PERCENT;
		
		//erp = effective replication (computed) = drp/ops = erp { 1.0 erp>1.0 }
	//	double erp = drp/ops;
		
	//	Analyser.log.info("Effective Replication (erp) ratio:" + erp);
		
		 
		 
		
		 
		 servers.clear();
		 		
		for(ASServer serv:manager.getASServers().values())
		{
			System.out.print("`= "+serv.getServerId());
		//	analyser.currentAnalysisPhase.servIndex.put(serv.serverNo, serv);
			servIndexReverse.put(serv, serv.serverNo);
			servers.add(serv.serverNo);
			//servers2[cntr]=cntr;
	
		
		}
		
		for(ASServer serv:analyser.currentAnalysisPhase.servIndex.values()){
			
			//servers2[cntr]=cntr;
			serv.curReqListString.clear();
    		serv.tmpObjList.clear();
		
		}
		
		// create lbPolicy and ASPolicy objects040
		analyser.currentAnalysisPhase.currentLBPolicy = new LoadBalancerPolicy();
		
		
		
		for(ASServer serv:analyser.currentAnalysisPhase.servIndex.values()){
			serv.currentPolicy = new AppServerPolicy();
			serv.tmpPolicy= new AppServerPolicy(); 

		}
		//Analyser.log.info("Effective Replication (erp) ratio:" + erp);
	//	Analyser.log.info("Effective Replication (erp) ratio:" + erp);
		
		
	
		int oldPolicy=-1;
	
		
		
		 
		HttpRequestObject maxAccess=new HttpRequestObject();
		
		//Analyser.log.info("CacheLogProcessor.oldLB==null" + servIndex.get(0).oldLB==null);
		if (analyser.currentAnalysisPhase.servIndex.get(0).oldLB!=null)
		{
			oldPolicy=1;
		}
		Analyser.log.info("=====analyser.totalWeight======"+analyser.totalWeight);
		Analyser.log.info("=====analyser.servCap======"+analyser.servCap);
		Analyser.log.info("=====analyser.variation======"+analyser.variation);
		
		
		
		//Analyser.log.info("httpListAllNew.size()" + analyser.currentAnalysisPhase.httpListAll.size());
		Analyser.log.info("httpListAllNew.size()" + analyser.currentAnalysisPhase.httpListAll.size());
		
		
		
		/*
		for (int x=0;x<analyser.currentAnalysisPhase.httpListAll.size();x++)
		{
			Analyser.log.info("httpListAllNew.size()" + analyser.currentAnalysisPhase.httpListAll.get(x).url+" "+analyser.currentAnalysisPhase.httpListAll.get(x).counter);

		}
		*/
		
	////PARTITION'S STEPS		
			
///////////////
		
		totalSearchReq=0;
		totalOtherReq=0;
		
		
		if(!analyser.replicationStrategy.equals("popular-only"))
    	{
		long partitionBF=System.currentTimeMillis();
		doGraphPartition();		
		long  partitionAF=System.currentTimeMillis();
    	
		
		
		
		
		
		long assignBF=System.currentTimeMillis();
		

			doReadPartitionResults();
    	}
		
		/*
		for (ASServer as : manager.servers.values()) 
		{
			//Analyser.log.info("BEFORE==============================================================="+as.serverId);
			for (String s:as.curReqListString)
			{
				Analyser.log.info(s);
			}
			as.currentPolicy = as.tmpPolicy.clone();
		} */
		
		/*
		doFindSpanObjs();
		doAssignObj();
		
		
		
		
		
		
		
		
		for(int i=0;i<manager.servers.size();i++)
		{
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", obj:" + cacheObjAllocated[i]);
			
		}
		
		doAssignObjSpanList();
		
		*/
		
		
		long assignAF=System.currentTimeMillis();
		///////////////////////	

		
		
///////////////////////////////////////////////
/// START: WITH NEW-REMAP
		/*
		doAssignObjTemp();	//manager
		Analyser.log.info("GOOOOOOOO");
		permPartServ.clear();
		permute(servers,0);
		//printBits();
		doFindAllCombiations(); // manager
		String bestComb=doGetBestCombination();
		Analyser.log.info("Best Comb= "+bestComb);
		*/
		// END: WITH NEW-REMAP
		//////////////////////////////
		
		
		
		
		///////////////////////////////////////////////
		 /// START: WITHOUT NEW-REMAP
		
		
		
		if (analyser.replicationStrategy.equals("dynamic")  && manager.counter==0)
		{
			doAssignSomeObjectToReplicate();
		}
		else if (analyser.replicationStrategy.equals("dynamic") || analyser.replicationStrategy.equals("dynamic-no-parameters-extraction"))
		{
			//1- partition both requests and objects
			doAssignAllObjFirstTempOri();
			
			Analyser.log.info("manager.readLocalCost="+manager.readLocalCost);
			Analyser.log.info("manager.readRemoteCost="+manager.readRemoteCost);
			Analyser.log.info("manager.putLocalCost="+manager.putLocalCost);
			Analyser.log.info("manager.putRemoteCost="+manager.putRemoteCost);
			
			generateReplicationFactor();
			
			//doAssignAllObjFirstReplicateDistributed();
			
			// The first temp generated objectPolicy will be stored at as.curObjList
			// The first temp generated requestPolicy will be stored at as.curReqListString
			
			

			//1- tag each object with a replication factor
			//2- replicate all objects with replication factor >0
			//3- check the size of the caches and picks objects with higher replication factor
			
		}
		else if (analyser.replicationStrategy.equals("distributed"))
			doAssignAllObjFirstReplicateDistributed();
		else if(analyser.replicationStrategy.equals("popular"))
			doAssignAllObjFirstReplicatePopular();
		else if(analyser.replicationStrategy.equals("low-update"))
			doAssignAllObjFirstLowUpdate();
		else if(analyser.replicationStrategy.equals("regression"))
		{
			if (analyser.manager.counter==0)
			{
				doAssignAllObjFirstTempOri();
				
			}
			else if (analyser.manager.counter==1)
			{
				doAssignAllObjFirstReplicatePopular();
				
			}
			else if (analyser.manager.counter==2)
			{
				doAssignAllObjFirstReplicatePopular();
				
			}
		
			 
		}
		
		
		Arrays.fill(cacheObjAllocated, 0);
			
		
		
		//doAssignAllObj();
		
		
		Analyser.log.info("---------- BEFORE PARTITION ASSIGNMENT---------");
		for(int i=0;i<manager.servers.size();i++)
		{
			Analyser.log.info("Server:" + i + ", req:" + manager.getASServer(i).curReqListString.size() + ", obj:" + manager.getASServer(i).curObjList.size());
		}
		
		
		
		
		if(!analyser.replicationStrategy.equals("popular-only") || !(analyser.replicationStrategy.equals("regression") && analyser.manager.counter==1))
    	{
		
		/*
		if (analyser.useOptimisation.equals("dm") || analyser.useOptimisation.equals("both"))
			doFindAllCombiations(); // manager For Mig. Optimization 
		
		else if (analyser.manager.counter%2==1)
			doRandomizeCombinations(); //manager for non-data migration
		*/
			
			//this is for comination to keep
		doFindAllCombiations(analyser.useOptimisation);
			
		//End zz
		
		doAssignReq();
		
    	}
		
		if (analyser.replicationStrategy.equals("distributed") || analyser.replicationStrategy.equals("dynamic") || analyser.replicationStrategy.equals("dynamic-no-parameters-extraction"))
			doAssignAllObjNewTempReplicateDistributed();
		else if(analyser.replicationStrategy.equals("popular") || (analyser.replicationStrategy.equals("low-update")))
			doAssignAllObjNewTempReplicatePopular();
		else if(analyser.replicationStrategy.equals("popular-only"))
			doAssignAllObjNewTempReplicatePopular();
		else if(analyser.replicationStrategy.equals("regression"))
		{
			if (analyser.manager.counter==0) //here we distribute all but aggregate hit rate abou objects
			{
				//analyser.manager.replRegInfoInstance= new replicationRegressionInfo(analyser.manager.globalObjectToRequestMap.size());
				//doAssignAllObjNew();
				//fillReplicationRegressionFeatures();
				
				printUpdateInfoTemp();
			}
			
			else if (analyser.manager.counter==1) // here we replicate all but aggregate hit rate abou objects
			{
				fillReplicationRegressionOutput(0);
				doAssignAllObjNewTempReplicatePopular();
				
				
			}
			
			else if (analyser.manager.counter==2) // here we need only to aggregate hit rate abou objects
			{
				fillReplicationRegressionOutput(1);
				doAssignAllObjNewTempReplicatePopular();
				analyser.manager.replRegInfoInstance.doRegression(); // perform the regression task
				
				
			}
			
			 
		}
		//doAssignAllObjNewTemp();
		
		
		/*
		calculateLoadModel();
		PRINTcalcLocalAccess();
		
		
		for (ASServer as : manager.servers.values())
		{
			int totalLoad=0;
			totalLoad=(2*as.localAccess+3*(as.remoteAccess)+2*as.fromRemoteAccess)/7;
			as.totalLoad=totalLoad;
			Analyser.log.info("AS: "+as.serverId +" Load= "+ totalLoad);
			Analyser.log.info("as.localAccess="+ as.localAccess + "(as.remoteAccess)=" +as.remoteAccess +"as.fromRemoteAccess="+as.fromRemoteAccess);
			
			as.localAccess=0;
			as.remoteAccess=0;
			as.fromRemoteAccess=0;
			
			
		}
		
		*/
		for (ASServer as : manager.servers.values())
		{
			as.currentPolicy = as.tmpPolicy.clone();
			
			
			// check the policy size 
			int cacheSz = Integer.parseInt(manager.props.getProperty(StatisticsManager.AS_CACHE_SIZE));
			
			if (as.currentPolicy.policyMap.size()>cacheSz)
			{
				 List<CacheObject> tempMap = new ArrayList<CacheObject>();
				 tempMap.clear();
				for (Entry<ObjectKey, RuleList> es: as.currentPolicy.policyMap.entrySet())
				{
					ObjectKey ok=es.getKey();
					CacheObject co=manager.globalCacheMap.get(ok.toString());
					
					
					
					// in case of non distributed object .. let the tempPopularity equal its origianl popularity 
					if (co.serverSpanCandidateHashObj.size()==1)
					{
						co.tempPopulariyt=co.totalCount;
						double temp=(double)co.local_hit * (manager.fetchDBCost-manager.readLocalCost) + 	(double)(co.totalCount-co.local_hit) * (manager.fetchDBCost-manager.readRemoteCost);						
						co.tempPopulariyt=(int) temp;
					}
					//else if co.serverSpanCandidateHashObj.containsKey(as.c)
					
					else if (co.serverSpanCandidateHashObj.size()>1)
					{
						//CacheObjectReplicationCandidate cor=manager.distributedObject.
						//Analyser.log.info("co"+co.cacheKey +"server="+as.serverId);
						//Analyser.log.info(co.serverSpanCandidateHashObj.toString());
						//Analyser.log.info(co.serverSpanCandidateHashObj.keySet().toString());
						
						//double pop=co.serverSpanCandidateHashObj.get(as.serverId);
					//	co.tempPopulariyt= (int)pop;
						
						double remote=co.serverSpanCandidateHashObj.get(as.serverId);
						double temp= remote*(manager.readRemoteCost-manager.readLocalCost);
						co.tempPopulariyt=(int) temp;
						
						
					}
					
					
					
					tempMap.add(co);
				}
				
				ManagingCacheSize MCS= new ManagingCacheSize(cacheSz, tempMap);
				MCS.removeObjects();
				
				
				Analyser.log.info("number of objects to remove : "+MCS.toRemove.size() +" as.currentPolicy.policyMap Before= "+ as.currentPolicy.policyMap.size());
				if (MCS.toRemove.size()>0)
				{
					for (CacheObject co:MCS.toRemove)
					{
						ObjectKey ok=as.currentPolicy.getKey(co.cacheKey);
						if (ok!=null)
							as.currentPolicy.policyMap.remove(ok);
						
						//as.currentPolicy.policyMap.remove(co.cacheKey);
					}
				}
				Analyser.log.info("number of objects to remove : "+MCS.toRemove.size() +" as.currentPolicy.policyMap After= "+ as.currentPolicy.policyMap.size());
				
			}
			
			
			/*
			as.tempPolicytoDelete= new AppServerPolicy();
			
			
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(as.tmpPolicy);
			oos.flush();
			oos.close();
			bos.close();
			byte[] byteData = bos.toByteArray();
			
			ByteArrayInputStream bais = new ByteArrayInputStream(byteData);
			as.tempPolicytoDelete = (AppServerPolicy) new ObjectInputStream(bais).readObject();	
			*/
		}
		
		for (ASServer as : manager.servers.values()) 
		{
			Analyser.log.info(" " +as.serverId +" "+as.currentPolicy.policyMap.size());
		}
		
		
		
		/*
		
		
		
		for (ASServer as:manager.servers.values())
		 {				 
			 as.currentPolicy=as.tmpPolicy.clone();
		 }
		*/
		// END: WITHOUT NEW-REMAP
		//////////////////////////////
		
		
		/*
		if (analyser.prevStats.prevData.size()>0 && analyser.currentAnalysisPhase.newRemap==1)
		{
			Analyser.log.info("GOOOOOOOO");
			permPartServ.clear();
			permute(servers,0);
			//printBits();
			doFindAllCombiations(); // manager
			String bestComb=doGetBestCombination();
			Analyser.log.info("Best Comb= "+bestComb);
			
			 
		}		
			
		else
		{
			doAssignReq();	
			for (ASServer as:manager.servers.values())
			 {				 
				 as.currentPolicy=as.tmpPolicy.clone();
				// as.curObjBits=(BitSet) as.tmpObjBits.clone();
			 }
		}*/
		
		
		
		
		manager.ReqOldLoc.clear();
		//manager.ReqOldLoc=(HashMap<String, String>) manager.ReqCurLoc.clone();
		manager.ReqCurLoc.clear();
		manager.gloablRequestIndexingIncremental.clear();
		manager.gloablRequestIndexing.clear();
		
		
		
		Analyser.log.info("---------- AFTER MAIN ASSIGNMENT---------");
		Analyser.log.info("totalOverlap:" + totalOverlap);
		for(int i=0;i<manager.servers.size();i++)
		{
			
			Analyser.log.info("============================================================= "+manager.getASServer(i).serverId);
			Analyser.log.info("serverId = "+manager.getASServer(i).serverId);
			Analyser.log.info("as.curReqListString.size() = "+manager.getASServer(i).curReqListString.size());
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", obj:" + cacheObjAllocated[i]);
			
		//	manager.getASServer(i).curObjList.clear();
			manager.getASServer(i).curReqList.clear();
			manager.getASServer(i).curReqListString.clear();
			
			
		}
		
		
		
	
		
		int totalWeightAfter=0;
		analyser.currentAnalysisPhase.servIndex.get(0).oldLB=analyser.currentAnalysisPhase.currentLBPolicy.clone();
		Analyser.log.info("---------- AFTER LB ONLY POLICY---------");
		for(int i=0;i<manager.servers.size();i++){
			Analyser.log.info("Server:" + i + ", req:" + reqAllocated[i] + ", obj:" + cacheObjAllocated[i]);
			Analyser.log.info("serverWeights["+ analyser.currentAnalysisPhase.servIndex.get(i).serverId.toString()+"]:" + serverWeights[i]);
			totalWeightAfter=totalWeightAfter+serverWeights[i];
		}
		Analyser.log.info("totalWeightBefore:" +analyser.currentAnalysisPhase.totalWeight);
		Analyser.log.info("totalWeightAfter:" +totalWeightAfter);
		Analyser.log.info("analyser.httpListAll:" +analyser.currentAnalysisPhase.httpListAll.size());
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
			
			
			/*
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
			
			*/
			
			for (ASServer serv: manager.servers.values())
			{
				Analyser.log.info("****" +serv.serverId);
				printPartReq(serv);
			}
		
			
			//Analyser.log.info("===> Graph Partioning Time:" + (partitionAF - partitionBF));
		//	Analyser.log.info("===> Assignment Time:" + (assignAF - assignBF));

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


