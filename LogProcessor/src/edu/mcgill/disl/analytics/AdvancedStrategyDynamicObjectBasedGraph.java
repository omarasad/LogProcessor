package edu.mcgill.disl.analytics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import edu.mcgill.disl.log.processor.CacheLogProcessor;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;

///////////////// THIS IS Object-based Hyper-GRAPH ALGORITHM

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

public class AdvancedStrategyDynamicObjectBasedGraph extends ObjectBasedAnalyserGraphStrategy {
	
	public AdvancedStrategyDynamicObjectBasedGraph(ObjectBasedAnalyserGraph analyser) {
		super(analyser);
	}
	
	
	public double STABLE_PERCENT = .2;
	
	public double REPLICATE_PERCENT = .1;
	
	public int MAX_ALLOC_DIVISOR = 1; // 1 is all cache, 2 is half, 3 is 1/3, 4 is quarter
	
	public int totalReqAllocated = 0;
	public int totalObjAllocated = 0;
	public int[] cacheObjAllocated ;
	public int[] serverWeights ;
	public int[] serverWeightsTemp ;

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
	public List<Integer> availableServers = new ArrayList<Integer>();
	
	public HashMap<Integer, Integer> serverSpanCandidateHash = new  HashMap<Integer,Integer>();

	
	
	
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
		 String inputGraphFile=analyser.inputGraphFile;
		 
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
		 
		
		 
		 
		 
		 
		// Process proc = Runtime.getRuntime().exec(paramsNewMetis, null,new File (filePathNewMetis));
		 
		 
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
		String filePath="";
		 if (analyser.graphType.equals("hg"))
		    { 
		  filePath=analyser.filePath;
		    }
		 else if (analyser.graphType.equals("g"))
		 {
			  filePath=analyser.filePathNewMetis;
		 }
			 
		String numServers=Integer.toString(analyser.numServers);
		 String resultGraphFile=filePath+analyser.inputGraphFile+".part."+numServers;
		 
		BufferedReader br = new BufferedReader(new FileReader(resultGraphFile));
		
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();
	        int counter=1;
	        while (line != null)
	        {
	        	//Analyser.log.info("counter "+counter);
	        	//Analyser.log.info("request "+manager.gloabCacheObjectIndexing.get(counter));
	        	//Analyser.log.info("line " +line);
	        	
	        	
	        	//Analyser.log.info("server"+ analyser.servIndex.get((Integer.parseInt(line))).serverId );
	        	
	       
	        	String cacheKey=manager.gloabCacheObjectIndexing.get(counter);
	        	
	        	if (cacheKey==null)
	        	{
	        		//Analyser.log.info("cacheKey==null " +counter);
	        		 sb.append(line);
	  	            line = br.readLine();
	  	            counter++;
	        		continue;
	        		
	        	}
	        	
	        	ASServer asServer=manager.getASServer((Integer.parseInt(line)));
	
	        	asServer.curObjList.add(cacheKey);
	 
	        	CacheObject co=manager.globalCacheMap.get(cacheKey);
	        	//Analyser.log.info("asServer " +asServer.serverNo);
	        
	        	if (co==null)
	        	{
	        		Analyser.log.info("co==null " +cacheKey);
	        		 sb.append(line);
	  	            line = br.readLine();
	  	            counter++;
	        		continue;
	        		
	        	}
	        	
	        	co.candidate=asServer.serverNo;
	        	
	        	
	        	
	        	if (manager.globalCacheMap.containsKey(cacheKey))
					manager.globalCacheMap.get(cacheKey).sites.add(asServer.serverId);
	        //	Analyser.log.info("co.candidate " +co.candidate);
	        	
	            sb.append(line);
	           // sb.append(System.lineSeparator());
	            line = br.readLine();
	            counter++;
	            
	        }
	        
	        Analyser.log.info("counter " +counter +"object size ="+manager.globalCacheMap.size());
	        
	        Analyser.log.info("---------- TEMP OMAR TEMP---------");
			for(int i=0;i<manager.servers.size();i++)
			{
				Analyser.log.info("Server:" + i + ", req:" + manager.getASServer(i).curReqListString.size() + ", obj:" + manager.getASServer(i).curObjList.size());
			}
	        
	   
	}
	
	
	private void doReadPartitionResultsSchismReplication() throws IOException
	{
		int totalNumberOfPhysicalObject=0;
		int objToRep=0;
		Analyser.log.info("doReadPartitionResultsSchismReplication");
		String filePath="";
		
		 if (analyser.graphType.equals("hg"))
		    { 
		  filePath=analyser.filePath;
		    }
		 else if (analyser.graphType.equals("g"))
		 {
			  filePath=analyser.filePathNewMetis;
		 }
			 
		String numServers=Integer.toString(analyser.numServers);
		String resultGraphFile=filePath+analyser.inputGraphFile+".part."+numServers;
		 
		BufferedReader br = new BufferedReader(new FileReader(resultGraphFile));
		
	        StringBuilder sb = new StringBuilder();
	        String line = br.readLine();
	        int counter=1;
	        while (line != null)
	        {
	        	//Analyser.log.info("counter "+counter);
	        	//Analyser.log.info("request "+manager.gloabCacheObjectIndexing.get(counter));
	        	//Analyser.log.info("line " +line);
	        	
	        	
	        	//Analyser.log.info("server"+ analyser.servIndex.get((Integer.parseInt(line))).serverId );
	        	
	        	CacheObject co=null;
	        	String cacheKey=manager.gloabCacheObjectIndexing.get(counter);
	        	
	        	//Analyser.log.info("cacheKey "+cacheKey);
	        	if (!manager.globalCacheMap.containsKey(cacheKey))
	        		{
	        		
	        		String oriCacheKey=analyser.getOriginalObjectSchismReplication(cacheKey);
	        		co=manager.globalCacheMap.get(oriCacheKey);
	        		cacheKey=oriCacheKey;
	        	
	        		}
	        	
	        	if (cacheKey==null)
	        	{
	        		//Analyser.log.info("cacheKey==null " +counter);
	        		 sb.append(line);
	  	            line = br.readLine();
	  	            counter++;
	        		continue;
	        		
	        	}
	        	
	        	ASServer asServer=manager.getASServer((Integer.parseInt(line)));
	
	        	asServer.curObjList.add(cacheKey);
	        	if (co==null)
	        		co=manager.globalCacheMap.get(cacheKey);
	        	//Analyser.log.info("asServer " +asServer.serverNo);
	        	
	        	
	        
	        	if (co==null)
	        	{
	        		Analyser.log.info("co==null " +cacheKey);
	        		 sb.append(line);
	  	            line = br.readLine();
	  	            counter++;
	        		continue;
	        		
	        	}
	        	
	        	//co.candidate=asServer.serverNo;
	        	
	        //	Analyser.log.info("asServer "+asServer.serverNo);
	        	
	        	if (manager.globalCacheMap.containsKey(cacheKey) && !manager.globalCacheMap.get(cacheKey).sites.contains(asServer.serverId))
	        	{
					manager.globalCacheMap.get(cacheKey).sites.add(asServer.serverId);
					// here we check if the object has been added before which means it is going to be replicated
					if (manager.globalCacheMap.get(cacheKey).sites.size()>1  && !co.replicate)
					{
						objToRep++;
						co.replicate=true;
						 //Analyser.log.info(objToRep +" replicate obj="+cacheKey); 
						// totalNumberOfPhysicalObject+=manager.globalCacheMap.get(cacheKey).sites.size();
					}
					
						
					
	        	}
	
	        	
	            sb.append(line);
	            line = br.readLine();
	            counter++;
	            
	        }
	        
	     //   Analyser.log.info("counter " +counter +"object size ="+manager.globalCacheMap.size());
	        
	        /*
	        for (CacheObject coTemp:manager.globalCacheMap.values())
	        {
	        	totalNumberOfPhysicalObject+=coTemp.sites.size();
	        }
	        */
	        
	        
	       // Analyser.log.info("---------- TEMP OMAR TEMP---------objToRep=" +objToRep);
	        Analyser.log.info("No. of object to replicate="+objToRep);
			//Analyser.log.info("No. of total physical object ="+totalNumberOfPhysicalObject);
			//double repFactor= (double)totalNumberOfPhysicalObject/(double)manager.globalCacheMap.size();
			//Analyser.log.info("Replication Factor ="+repFactor);
	        
	        
			for(int i=0;i<manager.servers.size();i++)
			{
				Analyser.log.info("Server:" + i + ", req:" + manager.getASServer(i).curReqListString.size() + ", obj:" + manager.getASServer(i).curObjList.size());
			}
			
			
			/*
			// TO PRINT THE RESULTS OF THE PARTITIONING ...
			 Analyser.log.info("---------- TEMP OMAR TEMPPP---------");
				for(int i=0;i<manager.servers.size();i++)
				{
					Analyser.log.info("Server:" + i + ", req:" + manager.getASServer(i).curReqListString.size() + ", obj:" + manager.getASServer(i).curObjList.size());
					for (String obj:manager.getASServer(i).curObjList)
					{
						 Analyser.log.info(i + " " +obj);
					}
				}
			
			*/
			
	        
	   
	}
	
	
	// for overloaded partition, sort requests ascendingly
	// get rid of requests starting from the least weighted one
	// add them to a temp list
	// after done with all overloaded partitions then take the list and for each request check for the underloaded partitions that has the max object overlap 
	// put the request objects excluding the ones that have been assigned before to this partition 
	// update the partition weights
	//continue
	

	
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
				
				if (ro==null)
				{
					Analyser.log.info("ERROR NULL HttpRequestObject"+ro.url);
					
				}
				
				if (co==null)
				{
					Analyser.log.info("ERROR NULL co==null"+co);
					
				}
				
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

		
		for (ASServer serv: manager.servers.values())
		{
			
			Iterator<String> irr = serv.curObjList.iterator();
			int wgtt=0;
			while (irr.hasNext())
			{
				String obj= irr.next();
				CacheObject ck=manager.globalCacheMap.get(obj);
				if (ck==null)
					continue;
				wgtt=wgtt+ck.getCount;
			}
			Analyser.log.info(" server=N=="+ serv.serverId +"object w===" +wgtt);
		}
		
	}
	
	public int getServerForReq()
	{
		int maxServ=0;
		int maxServValue=0;
		
	//	int k=0;
		
		Iterator irTemp=serverSpanCandidateHash.entrySet().iterator();
		if (irTemp.hasNext())
		{
			Map.Entry pairNew= (Map.Entry) irTemp.next();
			maxServ=(int) pairNew.getKey();
			
		}
		else return 0;
		
		Iterator ir=serverSpanCandidateHash.entrySet().iterator();
		
		
		while (ir.hasNext())
		{
			Map.Entry pair= (Map.Entry) ir.next();
			if ((int)pair.getValue() > maxServValue)
			{
				maxServ=(int) pair.getKey();
				maxServValue=(int) pair.getValue();
			}
			
		}
			
		return maxServ;
	}
	
	
	public void fillHashMap()
	{
		for (ASServer serv:manager.servers.values())
		{
			if (serverSpanCandidateHash.containsKey(serv.serverNo))
				serverSpanCandidateHash.put(serv.serverNo,0);
			
			/*
			if (serverWeightsTemp[serv.serverNo]>analyser.currentAnalysisPhase.servCap)
				serverSpanCandidateHash.remove(serv.serverNo);
				*/
		}
	}
	
	private void doAssignAllReqFirstTemp() {
		float threshold = new Float("0.50");
		int tempC=0;
		Analyser.log.info("^^^^^^^^^^ "+analyser.currentAnalysisPhase.servCap);
		
		//Iterator ir =analyser.currentAnalysisPhase.httpListAll.iterator();
		
		Iterator<String> ir =manager.globalRequestMap.keySet().iterator();
		while (ir.hasNext())
		{
			HashSet<String> objects = null;
			String reqKey=  ir.next(); 
			//HttpRequestObject httpReqObjTemp = (HttpRequestObject) ir.next(); // cacheKey is the object
			HttpRequestObject httpReqObjTemp = manager.globalRequestMap.get(reqKey); // cacheKey is the object
			HttpRequestObject httpReqObj=manager.globalRequestMap.get(httpReqObjTemp.url);
			//String reqKey=httpReqObj.url;
			
			if (!analyser.currentAnalysisPhase.ConsiderReq(reqKey))
				continue;
				
			
			objects = manager.globalRequestToObjectMap.get(reqKey);
			if (objects == null) {

				Analyser.log.info(" doFindSpanObjsnew requests==null="
						+ reqKey);
				continue;
			}
			
			
			
			
			//Analyser.log.info("reqKey "+reqKey+"weight"+httpReqObj.weight);
			
			Iterator<String> it = objects.iterator();
			Arrays.fill(serverSpanCandidate, 0);
			fillHashMap();
			while (it.hasNext())
			{
				String objString=it.next();
				//Analyser.log.info("reqString = "+reqString);
				if (!manager.globalCacheMap.containsKey(objString))
					continue;
				CacheObject co = manager.globalCacheMap.get(objString);	
				if (co==null)
				{
					Analyser.log.info("co==null === "+co);
					continue;
				}
				if (co.candidate<0)
				{
					//Analyser.log.info("co.candidate<0"+co);
					continue;
				}

				
				serverSpanCandidate[co.candidate] ++;
				
				if (serverSpanCandidateHash.containsKey(co.candidate))
				{
				int temp=serverSpanCandidateHash.get(co.candidate);
				temp++;
				serverSpanCandidateHash.put(co.candidate, temp);
				//Analyser.log.info("temp = "+temp);
				}
				
				
				
			//	Analyser.log.info("ro.candidate.serverNo = "+ro.candidate.serverNo);

			}

			
			int maxServ = 0;
			maxServ=getServerForReq();
			
			
			
			
			/* by Omar
			boolean flag=false;
			for (int i = 0; i < manager.servers.size(); i++)
			{
				
				if (serverSpanCandidate[i] > serverSpanCandidate[maxServ])
				{
					maxServ = i;
					/*
					if (serverWeightsTemp[i]<analyser.currentAnalysisPhase.servCap)
					{
					maxServ = i;
					flag=true;
					
					
					}
					*/
					/*
				}
			}
			
			*/
			// /////
			
			/*
			if (serverWeightsTemp[maxServ]>analyser.currentAnalysisPhase.servCap)
			{
				tempC++;
				continue;
			
			}
			*/
			
		
			
			/*
			if (!flag)
			{
				int minServ=0;
			//	Random randomGenerator = new Random();
			//	maxServ=randomGenerator.nextInt(manager.servers.size());
				for (int i = 0; i < manager.servers.size(); i++)
				{
					if (serverWeightsTemp[i] < serverWeightsTemp[minServ])
					{
					minServ=i;
					}
				
				}
				maxServ=minServ;
				
			}
			*/
			serverWeightsTemp[maxServ]+=httpReqObj.weight;
			
			ASServer as = manager.getASServer(maxServ);
			as.curReqListString.add(reqKey);
			manager.globalRequestMap.get(reqKey).candidate=as;
			
			if (serverSpanCandidateHash.containsKey(httpReqObj.candidate.serverNo))
			{
			int temp=serverSpanCandidateHash.get(httpReqObj.candidate.serverNo);
			temp+=httpReqObj.counter;
			serverSpanCandidateHash.put(httpReqObj.candidate.serverNo, temp);
			//Analyser.log.info("temp = "+temp);
			}

			Iterator ir1=serverSpanCandidateHash.entrySet().iterator();
			
			
			// new By OMAR EuroSys
			HashSet <String> cacheObjects = manager.globalRequestToObjectMap.get(reqKey);
			
			
			for (String cacheKey:cacheObjects)
			{
				if (!manager.globalCacheMap.containsKey(cacheKey))
					continue;
				
				CacheObject co= manager.globalCacheMap.get(cacheKey);
				if (co.serverSpanCandidateHashObj.containsKey(httpReqObj.candidate.serverId))
				{
					double freq=co.serverSpanCandidateHashObj.get(httpReqObj.candidate.serverId);
					double newFreq=freq+(double) httpReqObj.counter;
					co.serverSpanCandidateHashObj.put(httpReqObj.candidate.serverId, newFreq);
				}
				else if (!co.serverSpanCandidateHashObj.containsKey(httpReqObj.candidate.serverId))
				{
					co.serverSpanCandidateHashObj.put(httpReqObj.candidate.serverId, (double) httpReqObj.counter);
					
				}
				
				
			}
			
			
	//		manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
		//	Analyser.log.info("max Server = "+as.serverId);

		}
		
		
		Analyser.log.info("tempC = "+tempC);
		for (int x=0;x<manager.servers.size();x++)
		{
			Analyser.log.info("serverWeightsTemp="+serverWeightsTemp[x]);
			Analyser.log.info("curReqListString="+manager.getASServer(x).curReqListString.size());
			Analyser.log.info("curObjList="+manager.getASServer(x).curObjList.size());
		}
		
		
		for (int x=0;x<manager.servers.size();x++)
		{
			int w=0;
			ASServer serv=manager.getASServer(x);
			for (int y=0;y<serv.curReqListString.size();y++)
			{
				String req=serv.curReqListString.get(y);
				w+=manager.globalRequestMap.get(req).weight;
			}
			Analyser.log.info("serverWeightsTempAfter="+w);
			
		}
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
		//Analyser.log.info("toReplicateObject"+cacheKey +"as="+as.serverId);
		manager.globalCacheMap.get(cacheKey).replicate=true;
		
	
	
	}
	
	// This is for dynamic object replication
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
			
			if (!manager.globalCacheMap.containsKey(cacheKey))
				continue;
			// object downcasting problem
	
			CacheObjectReplicationCandidate cor=  new CacheObjectReplicationCandidate();
			cor.cacheKey=co.cacheKey;
			
			cor.defaultPartitionLocation=manager.servers.get(co.sites.get(0)); //erro look for this one
			
			//	cor= (CacheObjectReplicationCandidate)co;
			
			requests = entry.getValue();
			if (requests == null) 
			{
				Analyser.log.info(" doFindSpanObjsnew requests==null="+ cacheKey);
				continue;
			}
			//Analyser.log.info(requests.size()+"=================="+co.cacheKey +" site="+co.sites.get(0));
			
			Iterator<String> it=requests.iterator();
			
			while (it.hasNext()) 
			{
				String req=it.next();
				if (!manager.globalRequestMap.containsKey(req))
					continue;
				
				HttpRequestObject ro = manager.globalRequestMap.get(req);
				//Analyser.log.info();
				
				cor.distributedRequests.put(ro.url,ro.candidate.serverId);
				
				//Analyser.log.info("request="+ro.url +" server="+ ro.candidate.serverId);
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
				
				//Analyser.log.info("partitionsRemoteNew.size()=================="+co.serverSpanCandidateHashObj.size());
				
			/*
				
				for (String s:co.serverSpanCandidateHashObj.keySet())
				{
					System.out.println("sss="+s);
				}
				*/
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
				//Analyser.log.info("replicatedPartitions.length"+replicatedPartitions.length);
				
				
				
				if (replicatedPartitions.length>1)
				{
					ReplicaPermutation=true;
					numberOfObjectToReplicate++;
					for (int j=0;j<replicatedPartitions.length;j++)
					{
						if (replicatedPartitions[j].contains("/") || replicatedPartitions[j]=="" || replicatedPartitions[j]==null )
							continue;
						//Analyser.log.info("replicatedPartitions[j]"+replicatedPartitions[j]);
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
				//Analyser.log.info("replicaNo="+replicaNo +"UC(1)="+manager.replicateObjectCost.get(1)+" UC(N)="+manager.replicateObjectCost.get(replicaNo));
				//Analyser.log.info(" manager.replicateObjectCost.get(replicaNo)="+ manager.replicateObjectCost.get(replicaNo));
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
	
	private void doReplicateDistributedObjects()
	{
		
		Analyser.log.info("doReplicateDistributedObjects");
		
		int totalNumberOfPhysicalObject=0;
		
	//	Analyser.log.info("NEWWWWW OMARRR doAssignAllObjFirstTemp analyser.servCapObj="+analyser.servCapObj);
		List sortedKeys=new  ArrayList<CacheObject>();
		
		
		//List KeysTemp=new ArrayList(manager.globalObjectToRequestMap.keySet());
		
		List KeysTemp=new ArrayList(manager.globalCacheMap.keySet());
		
		
		//Collections.sort(sortedKeys, Collections.reverseOrder());
		
	
		
		//Iterator<String> iterator = sortedKeysTemp.iterator();
		//Iterator<CacheObject> iterator = sortedKeys.iterator();
		Iterator<CacheObject> iterator=manager.globalCacheMap.values().iterator();
		
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
			
			fillHashMap();
			
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
					//Analyser.log.info("------------------------------------");
					
				}
				
				//Analyser.log.info("spanServer="+spanServer +"x="+x+"toReplicateObject="+toReplicateObject);
					
			
				
				
				ASServer as = manager.getASServer(x);
				if (!as.curObjList.contains(cacheKey))
				{
					as.curObjList.add(cacheKey);
					cacheObjAllocated[as.serverNo]++;
					
				}
				
				
				
				//Analyser.log.info("as = "+as.serverId  +"cache size= "+as.curObjList.size());
				//by omar
				
				
				if (manager.globalCacheMap.containsKey(cacheKey) && !manager.globalCacheMap.get(cacheKey).sites.contains(as.serverId) )
				{
					manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
					
				}
				
			}
			
			if(toReplicateObject)
			{
				tempNoSites ++;
				//Analyser.log.info("-------------------------"+manager.globalCacheMap.get(cacheKey).sites.size());
				//Analyser.log.info("toReplicateObject"+cacheKey);
				manager.globalCacheMap.get(cacheKey).replicate=true;
				toReplicateObject=false;
				
				/*
				for (String serv:manager.globalCacheMap.get(cacheKey).sites)
				{
					Analyser.log.info("to server="+serv);
				}
				*/
				totalNumberOfPhysicalObject+=manager.globalCacheMap.get(cacheKey).sites.size();
					
				
			}
			else
				totalNumberOfPhysicalObject++;
			
			
		}
		
		Analyser.log.info("New No. of object to replicate="+tempNoSites);
		Analyser.log.info("No. of total physical object ="+totalNumberOfPhysicalObject);
		double repFactor= (double)totalNumberOfPhysicalObject/(double)manager.globalCacheMap.size();
		Analyser.log.info("Replication Factor ="+repFactor);
			
		
	}
	
	
	private void doAssignAllReqFirstTempSchismReplication() {
		float threshold = new Float("0.50");
		int tempC=0;
		Analyser.log.info("doAssignAllReqFirstTempSchismReplication New^^^^^^^^^^ "+analyser.currentAnalysisPhase.servCap);
		
		//Iterator ir =analyser.currentAnalysisPhase.httpListAll.iterator();
		
		Iterator<String> ir =manager.globalRequestMap.keySet().iterator();
		while (ir.hasNext())
		{
			HashSet<String> objects = null;
			String reqKey=  ir.next(); 
			//HttpRequestObject httpReqObjTemp = (HttpRequestObject) ir.next(); // cacheKey is the object
			HttpRequestObject httpReqObjTemp = manager.globalRequestMap.get(reqKey); // cacheKey is the object
			HttpRequestObject httpReqObj=manager.globalRequestMap.get(httpReqObjTemp.url);
			//String reqKey=httpReqObj.url;
			
			if (!analyser.currentAnalysisPhase.ConsiderReq(reqKey))
				continue;
				
			
			objects = manager.globalRequestToObjectMap.get(reqKey);
			if (objects == null) {

				Analyser.log.info(" doFindSpanObjsnew requests==null="
						+ reqKey);
				continue;
			}
			
			
			
			
		//	Analyser.log.info("reqKey "+reqKey+"weight"+httpReqObj.weight);
			
			Iterator<String> it = objects.iterator();
			Arrays.fill(serverSpanCandidate, 0);
			fillHashMap();
			while (it.hasNext())
			{
				String objString=it.next();
				//Analyser.log.info("objString = "+objString);
				if (!manager.globalCacheMap.containsKey(objString))
				{
					Analyser.log.info("!manager.globalCacheMap.containsKey(objString) contibue === "+objString);
					continue;
				}
				CacheObject co = manager.globalCacheMap.get(objString);	
				if (co==null)
				{
					Analyser.log.info("co==null === "+objString);
					continue;
				}
				
				/*
				if (co.candidate<0)
				{
					Analyser.log.info("co.candidate<0"+co);
					continue;
				}
*/
				
			
				
				for (ASServer serv:manager.servers.values())
				{		
					//Analyser.log.info("co.cacheKey ="+serv.serverId +"serv.serverNo"+serv.serverNo+""+serv.curObjList.size());
				
					if (serv.curObjList.contains(objString))
					{
						int temp=serverSpanCandidateHash.get(serv.serverNo);
						temp++;
						serverSpanCandidateHash.put(serv.serverNo, temp);
						//Analyser.log.info("co.cacheKey = "+co.cacheKey +" serv.serverNo= "+serv.serverNo);
					}
				}
				
				
				/*// old
				serverSpanCandidate[co.candidate] ++;
				
				if (serverSpanCandidateHash.containsKey(co.candidate))
				{
				int temp=serverSpanCandidateHash.get(co.candidate);
				temp++;
				serverSpanCandidateHash.put(co.candidate, temp);
				//Analyser.log.info("temp = "+temp);
				}
				*/
				
				
			//	Analyser.log.info("ro.candidate.serverNo = "+ro.candidate.serverNo);

			}

			
			int maxServ = 0;
			maxServ=getServerForReq();
			
			//Analyser.log.info("max Server = "+maxServ);
			
			/* by Omar
			boolean flag=false;
			for (int i = 0; i < manager.servers.size(); i++)
			{
				
				if (serverSpanCandidate[i] > serverSpanCandidate[maxServ])
				{
					maxServ = i;
					/*
					if (serverWeightsTemp[i]<analyser.currentAnalysisPhase.servCap)
					{
					maxServ = i;
					flag=true;
					
					
					}
					*/
					/*
				}
			}
			
			*/
			// /////
			
			/*
			if (serverWeightsTemp[maxServ]>analyser.currentAnalysisPhase.servCap)
			{
				tempC++;
				continue;
			
			}
			*/
			
		
			
			/*
			if (!flag)
			{
				int minServ=0;
			//	Random randomGenerator = new Random();
			//	maxServ=randomGenerator.nextInt(manager.servers.size());
				for (int i = 0; i < manager.servers.size(); i++)
				{
					if (serverWeightsTemp[i] < serverWeightsTemp[minServ])
					{
					minServ=i;
					}
				
				}
				maxServ=minServ;
				
			}
			*/
			serverWeightsTemp[maxServ]+=httpReqObj.weight;
			
			ASServer as = manager.getASServer(maxServ);
			as.curReqListString.add(reqKey);
			manager.globalRequestMap.get(reqKey).candidate=as;

	//		manager.globalCacheMap.get(cacheKey).sites.add(as.serverId);
		//	Analyser.log.info("max Server = "+as.serverId);

		}
		
		
		Analyser.log.info("tempC = "+tempC);
		for (int x=0;x<manager.servers.size();x++)
		{
			Analyser.log.info("serverWeightsTemp="+serverWeightsTemp[x]);
			Analyser.log.info("curReqListString="+manager.getASServer(x).curReqListString.size());
			Analyser.log.info("curObjList="+manager.getASServer(x).curObjList.size());
		}
		
		
		for (int x=0;x<manager.servers.size();x++)
		{
			int w=0;
			ASServer serv=manager.getASServer(x);
			for (int y=0;y<serv.curReqListString.size();y++)
			{
				String req=serv.curReqListString.get(y);
				w+=manager.globalRequestMap.get(req).weight;
			}
			Analyser.log.info("serverWeightsTempAfter="+w);
			
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
	
	private void doAssignAllObjNew() {
		for (ASServer as:manager.servers.values())
		{
			//Analyser.log.info("server = "+ as.serverId +"server obj list size ="+as.curObjList.size());
			Iterator<String> ir=as.curObjList.iterator();
			while (ir.hasNext())
			{
				String cacheKey= ir.next();
				if (cacheKey==null)
				{
				//	Analyser.log.info("cacheKey==null");
					continue;
				}
				
				ListKey lk = new ListKey();
				lk.addtoListKey(cacheKey);
				toServers.clear();
				toServers.add(as.getServerId());
				//RuleList rl = new RuleList(as.serverId, RuleList.REPLICATE,RuleList.STABLE_TTL, toServers);
				// as.currentPolicy.addNewPolicy(lk, rl);
				
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
				
				as.tmpPolicy.addNewPolicy(lk, rl);
				cacheObjAllocated[as.serverNo]++;
				as.curObjList.add(cacheKey);
				if (manager.globalCacheMap.containsKey(cacheKey))
					manager.globalCacheMap.get(cacheKey).assignedbefore = 1;				
			}
			
		}
	}
	
	
	private void doAssignAllObjNewSchismReplication() {
		
		
		for (ASServer as:manager.servers.values())
		{
			//Analyser.log.info("server = "+ as.serverId +"server obj list size ="+as.curObjList.size());
			Iterator<String> ir=as.curObjList.iterator();
			while (ir.hasNext())
			{
				String cacheKey= ir.next();
				if (cacheKey==null)
				{
				//	Analyser.log.info("cacheKey==null");
					continue;
				}
				
				ListKey lk = new ListKey();
				lk.addtoListKey(cacheKey);
				toServers.clear();
				toServers.add(as.getServerId());
				
				
				RuleList rl ;
				
				if (manager.globalCacheMap.containsKey(cacheKey) && manager.globalCacheMap.get(cacheKey).replicate)
				{
						 rl = new RuleList(as.serverId, RuleList.MOVE,	RuleList.STABLE_TTL, toServers);
						 //noOfObjtoReplicate++;
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
		//Analyser.log.info("noOfObjtoReplicate="+noOfObjtoReplicate);
	}
	
	private void doAssignReq()
	{
		Analyser.log.info("doAssignReq ");
		
		for (int curServ=0;curServ<manager.servers.size();curServ++)
		{
			ASServer as;
			as=manager.getASServer(curServ);
			
			Analyser.log.info("serverId = "+as.serverId);
			Analyser.log.info("as.curReqListString.size() = "+as.curReqListString.size());
			for (int reqId=0;reqId<as.curReqListString.size();reqId++)
			{
				
			    String curReq=as.curReqListString.get(reqId);
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
		
		for (CacheObjectReplicationCandidate cor:manager.distributedObject)
		{
			cor.updateLocations(newConfiguration, manager);
			
			CacheObject co=manager.globalCacheMap.get(cor.cacheKey);
			co.updateLocations(newConfiguration, manager);
		}
		
		return minCombination;
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
	
	
	private void doFindAllCombiations() {
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
		
		String bestComb=doGetBestCombination();
		Analyser.log.info("sbestCombr" + bestComb);
		

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
			availableServers.add(as.serverNo);
			serverSpanCandidateHash.put(as.serverNo,0);
		}
		
		Analyser.log.info("card counter."+ counter);
		Analyser.log.info("analyser.currentAnalysisPhase.tester"+ analyser.currentAnalysisPhase.tester);
		
		
		 cacheObjAllocated = new int [manager.servers.size()];
		 serverWeights =new int [manager.servers.size()];
		 serverWeightsTemp =new int [manager.servers.size()];
		 cacheObjMax =new int [manager.servers.size()];
		 serverSpanCandidate=new int[manager.servers.size()];
		 
		 combinations= new  int[manager.servers.size()][manager.servers.size()];
		 
		stableMax=new int [manager.servers.size()];
		
		 reqAllocated=new int [manager.servers.size()];
		 toServers = new ArrayList<String>(manager.servers.size());
		 
		Analyser.log.info("AdvancedStrategyDynamicGRAPH");
		
		
		
	
		 Arrays.fill(server0Req, 0);
			Arrays.fill(server1Req, 0);
			Arrays.fill(server2Req, 0);
			Arrays.fill(serverSpanCandidate, 0);
			
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
		long partitionBF=System.currentTimeMillis();
		doGraphPartition();		
		long  partitionAF=System.currentTimeMillis();
		
		
		
		
		
		
		long assignBF=System.currentTimeMillis();
		

			
		
		if (analyser.replicationStrategy.equals("schism"))
			doReadPartitionResultsSchismReplication();
		else
			doReadPartitionResults();
			
		
		
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
		
		if (analyser.replicationStrategy.equals("schism"))
			doAssignAllReqFirstTempSchismReplication();
		else
			doAssignAllReqFirstTemp();
		
		//doAssignAllObj();
		
		
		
		
		// HERE TO REPLICAE DISTRIBUTED OBJECTS
		if (analyser.replicationStrategy.equals("distributed"))
			doReplicateDistributedObjects();
		else if (analyser.replicationStrategy.equals("dynamic-no-parameters-extraction"))
		{
			Analyser.log.info("manager.readLocalCost="+manager.readLocalCost);
			Analyser.log.info("manager.readRemoteCost="+manager.readRemoteCost);
			Analyser.log.info("manager.putLocalCost="+manager.putLocalCost);
			Analyser.log.info("manager.putRemoteCost="+manager.putRemoteCost);
			
			generateReplicationFactor();
		}
		
		Analyser.log.info("---------- BEFORE PARTITION ASSIGNMENT---------");
		for(int i=0;i<manager.servers.size();i++)
		{
			Analyser.log.info("Server:" + i + ", req:" + manager.getASServer(i).curReqListString.size() + ", obj:" + manager.getASServer(i).curObjList.size());
		}
		
		
		/*
		if (analyser.useOptimisation.equals("dm") || analyser.useOptimisation.equals("both"))
			doFindAllCombiations(); // manager For Mig. Optimization 
		
		else if (analyser.manager.counter%2==1)
			doRandomizeCombinations(); //manager for non-data migration
			*/
		doFindAllCombiations(analyser.useOptimisation);
		
		//End 
		
		doAssignReq();
		
		if (analyser.replicationStrategy.equals("schism"))
			doAssignAllObjNewSchismReplication();
		else
			doAssignAllObjNew();
		
		
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
		if (analyser.replicationStrategy.equals("dynamic-no-parameters-extraction"))
		{
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
						
						/*
						for (String s: co.serverSpanCandidateHashObj.keySet())
						{
							Analyser.log.info("as="+as.serverId+ "serverSpanCandidateHashObj="+s +" counter="+co.serverSpanCandidateHashObj.get(as.serverId));
						}
						*/
						
						if (!co.serverSpanCandidateHashObj.containsKey(as.serverId))
						{
							Analyser.log.info("NULL as= "+as.serverId+" counter="+co.serverSpanCandidateHashObj.get(as.serverId));
							co.tempPopulariyt=co.tempPopulariyt+0;
						}
						else
						{
						double remote=co.serverSpanCandidateHashObj.get(as.serverId);
						double temp= remote*(manager.readRemoteCost-manager.readLocalCost);
						co.tempPopulariyt=co.tempPopulariyt+(int) temp;
						}
						
						
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
		}
		else
		{
			for (ASServer as : manager.servers.values())
			{
				as.currentPolicy = as.tmpPolicy.clone();
			}
		}
		
		for (ASServer as : manager.servers.values()) {
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
			
			Analyser.log.info("===> Graph Partioning Time:" + (partitionAF - partitionBF));
			Analyser.log.info("===> Assignment Time:" + (assignAF - assignBF));

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


