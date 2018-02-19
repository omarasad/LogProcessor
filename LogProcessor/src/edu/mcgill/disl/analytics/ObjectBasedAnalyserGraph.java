package edu.mcgill.disl.analytics;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.rmi.AccessException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.RestoreAction;

import com.nearinfinity.bloomfilter.BloomFilter;

import edu.mcgill.disl.analytics.CacheLogList.CacheLogSegment;
import edu.mcgill.disl.analytics.HttpLogList.HttpLogSegment;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.log.processor.CacheLogProcessor;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;
import edu.mcgill.disl.log.processor.RequestResourceMappingLogProcessor;

import org.jgrapht.*;
import org.jgrapht.alg.DirectedNeighborIndex;
import org.jgrapht.alg.NeighborIndex;
import org.jgrapht.graph.*;





public class ObjectBasedAnalyserGraph extends Analyser {
	
	public static final String RBA_STRATEGY_CLASS = "rba.strategy.class";
	public static final String RBA_STRATEGY_CONTINUELB = "rba.strategy.lbURLFreq";
	
	ObjectBasedAnalyserGraphStrategy strategy = null;
	
	//all variables that could be needed by strategy should be made public here or have public methods
	public int cacheMemorySize;	
	public int cacheSz;
	public int numServers=-1;
	public int numServersOri;
	public int totalCapacity;
	public int totalMemoryCapacity;
	public int servCap=0;
	public int totalWeight=0;
	public int variation=0;
	public int objectDrift=0;
	public int globalObjectDriftCounter=0;
	double avgExecTime=0;
	int addRemoveServ=0;
	String toAppend = "";
	
	int newRemap=-1;
	public int tester=-1;
	
	public int newRequests=0;
	public int requestsDrift=0;
	
	public UndirectedGraph<String, DefaultEdge> objectsGraph =
		      new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
	
	public boolean toUseRegression=true;

	
	//public AbstractBaseGraph<String, String> requestsGraphs =
		  //    new AbstractBaseGraph<String, String>(DefaultEdge.class);
	
	
	public ObjectBasedAnalyserGraph currentAnalysisPhase;
	

	
	public int lbURLFreq = 0;
	public int reqCounter=0; // counter for requests indexing
	HashMap<Integer, ASServer> servIndex = new HashMap<Integer,ASServer>();
	
	public RequestToResourcesMap reqToResAll=null;
	public ResourceToRequestMap resToReqAll=null;
	
	public HashMap<String, HashMap<String, CacheObject>> individualMap;
	public List<CacheObject> cacheList;
	
	public PrevIntervalStat prevStats=new PrevIntervalStat();

	 

	
	public HashMap<String,HttpRequestObject> mergedHttpMapAll = null;
	public HashMap<String,HttpRequestObject> mergedObjectMapAll = null;
	public HashMap<String,HttpRequestObject> mergedHttpMapAllPreviousInterval = null;
	public List<HttpRequestObject> httpListAll= new ArrayList<HttpRequestObject>();
	public HashMap<ASServer, CacheLogList> servCacheLogIndex = new HashMap<ASServer, CacheLogList>();
	public HashMap<ASServer, HttpLogList> servHttpLogIndex = new HashMap<ASServer, HttpLogList>();
	public HashMap<ASServer, RequestToResourcesMap> servReqtoResIndex = new HashMap<ASServer, RequestToResourcesMap>();
	public static HashMap<HttpRequestObject, List<HttpRequestObject>> sharedReq = new HashMap<HttpRequestObject, List<HttpRequestObject>>();
	public HashMap<ASServer, ResourceToRequestMap> servRestoReqIndex = new HashMap<ASServer, ResourceToRequestMap>();
	public HashMap<String, HashSet<String>> sharedReqNew = new HashMap<String, HashSet<String>>(100);
	public Set<String> nonSharedReq= new HashSet<String>() ;
	public List<String> SharedReqList= new ArrayList<String>() ;
	
	public NeighborIndex<String, DefaultWeightedEdge> NG; 


	
	//public PrevIntervalStat prevStat; 
	public HashMap<String, Integer> requestId = new HashMap<String, Integer>() ;
	public HashMap<Integer, String> requestIdIndex = new HashMap<Integer, String>() ;
	public HashMap<String, String> edges = new HashMap<String, String> () ;
	public final String  filename="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/req.hgr";
	public final String  filePath="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/";
//	public final String  partionerOld="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/khmetis";
	public final String  partioner="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/shmetis";
	public final String  filenameNewMetis="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/newMetisPartioner/req.hgr";
	
	public final String  partionerNewMetis="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/newMetisPartioner/gpmetis";
	public final String  filePathNewMetis="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/newMetisPartioner/";

	public int requestPopularityThresholdValue=-1;
	public Float requestDriftRateThresholdValue=new Float("-1");

	
	public final String  inputGraphFile="req.hgr";
	String vertexWgt="";
	String useOptimisation = manager.props.getProperty(StatisticsManager.USE_OPTIMISATION);
	String graphType = manager.props.getProperty(StatisticsManager.GRAPH_TYPE);
	String useRegression = manager.props.getProperty(StatisticsManager.USE_REGRESSION);
	 

	public float requestPopularityThreshold = Float.parseFloat(manager.props.getProperty(StatisticsManager.request_Pop_Thresh));
	
	public float requestDriftRateThreshold = Float.parseFloat(manager.props.getProperty(StatisticsManager.request_Drift_Thresh));
	
	public String replicationStrategy = manager.props.getProperty(StatisticsManager.replication_strategy);
	

	
	
	public UndirectedGraph<String, DefaultEdge> stringGraph;
	
	
	
	//should ideally be kept in a load balancer class that contains lb specific properties and policy
	
	public LoadBalancerPolicy currentLBPolicy = null;
	//public LoadBalancerPolicy oldLB = null;
	
	
	// to print out bloomset
	public void testBloomSet()
	{
		for (int s=0;s<httpListAll.size();s++)
			httpListAll.get(s).bloomFilter.test("sss");
	}
	
	// to match 2 requests objects// bloom
	

	
	public int getObjectKey(String s)
	{
		
		int start=s.indexOf("#");
		String r=(String) s.subSequence(start+1, s.length());
		return Integer.parseInt(r);
		
		
	}
	public boolean isShareqReq(HttpRequestObject req1, HttpRequestObject req2)
	{
		HashSet<String> objects = reqToResAll.map.get(req1.url);
		HashSet<String> objects2 = reqToResAll.map.get(req2.url);
		
		if(objects == null || objects2==null)
			return false;
		for(String obj : objects)
		{ 
			for(String obj2 : objects2)
			{ 
			if (obj.equals(obj2))
				return true;
			}
		}
		
		return false;
	}
	
	
	
	
	@SuppressWarnings("null")
	public boolean insertSharedGraph(String r1, String r2)
	{
		HashSet<String> requests = new HashSet<String>();
		HashSet<String> requests2 = new HashSet<String>();
		if (sharedReqNew!=null)
		{
			requests=sharedReqNew.get(r1);
			requests2=sharedReqNew.get(r2);
			if (requests !=null)
			{
					if (requests.contains(r2))
						return false;
					else
					{
						requests.add(r2);
						return true;
					}
				
				
			}
			else if (requests2!=null)
			{
				
				if (requests2.contains(r1))
					return false;
				else
				{
					requests2.add(r1);
					return true;
				}
				
						
			}
		else 
		{
			requests = new HashSet<String>();
			
			sharedReqNew.put(r1, requests);
			requests.add(r2);
			return true;
	
		}
		
		}
		return false;
		
	}
	
	
	@SuppressWarnings("unchecked")
	public void findSharedReqObj()
	{
		//HashSet<String> requests;
		
		
		 
		
		
		for(Entry<String, HashSet<String>> entry : resToReqAll.map.entrySet())
		{
			HashSet<String> requests = null;
			requests=resToReqAll.map.get(entry.getKey());
			
			if (requests !=null)
			{
				if(requests.size()>1)
				{
					Iterator it = requests.iterator();
					
					while (it.hasNext())
					{
						String req=(String) it.next();
						//HashMap<String,String> edges=null;
						
						HashSet<String> subRequests=null;
						subRequests=sharedReqNew.get(req);
						
						if(subRequests == null)
						{
							subRequests = (HashSet<String>) requests.clone();
							
							sharedReqNew.put(req,  subRequests);
							 
						}
						
						subRequests.addAll(requests);
						
						
						
					}
				}
			}
		}	
	}
	
	public void emptyresultGraphFile()
	{
		 
		
			 String resultGraphFile=this.filePath+this.inputGraphFile+".part."+Integer.toString(this.numServers);
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(resultGraphFile, false)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		out.println("");
		out.close();
	}
	
	public void writeToFile(String toPrint, String filePath)
	{
		 
		
			
		PrintWriter out = null;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(filePath, false)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		out.println(toPrint);
		out.close();
	}
	
	public void maintainRequestIndex(String s, int wgt)
	{
		if (s!=null)
		{
			if (manager.globalRequestMap.get(s)!=null)
			{
				
				if (requestId!=null)
				{
					if (!requestId.containsKey(s)) 
					{
						reqCounter++;
						requestId.put(s, reqCounter);
					    requestIdIndex.put(reqCounter, s);
					    vertexWgt= vertexWgt+ "\n"+ wgt;
					}
				}
				else
				{
					reqCounter++;
					requestId.put(s, reqCounter);
					requestIdIndex.put(reqCounter, s);
					vertexWgt= vertexWgt+ "\n"+ wgt;
				}
			}
		}
	}

	public void addRemoveServers()
	{
		if (avgExecTime<200000)
			if (this.numServers>1)
			{
				this.numServers--;
				
			}
		
		
		
	}
	
	public void writeToFile()
	{
		PrintWriter out = null;
		
		int edgesCount=0;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		out.println(toAppend);
		out.close();	
	}
	
	
	public static <V,E> void removeAllEdges(Graph<V, E> graph) {
		LinkedList<E> copy = new LinkedList<E>();
		for (E e : graph.edgeSet()) {
		copy.add(e);
		}
		graph.removeAllEdges(copy);
		}

		public static <V,E> void clearGraph(Graph<V, E> graph) {
		removeAllEdges(graph);
		removeAllVertices(graph);
		}

		public static <V,E> void removeAllVertices(Graph<V, E> graph) {
		LinkedList<V> copy = new LinkedList<V>();
		for (V v : graph.vertexSet()) {
		copy.add(v);
		}
		graph.removeAllVertices(copy);
		}
	
		public boolean ConsiderReq(String req)
		{
			if (manager.globalRequestMap.containsKey(req))
				return true;
			return false;
		}
		
		

		// TO KEEP TRACK OF OBJECT INDEX
		public int getSetObjectIndexSchismReplication(String cacheKey)
		{
			if (manager.globalCacheMap.containsKey(cacheKey))
			{
				CacheObject cacheObj=manager.globalCacheMap.get(cacheKey);
				int objectIndexToReturn=-1;
				if (cacheObj.index!=-1)
				{
					
					objectIndexToReturn=cacheObj.index;
				}
				else if (cacheObj.index==-1)
				{
				manager.gloabCacheObjectIndexing.put(manager.gloabalCacheKeyCounter, cacheKey);
				cacheObj.index=manager.gloabalCacheKeyCounter;
				objectIndexToReturn=manager.gloabalCacheKeyCounter;
				manager.gloabalCacheKeyCounter++;
				}
				return objectIndexToReturn;
			}
			else
			{
				int objectIndexToReturn=-1;
				if (manager.gloablRequestIndexingSchismReplication.containsKey(cacheKey))
				{
					objectIndexToReturn=manager.gloablRequestIndexingSchismReplication.get(cacheKey);
				}
				else
				{
					manager.gloabCacheObjectIndexing.put(manager.gloabalCacheKeyCounter, cacheKey);
					manager.gloablRequestIndexingSchismReplication.put(cacheKey, manager.gloabalCacheKeyCounter);
					objectIndexToReturn=manager.gloabalCacheKeyCounter;
					manager.gloabalCacheKeyCounter++;
					
				}
			
				return objectIndexToReturn;
			}
			
			
			
		}
		
		
		public String getOriginalObjectSchismReplication(String virtualCacheKey)
		{
			
			String []oriString=virtualCacheKey.split("##");
			return oriString[0];
		}
		
		// TO KEEP TRACK OF OBJECT INDEX
		public int getSetObjectIndex(String cacheKey)
		{
			
			CacheObject cacheObj=manager.globalCacheMap.get(cacheKey);
		//	Analyser.log.info(" !cacheObj.index!=-1" + cacheObj.index);
			int objectIndexToReturn=-1;
			if (cacheObj.index!=-1)
			{
				
				objectIndexToReturn=cacheObj.index;
			}
			else if (cacheObj.index==-1)
			{
			manager.gloabCacheObjectIndexing.put(manager.gloabalCacheKeyCounter, cacheKey);
			cacheObj.index=manager.gloabalCacheKeyCounter;
			objectIndexToReturn=manager.gloabalCacheKeyCounter;
			manager.gloabalCacheKeyCounter++;
			}
			return objectIndexToReturn;
			
		}
		
		
		private void toNeighborGraph(Graph G) 
		{
			 NG = new NeighborIndex<String, DefaultWeightedEdge>(G);
		}
		
		public void prepareToWriteGraphToFileNewMetis()
		{
			manager.gloabCacheObjectIndexing.clear();
			manager.gloabalCacheKeyCounter=1;
			
			emptyresultGraphFile();
			 requestId.clear();
			 requestIdIndex.clear();
			 toNeighborGraph(objectsGraph);
			
			 StringBuffer sentence = new StringBuffer();
			 String EdgePairs="";
			 int edgesCount=0;
			 long BF=System.currentTimeMillis();
			Set<String> objects=objectsGraph.vertexSet();
			Set<DefaultEdge> edges=objectsGraph.edgeSet();
	
			
			for (String obj:objects)
			{
				manager.globalCacheMap.get(obj).index=-1;
				
				
				
					getSetObjectIndex(obj);
				
				
				//this is in case of balancing object weight vs partitoin size
				/* 
				CacheObject cob=manager.globalCacheMap.get(obj);
				if (cob.getCount<144 && cob.getCount>12)
					cob.getCount=12;
				else if (cob.getCount>144)
					cob.getCount=(cob.getCount/12);
					*/
				
			}
			
			Analyser.log.info("manager.gloabCacheObjectIndexing.size()"+manager.gloabCacheObjectIndexing.size());
			
			for (int k=1;k<=manager.gloabCacheObjectIndexing.size();k++)
			{
				String obj=manager.gloabCacheObjectIndexing.get(k);
				
				if (!objects.contains(obj))
				{
					Analyser.log.info(" !objects.contains(obj)=" + obj  +"k="+k);
					continue;
					
				}
				
				
				
				CacheObject co=manager.globalCacheMap.get(obj);
				
			//	Analyser.log.info(" k" + k  +"co.getCount="+co.getCount);
				
				sentence.append("\n"+String.valueOf(co.getCount)+" ");
				
				
			//	String objLine=String.valueOf(co.getCount);
				
			//	Analyser.log.info(req);
				
							
				//List<String> OS = NG.neighborListOf(obj);
				
				/*
				if (RS == null)
				{
					Analyser.log.info(" RS == null cont." + req);
					continue;
					
				}
				*/
				
				for (String adjObj : NG.neighborListOf(obj)) 
					{
					
					CacheObject adjCObj=manager.globalCacheMap.get(adjObj);
					int edgeWgt=co.getCount + adjCObj.getCount;
					//int edgeWgt=1;
				//	objLine+=" "+adjCObj.index+" "+edgeWgt;
					sentence.append(adjCObj.index+" "+edgeWgt+" ");
				//	sentence.append(" "+adjCObj.index+" "+edgeWgt);
					}	
			//	sentence.append("\n"+ objLine);
				//objLine="";
				
				
			}
			
			
			
			Analyser.log.info("manager.gloabCacheObjectIndexing.size()"+manager.gloabCacheObjectIndexing.size());
			Analyser.log.info("gloabalCacheKeyCounter"+manager.gloabalCacheKeyCounter);
	        
			 
			 long AF=System.currentTimeMillis();
			 long Time= AF-BF;
			  Analyser.log.info(" ===> Graph Traversing Time=" + Time);
			  
				PrintWriter out = null;
				
				
				try {
					out = new PrintWriter(new BufferedWriter(new FileWriter(filenameNewMetis, false)));
				} catch (IOException e) {
					// TODO Auto-generated catch blockgloabObjectIndexing
					e.printStackTrace();
				}
			 int objSize=0;
				if (edges!=null)
				{
					 objSize=objects.size();
					 edgesCount=edges.size();
				}
				String toAppendFirstLine=null;
				toAppendFirstLine=objSize+" "+ edgesCount+ " 011";
				
				EdgePairs=sentence.toString();
				toAppend=toAppendFirstLine +EdgePairs;
				Analyser.log.info("toAppendFirstLine MMM" + toAppendFirstLine);
				Analyser.log.info("manager.vertexNo " + manager.vertexNo);
				
				out.println(toAppend);
				out.close();	
				
				EdgePairs="";
				sentence.setLength(0);
				EdgePairs="";
				
				
				
		}
		
		
		public void prepareToWriteGraphToFileNewMetisSchismReplication()
		{
			manager.gloabCacheObjectIndexing.clear();
			manager.gloabalCacheKeyCounter=1;
			
			emptyresultGraphFile();
			 requestId.clear();
			 requestIdIndex.clear();
			 toNeighborGraph(objectsGraph);
			
			 StringBuffer sentence = new StringBuffer();
			 String EdgePairs="";
			 int edgesCount=0;
			 long BF=System.currentTimeMillis();
			Set<String> objects=objectsGraph.vertexSet();
			Set<DefaultEdge> edges=objectsGraph.edgeSet();
	
			
			for (String obj:objects)
			{
				
				if (manager.globalCacheMap.containsKey(obj))
					manager.globalCacheMap.get(obj).index=-1;
				
				
					getSetObjectIndexSchismReplication(obj);
					//Analyser.log.info(x+" obj="+obj);
				
				
				//this is in case of balancing object weight vs partitoin size
				/* 
				CacheObject cob=manager.globalCacheMap.get(obj);
				if (cob.getCount<144 && cob.getCount>12)
					cob.getCount=12;
				else if (cob.getCount>144)
					cob.getCount=(cob.getCount/12);
					*/
				
			}
			
			/*
			for (DefaultEdge edge:edges)
			{
				Analyser.log.info("obj"+edge.toString());
			}
			*/
			
			Analyser.log.info("manager.gloabCacheObjectIndexing.size()"+manager.gloabCacheObjectIndexing.size());
			
			for (int k=1;k<=manager.gloabCacheObjectIndexing.size();k++)
			{
				String obj=manager.gloabCacheObjectIndexing.get(k);
				
				if (!objects.contains(obj))
				{
					Analyser.log.info(" !objects.contains(obj)=" + obj  +"k="+k);
					continue;
					
				}
				
				int getCount=1;
				
				if (manager.globalCacheMap.containsKey(obj))
				{
					CacheObject co=manager.globalCacheMap.get(obj);
					if (co.getCount>0)
						getCount =co.getCount;
					
					sentence.append("\n"+String.valueOf(getCount)+" ");
					
					if (manager.globalObjectToRequestMap.get(obj).size()>1) // in this case the object is replicated 
					{
						int edgeWgt=1;
						if (co.updateCount>0)
							 edgeWgt=co.updateCount;
						//Analyser.log.info("*************** obj=" + obj +"edgeWgt="+edgeWgt);
						
						for (String adjObj : NG.neighborListOf(obj)) 
						{
							//Analyser.log.info(" adjObj=" + adjObj  +"k="+(manager.gloablRequestIndexingSchismReplication.get(adjObj)));
							sentence.append(manager.gloablRequestIndexingSchismReplication.get(adjObj)+" "+edgeWgt+" ");						
						}	
					}
					else if (manager.globalObjectToRequestMap.get(obj).size()<=1) // in this case the object is NOT replicated 
					{
						for (String adjObj : NG.neighborListOf(obj)) 
						{
						
							if (manager.globalCacheMap.containsKey(adjObj))
							{
								CacheObject adjCObj=manager.globalCacheMap.get(adjObj);
								int edgeWgt=getCount + adjCObj.getCount;
								sentence.append(adjCObj.index+" "+edgeWgt+" ");
							}
							else // here the other key is replicated
							{
								String oriadjObj=getOriginalObjectSchismReplication(adjObj);
								
								CacheObject adjCObj=manager.globalCacheMap.get(oriadjObj);
								int edgeWgt=getCount + adjCObj.getCount;
								sentence.append(manager.gloablRequestIndexingSchismReplication.get(adjObj)+" "+edgeWgt+" ");
								
							}
						}	
					}
				}
				else
				{
					// here we have first to extract the original object
					String oriCacheObj=getOriginalObjectSchismReplication(obj);
					CacheObject coNew=manager.globalCacheMap.get(oriCacheObj);
					
					if (coNew.getCount>0) 
						getCount =coNew.getCount;
					
					sentence.append("\n"+String.valueOf(getCount)+" ");
					
					for (String adjObj : NG.neighborListOf(obj)) 
					{
					
						if (manager.globalCacheMap.containsKey(adjObj))
						{
							int edgeWgt=1;
							CacheObject adjCObj=manager.globalCacheMap.get(adjObj);
							if (oriCacheObj.equals(adjObj))
							{
								if (coNew.updateCount>0)
									edgeWgt=coNew.updateCount;
							}
							else
								edgeWgt=getCount + adjCObj.getCount;
							sentence.append(adjCObj.index+" "+edgeWgt+" ");
						}
						else
						{
							String oriadjObj=getOriginalObjectSchismReplication(adjObj);
							
							if (!oriCacheObj.equals(oriadjObj))
							{	
							CacheObject adjCObj=manager.globalCacheMap.get(oriadjObj);
							int edgeWgt=getCount + adjCObj.getCount;
							sentence.append(manager.gloablRequestIndexingSchismReplication.get(adjObj)+" "+edgeWgt+" ");
							}
							
						}
					}	
					
					
					
				}
				

				
			
				
				
			}
			
			
			
			Analyser.log.info("manager.gloabCacheObjectIndexing.size()"+manager.gloabCacheObjectIndexing.size());
			Analyser.log.info("gloabalCacheKeyCounter"+manager.gloabalCacheKeyCounter);
	        
			 
			 long AF=System.currentTimeMillis();
			 long Time= AF-BF;
			  Analyser.log.info(" ===> Graph Traversing Time=" + Time);
			  
				PrintWriter out = null;
				
				
				try {
					out = new PrintWriter(new BufferedWriter(new FileWriter(filenameNewMetis, false)));
				} catch (IOException e) {
					// TODO Auto-generated catch blockgloabObjectIndexing
					e.printStackTrace();
				}
			 int objSize=0;
				if (edges!=null)
				{
					 objSize=objects.size();
					 edgesCount=edges.size();
				}
				String toAppendFirstLine=null;
				toAppendFirstLine=objSize+" "+ edgesCount+ " 011";
				
				EdgePairs=sentence.toString();
				toAppend=toAppendFirstLine +EdgePairs;
				Analyser.log.info("toAppendFirstLine MMM" + toAppendFirstLine);
				Analyser.log.info("manager.vertexNo " + manager.vertexNo);
				
				out.println(toAppend);
				out.close();	
				
				EdgePairs="";
				sentence.setLength(0);
				EdgePairs="";
				
				
				
		}
		
		public String getVirtualObject(String req, String obj)
		{
			String toReturn="";
			HttpRequestObject ro=manager.globalRequestMap.get(req);
			toReturn=obj+"##"+ro.url;
			return toReturn;
		}
		
		public void printRequestMap()
		{
			for(Entry<String, HashSet<String>> entry : manager.globalRequestToObjectMap.entrySet())
			{
			HashSet<String> objects = null;
			objects=entry.getValue();
			String key=entry.getKey(); // key is the request
			
			Object[] obs= objects.toArray();
			
			
			}
			
		}
		
		public void addingVirtualVerticesForShcismRep()
		{
			if (objectsGraph.vertexSet().size()>0)
			{
				clearGraph(objectsGraph);
			}
			
			manager.globalRequestToObjectMapOld.clear();
			//manager.globalRequestToObjectMapOld.putAll(manager.globalRequestToObjectMap);// Here we will fill up a temp req-to-obj map first and deal with it
			
			
			// DeepCopy of the hashmap .. put all will do shallow copy only
			for (Map.Entry<String, HashSet<String>> entry: manager.globalRequestToObjectMap.entrySet())
			{
				manager.globalRequestToObjectMapOld.put(entry.getKey(), (HashSet<String>) entry.getValue().clone());
			}
			
			
			for(Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet())
			{
			HashSet<String> requests = null;
			requests=entry.getValue();
			String key=entry.getKey(); // key is the object
			
			if (requests.size()<=1)
			{
				//Analyser.log.info("size==1 continue"+key);
				continue;
			}
			
			Object[] reqs= requests.toArray();
	
			objectsGraph.addVertex(key);
			//Analyser.log.info("----------key="+key +"size="+reqs.length);
			for (int k=0;k<reqs.length;k++)
			{
				String curReq=(String) reqs[k];
				if (!manager.globalRequestMap.containsKey(curReq))
				{
					Analyser.log.info("if !manager.globalRequestMap.containsKey(curReq)"+curReq);
					continue;
				}
			//	Analyser.log.info("curReq="+curReq);
				HashSet<String> curObjs=manager.globalRequestToObjectMapOld.get(curReq);
				curObjs.remove(key);
				String newObj=getVirtualObject(curReq,key);
				curObjs.add(newObj);
				
				// here we add the newer vertex to graph
				objectsGraph.addVertex(newObj);
				
				if (!objectsGraph.containsEdge(key, newObj) && !objectsGraph.containsEdge(newObj, key) )
				{
					
					objectsGraph.addEdge(key,newObj); // here we connect the object to its virtual one
					
				}
				
			}
			
			
			
			}
			
			
		}
		
		public void construcJGraphTSchismReplication()
		{
	
			Analyser.log.info("SCHISM-REPLICATION...Constructing Class N...."+manager.globalObjectToRequestMap.size());
			
			for(Entry<String, HashSet<String>> entry : manager.globalRequestToObjectMapOld.entrySet())
			{
			HashSet<String> objects = null;
			String key=entry.getKey(); // key is the request
			
			if (!ConsiderReq(key))
			{
				Analyser.log.info("!ConsiderReq(key)");
				continue;
			}
			
			if (key.contains("-1") || (key==null) || key.contains("RegisterItem"))
			{
				Analyser.log.info("key.contains(\"-1\") || (key==null) || key.contains(\"RegisterItem\")");
				continue;
			}
			
			objects=entry.getValue();
			if (objects==null)
				{
					Analyser.log.info("objects==null=" +key);
					continue;
				}
			
		//	if (!ConsiderObjectOri(key))
		//		continue;
			
			
			
			
			if (objects.size()==1)
			{
				//Analyser.log.info("size==1 continue");
				continue;
			}
			
			/*
			if (objects.size()>35)
				continue;
			*/
			///////////
			
			/*
			if (key.contains("Option=1"))
			{
				continue;
			}
			*/
			//if (objects.size()>100)
			//if (objects.size()>120)
				//continue;
			
			
			Object[] arr= objects.toArray();
			
			int counter =0;
			for (int k=0;k<arr.length-1;k++)
			{

				String s11=(String) arr[k];
				
				if (!manager.globalCacheMap.containsKey(s11) && !manager.globalCacheMap.containsKey(getOriginalObjectSchismReplication(s11)))
				{
					Analyser.log.info("if (!manager.globalCacheMap.containsKey(s11))");
					continue;
				}
				objectsGraph.addVertex(s11);
				for (int j=k+1;j<arr.length;j++)
				{
					String s22=(String) arr[j];
					if (s22.contains("-1") || (s22==null) || s11.equals(s22) || s22.contains("RegisterItem"))
					{
						Analyser.log.info("s22.contains(\"-1\") || (s22==null) || s11.equals(s22) || s22.contains(\"RegisterItem\")");
						continue;
					}
					
					if (!manager.globalCacheMap.containsKey(s22) && !manager.globalCacheMap.containsKey(getOriginalObjectSchismReplication(s22)))
					{
						Analyser.log.info("!manager.globalCacheMap.containsKey(s22)");
						continue;
					}
					
					
					// the idea here is not to add any ORIGINAL vertex that has been expanded before
					if (manager.globalCacheMap.containsKey(s22) && manager.globalObjectToRequestMap.get(s22).size()>1)
						continue;
					
					if (manager.globalCacheMap.containsKey(s11) && manager.globalObjectToRequestMap.get(s11).size()>1)
						continue;

						
					
					objectsGraph.addVertex(s22);
					if (!objectsGraph.containsEdge(s11, s22) && !objectsGraph.containsEdge(s22, s11) )
					{
						
						objectsGraph.addEdge(s11,s22);
						counter++;
					}
					
				}
				
			}
			
			//if (objects.size()>100)
				//Analyser.log.info("objects.size()>100 "+key +"dr="+manager.globalRequestMap.get(key).driftRate);

				
			//Analyser.log.info("size="+objects.size()+ "edges="+counter);
			
			
			////////// Ori By Omar
			
			/*
			for (String s1 : objects)
			{
				objectsGraph.addVertex(s1);
				
				for (String s2 : objects)
				{
					
					if (s2.contains("-1") || (s2==null) || s1.equals(s2) || s2.contains("RegisterItem"))
						continue;
						
					objectsGraph.addVertex(s2);
					if (!objectsGraph.containsEdge(s1, s2) && !objectsGraph.containsEdge(s2, s1) )
						objectsGraph.addEdge(s1,s2);
						//DefaultEdge d= requestsGraph.addEdge(s1,s2);			
				}
			}
			
			
			*/
			
			
			}	
			
			Analyser.log.info("DONE Constructing Class N...."+manager.globalObjectToRequestMap.size());
		}
		
		
		
		//regular graph and GPMETIS
		public void construcJGraphT()
		{
			
			if (objectsGraph.vertexSet().size()>0)
			{
				clearGraph(objectsGraph);
			}
			
			
			
			Analyser.log.info("Constructing Class N...."+manager.globalObjectToRequestMap.size());
			
			for(Entry<String, HashSet<String>> entry : manager.globalRequestToObjectMap.entrySet())
			{
			HashSet<String> objects = null;
			String key=entry.getKey(); // key is the request
			
			if (!ConsiderReq(key))
			{
				Analyser.log.info("!ConsiderReq(key)");
				continue;
			}
			
			if (key.contains("-1") || (key==null) || key.contains("RegisterItem"))
			{
				Analyser.log.info("key.contains(\"-1\") || (key==null) || key.contains(\"RegisterItem\")");
				continue;
			}
			
			objects=entry.getValue();
			if (objects==null)
				{
					Analyser.log.info("objects==null=" +key);
					continue;
				}
			
		//	if (!ConsiderObjectOri(key))
		//		continue;
			
			
			
			
			if (objects.size()==1)
			{
				//Analyser.log.info("size==1 continue key="+key +"objects.toString()"+objects.toString());
				continue;
			}
			
			/*
			if (objects.size()>35)
				continue;
			*/
			///////////
			
			/*
			if (key.contains("Option=1"))
			{
				continue;
			}
			*/
			//if (objects.size()>100)
			//if (objects.size()>120)
				//continue;
			
			
			Object[] arr= objects.toArray();
			
			int counter =0;
			for (int k=0;k<arr.length-1;k++)
			{

				String s11=(String) arr[k];
				
				if (!manager.globalCacheMap.containsKey(s11))
				{
					Analyser.log.info("if (!manager.globalCacheMap.containsKey(s11))");
					continue;
				}
				objectsGraph.addVertex(s11);
				for (int j=k+1;j<arr.length;j++)
				{
					String s22=(String) arr[j];
					if (s22.contains("-1") || (s22==null) || s11.equals(s22) || s22.contains("RegisterItem"))
					{
						Analyser.log.info("s22.contains(\"-1\") || (s22==null) || s11.equals(s22) || s22.contains(\"RegisterItem\")");
						continue;
					}
					
					if (!manager.globalCacheMap.containsKey(s22))
					{
						Analyser.log.info("!manager.globalCacheMap.containsKey(s22)");
						continue;
					}
						
					
					objectsGraph.addVertex(s22);
					if (!objectsGraph.containsEdge(s11, s22) && !objectsGraph.containsEdge(s22, s11) )
					{
						
						objectsGraph.addEdge(s11,s22);
						counter++;
					}
					
				}
				
			}
			
		//	if (objects.size()>100)
		//		Analyser.log.info("objects.size()>100 "+key +"dr="+manager.globalRequestMap.get(key).driftRate);

				
			//Analyser.log.info("size="+objects.size()+ "edges="+counter);
			
			
			////////// Ori By Omar
			
			/*
			for (String s1 : objects)
			{
				objectsGraph.addVertex(s1);
				
				for (String s2 : objects)
				{
					
					if (s2.contains("-1") || (s2==null) || s1.equals(s2) || s2.contains("RegisterItem"))
						continue;
						
					objectsGraph.addVertex(s2);
					if (!objectsGraph.containsEdge(s1, s2) && !objectsGraph.containsEdge(s2, s1) )
						objectsGraph.addEdge(s1,s2);
						//DefaultEdge d= requestsGraph.addEdge(s1,s2);			
				}
			}
			
			
			*/
			
			
			}	
			
			Analyser.log.info("DONE Constructing Class N...."+manager.globalObjectToRequestMap.size());
		}
		
		
		public boolean overlappedObject(String o1, String o2)
		{
			HashSet<String> requests1,requests2 = null;
			
			requests1=manager.globalObjectToRequestMap.get(o1);
			requests2=manager.globalObjectToRequestMap.get(o2);
			
			if (requests1==null || requests2 == null)
				return false;
			
			//Analyser.log.info("requests1="+requests1.size()+ "requests2="+requests2.size());
			
			requests1.retainAll(requests2);
			
			if (requests1.size()==0)
				return false;
			else
				return true;
			
			
			
		}
		
		// New Graph Construction By Omar
		public void construcJGraphTNew()
		{
			
			if (objectsGraph.vertexSet().size()>0)
			{
				clearGraph(objectsGraph);
			}
			
			Analyser.log.info("Constructing Class N...."+manager.globalObjectToRequestMap.size());
			
			for(Entry<String, CacheObject> entry : manager.globalCacheMap.entrySet())
			{
				String key=entry.getKey(); // key is the Object
				objectsGraph.addVertex(key);
			}
			
			Object[] objects=manager.globalCacheMap.keySet().toArray();
			
			Analyser.log.info("objects.length="+objects.length);
			Analyser.log.info("objectsGraph.vertexSet().size()="+objectsGraph.vertexSet().size());
			
			int counter=0;
			for (int k=0;k<objects.length-1;k++)
			{
				String s11=(String) objects[k];
				
				for (int j=k+1;j<objects.length;j++)
				{
					String s22=(String) objects[j];
					if (s22.contains("-1") || (s22==null) || s11.equals(s22) || s22.contains("RegisterItem"))
						continue;
						
					//Analyser.log.info("k= "+k +"j= "+j);
					
					if (!objectsGraph.containsEdge(s11, s22) && !objectsGraph.containsEdge(s22, s11) )
						counter++;
						//objectsGraph.addEdge(s11,s22);
					
				}
				Analyser.log.info("counter="+counter);
				
				
			}			
		}
		
		
		public List<CacheObject> sortRequestObjectList(HashSet<String> objectList,final boolean desc)
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
					return  desc? (int)o2.tempreture - (int)o1.tempreture : (int)o1.tempreture - (int)o2.tempreture;
				}
				
			});
			
		return map;
			
		}
		
		// to construct object based hyper graph
		public void construcJHyperGraphT()
		{
			
			Analyser.log.info("manager.gloabCacheObjectIndexing="+manager.gloabCacheObjectIndexing.size());
			manager.gloabCacheObjectIndexing.clear();
			manager.gloabalCacheKeyCounter=1;
			 emptyresultGraphFile();
				PrintWriter out = null;
				
				 StringBuffer sentence = new StringBuffer();
				
			int edgesCount=0;
			try
			{
				out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
			} 
			catch (IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			String toAppend = "";
			String weight="";
			int counter=0;
			for(Entry<String, HashSet<String>> entry : manager.globalRequestToObjectMap.entrySet())
			{
				String hyperEdges="";
				HashSet<String> objects = null;
				String key=entry.getKey(); // key is the request
				objects=manager.globalRequestToObjectMap.get(key);
				
				
				
				if (objects==null)
					continue;
				
				HttpRequestObject hr=manager.globalRequestMap.get(key);
				
				if (hr==null)
				{
					Analyser.log.info("HttpRequestObject is null "+  key); 
					continue;
					
				}
				
				if (!ConsiderReq(key))
					continue;
				
				
				
				if (objects.size()>26 && hr.counter>2 && useRegression.equals("yes"))
				{
					//Analyser.log.info(hr.url +"###"+hr.counter);
					List<CacheObject> sortedObjectList = new ArrayList<CacheObject>();
					sortedObjectList=sortRequestObjectList(entry.getValue(), true);
					weight=Integer.toString(hr.counter);	
					
					
					Iterator<CacheObject> it = sortedObjectList.iterator();
					
					for (int x=0;x<25;x++)
					{
						

						CacheObject co=sortedObjectList.get(x);
						if (co==null)
						{
							//Analyser.log.info("ERROR NULL OBJ"+co);
							
						}
						else
						{
							if (!manager.globalCacheMap.containsKey(co.cacheKey))
								continue;
							
							//if (getSetObjectIndex(co.cacheKey)<1)
							//	Analyser.log.info("ERROR (co)<1 "); 
							//hyperEdges=hyperEdges+" "+co.index;sfd
							hyperEdges=hyperEdges+" "+getSetObjectIndex(co.cacheKey);
							//Analyser.log.info("co.cacheKey="+co.cacheKey +"co.temp="+co.tempreture);
							
						}
					}
					
				}
				
				else
				{
					weight=Integer.toString(hr.counter);				
					Iterator<String> it = objects.iterator();
					while (it.hasNext())
					{
						String m=it.next();
						CacheObject co=manager.globalCacheMap.get(m);
						if (co==null)
						{
							Analyser.log.info("ERROR NULL OBJ"+co);
							
						}
						else
						{
							//if (getSetObjectIndex(m)<1)
								//Analyser.log.info("ERROR (co)<1 "); 
							//hyperEdges=hyperEdges+" "+co.index;sfd
							hyperEdges=hyperEdges+" "+getSetObjectIndex(m);
							
						}
					}
				}
				sentence.append(weight+" "+hyperEdges+"\n");
				edgesCount++;
				}
			Analyser.log.info("manager.gloabCacheObjectIndexing="+manager.gloabCacheObjectIndexing.size());
					
					String toAppendFirstLine="";
					toAppendFirstLine=edgesCount+" "+ manager.gloabCacheObjectIndexing.size()+ " 11 \n";
					StringBuffer StringBufferVertexWgt=new StringBuffer();
					
					Analyser.log.info("manager.gloabCacheObjectIndexing="+manager.gloabCacheObjectIndexing.size());
					Analyser.log.info("manager.gloabalCacheKeyCounter="+manager.gloabalCacheKeyCounter);
					Analyser.log.info("manager.globalCacheMap="+manager.globalCacheMap.size());
					
					for (int x=1;x<manager.gloabCacheObjectIndexing.size();x++)
					{
						int vwgt=0;
						String cacheK=manager.gloabCacheObjectIndexing.get(x);
						CacheObject co=manager.globalCacheMap.get(cacheK);
						if (co==null)
						{
							//Analyser.log.info("co==null Error");
							continue;
						}
						
						if (useRegression.equals("yes"))
						{
							if (co.tempreture<2)
								vwgt=1;
							else
								vwgt=(int) co.tempreture;
						}
						else
						{
							vwgt=co.getCount;
						}
		
						StringBufferVertexWgt.append(vwgt+ "\n");
					}
					/*
					for (String cacheObj:manager.gloabCacheObjectIndexing.values())
					{
						int vwgt=0;
						CacheObject co=manager.globalCacheMap.get(cacheObj);
						if (co!=null)
							vwgt=co.getCount;
						
						
							StringBufferVertexWgt.append(vwgt+ "\n");
					}
					*/
					Analyser.log.info("toAppendFirstLine="+toAppendFirstLine);
					out.println(toAppendFirstLine +sentence.toString()+ StringBufferVertexWgt.toString());
					out.close();		
			
		}
			
	
	
		 
	
			
			
		
	
	
	
	
	
	
	
	public ObjectBasedAnalyserGraph(StatisticsManager manager) {
		super(manager);
		setupStrategy();
		
		
		
		
		individualMap = new HashMap<String, HashMap<String,CacheObject>>();

		
		
		
	}
	
	//make it read a strategy from configuration
	
	
	public void setupStrategy(){
		
		try{
		
			System.out.println("enter here");
		Class c = Class.forName(manager.props.getProperty(RBA_STRATEGY_CLASS), false, this.getClass()
				.getClassLoader());
		
		System.out.println("RBA_STRATEGY_CLASS\n"+manager.props.getProperty(RBA_STRATEGY_CLASS));
		
			
			
		strategy = (ObjectBasedAnalyserGraphStrategy) c.getConstructor(new Class[] { ObjectBasedAnalyserGraph.class }).newInstance(this);
		//strategy = (RequestBasedAnalyserGraphStrategy) c.getConstructor(new Class[] { RequestBasedAnalyserGraph.class }).newInstance(this.currentAnalysisPhase);
		
		}catch(Exception ex){
			System.out.println("RBA_STRATEGY_CLASS\n"+manager.props.getProperty(RBA_STRATEGY_CLASS));
			throw new RuntimeException(ex);
		}
	}

	/**
	 * Called by StatisticsManager periodically. Should create policy files for
	 * AS and LB.
	 *  
	 */
	
	
	public void countServererObjectsAccessFreq()
	{
		
		for(ASServer AStemp:manager.getASServers().values())
		{
			
			HashMap<String, CacheObject> tempObjects=null;
			tempObjects=individualMap.get(AStemp.serverId);
			if (tempObjects==null)
				continue;
			Collection<CacheObject> colCacObj = tempObjects.values();
			
			int counter=0;
			for(CacheObject co: colCacObj)
			{
				CacheObject cachObjTemp = manager.globalCacheMap.get(co.cacheKey);
				counter+=cachObjTemp.getCount;	
			}
			Analyser.log.info("Server " +AStemp.serverId+" ObjectsAccessFreq="+counter);
			
			
		}
	}
		
	
	public List<HttpRequestObject> sortRequestObjectList(List<HttpRequestObject> reqList,final boolean desc)
	{
		
		//List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map.values());
		
		
		
		Collections.sort(reqList, new Comparator<HttpRequestObject>(){

			@Override
			public int compare(HttpRequestObject r1, HttpRequestObject r2) 
			{
				return  desc? (int)r2.weight- (int)r1.weight : (int)r1.weight - (int)r2.weight;
			}
			
		});
		
	return reqList;	
	}
	
	public List<HttpRequestObject> sortRequestObjectListRegression(List<HttpRequestObject> reqList,final boolean desc)
	{
		
		//List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map.values());
		
		
		
		Collections.sort(reqList, new Comparator<HttpRequestObject>(){

			@Override
			public int compare(HttpRequestObject r1, HttpRequestObject r2) 
			{
				return  desc? (int)r2.tempreture- (int)r1.tempreture : (int)r1.tempreture - (int)r2.tempreture;
			}
			
		});
		
	return reqList;	
	}
	
	
	
	public void calculateRequestTempreture(HttpRequestObject hro)
	{
	//	boolean t=true;
	//	if (t==true)
	//		return;
				
		if (!useRegression.equals("yes"))
			return;
			
		
		
		if (hro.counter==1)
		{
			hro.tempreture=1;
			return;
		}
		
		int [] accessCountsFramed ;
		long startTime = manager.currentIntervalStartTime;
		long windowTime=Long.parseLong(manager.props.getProperty(manager.ANALYSIS_INTERVAL));
		long intervalCounts=Long.parseLong(manager.props.getProperty(manager.NO_SUB_INTERVALS));;
		long intervalLenth=windowTime/intervalCounts;
		accessCountsFramed= new int [(int) intervalCounts];
		Arrays.fill(accessCountsFramed, 0);
		
		Iterator<Long> ir=hro.accessTimes.iterator();
		//Analyser.log.info(startTime);
		//Analyser.log.info(intervalLenth);
		while (ir.hasNext())
		{
			
		long currTime=(long) ir.next();
		//Analyser.log.info(currTime);
		long intervalNo=(currTime-startTime)/intervalLenth;
		//Analyser.log.info(intervalNo +" XXX "+intervalCounts);
		if (intervalNo<0)
		{
			Analyser.log.info("wrong interval no");
			continue;
		}
		
		if (intervalNo==intervalCounts)
			intervalNo=intervalCounts-1;
		accessCountsFramed[(int)intervalNo]++;
		}

		long total=0;
		long res=0;
		double[] xx = new double[(int) intervalCounts];
		double[] yy = new double[(int) intervalCounts];
		for (long x=0;x<intervalCounts;x++)
		{
			xx [(int) x]=x;
			yy [(int) x]=accessCountsFramed[(int)x];
			// res+= (x+1) * accessCountsFramed[(int)x];
		//	total+=x;
		}
		
	LinearRegression lr= new LinearRegression(xx, yy);	
	
	
	for (long x=intervalCounts;x<2*intervalCounts;x++)
	{
		hro.tempreture+=lr.predict(x);
	}
	
	
	
	if (hro.counter>2)
	{
	Analyser.log.info("-------------------------------------------------");
	Analyser.log.info(hro.url + "hro.getCount="+hro.counter +"hro.tempreture="+hro.tempreture);
	//	Analyser.log.info(co.accessTimes.size()+""+co.getCount);
	
	String s="";
	int first=0;
	int second=0;
			for (long x=0;x<intervalCounts;x++)
		{
				s=s+" "+accessCountsFramed[(int) x];
				if (x<intervalCounts/2)
					first+=accessCountsFramed[(int)x];
				else
					second+=accessCountsFramed[(int)x];
			
		}	
			//if (manager.counter>0)
				Analyser.log.info(s +"===>"+first+"/"+second);
	}
	}
	
	
	public void calculateObjectTempreture(CacheObject co)
	{
			
			boolean t=true;
			if (t==true)
				return;
			
		if (!useRegression.equals("yes"))
			return;
			
		
		if (co.getCount==1)
		{
			co.tempreture=1;
			return;
		}
		int [] accessCountsFramed ;
		long startTime = manager.currentIntervalStartTime;
		long windowTime=Long.parseLong(manager.props.getProperty(manager.ANALYSIS_INTERVAL));
		long intervalCounts=Long.parseLong(manager.props.getProperty(manager.NO_SUB_INTERVALS));;
		long intervalLenth=windowTime/intervalCounts;
		accessCountsFramed= new int [(int) intervalCounts];
		Arrays.fill(accessCountsFramed, 0);
		
		Iterator<Long> ir=co.accessTimes.iterator();
		//Analyser.log.info(startTime);
		//Analyser.log.info(intervalLenth);
		while (ir.hasNext())
		{
			
		long currTime=(long) ir.next();
		//Analyser.log.info(currTime);
		long intervalNo=(currTime-startTime)/intervalLenth;
		//Analyser.log.info(intervalNo +" XXX "+intervalCounts);
		if (intervalNo<0)
		{
			Analyser.log.info("wrong interval no");
			continue;
		}
		
		if (intervalNo==intervalCounts)
			intervalNo=intervalCounts-1;
		accessCountsFramed[(int)intervalNo]++;
		}

		long total=0;
		long res=0;
		double[] xx = new double[(int) intervalCounts];
		double[] yy = new double[(int) intervalCounts];
		for (long x=0;x<intervalCounts;x++)
		{
			xx [(int) x]=x;
			yy [(int) x]=accessCountsFramed[(int)x];
			// res+= (x+1) * accessCountsFramed[(int)x];
		//	total+=x;
		}
		
	LinearRegression lr= new LinearRegression(xx, yy);		
	
	for (long x=intervalCounts;x<2*intervalCounts;x++)
	{
		co.tempreture+=lr.predict(x);
	}
	
	
	
	if (co.getCount>2)
		{
		Analyser.log.info("-------------------------------------------------");
		Analyser.log.info(co.cacheKey.toString() + "co.getCount="+co.getCount +"co.tempreture="+co.tempreture);
		//	Analyser.log.info(co.accessTimes.size()+""+co.getCount);
		
		String s="";
				for (long x=0;x<intervalCounts;x++)
			{
					s=s+" "+accessCountsFramed[(int) x];
				
			}	
				//if (manager.counter>0)
					Analyser.log.info(s);
		}
		
		//co.tempreture=res/total;
		
		
		
		
	}
		
	public void  mergeASCacheLogs(){
		manager.globalCacheMap.clear();
		manager.gloabalCacheKeyCounter=1;
		for(ASServer AStemp:manager.getASServers().values())
		{
			
			CacheLogList cl = servCacheLogIndex.get(AStemp);
		//	CacheLogList cl= (CacheLogList) AStemp.getStruct("cacheLogGroup");
			HashMap<String, CacheObject> tempMap = cl.aggregateSegments();
			
			if(tempMap == null)
				continue;
					
				individualMap.put(AStemp.serverId, tempMap);
				
				Collection<CacheObject> colCacObj = tempMap.values();
				
				for(CacheObject co: colCacObj){
					CacheObject cachObjTemp = manager.globalCacheMap.get(co.cacheKey);
					
					if(cachObjTemp == null)
					{
						//if (!co.sites.contains(AStemp.getServerId()))
							//co.sites.add(AStemp.getServerId());
						manager.globalCacheMap.put(co.cacheKey, co);
						calculateObjectTempreture(co);
						/////manager.gloabCacheObjectIndexing.put(manager.gloabalCacheKeyCounter, co.cacheKey);
						/////co.index=manager.gloabalCacheKeyCounter;
						manager.totVtxWgt+=co.getCount; // for graph comparison
						if (co.size==1)
						{
							//Analyser.log.info(co.cacheKey.toString());
							//AStemp.curObjBits.set(co.index);
							//co.sites.add(AStemp.getServerId());
							co.last_put_server=AStemp.serverId;
							co.last_put_time_global=co.last_put_time;
						}
						////manager.gloabalCacheKeyCounter++;
						
					}
					else
					{
						manager.totVtxWgt+=co.getCount; // for graph comparison
						cachObjTemp.getCount = cachObjTemp.getCount + co.getCount;
						cachObjTemp.no_sites++;
						//cachObjTemp.sites.add(AStemp.getServerId());
						if (co.size==1 && cachObjTemp.size==1)
						{	
							
							if (co.last_put_server.equals(cachObjTemp.last_put_server))
								if (cachObjTemp.last_put_time_global<co.last_put_time)
								cachObjTemp.last_put_time_global=co.last_put_time;
							
							else if (cachObjTemp.last_put_time_global<co.last_put_time)
							{
								Analyser.log.info("start1");
								//AStemp.curObjBits.set(cachObjTemp.index);
								//co.sites.add(AStemp.getServerId());
						//		manager.servers.get(cachObjTemp.last_put_server).curObjBits.set(cachObjTemp.index, false);
								cachObjTemp.last_put_time_global=co.last_put_time;
								cachObjTemp.last_put_server=AStemp.serverId;
							}
								

						}
						else if (co.size==1)
						{
							//Analyser.log.info("start2");
					//		AStemp.curObjBits.set(cachObjTemp.index);
							//co.sites.add(AStemp.getServerId());
							cachObjTemp.last_put_time_global=co.last_put_time;
							cachObjTemp.last_put_server=AStemp.serverId;
							cachObjTemp.size=1;
							
						}
						
					}
					
				}
			}
			//Collections.sort(cacheList); // sort on getCount
		}
		
////////////////////////////NEW DS///////////////////
		@SuppressWarnings("unchecked")
		private HashMap<String, HttpRequestObject> mergeHttpRequestObjectsFromServers()
		{			
		return (HashMap<String, HttpRequestObject>) manager.globalRequestMap.clone();	
		}
		
		/**
		 * this method will first do segment merge.. and the all server merge
		 */
		private HashMap<String, HttpRequestObject> mergeHttpRequestObjectsFromServersOri(){
			List<HashMap<String,HttpRequestObject>> list = new ArrayList<HashMap<String,HttpRequestObject>>();
			
			for(ASServer serv:manager.getASServers().values()){
				//HttpLogList hll = (HttpLogList)serv.getStruct(HttpRequestProcessor.REQ_LOG_GROUP);
				HttpLogList hll = servHttpLogIndex.get(serv);
				
				//HashMap<String,HttpRequestObject> aggMap = hll.aggregateSegments();
				if (hll.httpLogList.size()<1) // may be some servers have not received requests yet!
					continue;
				HashMap<String,HttpRequestObject> aggMap = hll.getSegment(hll.httpLogList.size()-1);
				list.add(aggMap);	
			//	System.out.println("Omar======serv.serverId:" +serv.serverId );
			//	System.out.println("Omar======hll.httpLogList.size():" +hll.httpLogList.size());
			//	System.out.println("Omar======aggMap.size():" +aggMap.size());
			}
			
			return HttpLogList.mergeHttpRequestObjects(list);
			
		}
		
		
		
		
		
		
		
		public void toCheckRequestEligibilty(HashSet<String> res, String hr)
		{
			HttpRequestObject hro=manager.globalRequestMap.get(hr);
			//Analyser.log.info(hro.url );
			if (hro.counter<2)
				return;
			
			ASServer serv = manager.getASServer(0);
			RequestToResourcesMap reqToRes = (RequestToResourcesMap) serv.getStruct("reqObjMap");
			Map<String, Integer> objToFreq=reqToRes.mapNew.get(hr);
			int objCounter=0;
			for (Integer freq:objToFreq.values())
			{
				objCounter+=freq;
				//Analyser.log.info(hro.url +"$$$"+hro.driftRate);
				
			}
			
			
					
		//	hro.toConsiderValue=((float)objCounter/(float)objToFreq.size())/(float)hro.counter;
			
			hro.ObjNo=(float)objCounter/(float)hro.counter;
			float objChng=((float)objToFreq.size()-hro.ObjNo)/((float)hro.counter-1);
			hro.driftRate=objChng/hro.ObjNo;
			
			/*
			Analyser.log.info("--------------------------------------------");
			Analyser.log.info(hro.url +"$$$"+hro.driftRate);
			
			if (hro.driftRate<0)
			
			Analyser.log.info(hro.url +"$$$"+hro.driftRate);			
			Analyser.log.info(hro.ObjNo);
			Analyser.log.info("objCounter="+objCounter);
			Analyser.log.info("objToFreq.size()="+objToFreq.size());
			Analyser.log.info("hro.counter="+hro.counter);
			Analyser.log.info("objChng="+objChng);
			
			Iterator ir=objToFreq.keySet().iterator();
			while (ir.hasNext())
			{
				String obj=(String) ir.next();
				int freq= objToFreq.get(obj);
				Analyser.log.info(obj +"*****"+ freq);
				
				
			}*/
		
		}
		
		
		
		private void mergeCurrentHttpLogGlobal()
		{
			manager.gloabalRequestKeyCounter=1;
			manager.globalRequestMap.clear();
			manager.gloablRequestIndexing.clear();
			
			// for incremental
		
			
			
			for (HttpRequestObject key:mergedHttpMapAll.values())
			{
				HashSet<String> res=manager.globalRequestToObjectMap.get(key.url);
				if (res==null)
					continue;
		//		toCheckRequestEligibilty(res,key);
				
			//	key.weight=key.counter*res.size();
				
				HttpRequestObject obj = manager.globalRequestMap.get(key.url);
				
				
				
				//toCheckRequestEligibilty(res,key);
				
				
				
				
				if(obj == null)
				{
					
					obj = (HttpRequestObject) key.clone();
					manager.gloablRequestIndexing.put(manager.gloabalRequestKeyCounter,obj.url);
					manager.globalRequestMap.put(obj.url,obj);
					key.index=manager.gloabalRequestKeyCounter;
					obj.index=manager.gloabalRequestKeyCounter;
				//	Analyser.log.info("w"+ key.weight +" erer "+obj.weight);
					
					manager.gloabalRequestKeyCounter++;
					newRequests++;
					
					manager.totEdgeWgt+=obj.counter;
						
					ASServer serv = manager.getASServer(0);
					RequestToResourcesMap reqToRes = (RequestToResourcesMap) serv.getStruct("reqObjMap");
					//obj.objectFreq.putAll(reqToRes.mapNew.get(obj.url));
					
				}
				else
				{
					
					//if (obj.counter !=0 && reqObj.counter !=0)
					//	obj.execution_time= ((obj.execution_time * obj.counter) + (reqObj.execution_time * reqObj.counter)) / (obj.counter+reqObj.counter);
					obj.counter += key.counter;
					obj.accessTimes.addAll(key.accessTimes);
					manager.totEdgeWgt+=obj.counter;
					 
					
					//TODO update other variables too
				}		
				
				
				toCheckRequestEligibilty(res,key.url);
				//calculateRequestTempreture(key);
				//httpListAll.add(key);
			}
			
			Analyser.log.info("-----manager.globalRequestMap.size()"+ manager.globalRequestMap.size());
			Analyser.log.info("-----httpListAll.size()"+ httpListAll.size());
		}
		
		
		
		
		
		
		private void driftCounter()
		{
			
			for (int x=0;x<httpListAll.size();x++)
			{
				
				HttpRequestObject curReq= httpListAll.get(x);
				HashSet<String> objects = manager.globalRequestToObjectMap.get(curReq.url);
				if(objects == null)
				{
				//	Analyser.log.info("curReq objects is nul"+ curReq.url);
					continue;
				}
				Iterator ir= objects.iterator();
				curReq.visitedReq=false;
				
				
				if (manager.globalRequestMap!=null) // the old time window is not empty
				{
					int localCounter=0;
					//HttpRequestObject oldReq=mergedHttpMapAllPreviousInterval.get(curReq.url);
					HttpRequestObject oldReq=manager.globalRequestMap.get(curReq.url);
					if (oldReq ==null)
					{
						globalObjectDriftCounter+=objects.size();
						Analyser.log.info("oldReq ==null");
						continue;
					}
					oldReq.visitedReq=false;
					while (ir.hasNext() && curReq.bloomFilter!=null && !oldReq.objectDrift)	
					{
						
						String s=(String) ir.next();
					//	Analyser.log.info("Wilte true");
						if (!oldReq.bloomFilter.test(s))
						{
							localCounter++;
						//	Analyser.log.info("localCounter= " +localCounter);
						}
						curReq.bloomFilter.add(s);
							
					}	
					oldReq.objectDrift=true;
					globalObjectDriftCounter+=localCounter;
				}
				else if (manager.globalRequestMap==null) // the old time window is empty
				{
					while (ir.hasNext() && curReq.bloomFilter!=null)
					{
						//Analyser.log.info("Analayser add first time BF");
						String s=(String) ir.next();
						curReq.bloomFilter.add(s);
					}
					globalObjectDriftCounter+=objects.size();
				}
					
				
				
			}
		}
		
		
		@SuppressWarnings("unchecked")
		private RequestToResourcesMap mergeAllReqToRes() throws CloneNotSupportedException{
			
			RequestToResourcesMap mapAll = new RequestToResourcesMap(manager);
			
			
			
			globalObjectDriftCounter=0;
			for(ASServer serv: manager.getASServers().values()){
			//	RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
				
				RequestToResourcesMap map =	servReqtoResIndex.get(serv);
				CacheLogList cl=servCacheLogIndex.get(serv);
				
				System.out.println("Omar======"+serv.serverId+" size= " +map.map.size());
				
				Analyser.log.info("mergedHttpMapAll size " +this.mergedHttpMapAll.size());
				if (mergedHttpMapAllPreviousInterval!=null)
					Analyser.log.info("mergedHttpMapAllPreviousInterval size" +this.mergedHttpMapAllPreviousInterval.size());
				for(Entry<String, HashSet<String>> entry : map.map.entrySet())
				{
					
					HashSet<String> set = mapAll.map.get(entry.getKey());
					if(set == null)
					{
						set = new HashSet<String>();
						mapAll.map.put(entry.getKey(), set);
					}
					
					set.addAll(entry.getValue());
					Iterator ir=set.iterator();
					HttpRequestObject hr= mergedHttpMapAll.get(entry.getKey());
				
					
			}
			}
			return mapAll;
		}
		
		
		
		
		@SuppressWarnings("unchecked")
		private void mergeAllReqToResGlobal() throws CloneNotSupportedException
		{
			manager.globalRequestToObjectMap.clear();
			globalObjectDriftCounter=0;
			for(ASServer serv: manager.getASServers().values())
			{
				RequestToResourcesMap map =	servReqtoResIndex.get(serv);	
				System.out.println("Omar======"+serv.serverId+" size= " +map.map.size());
				
				for(Entry<String, HashSet<String>> entry : map.map.entrySet())
				{	
					HashSet<String> set = manager.globalRequestToObjectMap.get(entry.getKey());
					if(set == null)
					{
						set = new HashSet<String>();
						//mapAll.map.put(entry.getKey(), set);
						manager.globalRequestToObjectMap.put(entry.getKey(), set);
					}
					
					set.addAll(entry.getValue());
					
					
				
					
			    }
			}
			
		}
		
		
		
		
		/*
		 * 
		 * private RequestToResourcesMap mergeAllReqToRes() throws CloneNotSupportedException{
			
			RequestToResourcesMap mapAll = new RequestToResourcesMap(manager);
			
			globalObjectDriftCounter=0;
			for(ASServer serv: manager.getASServers().values()){
			//	RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
				
				RequestToResourcesMap map =	servReqtoResIndex.get(serv);
				
				
				System.out.println("Omar======"+serv.serverId+" size= " +map.map.size());
				
				Analyser.log.info("mergedHttpMapAll size " +mergedHttpMapAll.size());
				if (mergedHttpMapAllPreviousInterval!=null)
					Analyser.log.info("mergedHttpMapAllPreviousInterval size" +mergedHttpMapAllPreviousInterval.size());
				for(Entry<String, HashSet<String>> entry : map.map.entrySet()){
					
					HashSet<String> set = mapAll.map.get(entry.getKey());
					if(set == null){
						set = new HashSet<String>();
						mapAll.map.put(entry.getKey(), set);
					}
					
					set.addAll(entry.getValue());
					Iterator ir=set.iterator();
					HttpRequestObject hr= mergedHttpMapAll.get(entry.getKey());
					if (hr!=null)
						Analyser.log.info("Drift " +hr.url);
					int localCounter=0;
					if (hr!=null && mergedHttpMapAllPreviousInterval!=null)
					{
						
						
						if (mergedHttpMapAllPreviousInterval.get(entry.getKey()) != null)
						{
							Analyser.log.info("OLD INTERVAL != NULL ");
							HttpRequestObject hrOld= mergedHttpMapAllPreviousInterval.get(entry.getKey());
							
						//	Analyser.log.info(" BL Old"+ hrOld.bloomFilter.bitSet.toString());
							while (ir.hasNext() && hr.bloomFilter!=null && !hrOld.objectDrift)
							{
								
								String s=(String) ir.next();
								Analyser.log.info("Wilte true");
								
							//	Analyser.log.info("ttt3");
								if (!hrOld.bloomFilter.test(s))
								{
									localCounter++;
									Analyser.log.info("localCounter= " +localCounter);
								}
								hr.bloomFilter.add(s);
									
							}	
							hrOld.objectDrift=true;
						}
						
					if (localCounter>0)
					{
						//globalObjectDriftCounter+=localCounter * hr.counter;
						globalObjectDriftCounter+=localCounter;
						//hr.bloomFilterOld=hr.bloomFilter.cloneCo();
						//hr.bloomFilterOld= hr.bloomFilter.cloneCo();
					//	Analyser.log.info("globalObjectinnnnnDriftCounter "+ globalObjectDriftCounter);
						//Analyser.log.info("Equal !!! "+ hrOld.bloomFilter.equals(hr.bloomFilter));
						localCounter=0;
						
					}
					}
					
					else if  (hr!=null && mergedHttpMapAllPreviousInterval==null)
					{
						Analyser.log.info("OLD INTERVAL == NULL ");
					//	Analyser.log.info("ttt0" +hr.url);
					//	Analyser.log.info(" BL Before"+ hr.bloomFilter.bitSet.toString());
						while (ir.hasNext() && hr.bloomFilter!=null)
						{
							
							Analyser.log.info("Analayser add first time BF");
							
							String s=(String) ir.next();
						//	Analyser.log.info("ttt2" +s);
							
						//	Analyser.log.info("ttt3");
							hr.bloomFilter.add(s);
							
						}
					//	Analyser.log.info(" BL "+ hr.bloomFilter.bitSet.toString());
						} 
				}
			}
			
			return mapAll;
		}
		
		
		 * 
		 * 
		 */
		
	// new	
	private ResourceToRequestMap mergeAllResToReq(){
			
		ResourceToRequestMap mapAll = new ResourceToRequestMap(manager);
			
			for(ASServer serv: manager.getASServers().values()){
			//	RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
				
				ResourceToRequestMap map =	servRestoReqIndex.get(serv);
				
				
				System.out.println("Omar======"+serv.serverId+" size= " +map.map.size());
				
				
				for(Entry<String, HashSet<String>> entry : map.map.entrySet()){
					
					HashSet<String> set = mapAll.map.get(entry.getKey());
					if(set == null){
						set = new HashSet<String>();
						mapAll.map.put(entry.getKey(), set);
					}
					set.addAll(entry.getValue());
				}
			}
			
			return mapAll;
		}
		
	
	
	
	private void mergeAllResToReqGlobal(){
		
		
		manager.globalObjectToRequestMap.clear();
			for(ASServer serv: manager.getASServers().values())
			{				
				ResourceToRequestMap map =	servRestoReqIndex.get(serv);
				System.out.println("Omar======"+serv.serverId+" size= " +map.map.size());
				for(Entry<String, HashSet<String>> entry : map.map.entrySet())
				{
		    		HashSet<String> set = manager.globalObjectToRequestMap.get(entry.getKey());
					if(set == null)
					{
						set = new HashSet<String>();
						//mapAll.map.put(entry.getKey(), set);
						manager.globalObjectToRequestMap.put(entry.getKey(), set);
					}
					set.addAll(entry.getValue());
				}
			}
		}
	
	
	
	public void resetObjects()
	{
		for(CacheObject co:manager.globalCacheMap.values())
		{
			co.assignedbefore=-1;
		}
	}
	
	public void resetRequests()
	{
		for(HttpRequestObject co:manager.globalRequestMap.values())
		{
			co.visitedReq=false;
		}
	}
		
		public int getUniqueResourceCount(Collection<ASServer> asCon){
			
			HashSet<String> resources = new HashSet<String>();

			for(ASServer AStemp : asCon){
			//	CacheLogList cl = (CacheLogList) AStemp.getStruct("cacheLogGroup");
				CacheLogList cl =  servCacheLogIndex.get(AStemp);
				
				for(CacheLogSegment seg : cl.cacheLogList){
					for(String res : seg.cacheMap.keySet()){
						resources.add(res);
					}
				}
			}
			return resources.size();
		}
		
		
	public void updateServersBitSet()
	{
		for(ASServer serv: manager.getASServers().values())
		{
			for (String cacheKey:serv.tmpObjList)
			{	
				CacheObject co=manager.globalCacheMap.get(cacheKey);
				serv.curObjBits.set(co.index);
				for(ASServer serv2: manager.getASServers().values())
				{
					if (!serv2.serverId.equals(serv.serverId))
						serv2.curObjBits.set(co.index,false);
				}
			}		
		}
		for(ASServer serv: manager.getASServers().values())
		{
			serv.curObjList.clear();
			serv.tmpObjList.clear();
		}
		
	}
	
	
	public void updateObjectLoc()
	{
		for(ASServer serv: manager.getASServers().values())
		{
			for (String cacheKey:serv.tmpObjList)
			{	
				CacheObject co=manager.globalCacheMap.get(cacheKey);
				
				co.locations.clear();
				co.locations.add(serv.serverNo);
				
				
			}		
		}
		for(ASServer serv: manager.getASServers().values())
		{
			serv.oldObjList.clear();
			serv.oldObjList.addAll(serv.curObjList);
			
			serv.curObjList.clear();
			serv.tmpObjList.clear();
			
			
		}
		
	}
	////////////////////////////////////////////////
	
	// START FROM HERE
	
	@Override

	public void analyse()throws Exception {
		
		
		this.currentAnalysisPhase= new ObjectBasedAnalyserGraph(manager);
		
		this.currentAnalysisPhase.newRemap=1;
		for(ASServer serv:manager.getASServers().values()){
			System.out.print("serv= "+serv.getServerId());
			currentAnalysisPhase.servIndex.put(serv.serverNo, serv);		
		}
		
		System.out.print("hiiiiiiiiiiiiiii");
		Analyser.log.info("Start Analyser");
		
		//GIVEN DATA
		
		/*
		 * 1. Individual Cache Capacity  
		 * 2. Derived: Total cache capacity
		 * 
		 * 
		 */
		
		long l1=System.currentTimeMillis();
		
		cacheSz = Integer.parseInt(manager.props.getProperty(StatisticsManager.AS_CACHE_SIZE));
		
		if (this.numServers==-1)
			this.numServers = manager.getASServers().size();
	//	this.prevStat.numServ=this.numServersOri;
		
		//prevStat.numServ=numServers;
		
		totalCapacity = cacheSz * numServers;
		
		lbURLFreq = Integer.parseInt(manager.props.getProperty(RBA_STRATEGY_CONTINUELB));
		
		// From Strucutres
		
		/*
		 * get req->objs map for each server and merge them. 
		 * 
		 * get req frequency list
		 * 
		 * sort req frequency
		 * 
		 * 
		 * 
		 */
		
////////////////////////////NEW DS///////////////////

		
		/*
		for(ASServer serv:manager.getASServers().values())
		{
			
			
			
			
			RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(
					RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
			currentAnalysisPhase.servReqtoResIndex.put(serv, map.cloneReqResMap());
			System.out.print("good1 ");
			ResourceToRequestMap map2 =	(ResourceToRequestMap)serv.getStruct(
					RequestResourceMappingLogProcessor.OBJ_REQ_MAP);
			
			currentAnalysisPhase.servRestoReqIndex.put(serv, map2.cloneResReqMap());
			System.out.print("good2 ");
			HttpLogList hll = (HttpLogList)serv.getStruct(HttpRequestProcessor.REQ_LOG_GROUP);
			currentAnalysisPhase.servHttpLogIndex.put(serv, hll.cloneHLL());
			System.out.print("good3");
			
			
			HttpLogList hl = (HttpLogList) serv.getStruct("reqLogGroup");
			
			
			CacheLogList cl= (CacheLogList) serv.getStruct("cacheLogGroup");
			System.out.print("cl.cacheLogList.size() before= "+cl.cacheLogList.size());
			currentAnalysisPhase.servCacheLogIndex.put(serv, (CacheLogList) cl.clone());	
			System.out.print("cl.cacheLogList.size() after= "+cl.cacheLogList.size());
			
		//	this.avgExecTime=+hl.currentLogInterval.avgResp;			
		}
		*/
		/// OMAR 
		/*
		this.avgExecTime=this.avgExecTime/manager.servers.size();
		Analyser.log.info(" avg exec. time =" + this.avgExecTime);
		*/
		
		currentAnalysisPhase.tester=99;
		
		
		currentAnalysisPhase.mergedHttpMapAll = currentAnalysisPhase.mergeHttpRequestObjectsFromServers(); // OLD
	
		
		
		//currentAnalysisPhase.mergeHttpRequestObjectsFromServersForGlobal();
		System.out.println("manager.globalCacheMap.size()===================\n"+ manager.globalCacheMap.size());
		
		
		
	//	currentAnalysisPhase.cacheList=currentAnalysisPhase.mergeASCacheLogs(); //OLD
		

////////////////////////////NEW DS///////////////////

//mergeASCacheLogs();

		
		
		
		Analyser.log.info(" this.globalCacheMap.size() =" + manager.globalCacheMap.size());
		Analyser.log.info(" this.manager.gloabCacheObjectIndexing.size() =" + manager.gloabCacheObjectIndexing.size());
		
		
		
		
		//currentAnalysisPhase.countServererObjectsAccessFreq(); //OLD-TO-REMAIN
		
		
		
		//currentAnalysisPhase.mergedHttpMapAllPreviousInterval=currentAnalysisPhase.mergeHttpRequestObjectsFromServersPrevInterval(); // OLD
		  
		
		
		//currentAnalysisPhase.reqToResAll = currentAnalysisPhase.mergeAllReqToRes(); // OLD
		
////////////////////////////NEW DS///////////////////
//mergeAllReqToResGlobal();
Analyser.log.info("manager.globalRequestToObjectMap.size()="+ manager.globalRequestToObjectMap.size());



		//currentAnalysisPhase.resToReqAll = currentAnalysisPhase.mergeAllResToReq(); //OLD
////////////////////////////NEW DS///////////////////
//currentAnalysisPhase.mergeAllResToReqGlobal();
Analyser.log.info("manager.globalObjectToRequestMap.size()==================="+ manager.globalObjectToRequestMap.size());
	
		
		//System.out.println("Omar======mergedHttpMapAll.size()===========\n"+ currentAnalysisPhase.mergedHttpMapAll.size());
		
		
		
////////////////////////////NEW DS///////////////////
		// currentAnalysisPhase.mergeCurrentHttpLogGlobal(); //new
		 Analyser.log.info(" manager.globalRequestMap.size()=" + manager.globalRequestMap.size());
	
		
		
		//// currentAnalysisPhase.calculateRequestsObjectsWeights();
	//	 currentAnalysisPhase.CalculateToConsider();
		
		int mergesize=manager.globalRequestMap.size();
		
		
		/*
		long sharedListBefore0=System.currentTimeMillis();
		

		long sharedListAfter0=System.currentTimeMillis();
		long constructingBitSet=sharedListAfter0-sharedListBefore0;
		
		Analyser.log.info(" constructingBitSet=" + constructingBitSet);
		
		*/
		// we need it for the current requests		
////////////////////////////NEW DS///////////////////

		//currentAnalysisPhase.httpListAll = HttpLogList.getHttpRequestObjectListWeightedNew(currentAnalysisPhase.httpListAll, true);
	//	System.out.println("currentAnalysisPhase.httpListAll.size()===================\n"+ currentAnalysisPhase.httpListAll.size());
		
		
////////////////////////////NEW DS///////////////////
/*
		long tDrift=System.currentTimeMillis();
		currentAnalysisPhase.driftCounter();
		long tDriftAf=System.currentTimeMillis();
		*/
		
		
		//currentAnalysisPhase.prevStat.driftCounter=currentAnalysisPhase.globalObjectDriftCounter;
	//	long driftTime= tDriftAf-tDrift;
	//	Analyser.log.info(" driftTime=" + driftTime);
	//	Analyser.log.info(" globalObjectDriftCounter=" + currentAnalysisPhase.globalObjectDriftCounter);
		
		currentAnalysisPhase.servCap=0;
		currentAnalysisPhase.totalWeight=0;
		currentAnalysisPhase.variation=0;
		
		int counter=0;
		
		if (mergesize>0)
		{
	
			
			for (HttpRequestObject key:manager.globalRequestMap.values())
			{
				key.assignedbefore=-1;
				HashSet<String> objects = manager.globalRequestToObjectMap.get(key.url);
				
				if (objects==null)
				{
					//Analyser.log.info(" objects==null= continue");
					continue;
				}
			//	else if (key.toConsiderValue<0.5 || key.counter<2) // newwwwwwww
				//	continue;
				else
				{
					if (manager.globalRequestMap.get(key.url)!=null)
					{
						manager.globalRequestMap.get(key.url).weight+=(key.counter)*objects.size();
						
						calculateRequestTempreture(key);
						totalWeight+=key.weight;
						counter++;
						httpListAll.add(key); // I added it here to have the weight assigned to requests 
					}
					
				
				}
			}
			
			/*
			Analyser.log.info(" print requests First "+httpListAll.size());
			Iterator<HttpRequestObject> ir=httpListAll.iterator();
			while (ir.hasNext())
			{
				HttpRequestObject hr=ir.next();	
				Analyser.log.info(hr.url +" ==>"+manager.globalRequestMap.get(hr.url).weight);
			}
			
			*/
			
			// this is for unimportant requests reduction

			
			
			
			
			//boolean flag=true;
			//if (flag)
			//{
			if (useOptimisation.equals("gr") || useOptimisation.equals("both"))
			{

			//	httpListAll=sortRequestObjectList(httpListAll,true);
				
				
				if (useRegression.equals("yes"))
				{
					httpListAll=sortRequestObjectListRegression(httpListAll,true);
				}
				else
				{
					httpListAll=sortRequestObjectList(httpListAll,true);	
				}
				int reqFreqThreshold= (int) (requestPopularityThreshold *httpListAll.size());
				
				Analyser.log.info(" print requests "+httpListAll.size());
				Iterator<HttpRequestObject> ir=httpListAll.iterator();
				
				int tempCounter=0;
				while (ir.hasNext())
				{
					if (tempCounter==reqFreqThreshold)
						Analyser.log.info("===============Start Removing=====================");
						
					HttpRequestObject hr=ir.next();	
					//Analyser.log.info(hr.url +" ==>"+hr.counter+" ==>"+hr.tempreture);
					tempCounter++;
				}
				
				
				Analyser.log.info("httpListAll size before"+ manager.globalRequestMap.size());
				Analyser.log.info("reqFreqThreshold"+ reqFreqThreshold);
				
				for (int x=reqFreqThreshold;x<httpListAll.size();x++)
				{
					String req=httpListAll.get(x).url;
					//Analyser.log.info("to remove req"+ req);
					manager.globalRequestMap.remove(req);
					manager.globalRequestToObjectMap.remove(req);
					
					//httpListAll.remove(x);
				}
				for (int x=0;x<httpListAll.size();x++)
				{
					HttpRequestObject reqObj=httpListAll.get(x);
					if (reqObj.driftRate>requestDriftRateThreshold)
					{
					manager.globalRequestMap.remove(reqObj.url);
					manager.globalRequestToObjectMap.remove(reqObj.url);
					}
					
					//httpListAll.remove(x);
				}
				Analyser.log.info("httpListAll size after"+ manager.globalRequestMap.size());
				
				
				
				
				
				// this is to aggregate all objects that have to be assigned in a temp DS (toConsiderObjects) then remove them
				// individually from globalObjectToRequestMap
				HashSet<String> toConsiderObjects = new HashSet<String>();
				for (HashSet<String>objects:manager.globalRequestToObjectMap.values())
				{
					toConsiderObjects.addAll(objects);
				}
				
				Analyser.log.info("toConsiderObjects Before"+ toConsiderObjects.size());
				
				
				
				// this is in case of GR
				if (useRegression.equals("yes"))
				{
					List<CacheObject> sortedObjectList = new ArrayList<CacheObject>();
					Collection<String>obj=manager.globalCacheMap.keySet();
					HashSet<String> ObjList = new HashSet<String>(obj);
					sortedObjectList=sortRequestObjectList(ObjList, true);
					
					
					for (int x=0;x<sortedObjectList.size();x++)
					{ 
						CacheObject co=sortedObjectList.get(x);
						//Analyser.log.info("toConsiderObjects After"+ co.cacheKey +" temp="+co.tempreture);
						if (co.tempreture<1)
							if (toConsiderObjects.contains(co.cacheKey))
							{
								//Analyser.log.info("to remove obj"+ co.cacheKey);
								toConsiderObjects.remove(co.cacheKey);
							}
								
								
							
					}
					sortedObjectList.clear();
					ObjList.clear();
					
				}
				
				
				Analyser.log.info("toConsiderObjects After"+ toConsiderObjects.size());
				for (String object:manager.globalCacheMap.keySet())
				{
					if (!toConsiderObjects.contains(object))
						manager.globalObjectToRequestMap.remove(object);
						//manager.globalCacheMap.remove(object);
					
				}
				
				
				
				toConsiderObjects.clear();				
			}
			
			if (useOptimisation.equals("dr") || useOptimisation.equals("both"))
			{

			//	httpListAll=sortRequestObjectList(httpListAll,true);
				//int reqFreqThreshold= (int) (requestDriftRateThreshold *httpListAll.size());

				Analyser.log.info("httpListAll size DRIFT before"+ manager.globalRequestMap.size() +"drift rate"+requestDriftRateThreshold );
				Analyser.log.info("globalObjectToRequestMap size DRIFT before"+ manager.globalObjectToRequestMap.size());
				//Analyser.log.info("reqFreqThreshold"+ reqFreqThreshold);
				
				for (int x=0;x<httpListAll.size();x++)
				{
					
					HttpRequestObject reqObj=httpListAll.get(x);
					
					//Analyser.log.info("reqObj.driftRate= "+reqObj.driftRate);
					if (reqObj.driftRate>requestDriftRateThreshold)
					{
					manager.globalRequestMap.remove(reqObj.url);
					manager.globalRequestToObjectMap.remove(reqObj.url);
					Analyser.log.info("req="+reqObj.url+ "XXX"+reqObj.driftRate);
					}
					//else
					//	Analyser.log.info("req="+reqObj.url+ "|||"+reqObj.driftRate +"|||"+manager.globalRequestToObjectMap.get(reqObj.url).size());
					
					//httpListAll.remove(x);
				}
				Analyser.log.info("httpListAll size DRIFT after"+ manager.globalRequestMap.size());
				
				
				// this is to aggregate all objects that have to be assigned in a temp DS (toConsiderObjects) then remove them
				// individually from globalObjectToRequestMap
				HashSet<String> toConsiderObjects = new HashSet<String>();
				for (HashSet<String>objects:manager.globalRequestToObjectMap.values())
				{
					toConsiderObjects.addAll(objects);
				}
				
				
				for (String object:manager.globalCacheMap.keySet())
				{
					if (!toConsiderObjects.contains(object))
						manager.globalObjectToRequestMap.remove(object);
						//manager.globalCacheMap.remove(object);
					
				}
				toConsiderObjects.clear();
				Analyser.log.info("globalObjectToRequestMap size DRIFT After"+ manager.globalObjectToRequestMap.size());
				
				
			}
			
			
			// this is for drift rate requests reduction
			/*
			if (useOptimisation.equals("gr") || useOptimisation.equals("both"))
			{

			//	httpListAll=sortRequestObjectList(httpListAll,true);
				//int reqFreqThreshold= (int) (requestDriftRateThreshold *httpListAll.size());

				Analyser.log.info("httpListAll size DRIFT before"+ manager.globalRequestMap.size());
				//Analyser.log.info("reqFreqThreshold"+ reqFreqThreshold);
				
				for (int x=0;x<httpListAll.size();x++)
				{
					HttpRequestObject reqObj=httpListAll.get(x);
					if (reqObj.driftRate>requestDriftRateThreshold)
					{
					manager.globalRequestMap.remove(reqObj.url);
					manager.globalRequestToObjectMap.remove(reqObj.url);
					}
					
					//httpListAll.remove(x);
				}
				Analyser.log.info("httpListAll size after"+ manager.globalRequestMap.size());
				
				
				// this is to aggregate all objects that have to be assigned in a temp DS (toConsiderObjects) then remove them
				// individually from globalObjectToRequestMap
				HashSet<String> toConsiderObjects = new HashSet<String>();
				for (HashSet<String>objects:manager.globalRequestToObjectMap.values())
				{
					toConsiderObjects.addAll(objects);
				}
				
				
				for (String object:manager.globalCacheMap.keySet())
				{
					if (!toConsiderObjects.contains(object))
						manager.globalObjectToRequestMap.remove(object);
						//manager.globalCacheMap.remove(object);
					
				}
				toConsiderObjects.clear();
				servObjCap=(int) (manager.globalObjectToRequestMap.size()/numServers);
				servCapObj=(servObjCap)+(servObjCap/10);
	
				
			}
			
			*/
			//	Iterator itr = currentAnalysisPhase.httpListAll.iterator();
		
		/*			
					
			
			
			
			//Iterator it =httpListAll.iterator();
		while (itr.hasNext())
		{
			
			 HttpRequestObject hr= (HttpRequestObject) itr.next();
			 hr.assignedbefore=-1;
			
			
			
			
			HashSet<String> objects = manager.globalRequestToObjectMap.get(hr.url);
			
			if (objects==null)
			{
				continue;
			}
			else
			{
				if (manager.globalRequestMap.get(hr.url)!=null)
				{
					manager.globalRequestMap.get(hr.url).weight+=(hr.counter)*objects.size();
					currentAnalysisPhase.totalWeight+=hr.weight;
					counter++;
			}
		}
		// avgExecTime=avgExecTime/counter;
		
		}
		
*/
		
		
		
		
		
		  	Analyser.log.info("manager.globalObjectToRequestMap.size= " +manager.globalObjectToRequestMap.size());
		    long sharedListBefore=System.currentTimeMillis();
		   // findSharedReqBit();
		   // findSharedReqObj();
		    
		    long constructBF=System.currentTimeMillis();
		    
		    
		    //currentAnalysisPhase.constructGraph();
		    
		   
		    
		    if (graphType.equals("g"))
		    {
		    	
		    	if (replicationStrategy.equals("schism"))
				{
		    		Analyser.log.info("GRAPH-Object-Based - SCHISM-Replication");
		    		// 1. For each object that is accessed by more than 1 txn add a new equivelent temp object to each of its txn
		    		
		    		/*
		    		for (String obj:manager.globalCacheMap.keySet())
		    		{
		    			System.out.println(obj);
		    		}*/
		    		
		    		currentAnalysisPhase.addingVirtualVerticesForShcismRep();
		    		
		    		//2. CONSTRUCTS EDGES FOR EACH REQUEST
		    		currentAnalysisPhase.construcJGraphTSchismReplication();
		    		
		    		//3. WRITE TO A FILE
		    		currentAnalysisPhase.prepareToWriteGraphToFileNewMetisSchismReplication();
		    		
		    		
		    		
		 
				}
		    	else
		    	{
		    	Analyser.log.info("GRAPH-Object-Based");
		    	currentAnalysisPhase.construcJGraphT();
		    	currentAnalysisPhase.prepareToWriteGraphToFileNewMetis();
		    	}
		    }
		    
		    else if (graphType.equals("hg"))
		    {
		    	Analyser.log.info("Hyper-GRAPH-Object-Based");
		    currentAnalysisPhase.construcJHyperGraphT();
		    
		    }
		    
		    Analyser.log.info("objectsGraph.vertexSet().size() "+ currentAnalysisPhase.objectsGraph.vertexSet().size());
		    
		 // IMP   currentAnalysisPhase.prepareToWriteGraphToFile();
		    
		    /*
		    if (this.globalObjectDriftCounter<1000)
		    {
		    	writeToFile(toAppend, this.filename);
		    }
		    else
		    {
		    	constructGraph();
		    }
		    
		    */
		    long constructAF=System.currentTimeMillis();
		    
		   // addRemoveServers();
		    
		    currentAnalysisPhase.servCap=currentAnalysisPhase.totalWeight/numServers;
		    
		    System.out.println("Omar======totalWeight==========\n" +currentAnalysisPhase.totalWeight);
			System.out.println("Omar======servCap==========\n" +currentAnalysisPhase.servCap);
			//variation=servCap/40;
			
			//variation=httpListAll.get(0).weight; //does not work
			
		    
		   // Analyser.log.info(" constructingBitSet=" + constructingBitSet);
		 //   long overlaptime=sharedListAfter - sharedListBefore;
		  //  Analyser.log.info("overlap constructing time "+ overlaptime);
		    Analyser.log.info(" sharedReqNew.size()=" + sharedReqNew.size());
		    
		  
	    
	  
		//call the strategy to generate policy 
		
		
	    Analyser.log.info("Omar======before==========:");
	    long l0=System.currentTimeMillis();
		boolean success = strategy.generatePolicies();
		
		long l2=System.currentTimeMillis();
		
		long consumedTime=l2-l1;
		
		Analyser.log.info("total time =" + consumedTime);
		
		
		System.out.println("Omar======Generated==========:" + success);
		Analyser.log.info("Omar======Generated==========:" + success);
		
		
		
		//Analyser.log.info("BitSet Time:" + (sharedListAfter0 - sharedListBefore0));
		Analyser.log.info("Req Size:" +this.currentAnalysisPhase.httpListAll.size());
		Analyser.log.info("===> Graph Construction Time:" + (constructAF - constructBF));
		Analyser.log.info("Process Time:" + (l2-l0));
		Analyser.log.info("Total Time:" + consumedTime);
		
		if(success)
		{
			// send all AS policies
			
			
			
			int size=prevStats.prevData.size();
			if (size>0)
				Analyser.log.info("check total weight" + prevStats.prevData.get(size-1).totalWeight);
			
			
			String policyFor = manager.props.getProperty(StatisticsManager.POLICY_FOR);
			if(policyFor == null || policyFor.equals(""))
				policyFor = "both";
			
			if(policyFor.equalsIgnoreCase("as") || policyFor.equalsIgnoreCase("both"))
			{		
				for(ASServer serv : currentAnalysisPhase.servIndex.values())
				{
					//TODO update the port logic in base class
					sendASPolicy(serv.currentPolicy, serv.getServerId());
				//	System.out.println("#############################################################"+serv.serverId);
				//	System.out.println(serv.currentPolicy.policyMap.entrySet().toString());
					
					
					Analyser.log.info("serv.getServerId()" + serv.getServerId());
					Analyser.log.info("serv.currentPolicy" + serv.currentPolicy.policyMap.size());
					
					serv.currentPolicy.policyMap.clear();
					//if (serv.serverNo==0)
					//	Analyser.log.info("serv.currentPolicy" + serv.currentPolicy.policyMap.toString());
					
				}
				
				
				
			}
			
			//send lb policy
		//	System.out.println("Omar======currentLBPolicy:" + currentLBPolicy);
			//System.out.println("Omar======LBSERVER:" +  manager.props.getProperty(StatisticsManager.LBSERVER));
			
			
			if(policyFor.equalsIgnoreCase("lb") || policyFor.equalsIgnoreCase("both"))
			{
				sendLBPolicy(currentAnalysisPhase.currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
				
				
				Analyser.log.info("###############################################################" );
				Analyser.log.info("manager.globalRequestMap.size=" +manager.globalRequestMap.size() );	
			//Analyser.log.info(currentAnalysisPhase.currentLBPolicy.policyMap.toString());
			
			
			
				//sendLBPolicy(currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
			}
			
			currentAnalysisPhase.currentLBPolicy.policyMap.clear();
		}
		//prevStats.assignBitSet(this.currentAnalysisPhase);
		//updateServersBitSet();
		
		/*
		if (manager.counter==0)
		  {
			  manager.globalRequestMapOld=(HashMap<String, HttpRequestObject>) manager.globalRequestMap.clone();
			  manager.globalCacheMapOld=(HashMap<String, CacheObject>) manager.globalCacheMap.clone();
			  manager.cloneReqClass();
			  manager.totEdgeWgtOld=manager.totEdgeWgt;
			  manager.totVtxWgtOld=manager.totVtxWgt;
		  }
		  else
		  {  
			  compareHGObject cHg= new compareHGObject(manager.totEdgeWgtOld, manager.totEdgeWgt, manager.totVtxWgtOld, manager.totVtxWgt, manager);	  
			 // int totNewEdgeCount=cHg.edgeWithOneVrtxIsNewCounter + cHg.edgeWithBothVrtxAreNewCounter;		  
			  cHg.printValidateNewEdgesWeight();			  
			
			  manager.globalRequestMapOld=(HashMap<String, HttpRequestObject>) manager.globalRequestMap.clone();
			  manager.globalCacheMapOld=(HashMap<String, CacheObject>) manager.globalCacheMap.clone();
			  manager.cloneReqClass();
			  manager.totEdgeWgtOld=manager.totEdgeWgt;
			  manager.totVtxWgtOld=manager.totVtxWgt;			  
		  }
		
		
		
		*/
		int totalNumberOfPhysicalObject=0;
		double replicatedPhysicalObject=0;
		 for (CacheObject coTemp:manager.globalCacheMap.values())
	        {
	        	totalNumberOfPhysicalObject+=coTemp.sites.size();
	        	if (coTemp.sites.size()>1)
	        		replicatedPhysicalObject++;
	        }
	        
	        
	        
	       // Analyser.log.info("---------- TEMP OMAR TEMP---------objToRep=" +objToRep);
		 
		 double totalPhysicalObjectRight=totalNumberOfPhysicalObject - replicatedPhysicalObject;
	        
			Analyser.log.info("No. of total physical object ="+totalPhysicalObjectRight);
			double repFactor= totalPhysicalObjectRight /(double)manager.globalCacheMap.size();
			Analyser.log.info("Replication Factor ="+repFactor);
		
		updateObjectLoc();
		resetObjects();
		resetRequests();
		//prevStats.prevData.add(this.currentAnalysisPhase);
		
		
	}	


	}
}


