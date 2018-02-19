package edu.mcgill.disl.analytics;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.Map.Entry;

import javax.swing.plaf.basic.BasicInternalFrameTitlePane.RestoreAction;

import com.nearinfinity.bloomfilter.BloomFilter;

import edu.mcgill.disl.analytics.CacheLogList.CacheLogSegment;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.analytics.policy.ObjectKey;
import edu.mcgill.disl.analytics.policy.RuleList;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.log.processor.CacheLogProcessor;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;
import edu.mcgill.disl.log.processor.RequestResourceMappingLogProcessor;

import org.jgrapht.*;
import org.jgrapht.alg.DirectedNeighborIndex;
import org.jgrapht.alg.NeighborIndex;
import org.jgrapht.graph.*;





public class RequestBasedAnalyserGraphMetisRep extends Analyser {
	
	

	public static final String RBA_STRATEGY_CLASS = "rba.strategy.class";
	public static final String RBA_STRATEGY_CONTINUELB = "rba.strategy.lbURLFreq";
	
	RequestBasedAnalyserGraphMetisStrategyRep strategy = null;
	
	//all variables that could be needed by strategy should be made public here or have public methods
	public int cacheMemorySize;	
	public int cacheSz;
	public int numServers=-1;
	public int numServersOri;
	public int totalCapacity;
	public int totalMemoryCapacity;
	public int servCap=0;
	public int servCapObj=0;
	public int totalWeight=0;
	public int totalFreqs=0;
	public int totalObjFreqs=0;
	public int variation=0;
	public int objectDrift=0;
	public int globalObjectDriftCounter=0;
	double avgExecTime=0;
	int addRemoveServ=0;
	String toAppend = "";
	public int MaxUpdateCount=0;
	
	int newRemap=-1;
	public int tester=-1;
	
	public int newRequests=0;
	public int requestsDrift=0;
	
	public UndirectedGraph<String, DefaultEdge> requestsGraph =
		      new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
	
	
	public int requestPopularityThresholdValue=-1;
	public Float requestDriftRateThresholdValue=new Float("-1");

	
	//public AbstractBaseGraph<String, String> requestsGraphs =
		  //    new AbstractBaseGraph<String, String>(DefaultEdge.class);
	
	
	public RequestBasedAnalyserGraphMetisRep currentAnalysisPhase;
	

	
	public int lbURLFreq = 0;
	public int reqCounter=0; // counter for requests indexing
	HashMap<Integer, ASServer> servIndex = new HashMap<Integer,ASServer>();
	
	public RequestToResourcesMap reqToResAll=null;
	public ResourceToRequestMap resToReqAll=null;
	
	public HashMap<String, HashMap<String, CacheObject>> individualMap;
	public List<CacheObject> cacheList=new ArrayList<CacheObject>();
	
	public PrevIntervalStat prevStats=new PrevIntervalStat();

	public NeighborIndex<String, DefaultWeightedEdge> NG; 

	
	public HashMap<String,HttpRequestObject> mergedHttpMapAll = null;
	public HashMap<String,HttpRequestObject> mergedObjectMapAll = null;
	//public HashMap<String,HttpRequestObject> mergedHttpMapAllPreviousInterval = null;
	public List<HttpRequestObject> httpListAll= new ArrayList<HttpRequestObject>();
	public HashMap<ASServer, CacheLogList> servCacheLogIndex = new HashMap<ASServer, CacheLogList>();
	public HashMap<ASServer, HttpLogList> servHttpLogIndex = new HashMap<ASServer, HttpLogList>();
	public HashMap<ASServer, RequestToResourcesMap> servReqtoResIndex = new HashMap<ASServer, RequestToResourcesMap>();
	
	public HashMap<ASServer, ResourceToRequestMap> servRestoReqIndex = new HashMap<ASServer, ResourceToRequestMap>();
	
	//public static HashMap<HttpRequestObject, List<HttpRequestObject>> sharedReq = new HashMap<HttpRequestObject, List<HttpRequestObject>>();
	//public HashMap<String, HashSet<String>> sharedReqNew = new HashMap<String, HashSet<String>>(100);
	

	
	//public PrevIntervalStat prevStat; 
	public HashMap<String, Integer> requestId = new HashMap<String, Integer>() ;
	public HashMap<Integer, String> requestIdIndex = new HashMap<Integer, String>() ;
	public HashMap<String, String> edges = new HashMap<String, String> () ;
	public final String  filename="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/req.hgr";
	public final String  filenameNewMetis="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/newMetisPartioner/req.hgr";

	
	public final String  filePath="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/";
	public final String  filePathNewMetis="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/newMetisPartioner/";

	
	public final String  partionerOld="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/khmetis";
	
	
	public final String  partioner="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/shmetis";
	
	public final String  partionerNewMetis="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/newMetisPartioner/gpmetis";
	
	
	public final String  inputGraphFile="req.hgr";
	String vertexWgt="";
	String useOptimisation = manager.props.getProperty(StatisticsManager.USE_OPTIMISATION);
	String graphType = manager.props.getProperty(StatisticsManager.GRAPH_TYPE);
	String useRegression = manager.props.getProperty(StatisticsManager.USE_REGRESSION);
	
	public Float requestPopularityThreshold = Float.parseFloat(manager.props.getProperty(StatisticsManager.request_Pop_Thresh));
	
	public Float requestDriftRateThreshold = Float.parseFloat(manager.props.getProperty(StatisticsManager.request_Drift_Thresh));
	
	
	public String replicationStrategy = manager.props.getProperty(StatisticsManager.replication_strategy);
	public Float replicationPopularObjectPercentage = Float.parseFloat(manager.props.getProperty(StatisticsManager.replication_popular_object_percentage));
	public Float replicationLowWriteObjectPercentage = Float.parseFloat(manager.props.getProperty(StatisticsManager.replication_low_write_object_percentage));

	
	
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
	
	// TO KEEP TRACK OF REQUEST INDEX
			public int getSetRequestIndex(String reqKey)
			{
				HttpRequestObject ro=manager.globalRequestMap.get(reqKey);
				int reqIndexToReturn=0;
			//	Analyser.log.info("ro.index="+  ro.index);
				if (ro.index!=0)
				{
					reqIndexToReturn=ro.index;
				}
				else if (ro.index==0)
				{
				manager.gloablRequestIndexing.put(manager.gloabalRequestKeyCounter, reqKey);
				ro.index=manager.gloabalRequestKeyCounter;
				reqIndexToReturn=manager.gloabalRequestKeyCounter;
				manager.gloabalRequestKeyCounter++;
				//Analyser.log.info("manager.gloablRequestIndexing.size() NEWW="+  manager.gloablRequestIndexing.size()+"couter="+manager.gloabalRequestKeyCounter);
				}
				
				return reqIndexToReturn;
				
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
			
			public List<CacheObject> sortCacheObjectList(List<CacheObject> objList,final boolean desc)
			{
				
				//List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map.values());
				
				
				
				Collections.sort(objList, new Comparator<CacheObject>(){

					@Override
					public int compare(CacheObject co1, CacheObject co2) 
					{
						//return  desc? (int)co2.updateCount- (int)co1.updateCount : (int)co1.updateCount - (int)co2.updateCount;
						return  desc? (int)(co2.getCount- 3*co2.updateCount)- (int)(co1.getCount- 3*co1.updateCount) : (int)(co1.getCount- 3*co1.updateCount) - (int)(co2.getCount- 3*co2.updateCount);
					}
					
				});
				
			return objList;	
			}
			
			public List<CacheObject> sortCacheObjectListByUpdateCount(List<CacheObject> objList,final boolean desc)
			{
				
				//List<HttpRequestObject> col = new ArrayList<HttpRequestObject>(map.values());
				
				
				
				Collections.sort(objList, new Comparator<CacheObject>(){

					@Override
					public int compare(CacheObject co1, CacheObject co2) 
					{
						//return  desc? (int)co2.updateCount- (int)co1.updateCount : (int)co1.updateCount - (int)co2.updateCount;
						return  desc? (int)co2.updateCount- (int)co1.updateCount : (int)co1.updateCount- (int)co2.updateCount;
					}
					
				});
				
			return objList;	
			}
	
	// to construct request based hyper graph
			public void construcJHyperGraphT()
			{
				manager.gloabalRequestKeyCounter=1;
				manager.gloablRequestIndexing.clear();
				 emptyresultGraphFile();
					PrintWriter out = null;
					Analyser.log.info("manager.globalObjectToRequestMap ="+manager.globalObjectToRequestMap.size()); 
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
				
				for(Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet())
				{
					String hyperEdges="";
					HashSet<String> requests = null;
					String key=entry.getKey(); // key is the object
					requests=manager.globalObjectToRequestMap.get(key);
					
					
					
					if (requests==null)
					{
						Analyser.log.info("requests==null "+  key); 
						continue;
					}
					
					CacheObject co=manager.globalCacheMap.get(key);
					
					
					if (co==null)
					{
						Analyser.log.info("CacheObject is null "+  key); 
						continue;
						
					}
					
					
					
					
					if (requests.size()==0)
					{
						Analyser.log.info("requests.size()==0 "+  key); 
						continue;
					}
					
					weight=Integer.toString(co.getCount);				
						Iterator<String> it = requests.iterator();
						int cntFlag=0;
						while (it.hasNext())
						{
							String m=it.next();
							
							if (!ConsiderReq(m))
							{
								//Analyser.log.info("!ConsiderReq(key) "+  key); 
								continue;
							}
							
							HttpRequestObject ro=manager.globalRequestMap.get(m);
							if (ro==null)
							{
								Analyser.log.info("ERROR NULL Req"+m);
								
							}
							else
							{
								if (getSetRequestIndex(ro.url)<1)
									Analyser.log.info("ERROR (co)<1 "); 
								//hyperEdges=hyperEdges+" "+co.index;sfd
								hyperEdges=hyperEdges+" "+getSetRequestIndex(m);
								cntFlag++;
								//Analyser.log.info("getSetRequestIndex(m) "+getSetRequestIndex(m)); 
								
							}
						}
					
						if (cntFlag>0)
						{
							sentence.append(weight+" "+hyperEdges+"\n");
							edgesCount++;
						}
					}
				Analyser.log.info("manager.gloablRequestIndexing.size()= "+  manager.gloablRequestIndexing.size());
				
						
						String toAppendFirstLine="";
						toAppendFirstLine=edgesCount+" "+ manager.globalRequestMap.size()+ " 11 \n";
						StringBuffer StringBufferVertexWgt=new StringBuffer();
						
						Analyser.log.info("manager.gloablRequestIndexing="+manager.gloablRequestIndexing.size());
						Analyser.log.info("manager.globalRequestMap="+manager.globalRequestMap.size());
						
						for (int x=1;x<manager.gloablRequestIndexing.size();x++)
						{
							int vwgt=0;
							String reqK=manager.gloablRequestIndexing.get(x);
							HttpRequestObject ro=manager.globalRequestMap.get(reqK);
							if (ro==null)
							{
								Analyser.log.info("ro==null Error");
								continue;
							}
							
							vwgt=ro.counter;			
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
				
	
	
	@SuppressWarnings("unchecked")

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
	
	public void construcJGraphT()
	{
		
		if (requestsGraph.vertexSet().size()>0)
		{
			clearGraph(requestsGraph);
		}
		
		Analyser.log.info("Constructing Class N...."+manager.globalObjectToRequestMap.size());
		for(Entry<String, HashSet<String>> entry : manager.globalObjectToRequestMap.entrySet())
		{
		HashSet<String> requests = null;
		String key=entry.getKey(); // key is the object
		
		
		
		requests=entry.getValue();
		if (requests==null)
			{
				Analyser.log.info("requests==null=" +key);
				continue;
			}
		
	//	if (!ConsiderObjectOri(key))
	//		continue;
		
		if (requests.size()==1)
		{
			//Analyser.log.info("size==1 continue");
			continue;
		}
			
			
		for (String s1 : requests)
		{
			if (!ConsiderReq(s1))
			{
				//Analyser.log.info("ConsiderReq(s1) continue ="+s1);
				continue;
			}
			
			requestsGraph.addVertex(s1);
			if (s1.contains("-1") || (s1==null) || s1.contains("RegisterItem"))
				continue;
			for (String s2 : requests)
			{
				if (!ConsiderReq(s2))
					continue;
				if (s2.contains("-1") || (s2==null) || s1.equals(s2) || s2.contains("RegisterItem"))
					continue;
					
				requestsGraph.addVertex(s2);
				if (!requestsGraph.containsEdge(s1, s2) && !requestsGraph.containsEdge(s2, s1) )
					requestsGraph.addEdge(s1,s2);
					//DefaultEdge d= requestsGraph.addEdge(s1,s2);			
			}
		}
		
		}
		Analyser.log.info("requestsGraph.vertexSet().size()...."+requestsGraph.vertexSet().size());
	}
	

	
public boolean ConsiderObjectOri(String obj)
{
	boolean toConsider=true;
	CacheObject co= manager.globalCacheMap.get(obj);
	 if (co.getCount<2)
		 toConsider=false;
	 return toConsider;
	 
}


public boolean ConsiderReqOri(String req)
{
	
	boolean toConsider=true;
	HttpRequestObject hr= manager.globalRequestMap.get(req);
	 if (hr.counter<2)
		 toConsider=false;
	 return toConsider;
	 
}

public boolean ConsiderReq(String req)
{
	//if (useOptimisation.equals("gr"))
	if (manager.globalRequestMap.containsKey(req))
		return true;
	return false;
}

public boolean ConsiderObject(String obj, HashSet<String> requests)
{
	boolean toConsider=true;
	CacheObject co= manager.globalCacheMap.get(obj);
	int counter=0;
	for (String s:requests)
	{
		counter+=manager.globalRequestMap.get(s).counter;
	}
	
	if (co.getCount!=counter)
	{
	Analyser.log.info("-----------------");
	Analyser.log.info(obj);
	Analyser.log.info("freq="+co.getCount);
	Analyser.log.info("counter="+counter);
	
	float f= (float)co.getCount/(float)counter;
	Analyser.log.info("result="+f);
	co.toConsider=f;
	if (f<0.8)
		toConsider=false;
	}
	
	
	 return toConsider;
	 
}

public void checkIncCounter(String req)
{
	if (!manager.gloablRequestIndexingIncremental.containsValue(req))
	{
	manager.gloablRequestIndexingIncremental.put(manager.gloabalRequestKeyCounterIncremental, req);
	HttpRequestObject hr=manager.globalRequestMap.get(req);
	hr.indexIncremental=manager.gloabalRequestKeyCounterIncremental;
	manager.gloabalRequestKeyCounterIncremental++;
	}
}


public void generateVertexIds()
{
	
	Set<String> requests=requestsGraph.vertexSet();
	Analyser.log.info("Generating Vertex IDS"+ requests.size());
	Iterator<String> ir=requests.iterator();
	while (ir.hasNext())
	{
		String req=ir.next();
		manager.gloablRequestIndexingIncremental.put(manager.gloabalRequestKeyCounterIncremental, req);
		HttpRequestObject hr=manager.globalRequestMap.get(req);
		hr.indexIncremental=manager.gloabalRequestKeyCounterIncremental;
		manager.gloabalRequestKeyCounterIncremental++;
	}
	Analyser.log.info("Generating Vertex IDS size" +manager.gloablRequestIndexingIncremental.size());
	
}

public void generateVertexIdsForNonInc()
{
	manager.gloabalRequestKeyCounter=1;
	Set<String> requests=requestsGraph.vertexSet();
	Analyser.log.info("Generating Vertex IDS Non Inc"+ requests.size());
	Iterator<String> ir=requests.iterator();
	while (ir.hasNext())
	{
		String req=ir.next();
		manager.gloablRequestIndexing.put(manager.gloabalRequestKeyCounter, req);
		HttpRequestObject hr=manager.globalRequestMap.get(req);
		hr.index=manager.gloabalRequestKeyCounter;
		manager.gloabalRequestKeyCounter++;
	}
	Analyser.log.info("Generating Vertex IDS size Non Inc" +manager.gloablRequestIndexing.size());
	
}
	
	
	// this graph based ===> hMETIS
	public void prepareToWriteGraphToFileNew()
	{
		 requestId.clear();
		 requestIdIndex.clear();
		 
		 String EdgePairs="";
		 int edgesCount=0;
		 long BF=System.currentTimeMillis();
		Set<String> requests=requestsGraph.vertexSet();
		Set<DefaultEdge> edges=requestsGraph.edgeSet();
		
		manager.vertexWgt="";
		//manager.vertexNo=1;
		
		manager.gloabalRequestKeyCounter=1;
		manager.gloablRequestIndexing.clear();
		for (String req:requests)
		{
			manager.gloablRequestIndexing.put(manager.gloabalRequestKeyCounter, req);
			manager.globalRequestMap.get(req).index=manager.gloabalRequestKeyCounter;
			manager.gloabalRequestKeyCounter++;
		}
		
		
		for (int k=1;k<=manager.gloablRequestIndexing.size();k++)
	    {
	    	
	    	String hrString=manager.gloablRequestIndexing.get(k);
	    	if (!requests.contains(hrString))
	    	{
	    		Analyser.log.info("to continue+++"+hrString);
	    		continue;
	    	}
	    	
	    	int wgt=manager.globalRequestMap.get(hrString).weight;
	    	if (wgt==0)
	    		continue;
	    	manager.vertexWgt= manager.vertexWgt+ "\n"+ wgt;
	    	manager.vertexNo++;
	    			
	    			//Analyser.log.info("manager.vertexWgt"+ manager.globalRequestMap.get(hrString).weight);
	    }
    	
		
        StringBuffer sentence = new StringBuffer();
		 for (DefaultEdge edge:edges)
		 {
			 int edgeWgt=0;
			 requestsGraph.getEdgeWeight(edge);
			 String rS=requestsGraph.getEdgeSource(edge);
			 String rT=requestsGraph.getEdgeTarget(edge);
		 edgeWgt=manager.globalRequestMap.get(rS).weight+manager.globalRequestMap.get(rT).weight; // to Edit later by including the shared objects
			// Analyser.log.info("edgeWgt "+  edgeWgt); 
			// int edgeWgt=(int) requestsGraph.getEdgeWeight(edge);
			//ORi EdgePairs=EdgePairs +"\n"+ edgeWgt+ " " + manager.globalRequestMap.get(rS).index + " " +manager.globalRequestMap.get(rT).index;
			 
			 //EdgePairs=EdgePairs +"\n"+ edgeWgt+ " " + edgeWgt + " " + edgeWgt;
			 sentence.append("\n"+ edgeWgt+ " " + manager.globalRequestMap.get(rS).index + " " +manager.globalRequestMap.get(rT).index);
			 
		 }		 
		 long AF=System.currentTimeMillis();
		 long Time= AF-BF;
		  Analyser.log.info(" ===> Graph Traversing Time=" + Time);
		  
			PrintWriter out = null;
			
			
			try {
				out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
			} catch (IOException e) {
				// TODO Auto-generated catch blockgloabObjectIndexing
				e.printStackTrace();
			}
		 int reqSize=0;
			if (edges!=null)
			{
				 reqSize=requests.size();
				 edgesCount=edges.size();
			}
			String toAppendFirstLine=null;
			toAppendFirstLine=edgesCount+" "+ reqSize+ "  11";
			
			EdgePairs=sentence.toString();
			toAppend=toAppendFirstLine +EdgePairs+ manager.vertexWgt;
			Analyser.log.info("toAppendFirstLine " + toAppendFirstLine);
			Analyser.log.info("manager.vertexNo " + manager.vertexNo);
			
			out.println(toAppend);
			out.close();	
	}
	
	
	private void toNeighborGraph(Graph G) 
	{
		 NG = new NeighborIndex<String, DefaultWeightedEdge>(G);
	}
	
	// THIS IS GRAPH BASED-GPMETIS
	public void prepareToWriteGraphToFileNewMetis()
	{
		 requestId.clear();
		 requestIdIndex.clear();
		 toNeighborGraph(requestsGraph);
		
		 StringBuffer sentence = new StringBuffer();
		 String EdgePairs="";
		 int edgesCount=0;
		 long BF=System.currentTimeMillis();
		Set<String> requests=requestsGraph.vertexSet();
		Set<DefaultEdge> edges=requestsGraph.edgeSet();
		
		
		manager.gloabalRequestKeyCounter=1;
		manager.gloablRequestIndexing.clear();
		for (String req:requests)
		{
			manager.gloablRequestIndexing.put(manager.gloabalRequestKeyCounter, req);
			manager.globalRequestMap.get(req).index=manager.gloabalRequestKeyCounter;
			manager.gloabalRequestKeyCounter++;
		}
		
		//Analyser.log.info("sssssssss");
		
		for (int k=1;k<=manager.gloablRequestIndexing.size();k++)
		{
			String req=manager.gloablRequestIndexing.get(k);
			if (!requests.contains(req))
				continue;
			
			HttpRequestObject reqObj  =manager.globalRequestMap.get(req);
			String reqLine=String.valueOf(reqObj.weight);
		//	Analyser.log.info(req);
			
						
			List<String> RS = NG.neighborListOf(req);
			
			/*
			if (RS == null)
			{
				Analyser.log.info(" RS == null cont." + req);
				continue;
				
			}
			*/
			for (String adjReq : RS) 
				{
				HttpRequestObject adjReqObj  =manager.globalRequestMap.get(adjReq);
				int edgeWgt=reqObj.weight + adjReqObj.weight;
				reqLine+=" "+adjReqObj.index+" "+edgeWgt;
				}	
			sentence.append("\n"+ reqLine);
			reqLine="";
			
			
		}
		
		
		
		
        
		 
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
		 int reqSize=0;
			if (edges!=null)
			{
				 reqSize=requests.size();
				 edgesCount=edges.size();
			}
			String toAppendFirstLine=null;
			toAppendFirstLine=reqSize+" "+ edgesCount+ " 011";
			
			EdgePairs=sentence.toString();
			toAppend=toAppendFirstLine +EdgePairs;
			Analyser.log.info("toAppendFirstLine MMM" + toAppendFirstLine);
			Analyser.log.info("manager.vertexNo " + manager.vertexNo);
			
			out.println(toAppend);
			out.close();	
	}
	
	
	public void printResToReq()
	{
		
		for(Entry<String, HashSet<String>> entry : resToReqAll.map.entrySet())
		{
			Analyser.log.info(" *********************************** "); 
			Analyser.log.info("Object Key "+  entry.getKey()); 
			Analyser.log.info("URL Values:"); 
			HashSet<String> requests = null;
			requests=resToReqAll.map.get(entry.getKey());
			if (requests!=null)
			{
			Iterator it = requests.iterator();
			while (it.hasNext())
			{
				String s=(String) it.next();
				Analyser.log.info("Req " +s);  
			}
			}
		}
	}
	
	
	
	public static void main(String args[])
	{
		List<String> tempReq = new ArrayList<String>();
		tempReq.add("1");
		tempReq.add("2");
		System.out.println(tempReq.get(0));
		System.out.println(tempReq.get(1));
		
		for (int x=0;x<5;x++)
		{
		//	System.out.println(x);
			
		}
	}
	
	
	
	
	
	public RequestBasedAnalyserGraphMetisRep(StatisticsManager manager) {
		super(manager);
		setupStrategy();
		
		
		
		
		individualMap = new HashMap<String, HashMap<String,CacheObject>>();

		
		
		
	}
	
	//make it read a strategy from configuration
	
	
	public void setupStrategy(){
		
		try{
		
			System.out.println("enter here RBA Metis");
		Class c = Class.forName(manager.props.getProperty(RBA_STRATEGY_CLASS), false, this.getClass()
				.getClassLoader());
		
		System.out.println("RBA_STRATEGY_CLASS\n"+manager.props.getProperty(RBA_STRATEGY_CLASS));
		
			
			
		strategy = (RequestBasedAnalyserGraphMetisStrategyRep) c.getConstructor(new Class[] { RequestBasedAnalyserGraphMetisRep.class }).newInstance(this);
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
		
	
	
		
		
	public void  mergeASCacheLogs(){
		manager.globalCacheMap.clear();
		
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
						if (!co.sites.contains(AStemp.getServerId()))
							co.sites.add(AStemp.getServerId());
						manager.globalCacheMap.put(co.cacheKey, co);
						manager.gloabCacheObjectIndexing.put(manager.gloabalCacheKeyCounter, co.cacheKey);
						co.index=manager.gloabalCacheKeyCounter;
						if (co.size==1)
						{
							//Analyser.log.info(co.cacheKey.toString());
							AStemp.curObjBits.set(co.index);
							co.last_put_server=AStemp.serverId;
							co.last_put_time_global=co.last_put_time;
						}
						manager.gloabalCacheKeyCounter++;
						totalObjFreqs+=co.getCount;
						
					}
					else
					{
						totalObjFreqs+=co.getCount;
						cachObjTemp.getCount = cachObjTemp.getCount + co.getCount;
						cachObjTemp.no_sites++;
						cachObjTemp.sites.add(AStemp.getServerId());
						if (co.size==1 && cachObjTemp.size==1)
						{	
							
							if (co.last_put_server.equals(cachObjTemp.last_put_server))
								if (cachObjTemp.last_put_time_global<co.last_put_time)
								cachObjTemp.last_put_time_global=co.last_put_time;
							
							else if (cachObjTemp.last_put_time_global<co.last_put_time)
							{
								Analyser.log.info("start1");
								AStemp.curObjBits.set(cachObjTemp.index);
								manager.servers.get(cachObjTemp.last_put_server).curObjBits.set(cachObjTemp.index, false);
								cachObjTemp.last_put_time_global=co.last_put_time;
								cachObjTemp.last_put_server=AStemp.serverId;
							}
								

						}
						else if (co.size==1)
						{
							//Analyser.log.info("start2");
							AStemp.curObjBits.set(cachObjTemp.index);
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
		
	
		// TO CHECK WETHER TO CONSIDER A REQUEST OR NOT
		
		
		public void removeRequest(String req)
		{
			manager.globalRequestMap.remove(req);
			manager.globalRequestToObjectMap.remove(req);
			//manager.gloablRequestIndexing.
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
			
			
			for (HttpRequestObject key:mergedHttpMapAll.values())
			{
				HashSet<String> res=manager.globalRequestToObjectMap.get(key.url);
				if (res==null)
					continue;
		
				HttpRequestObject obj = manager.globalRequestMap.get(key.url);
				if(obj == null)
				{
					obj = (HttpRequestObject) key.clone();
					//manager.gloablRequestIndexing.put(manager.gloabalRequestKeyCounter,obj.url);
					manager.globalRequestMap.put(obj.url,obj);
					//key.index=manager.gloabalRequestKeyCounter;
					//obj.index=manager.gloabalRequestKeyCounter;		
					//manager.gloabalRequestKeyCounter++;
					newRequests++;			
				}
				else
				{
					obj.counter += key.counter;
					obj.accessTimes.addAll(key.accessTimes);
				}				
				// httpListAll.add(key);// by OMar
				
				// comment by omar
				toCheckRequestEligibilty(res,key.url);
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
					Analyser.log.info("curReq objects is nul"+ curReq.url);
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
		
		public float tempCalcReq(String req)
		{
			HashSet<String> res1=manager.globalRequestToObjectMap.get(req);
			if (res1==null)
				return -1;
			
			for (String obj:res1)
			{
				
				
				CacheObject co= manager.globalCacheMap.get(obj);
				int counter=0;
				
				HashSet<String> res=manager.globalObjectToRequestMap.get(obj);
				if (res==null)
					continue;
				
				for (String s:res)
				{
					counter+=manager.globalRequestMap.get(s).counter;
				}
				
				float f= (float)co.getCount/(float)counter;
				co.toConsider=f;
				if (f==0)
					Analyser.log.info("f==0"+co.toString());
				return f;
			}
			return -1;		
			
		}
		
		public void calculateRequestsObjectsWeights()
		{
			for (Entry<String, HashSet<String>> entry:manager.globalObjectToRequestMap.entrySet())
			{
				String obj=entry.getKey();
				
				CacheObject co= manager.globalCacheMap.get(obj);
				int counter=0;
				
				HashSet<String> res=entry.getValue();
				if (res==null)
					continue;
				
				for (String s:res)
				{
					counter+=manager.globalRequestMap.get(s).counter;
				}
				
				float f= (float)co.getCount/(float)counter;
				co.toConsider=f;
				if (f==0)
					Analyser.log.info("f==0"+co.toString());
					
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
		
		currentAnalysisPhase.mergedHttpMapAll.clear();
		currentAnalysisPhase.mergedHttpMapAll.clear();
		currentAnalysisPhase.httpListAll.clear();
		currentAnalysisPhase.servCacheLogIndex.clear();
		currentAnalysisPhase.servHttpLogIndex.clear();
		currentAnalysisPhase.servReqtoResIndex.clear();
		manager.globalCacheMap.clear();
		manager.globalObjectToRequestMap.clear();
		manager.globalRequestMap.clear();
		manager.globalRequestToObjectMap.clear();
	


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
		
		
		this.currentAnalysisPhase= new RequestBasedAnalyserGraphMetisRep(manager);
		
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

		
		
		
		Analyser.log.info(" this.globalCacheMap.size()OMAR =" + manager.globalCacheMap.size());
		
		/*
		for (CacheObject co:manager.globalCacheMap.values())
		{
			Analyser.log.info("obj="+co.cacheKey +"co.updateCount="+co.updateCount);
		}
		
		*/
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
		
	int servObjCap=currentAnalysisPhase.manager.globalObjectToRequestMap.size()/numServers;
		
		currentAnalysisPhase.servCapObj=(servObjCap)+(servObjCap/10);
		//System.out.println("Omar======mergedHttpMapAll.size()===========\n"+ currentAnalysisPhase.mergedHttpMapAll.size());
		
		
		
		
		 currentAnalysisPhase.mergeCurrentHttpLogGlobal(); //new
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
		currentAnalysisPhase.httpListAll.clear();
		httpListAll.clear();
		
		
		currentAnalysisPhase.httpListAll = HttpLogList.getHttpRequestObjectListWeightedNew(currentAnalysisPhase.httpListAll, true);
		System.out.println("currentAnalysisPhase.httpListAll.size()===================\n"+ currentAnalysisPhase.httpListAll.size());
		
		long tDrift=System.currentTimeMillis();
		currentAnalysisPhase.driftCounter();
		long tDriftAf=System.currentTimeMillis();
		
		//currentAnalysisPhase.prevStat.driftCounter=currentAnalysisPhase.globalObjectDriftCounter;
		long driftTime= tDriftAf-tDrift;
		Analyser.log.info(" driftTime=" + driftTime);
		Analyser.log.info(" globalObjectDriftCounter=" + currentAnalysisPhase.globalObjectDriftCounter);
		
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
						totalFreqs+=key.counter;
						totalWeight+=key.weight;
						counter++;
						httpListAll.add(key); // I added it here to have the weight assigned to requests 
					}
					
				
				}
			}
			
			Analyser.log.info(" totalFreqs= "+totalFreqs);
			
			// TO REPLICATE TOP POPULAR REQUESTS AND THEIR RESPECIVE OBJECTS
			
	
			if (replicationStrategy.equals("low-update"))
			{
				
				
				Analyser.log.info("manager.globalCacheMap.values().size())="+manager.globalCacheMap.values().size());
				
				cacheList.addAll(manager.globalCacheMap.values());
				Analyser.log.info("cache size before="+cacheList.size());
				Analyser.log.info(" No of requests to replication Low-update ObjectPercentage =  "+replicationLowWriteObjectPercentage +"all objects="+manager.globalCacheMap.size());
				cacheList=sortCacheObjectList(cacheList,true);
				Analyser.log.info("cache size="+cacheList.size());
				
				int objWriteThreshold= (int) (replicationLowWriteObjectPercentage *cacheList.size());
				MaxUpdateCount=cacheList.get(objWriteThreshold).updateCount;
				
				
				Analyser.log.info(" No of objects to replicate =  "+objWriteThreshold);
				for (int x=0;x<objWriteThreshold;x++)
				 // for (int x=0;x<cacheList.size();x++)
				{
					
					
					CacheObject cacheObj=cacheList.get(x);
				//	CacheObject cacheObj=manager.globalCacheMap.get(obj);
					
					Analyser.log.info(" x= "+ x +" obj="+cacheObj.cacheKey+" popularity="+cacheObj.getCount +" updateCount="+cacheObj.updateCount);
					Set<String>requests=manager.globalObjectToRequestMap.get(cacheObj.cacheKey);
					if (requests!=null)
					{
					for (String req:requests)
					{
						if (req!=null && manager.globalRequestMap.get(req)!=null)
							manager.globalRequestMap.get(req).toReplicate=true;
					}
					}
					
				//	Analyser.log.info(" x= "+ x +" req="+req+" weight="+manager.globalRequestMap.get(req).weight);
					//httpListAll.remove(x);
				}
				
				
				servObjCap=(int) (manager.globalObjectToRequestMap.size()/numServers);
				servCapObj=(servObjCap)+(servObjCap/10);
				
				Analyser.log.info(" servCapObj="+servCapObj);
			//	int approximateNoOfObjToReplicate=(int) (replicationPopularObjectPercentage*manager.globalCacheMap.size());
				

				for (ASServer serv:manager.servers.values())
    			{
					serv.totalObjectCapacity=servCapObj;
    			}
				
				Analyser.log.info(" servCapObj="+servCapObj);
				
			}
			
			if (replicationStrategy.equals("popular") || (replicationStrategy.equals("regression")&& manager.counter==1))
			{
				Analyser.log.info("manager.counter =  "+manager.counter);
				if (replicationStrategy.equals("regression")&& manager.counter==1) // this for generating the learning model only
					replicationPopularObjectPercentage=(float) 1.0;
				
				//else if (replicationStrategy.equals("regression")&& manager.counter==2)
					//replicationPopularObjectPercentage=(float) 0.0;
				
				Analyser.log.info("No of requests to replicationPopularObjectPercentage =  "+replicationPopularObjectPercentage);
				Analyser.log.info(" httpListAll.size() =  "+httpListAll.size());
				
				httpListAll=sortRequestObjectList(httpListAll,true);
				Analyser.log.info(" httpListAll.size() =  "+httpListAll.size());
				int reqFreqThreshold= (int) (replicationPopularObjectPercentage *httpListAll.size());
				Analyser.log.info(" No of requests to replicate =  "+reqFreqThreshold);
				for (int x=0;x<reqFreqThreshold;x++)
				{
					
					
					String req=httpListAll.get(x).url;
					manager.globalRequestMap.get(req).toReplicate=true;
				//	Analyser.log.info(" x= "+ x +" req="+req+" weight="+manager.globalRequestMap.get(req).weight);
					//httpListAll.remove(x);
				}
				
				
				servObjCap=(int) (manager.globalObjectToRequestMap.size()/numServers);
				servCapObj=(servObjCap)+(servObjCap/10);
				
				Analyser.log.info(" servCapObj="+servCapObj);
			//	int approximateNoOfObjToReplicate=(int) (replicationPopularObjectPercentage*manager.globalCacheMap.size());
				

				for (ASServer serv:manager.servers.values())
    			{
					serv.totalObjectCapacity=servCapObj;
    			}
				
				Analyser.log.info(" servCapObj="+servCapObj);
				
			}
			
			if (replicationStrategy.equals("popular-only") )
			{
				 
				
				int objToReplicateCount=0;
				Analyser.log.info("popular-all No of requests to replicationPopularObjectPercentage =  "+replicationPopularObjectPercentage);
				httpListAll=sortRequestObjectList(httpListAll,true);
				int reqFreqThreshold= (int) (replicationPopularObjectPercentage *httpListAll.size());
				Analyser.log.info(" No of requests to replicate =  "+reqFreqThreshold);
				for (int x=0;x<reqFreqThreshold;x++)
				{
					
					
					String req=httpListAll.get(x).url;
					manager.globalRequestMap.get(req).toReplicate=true;
					HttpRequestObject ro= manager.globalRequestMap.get(req);
					
					HashSet<String> objects = null;
					objects=manager.globalRequestToObjectMap.get(req);
					if (objects == null)
					{

						Analyser.log.info(" objects=null="+ req);
						continue;
					}
					Iterator<String> it = objects.iterator();
					
					while (it.hasNext())
					{
						String cacheKey=it.next();
						CacheObject co1= manager.globalCacheMap.get(cacheKey);
						for (ASServer serv:manager.servers.values())
	        			{				
							serv.curObjList.add(cacheKey);
							co1.sites.add(serv.serverId);
							co1.replicate=true;
							serv.totalObjectCapacity++;
						//	Analyser.log.info("max Server = "+serv.serverId +"cacheObjAllocated"+cacheObjAllocated[serv.serverNo]);

	        			}
						
						objToReplicateCount++;
					}
				
				//	Analyser.log.info(" x= "+ x +" req="+req+" weight="+manager.globalRequestMap.get(req).weight);
					//httpListAll.remove(x);
				}
				
				
				servObjCap=(int) (manager.globalObjectToRequestMap.size()/numServers);
				servCapObj=(servObjCap)+(servObjCap/10);
				
				Analyser.log.info(" objToReplicateCount="+objToReplicateCount);
			//	int approximateNoOfObjToReplicate=(int) (replicationPopularObjectPercentage*manager.globalCacheMap.size());
				

				for (ASServer serv:manager.servers.values())
    			{
					serv.totalObjectCapacity=servCapObj;
    			}
				
				Analyser.log.info(" servCapObj="+servCapObj);
				
			}
			
			
			if (replicationStrategy.equals("dynamic") && manager.counter==0)
			{
				 // here we want to find the semantic of update txn by
				//1. find the most k updated objects where k is the number of servers 
				//2. replicate them and keep track about each object and how many times it is replicated 
				
				int objToReplicateCount=0;
				Analyser.log.info("popular-all No of requests to replicationPopularObjectPercentage =  "+replicationPopularObjectPercentage);
				
				cacheList.addAll(manager.globalCacheMap.values());
				Analyser.log.info("cache size before="+cacheList.size());
				//sortCacheObjectListByUpdateCount(cacheList, true);
				cacheList=sortCacheObjectListByUpdateCount(cacheList,false);
				Analyser.log.info("cache size="+cacheList.size());
				
				
				int numServers=this.numServers;
				int cacheSize=cacheList.size();
				
				for (int x=numServers;x>0;x--)
				{
				
					cacheSize--;
					Analyser.log.info("ObjectToReplicate "+cacheList.get(cacheSize).cacheKey+"updateCount="+cacheList.get(cacheSize).updateCount);
					manager.updatedObjects.put(x, cacheList.get(cacheSize).cacheKey);
					manager.replicateObjectCounter.put(cacheList.get(cacheSize).cacheKey,0);
					manager.replicateObjectCost.put(x,0.0);
					
					
				}
				
					
					
				Analyser.log.info(" servCapObj="+servCapObj);
				
			}
			
			
			// here goes the dynamic nature
			/*
			if (replicationStrategy.equals("dynamic") )
			{
				
			}
				
			*/	
			
			if (useOptimisation.equals("gr") || useOptimisation.equals("both"))
			{

				httpListAll=sortRequestObjectList(httpListAll,true);
				int reqFreqThreshold= (int) (requestPopularityThreshold *httpListAll.size());
				
				/*
				Analyser.log.info(" print requests "+httpListAll.size());
				Iterator<HttpRequestObject> ir=httpListAll.iterator();
				while (ir.hasNext())
				{
					HttpRequestObject hr=ir.next();	
					Analyser.log.info(hr.url +" ==>"+hr.weight);
				}
				*/
				
				Analyser.log.info("httpListAll size before"+ manager.globalRequestMap.size());
				Analyser.log.info("reqFreqThreshold"+ reqFreqThreshold);
				
				for (int x=reqFreqThreshold;x<httpListAll.size();x++)
				{
					String req=httpListAll.get(x).url;
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
			
		
			
			if (useOptimisation.equals("dr") || useOptimisation.equals("both"))
			{

			//	httpListAll=sortRequestObjectList(httpListAll,true);
				//int reqFreqThreshold= (int) (requestDriftRateThreshold *httpListAll.size());

				Analyser.log.info("httpListAll size DRIFT before"+ manager.globalRequestMap.size() +"drift rate"+requestDriftRateThreshold );
				//Analyser.log.info("reqFreqThreshold"+ reqFreqThreshold);
				
				for (int x=0;x<httpListAll.size();x++)
				{
					
					HttpRequestObject reqObj=httpListAll.get(x);
					
					//Analyser.log.info("reqObj.driftRate= "+reqObj.driftRate);
					if (reqObj.driftRate>requestDriftRateThreshold)
					{
					manager.globalRequestMap.remove(reqObj.url);
					manager.globalRequestToObjectMap.remove(reqObj.url);
					}
					
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
				
				
			}
			
			
		System.out.println("Omar======totalWeight==========\n" +currentAnalysisPhase.totalWeight);
		System.out.println("Omar======servCap==========\n" +currentAnalysisPhase.servCap);
		
		
		
		
		
		
		
		  	Analyser.log.info("manager.globalObjectToRequestMap.size= " +manager.globalObjectToRequestMap.size());
		    long sharedListBefore=System.currentTimeMillis();
		   // findSharedReqBit();
		   // findSharedReqObj();
		    
		    
		    
		    
		    //currentAnalysisPhase.constructGraph();
		    manager.vertexWgt="";
		    
		    
		    emptyresultGraphFile();
		    
		    long constructBF=0;
		    long constructAF=0;
		    
		    
		    
		   
		    	manager.ReqOldLoc.clear();
		    	Analyser.log.info("useOptimisation.equals.NO");
		    
		    	
		    	if(!replicationStrategy.equals("popular-only"))
		    	{
		    	 if (graphType.equals("g"))
				    {
				    	Analyser.log.info("GRAPH-Request-based");
				    	//currentAnalysisPhase.prepareToWriteGraphToFileNew(); // hMETIS
				    	currentAnalysisPhase.construcJGraphT();
				    	currentAnalysisPhase.prepareToWriteGraphToFileNewMetis();
				    }
				    
				    else if (graphType.equals("hg"))
				    {
				    	Analyser.log.info("Hyper-GRAPH-Request-Based");
				    currentAnalysisPhase.construcJHyperGraphT();
				    
				    }
		   
		    	}
			         manager.vertexNo=0;
			    
		    	
		    	
			     constructAF=System.currentTimeMillis();
		    
		   
		    
		   
		   
		
		    
		    
		    
		    
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
		    
		    
		   // addRemoveServers();
		    
		    currentAnalysisPhase.servCap=currentAnalysisPhase.totalWeight/numServers;
			//variation=servCap/40;
			
			//variation=httpListAll.get(0).weight; //does not work
			
		    
		   // Analyser.log.info(" constructingBitSet=" + constructingBitSet);
		 //   long overlaptime=sharedListAfter - sharedListBefore;
		  //  Analyser.log.info("overlap constructing time "+ overlaptime);
		    
		  
	    
	  
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
					
					
					System.out.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"+serv.serverId);
					System.out.println("serv.getServerId()" + serv.getServerId());
					System.out.println("serv.currentPolicy" + serv.currentPolicy.policyMap.size());
					for (Entry<ObjectKey, RuleList> es:serv.currentPolicy.policyMap.entrySet())
					{
						System.out.println(serv.serverId +":" + es.getKey().toString() +":"+es.getValue().toString());
					}
					
					
					
				//	System.out.println("==================================================================="+serv.serverId);
					//System.out.println(serv.currentPolicy);
					
					
					
					
					
					//Analyser.log.info("serv.currentPolicy" + serv.currentPolicy.policyMap.toString());
					
					// comment it out to use the same policy for the replication 
					//serv.currentPolicy.policyMap.clear();
					//if (serv.serverNo==0)
					
					
						
					
				}
				
				
				
			}
			
			
			
			//send lb policy
		//	Analyser.log.info("Omar======currentLBPolicy:" + currentAnalysisPhase.currentLBPolicy.policyMap.toString());
			//System.out.println("Omar======LBSERVER:" +  manager.props.getProperty(StatisticsManager.LBSERVER));
			
								// the second conidtion is to extract update semantic ...
			if(policyFor.equalsIgnoreCase("lb") || policyFor.equalsIgnoreCase("both") && !(replicationStrategy.equals("dynamic") && manager.counter==0))
			{
				sendLBPolicy(currentAnalysisPhase.currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
				
				
				//System.out.println(currentAnalysisPhase.currentLBPolicy.policyMap);
				
				Analyser.log.info("###############################################################" );
				Analyser.log.info("manager.globalRequestMap.size=" +manager.globalRequestMap.size() );	
			Analyser.log.info("policy size=" +currentAnalysisPhase.currentLBPolicy.policyMap.size() );
				//sendLBPolicy(currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
			}
			
			
			
			
			currentAnalysisPhase.currentLBPolicy.policyMap.clear();
		}
		//prevStats.assignBitSet(this.currentAnalysisPhase);
		updateObjectLoc();
		resetObjects();
		resetRequests();
		//prevStats.prevData.add(this.currentAnalysisPhase);
		
		
	}	


	}
}


