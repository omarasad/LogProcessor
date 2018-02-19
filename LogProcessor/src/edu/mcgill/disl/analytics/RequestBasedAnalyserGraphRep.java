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
import edu.mcgill.disl.analytics.HttpLogList.HttpLogSegment;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.log.StatisticsManager;
import edu.mcgill.disl.log.processor.CacheLogProcessor;
import edu.mcgill.disl.log.processor.HttpRequestProcessor;
import edu.mcgill.disl.log.processor.RequestResourceMappingLogProcessor;

import org.jgrapht.*;
import org.jgrapht.alg.DirectedNeighborIndex;
import org.jgrapht.graph.*;





public class RequestBasedAnalyserGraphRep extends Analyser {
	
	public static final String RBA_STRATEGY_CLASS = "rba.strategy.class";
	public static final String RBA_STRATEGY_CONTINUELB = "rba.strategy.lbURLFreq";
	
	RequestBasedAnalyserGraphStrategyRep strategy = null;
	
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
	
	
	//public Float requestPopularityThreshold=new Float("0.80");
	//public Float requestDriftRateThreshold=new Float("0.30");
	
	public int requestPopularityThresholdValue=-1;
	public Float requestDriftRateThresholdValue=new Float("-1");
	
	public UndirectedGraph<String, DefaultEdge> requestsGraph =
		      new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
	
	
	
	

	
	//public AbstractBaseGraph<String, String> requestsGraphs =
		  //    new AbstractBaseGraph<String, String>(DefaultEdge.class);
	
	
	//public RequestBasedAnalyserGraph currentAnalysisPhase;
	

	
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

	
	//public PrevIntervalStat prevStat; 
	public HashMap<String, Integer> requestId = new HashMap<String, Integer>() ;
	public HashMap<Integer, String> requestIdIndex = new HashMap<Integer, String>() ;
	public HashMap<String, String> edges = new HashMap<String, String> () ;
	public final String  filename="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/req.hgr";
	public final String  filePath="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/";
//	public final String  partionerOld="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/khmetis";
	public final String  partioner="/home/db/dislcluster/thesis/jboss-5.1.0.GA/server/minimal/graphPartitoner/shmetis";
	public final String  inputGraphFile="req.hgr";
	String vertexWgt="";
	String useOptimisation = manager.props.getProperty(StatisticsManager.USE_OPTIMISATION);
	
																											
	public Float requestPopularityThreshold = Float.parseFloat(manager.props.getProperty(StatisticsManager.request_Pop_Thresh));
			
	public Float requestDriftRateThreshold = Float.parseFloat(manager.props.getProperty(StatisticsManager.request_Drift_Thresh));
	

	//public Float requestPopularityThreshold=new Float("0.80");
	//public Float requestDriftRateThreshold=new Float("0.30");
	
	
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
		
		public boolean ConsiderReqOri(String req)
		{
			//Analyser.log.info("ConsiderReq size "+httpListAll.size());
			
			
			if (useOptimisation.equals("dm") || useOptimisation.equals("none"))
				return true;
				
				
			//float threshold = new Float("0.30");
			boolean toConsider=true;
			HttpRequestObject hr= manager.globalRequestMap.get(req);
			 //if (hr.driftRate>threshold || hr.counter<2)
			 //if (hr.driftRate>requestDriftRateThreshold || hr.counter<this.requestPopularityThresholdValue)
			
			if (!httpListAll.contains(hr))
			 {
				// Analyser.log.info("do not consider "+req +"  %%%%%%%%%% "+hr.toConsiderValue +"  ##### "+hr.counter);
				 toConsider=false;
			 }
			
			/*
			if (hr.counter<this.requestPopularityThresholdValue)
			 {
				// Analyser.log.info("do not consider "+req +"  %%%%%%%%%% "+hr.toConsiderValue +"  ##### "+hr.counter);
				 toConsider=false;
			 }
			 */
			 return toConsider;
			 
			 
			 
		}
		
		public void updateTotVtxWgt(String ver)
		{
			
			int vrtx = 0;
			
			if (manager.globalRequestMap.get(ver)!=null)
				vrtx=manager.globalRequestMap.get(ver).counter;
			/*
			else
			{
				Analyser.log.info("null="+ver);
			}
			*/
				
			//manager.totVtxWgt+=manager.globalRequestMap.get(ver).weight;
			manager.totVtxWgt+=vrtx;
			
			
				
			//Analyser.log.info("totVtxWgt="+this.totVtxWgt);
		}
		
		public void updateTotEdgeWgt(String ver1, String ver2)
		{
			int edge1 = 0,edge2=0;
			
			if (manager.globalRequestMap.get(ver1)!=null)
				edge1=manager.globalRequestMap.get(ver1).counter;
			if (manager.globalRequestMap.get(ver2)!=null)
				edge2=manager.globalRequestMap.get(ver2).counter;
				
			//int edgeWgt=manager.globalRequestMap.get(ver1).counter + manager.globalRequestMap.get(ver2).counter;
			int edgeWgt=edge1+edge2;
			manager.totEdgeWgt+=edgeWgt;
		//	Analyser.log.info("totEdgeWgt="+this.totEdgeWgt);
		}
		
		public void construcJGraphT()
		{
			
			if (requestsGraph.vertexSet().size()>0)
			{
				clearGraph(requestsGraph);
			}
			
			Analyser.log.info("Constructing Class New ...."+manager.globalObjectToRequestMap.size());
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
				
			for (String s1 : requests)
			{
				if (!ConsiderReq(s1))
					continue;
				
				requestsGraph.addVertex(s1);
				
				//this.updateTotVtxWgt(s1);
				
				if (s1.contains("-1") || (s1==null) || s1.contains("RegisterItem"))
					continue;
				for (String s2 : requests)
				{
					if (!ConsiderReq(s2))
						continue;
					if (s2.contains("-1") || (s2==null) || s1.equals(s2) || s2.contains("RegisterItem"))
						continue;
						
					requestsGraph.addVertex(s2);
					
					this.updateTotVtxWgt(s2);
					
					if (!requestsGraph.containsEdge(s1, s2) && !requestsGraph.containsEdge(s2, s1))
						requestsGraph.addEdge(s1,s2);
					/*
					else
					{
						Analyser.log.info("requestsGr");
					}*/
						//DefaultEdge d= requestsGraph.addEdge(s1,s2);
						
						this.updateTotEdgeWgt(s1, s2);
				}
			}
			
			}
			
			Analyser.log.info("requestsGraph.vertexSet().size() NEWW ...."+requestsGraph.vertexSet().size() +"==="+requestsGraph.edgeSet().size());
			requestsGraph.edgeSet().size();
			
		}
			
	
	public void prepareToWriteGraphToFile()
	{
		 
		 String EdgePairs="";
		 int edgesCount=0;
		 Set<DefaultEdge> ProcessedEdges = new HashSet<DefaultEdge>();
		 
		
		Set<String> requests=requestsGraph.vertexSet();
		
		 
		 for (String MainV:requests)
		 {
			HttpRequestObject req=manager.globalRequestMap.get(MainV);
			
			if (req==null)
				continue;
			maintainRequestIndex(MainV,req.weight);
			 if (!req.prcsd)
			 {
				 
				 Set<DefaultEdge> edges=requestsGraph.edgesOf(MainV);
				 for (DefaultEdge edge:edges)
				 {
					 if (!ProcessedEdges.contains(edge))
					 {
						 
						 String SubV=requestsGraph.getEdgeTarget(edge);
						 if (!MainV.equals(SubV))
						 {
						 maintainRequestIndex(SubV,manager.globalRequestMap.get(SubV).weight);
						 int edgeWgt=manager.globalRequestMap.get(SubV).weight+req.weight; // to Edit later by including the shared objects
						 EdgePairs=EdgePairs +"\n"+ edgeWgt+ " " + requestId.get(MainV) + " " +requestId.get(SubV);
						 ProcessedEdges.add(edge);
						 edgesCount++;
						 }
						 
						
								 
					 }
				 }
				 req.prcsd=true; 

			 }
		 }
		 
		 
		 emptyresultGraphFile();
			PrintWriter out = null;
			
			
			try {
				out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
			} catch (IOException e) {
				// TODO Auto-generated catch blockgloabObjectIndexing
				e.printStackTrace();
			}
		 int reqSize=0;
			if (requestId!=null)
				 reqSize=requestId.size();
			String toAppendFirstLine=null;
			toAppendFirstLine=edgesCount+" "+ reqSize+ "  11";
			
			toAppend=toAppendFirstLine +EdgePairs+vertexWgt;
			Analyser.log.info("toAppendFirstLine " + toAppendFirstLine);
			
			out.println(toAppend);
			out.close();	
	}
	
	
	@SuppressWarnings("null")
	public void constructGraph()
	{
		toAppend="";
		boolean FlagOmm=true;
		emptyresultGraphFile();
		PrintWriter out = null;
		
		int edgesCount=0;
		try {
			out = new PrintWriter(new BufferedWriter(new FileWriter(filename, false)));
		} catch (IOException e) {
			// TODO Auto-generated catch blockgloabObjectIndexing
			e.printStackTrace();
		}
		
		
	//	for(Entry<String, HashSet<String>> entry : resToReqAll.map.entrySet())
	//	{
			
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
			
			Analyser.log.info("********************************************");
			Analyser.log.info("object=" +key);
			List<String> tempReq = new ArrayList<String>(); // tempReq contains all requests that access an object o
			
			
			 
			Iterator<String> it = requests.iterator();
			while (it.hasNext())
			{
				String m=it.next();
				tempReq.add(m);
				Analyser.log.info("requests=" +m);
				
			}
			
		//	Analyser.log.info("tempReq.size()=" +tempReq.size());
		
			//if (FlagOmm)
			if (tempReq.size()>1) // OMM
			{
				//Analyser.log.info("if");
			for (int x=0;x<tempReq.size();x++)
			{
				
				
				String s=tempReq.get(x);
				
				int w1=0;
				if (s.contains("-1")) // run analyser initiator
					continue;
				
				w1=manager.globalRequestMap.get(s).weight ;
			//	Analyser.log.info("reqA=" +s);
				this.maintainRequestIndex(s,w1);
				//Analyser.log.info("s1 counter" + requestId.get(s));
				for (int y=x+1;y<tempReq.size();y++)
				{
					String s2=tempReq.get(y);
					
					int w2=0;
					if (s.contains("-1")) // run analyser initiator
						continue;
					if (s2==null)
						continue;
					w2=manager.globalRequestMap.get(s2).weight ;
				//	Analyser.log.info("reqB=" +s2);
					this.maintainRequestIndex(s2, w2);
				//	Analyser.log.info("s2 counter" + requestId.get(s2));
				//	insertSharedGraph(s,s2);
					
					
						
					int total= w1+w2;
					if (insertSharedGraph(s,s2))
						{
						toAppend=toAppend+ "\n" + total+" "+ requestId.get(s) +" "+requestId.get(s2);
						edgesCount++;
						/*
						if (!SharedReqList.contains(s))
						{
							SharedReqList.add(s);
							nonSharedReq.remove(s);
							
						}
						if (!SharedReqList.contains(s2))
						{
							SharedReqList.add(s2);
							nonSharedReq.remove(s2);
						}
						*/
						
					//	Analyser.log.info("toAppendr" + toAppend);
						
						nonSharedReq.remove(s);
						nonSharedReq.remove(s2);
						}
				}
				
				
				
					
				
			}
			
		}
		
			
			
		 else if (tempReq.size()==1) // omm
		{
				//Analyser.log.info("else");
			String s=tempReq.get(0);
			if (requestId!=null)
			{
				if (manager.globalRequestMap.get(s)==null)
				{
					Analyser.log.info("s= " + s);
					continue;
				}
				int ww=manager.globalRequestMap.get(s).weight;
				this.maintainRequestIndex(s,ww);
				if (!(nonSharedReq.contains(s) || SharedReqList.contains(s)))
					nonSharedReq.add(s);
				
			}
		}
		
		
			
		}
		int noSharedReqSize=nonSharedReq.size();
		
		Analyser.log.info("noSharedReqSize= " + noSharedReqSize);
		
		Iterator<String> ir = nonSharedReq.iterator();
		
		while (ir.hasNext())
		{
			String s= ir.next();
			int w1=0;
			
			if (manager.globalRequestMap.get(s)!=null)
				w1=manager.globalRequestMap.get(s).counter ;
			
			
			toAppend=toAppend+ "\n" + w1+" "+ requestId.get(s)+ " "+requestId.get(s);
			
			//if (edgesCount==0)
				edgesCount++;
		}
		
		
		
		/*
		if (noSharedReqSize>0)
		{
			toAppend=toAppend+ "\n" +"0 1 2";
		    edgesCount+=1;	
		}
		*/
		if (noSharedReqSize>0)
		{
			toAppend=toAppend+ "\n" +"0 1 2";
		    toAppend=toAppend+ "\n" +"0 3 2";
		    toAppend=toAppend+ "\n" +"0 4 2";
		    toAppend=toAppend+ "\n" +"0 5 2";
		    toAppend=toAppend+ "\n" +"0 6 2";
		    toAppend=toAppend+ "\n" +"0 7 2";
		    toAppend=toAppend+ "\n" +"0 8 2";
		    toAppend=toAppend+ "\n" +"0 9 2";
		    toAppend=toAppend+ "\n" +"0 11 2";
		    toAppend=toAppend+ "\n" +"0 22 2";
		    edgesCount+=10;	
		}
	
				int reqSize=0;
				if (requestId!=null)
					 reqSize=requestId.size();
				String toAppendFirstLine=null;
				toAppendFirstLine=edgesCount+" "+ reqSize+ "  11";
				
				toAppend=toAppendFirstLine +toAppend+vertexWgt;
				Analyser.log.info("toAppendFirstLine " + toAppendFirstLine);
				
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
	
	public void printSharedNew()
	{
		
		for(Entry<String, HashSet<String>> entry : sharedReqNew.entrySet())
		{
			Analyser.log.info(" *********************************** "); 
			Analyser.log.info("URL Key "+  entry.getKey()); 
			Analyser.log.info("URL Values"); 
			HashSet<String> requests = sharedReqNew.get(entry.getKey());
			if (requests!=null)
			{
			Iterator<String> it = requests.iterator();
			while (it.hasNext())
			{
				Analyser.log.info((String) it.next());  
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
	
	
	
	
	
	public RequestBasedAnalyserGraphRep(StatisticsManager manager) {
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
		
			
			
		strategy = (RequestBasedAnalyserGraphStrategyRep) c.getConstructor(new Class[] { RequestBasedAnalyserGraphRep.class }).newInstance(this);
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
						//if (!co.sites.contains(AStemp.getServerId()))
							//co.sites.add(AStemp.getServerId());
						manager.globalCacheMap.put(co.cacheKey, co);
						manager.gloabCacheObjectIndexing.put(manager.gloabalCacheKeyCounter, co.cacheKey);
						co.index=manager.gloabalCacheKeyCounter;
						if (co.size==1)
						{
							//Analyser.log.info(co.cacheKey.toString());
							AStemp.curObjBits.set(co.index);
							//co.sites.add(AStemp.getServerId());
							co.last_put_server=AStemp.serverId;
							co.last_put_time_global=co.last_put_time;
						}
						manager.gloabalCacheKeyCounter++;
						
					}
					else
					{
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
								AStemp.curObjBits.set(cachObjTemp.index);
								//co.sites.add(AStemp.getServerId());
								manager.servers.get(cachObjTemp.last_put_server).curObjBits.set(cachObjTemp.index, false);
								cachObjTemp.last_put_time_global=co.last_put_time;
								cachObjTemp.last_put_server=AStemp.serverId;
							}
								

						}
						else if (co.size==1)
						{
							//Analyser.log.info("start2");
							AStemp.curObjBits.set(cachObjTemp.index);
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
		
		
		
		/**
		 * this method will first do segment merge.. and the all server merge
		 */
	
	//////////////////////////// NEW DS///////////////////
		@SuppressWarnings("unchecked")
		private HashMap<String, HttpRequestObject> mergeHttpRequestObjectsFromServers()
		{			
			return (HashMap<String, HttpRequestObject>) manager.globalRequestMap.clone();	
		}
		
		
		
		
		private HashMap<String, HttpRequestObject> mergeHttpRequestObjectsFromServersOri()
		{
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
		
		public void toCheckRequestEligibiltyOri(HashSet<String> res, String hr)
		{
			
			HttpRequestObject hro=manager.globalRequestMap.get(hr);
			Iterator<String> ir=res.iterator();
			int counter=0;
			int counter2=0;
			
			int objCounter=0;
			for (String obj:res)
			{
				CacheObject co =manager.globalCacheMap.get(obj);
				if (co==null)
				{
					Analyser.log.info("null");
					continue;
				}
				if (co.getCount>hro.counter)
					objCounter+=hro.counter;
				else
				objCounter+=co.getCount;
				
			}
			//hro.toConsiderValue=((float)objCounter/(float)res.size())/(float)hro.counter;
		//	Analyser.log.info("request="+hr+" toConsiderValue="+hro.toConsiderValue);
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
					manager.gloablRequestIndexing.put(manager.gloabalRequestKeyCounter,obj.url);
					manager.globalRequestMap.put(obj.url,obj);
					key.index=manager.gloabalRequestKeyCounter;
					obj.index=manager.gloabalRequestKeyCounter;		
					manager.gloabalRequestKeyCounter++;
					newRequests++;			
				}
				else
				{
					obj.counter += key.counter;
					obj.accessTimes.addAll(key.accessTimes);
				}				
				// httpListAll.add(key);// by OMar
				
				//commented by Omar
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
		
		
		//this.currentAnalysisPhase= new RequestBasedAnalyserGraph(manager);
		
		newRemap=1;
		for(ASServer serv:manager.getASServers().values()){
			System.out.print("serv= "+serv.getServerId());
			servIndex.put(serv.serverNo, serv);		
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
		
		//////////////////////////// NEW DS///////////////////

		/*
		
		for(ASServer serv:manager.getASServers().values())
		{

			
			RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(
					RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
			servReqtoResIndex.put(serv, map.cloneReqResMap());
			System.out.print("good1 ");
			ResourceToRequestMap map2 =	(ResourceToRequestMap)serv.getStruct(
					RequestResourceMappingLogProcessor.OBJ_REQ_MAP);
			
			servRestoReqIndex.put(serv, map2.cloneResReqMap());
			System.out.print("good2 ");
			
			
			
			CacheLogList cl= (CacheLogList) serv.getStruct("cacheLogGroup");
			System.out.print("cl.cacheLogList.size() before= "+cl.cacheLogList.size());
			servCacheLogIndex.put(serv, (CacheLogList) cl.clone());	
			System.out.print("cl.cacheLogList.size() after= "+cl.cacheLogList.size());
			
			
		//	this.avgExecTime=+hl.currentLogInterval.avgResp;			
		} */
	
	
		
		/*
		
		for(ASServer serv:manager.getASServers().values())
		{

			RequestToResourcesMap map =	(RequestToResourcesMap)serv.getStruct(
					RequestResourceMappingLogProcessor.REQ_OBJ_MAP);
			servReqtoResIndex.put(serv, map.cloneReqResMap());
			System.out.print("good1 ");
			ResourceToRequestMap map2 =	(ResourceToRequestMap)serv.getStruct(
					RequestResourceMappingLogProcessor.OBJ_REQ_MAP);
			
			servRestoReqIndex.put(serv, map2.cloneResReqMap());
			System.out.print("good2 ");
			
			HttpLogList hll = (HttpLogList)serv.getStruct(HttpRequestProcessor.REQ_LOG_GROUP);
			servHttpLogIndex.put(serv, hll.cloneHLL());
			System.out.print("good3");
			
			
			HttpLogList hl = (HttpLogList) serv.getStruct("reqLogGroup");
			
			
			CacheLogList cl= (CacheLogList) serv.getStruct("cacheLogGroup");
			System.out.print("cl.cacheLogList.size() before= "+cl.cacheLogList.size());
			servCacheLogIndex.put(serv, (CacheLogList) cl.clone());	
			System.out.print("cl.cacheLogList.size() after= "+cl.cacheLogList.size());
			
		//	this.avgExecTime=+hl.currentLogInterval.avgResp;			
		}
		*/
		/// OMAR 
		/*
		this.avgExecTime=this.avgExecTime/manager.servers.size();
		Analyser.log.info(" avg exec. time =" + this.avgExecTime);
		*/
		
		tester=99;
		
		
		mergedHttpMapAll = mergeHttpRequestObjectsFromServers(); // OLD
		Analyser.log.info(" mergedHttpMapAll.SIZE() =" + mergedHttpMapAll.size());

		
		
		//currentAnalysisPhase.mergeHttpRequestObjectsFromServersForGlobal();
		System.out.println("manager.globalCacheMap.size()===================\n"+ manager.globalCacheMap.size());
		
		
		
	//	currentAnalysisPhase.cacheList=currentAnalysisPhase.mergeASCacheLogs(); //OLD
		
		// this is to aggregate all 
		
		
		
		//////////////////////////// NEW DS///////////////////

		//mergeASCacheLogs();

		
		
		
		Analyser.log.info(" manager.globalCacheMap.size() =" + manager.globalCacheMap.size());
		
		
		//currentAnalysisPhase.countServererObjectsAccessFreq(); //OLD-TO-REMAIN
		
		
		
		//currentAnalysisPhase.mergedHttpMapAllPreviousInterval=currentAnalysisPhase.mergeHttpRequestObjectsFromServersPrevInterval(); // OLD
		  
		
		
		//currentAnalysisPhase.reqToResAll = currentAnalysisPhase.mergeAllReqToRes(); // OLD
		
////////////////////////////NEW DS///////////////////
		//mergeAllReqToResGlobal();
		Analyser.log.info("manager.globalRequestToObjectMap.size()="+ manager.globalRequestToObjectMap.size());
		Analyser.log.info("manager.globalRequestToObjectMap.size()="+ manager.globalRequestToObjectMap.size());
		
		
		
		//currentAnalysisPhase.resToReqAll = currentAnalysisPhase.mergeAllResToReq(); //OLD
		
		////////////////////////////NEW DS///////////////////
		//mergeAllResToReqGlobal();
		Analyser.log.info("manager.globalObjectToRequestMap.size()="+ manager.globalObjectToRequestMap.size());
		
		
		int servObjCap=manager.globalObjectToRequestMap.size()/numServers;
		
		servCapObj=(servObjCap)+(servObjCap/10);
		
		
		
		//System.out.println("Omar======mergedHttpMapAll.size()===========\n"+ currentAnalysisPhase.mergedHttpMapAll.size());
		
		
		
		
		 mergeCurrentHttpLogGlobal(); //new
		 Analyser.log.info(" manager.globalRequestMap.size()=" + manager.globalRequestMap.size());
	
		
		
		//// currentAnalysisPhase.calculateRequestsObjectsWeights();
	//	 currentAnalysisPhase.CalculateToConsider();
		
		int mergesize=manager.globalRequestMap.size();
		
		
		/*
		long sharedListBefore0=System.currentTimeMillis();
		

		long sharedListAfter0=System.currentTimeMillis();
		long constructingBitSet=sharedListAfter0-sharedListBefore0;
		
		Analyser.log.info(" constructingBitSet=" + constructingBitSet);
		
		
		
		// NEW OMMMMAR
		 * 
		 * 
		 * 
		*/
		
		
		
		
		// we need it for the current requests	
		
		
		
	//	currentAnalysisPhase.httpListAll = HttpLogList.getHttpRequestObjectListWeightedNew(currentAnalysisPhase.httpListAll, true);
	//	System.out.println("currentAnalysisPhase.httpListAll.size()===================\n"+ currentAnalysisPhase.httpListAll.size());
		
		long tDrift=System.currentTimeMillis();
		driftCounter();
		long tDriftAf=System.currentTimeMillis();
		
		//currentAnalysisPhase.prevStat.driftCounter=currentAnalysisPhase.globalObjectDriftCounter;
		long driftTime= tDriftAf-tDrift;
		Analyser.log.info(" driftTime=" + driftTime);
		Analyser.log.info(" globalObjectDriftCounter=" + globalObjectDriftCounter);
		
		servCap=0;
		totalWeight=0;
		variation=0;
		
		manager.totVtxWgt=0;
		manager.totEdgeWgt=0;
		
		
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

			
	    	
	    	/*
	    	httpListAll=sortRequestObjectList(httpListAll,true);
	    	
	    	Iterator<HttpRequestObject> ir=httpListAll.iterator();
			while (ir.hasNext())
			{
				HttpRequestObject hr=ir.next();	
				Analyser.log.info(hr.url +" ==>"+hr.weight);
			}
			
			*/
	    	httpListAll=sortRequestObjectList(httpListAll,true);
			boolean flag=true;
			if (flag)
			{
			if (useOptimisation.equals("gr") || useOptimisation.equals("both"))
			{

				
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
				Analyser.log.info("XXXglobalObjectToRequestMap size before"+ manager.globalObjectToRequestMap.size());
				
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
				
				Analyser.log.info("XXXglobalObjectToRequestMap size After"+ manager.globalObjectToRequestMap.size());

	
				
			}
			}
			// this is for drift rate requests reduction
			
			// dr===> for drift rate
			
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
				servObjCap=(int) (manager.globalObjectToRequestMap.size()/numServers);
				servCapObj=(servObjCap)+(servObjCap/10);
	
				
			}
			
			
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
		
	
		System.out.println("Omar======totalWeight==========\n" +totalWeight);
		System.out.println("Omar======servCap==========\n" +servCap);
		
		
		
		Analyser.log.info("sharedReq.size()"+sharedReq.size());
		
		
		
		
		  	Analyser.log.info("manager.globalObjectToRequestMap.size= " +manager.globalObjectToRequestMap.size());
		    long sharedListBefore=System.currentTimeMillis();
		   // findSharedReqBit();
		   // findSharedReqObj();
		    
		    long constructBF=System.currentTimeMillis();
		    
		    
		    //currentAnalysisPhase.constructGraph();
		    
		    
		    construcJGraphT();
		    
		    
		    
		    Analyser.log.info("requestsGraph.vertexSet().size() "+ requestsGraph.vertexSet().size());
		    
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
		    
		    servCap=totalWeight/numServers;
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
		Analyser.log.info("Req Size:" +httpListAll.size());
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
			
			if(policyFor.equalsIgnoreCase("as") || policyFor.equalsIgnoreCase("both") )
			{		
				for(ASServer serv : servIndex.values())
				{
					//TODO update the port logic in base class
					sendASPolicy(serv.currentPolicy, serv.getServerId());
				//	System.out.println(serv.currentPolicy);
					
					/*
					Analyser.log.info("serv.getServerId()" + serv.getServerId());
					Analyser.log.info("serv.currentPolicy" + serv.currentPolicy.policyMap.size());
					*/
					serv.currentPolicy.policyMap.clear();
					//if (serv.serverNo==0)
					//	Analyser.log.info("serv.currentPolicy" + serv.currentPolicy.policyMap.toString());
					
				}
				
				
				
			}
			
			//send lb policy
		//	System.out.println("Omar======currentLBPolicy:" + currentLBPolicy);
			//System.out.println("Omar======LBSERVER:" +  manager.props.getProperty(StatisticsManager.LBSERVER));
			
			//if(policyFor.equalsIgnoreCase("lb") || policyFor.equalsIgnoreCase("both"))
			if(policyFor.equalsIgnoreCase("lb") || policyFor.equalsIgnoreCase("both") )
			{
				sendLBPolicy(currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
				Analyser.log.info("###############################################################" );
				Analyser.log.info("manager.globalRequestMap.size=" +manager.globalRequestMap.size() );	
			Analyser.log.info("policy size=" +currentLBPolicy.policyMap.size() );
				//sendLBPolicy(currentLBPolicy, manager.props.getProperty(StatisticsManager.LBSERVER));
			}
			
			currentLBPolicy.policyMap.clear();
		}
		//prevStats.assignBitSet(this.currentAnalysisPhase);
		//updateServersBitSet(); 
		
		updateObjectLoc();
		/*
		resetObjects();
		resetRequests();
		httpListAll.clear();
		*/
		
		//create problem to the performance
		/*
		if (manager.counter==0)
		  {
		
			  manager.oldGraph=requestsGraph;
			  manager.globalRequestMapOld=(HashMap<String, HttpRequestObject>) manager.globalRequestMap.clone();
			  manager.totEdgeWgtOld=0;
			  manager.totVtxWgtOld=0;
			  manager.totEdgeWgtOld=manager.totEdgeWgt;
			  manager.totVtxWgtOld=manager.totVtxWgt;
			  org.jgrapht.Graphs.addGraph(manager.requestsGraphOld, requestsGraph);	
			  manager.totEdgeWgt=0;
			  manager.totVtxWgt=0;
		  }
		  else
		  {  
			  Analyser.log.info("STARTING GRAPH COMPARISON"+manager.totVtxWgt + "AAA "+manager.totVtxWgtOld);
			  compareGraphs cg= new compareGraphs(manager.requestsGraphOld,requestsGraph , manager.totEdgeWgtOld, manager.totEdgeWgt, manager.totVtxWgtOld, manager.totVtxWgt, manager);	  
			  int totNewEdgeCount=cg.edgeWithOneVrtxIsNewCounter + cg.edgeWithBothVrtxAreNewCounter;		  
			  cg.printValidateNewEdgesWeight();			  
			  manager.oldGraph=requestsGraph;
			  manager.globalRequestMapOld=(HashMap<String, HttpRequestObject>) manager.globalRequestMap.clone();
			  manager.totEdgeWgtOld=0;
			  manager.totVtxWgtOld=0;
			  manager.totEdgeWgtOld=manager.totEdgeWgt;
			  manager.totVtxWgtOld=manager.totVtxWgt;
			  manager.requestsGraphOld =new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
			  org.jgrapht.Graphs.addGraph(manager.requestsGraphOld, requestsGraph);
			  manager.totEdgeWgt=0;
			  manager.totVtxWgt=0;
		  }
		
		*/
		  /*
		  if (manager.counter>0)
		  {
			  NMSimilarity similarityMeasure = new NMSimilarity(manager.oldGraph,currentAnalysisPhase.requestsGraph,  0.0001);
			  manager.oldGraph=currentAnalysisPhase.requestsGraph;
			  similarityMeasure.makeSure();
			  Analyser.log.info("\nTwo graphs have " + similarityMeasure.getGraphSimilarity() + "% of similarity");
			  manager.oldGraph=currentAnalysisPhase.requestsGraph;
			  
		  }
		  else
		  {
			  NMSimilarity similarityMeasure = new NMSimilarity(currentAnalysisPhase.requestsGraph,currentAnalysisPhase.requestsGraph,  0.0001);
			  manager.oldGraph=currentAnalysisPhase.requestsGraph;
			  similarityMeasure.makeSure();
			  Analyser.log.info("\nTwo graphs have " + similarityMeasure.getGraphSimilarity() + "% of similarity");
			  
		  }
		*/
		
	}	


	}
}


