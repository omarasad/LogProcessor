package edu.mcgill.disl.log;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.jgrapht.Graph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import edu.mcgill.disl.analytics.ASServer;
import edu.mcgill.disl.analytics.Analyser;
import edu.mcgill.disl.analytics.CacheLogList;
import edu.mcgill.disl.analytics.CacheObject;
import edu.mcgill.disl.analytics.CacheObjectReplicationCandidate;
import edu.mcgill.disl.analytics.HttpLogList;
import edu.mcgill.disl.analytics.HttpRequestObject;
import edu.mcgill.disl.analytics.RequestToResourcesMap;
import edu.mcgill.disl.analytics.ResourceToRequestMap;
import edu.mcgill.disl.log.processor.*;

public class StatisticsManager implements Runnable {
										
	
	//public static final String RootPath = System.getenv("JBOSS_HOME") + "/server/default/deploy/rubis.war/";
	//public static final String CONFIG_FILE = "/home/2011/oasad/workspace/LogProcessor/src/config.xml";
	public static final String CONFIG_FILE = System.getenv("JBOSS_HOME") + "/server/minimal/lib/config.xml";
	public static final String PROCESSOR_CLASSES = "processor.classes";
	public static final String ANALYSER_CLASSES = "analyser.classes";
	public static final String APPSERVERS = "appservers";
	public static final String LBSERVER = "lbserver";
	public static final String ANALYSIS_INTERVAL = "analysis.interval";
	public static final String USE_REGRESSION = "use.regression";
	
	public String analyserClassString="";



	public static final String AS_CACHE_SIZE = "as.cache.size";
	public static final String ANALYSIS_ITERATOR = "analysis.iterations";

	public static final String AS_CACHE_MEMORY = "as.cache.memory";
	public static final String REPL_PERCENT = "repl_percent";
	public static final String EACH_CACHE = "each_cache";
	public static final String SEGMENT_INTERVAL = "segment_interval";
	public static final String NO_INTERVALS = "no_intervals";
	public static final String NO_CACHE_OBJECT = "no_cache_object";
	public static final String OBJ_FOR_REQ = "obj_for_req";
	public static final String USE_OPTIMISATION = "use.optimisation"; 
	public static final String GRAPH_TYPE = "graph.type";
	public static final String NO_SUB_INTERVALS = "no.sub.intervals";

	public static final String request_Pop_Thresh = "request.popularity.percentage"; 
	public static final String request_Drift_Thresh = "request.drift.percentage"; 
	
	public static final String replication_strategy = "replication.strategy";
	public static final String replication_popular_object_percentage = "replication.popular.object.percentage";
	public static final String replication_low_write_object_percentage = "replication.low.write.object.percentage";
	
	public static final int NO_WORKLOAD_CHANGE=-1;
	public static final int LOCAL_HIT_WORKLOAD_CHANGE=1;
	public static final int READ_WRITE_RATIO_WORKLOAD_CHANGE=2;
	public static boolean currentReadWriteChangePhase=false; // when true: this is to know that the analyser is currently in a read write change phase when done turn to false
	
	public static int WORKLOAD_CHANGE=NO_WORKLOAD_CHANGE;
	
	public Map<Integer,Double> readWriteRatioMap= new HashMap<Integer, Double>(); // this map keeps track of the last read/write ratio after detecting workload changes
	public Set <CacheObjectReplicationCandidate> distributedObject= new HashSet<CacheObjectReplicationCandidate>(); // this DS keeps track of all object that are in the cut but necessarily replicated
	
	
	
	
	//to keep track of the number of txn executed in the last interval
	public static long reqCounter=0;
	public static double maxThpt=0;
	
	public static double maxLocalHit=0;
	public static double lastReadWriteRatio=0;
	
	public static double bestLatency=999999;
	public static double totalExecTime=0;


	

	public int tempCnt=0;
	
	 public int counter=0;
	
	public static final String POLICY_FOR = "policy.for";
	public String vertexWgt="";
	public int vertexNo=0;
	
	// OMAR
	
	public Map<Integer, String> gloabCacheObjectIndexing=new HashMap<Integer, String>();
	public HashMap<String, CacheObject> globalCacheMap=new HashMap<String, CacheObject>();
	public HashMap<String, CacheObject> globalCacheMapOld=new HashMap<String, CacheObject>();

	public int gloabalCacheKeyCounter=0;
	
	public long currentIntervalStartTime=0;
	
	public Map<Integer, String> gloablRequestIndexing=new HashMap<Integer, String>();
	public Map<Integer, String> gloablRequestIndexingIncremental=new HashMap<Integer, String>();

	public Map<String, Integer> gloablRequestIndexingSchismReplication=new HashMap<String, Integer>();
	
	public HashMap<String, HttpRequestObject> globalRequestMap=new HashMap<String, HttpRequestObject>();	
	public HashMap<String, HttpRequestObject> globalRequestMapOld=new HashMap<String, HttpRequestObject>();
	
	public long totEdgeWgtOld=0;
	public long totVtxWgtOld=0;
	public long totEdgeWgt=0;
	public long totVtxWgt=0;
	
	public double readLocalCost=0.004;
	public double readRemoteCost=1.06;
	public double updateRemoteCost=5.0;
	public double putLocalCost=0.062;
	public double putRemoteCost=1.07;
	public double fetchDBCost=1.5;
	
	
	public static double readLocalTimeTotal=0.0;
	public static double readRemoteTimeTotal=0.0;
	public static double updateRemoteTimeTotal=0.0;
	public static double putLocalTimeTotal=0.0;
	public static double putRemoteTimeTotal=0.0;
	
	public static double readLocalCount=0.0;
	public static double readRemotecount=0.0;
	public static double updateRemoteCount=0.0; // this is actually for both local and remote
	public static double putLocalCount=0.0;
	public static double putRemoteCount=0.0;

	
	
	public UndirectedGraph<String, DefaultEdge> requestsGraphOld =
		      new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);

	
	public int gloabalRequestKeyCounter=0;
	public int gloabalRequestKeyCounterIncremental=0;
	
	public Map<String,HashSet<String>> globalObjectToRequestMap = new HashMap<String, HashSet<String>>(100);
	
	public Map<String,HashSet<String>> globalRequestToObjectMap = new HashMap<String, HashSet<String>>(100);
	
	public Map<String,HashSet<String>> globalRequestToObjectMapOld = new HashMap<String, HashSet<String>>(100);

	
	public ArrayList<String> ReqClassOld = new ArrayList<String>(); // this is for heuristic
	public HashMap<String, String> ReqOldLoc = new HashMap<String, String>(); // this is for KMETIS
	public HashMap<String, String> ReqCurLoc = new HashMap<String, String>(); // this is for KMETIS
	
	
	
	public Graph<String, DefaultEdge> oldGraph =new SimpleGraph<String, DefaultEdge>(DefaultEdge.class); // this is for graph similarity
	
	
	public Map<Integer, String> updatedObjects= new HashMap<Integer, String>();
	
	public Map<Integer, Double> replicateObjectCost= new HashMap<Integer, Double>();
	
	// start working with thisssss
	public Map<Integer, List<Double>> replicateObjectCostTemp= new HashMap<Integer, List<Double>>();
	
	public Map<String, ArrayList<String>> updatedObjectsOperations= new HashMap<String, ArrayList<String>>();
	public Map<String, String> updatedObjectsGUID= new HashMap<String, String>();
	
	public Map<Integer, Double> updatedObjectReplicaCosts= new HashMap<Integer, Double>();
	
	public Map<String, Integer> replicateObjectCounter= new HashMap<String, Integer>(); // this data structure keeps track of the number of updates needs for each replica#

	
	
	// OMAR

	public boolean acceptLogs = true;
	

	static StatisticsManager manager;
	
	public replicationRegressionInfo replRegInfoInstance; // this is the instance for replicationRegrssionInfo object that will be accessed by all working threads...
	

	ArrayList<LogProcessor> processors = new ArrayList<LogProcessor>();

	public ArrayList<Analyser> analysers = new ArrayList<Analyser>();

	public Map<String, ASServer> servers = new HashMap<String, ASServer>();

	static final Logger log = Logger.getLogger(StatisticsManager.class
			.getName());

	public Properties props; // the properaties of the config_file

	public static StatisticsManager getInstance() {
		if (manager == null) {
			manager = new StatisticsManager();
		}
		return manager;
	}

	private StatisticsManager() {

		try {
			// load properties
			props = new Properties();
			props.loadFromXML(new FileInputStream(CONFIG_FILE));

			// load server instances
			instantiateASServers();

			// load log processors
			instantiateLogProcessors();

			// load analytics processors
			instantiateAnalysers();

			// run the thread
			//Thread t = new Thread(this);
			//t.start();

		} catch (Exception ex) {
			log.fatal(ex.getMessage(), ex);
		}
	}
	
	public void cloneReqClass()
	{
		this.globalRequestToObjectMapOld.clear();
		for (String s:this.globalRequestToObjectMap.keySet())
		{
			HashSet<String> objects= this.globalRequestToObjectMap.get(s);
			this.globalRequestToObjectMapOld.put(s, objects);
		}
		
	}

	public void fillRepCost(int servNo)
	{
		if (servNo==1)
			replicateObjectCost.put(servNo, 4.0);
		else if (servNo==2)
			replicateObjectCost.put(servNo, 9.0);
		else if (servNo==3)
			replicateObjectCost.put(servNo, 12.0);
		else if (servNo==4)
			replicateObjectCost.put(servNo, 16.0)	;
			
	}
	
	public void instantiateASServers() {
         String[] asServers = props.getProperty(APPSERVERS).split(",");
         boolean dynamicReplicationWithNoParameters=props.getProperty(replication_strategy).equals("dynamic-no-parameters-extraction");
		System.out.print("APPSERVERS =" +asServers);
		int servNo = 0;
		for (String server : asServers) { // loop on the asServers Array items
			server = server.trim();
			log.debug("Omar Sever Name: " + server);
			if (!server.equals("")) {
				servers.put(server, new ASServer(server,servNo++));
			}
			
			if (dynamicReplicationWithNoParameters)
				fillRepCost(servNo);
			
		}
		log.debug("======AS Instances======" + servers);
	}

	
	public void instantiateLb() {
	//	 LBSERVER = props.getProperty(LBSERVER);
		System.out.print("APPSERVERS =" +LBSERVER);
		log.debug("======AS Instances======" + servers);
	}
	
	
	public void instantiateLogProcessors() throws Exception {
		// create log processors and add to the list
		String[] classes = props.getProperty(PROCESSOR_CLASSES).split(",");
		System.out.print("PROCESSOR_CLASSES =" +classes);
		for (String cls : classes) {
			cls = cls.trim();
			if (!cls.equals("")) {
				Class c = Class.forName(cls, false, this.getClass()
						.getClassLoader());
				c.getConstructor(new Class[] { StatisticsManager.class })
						.newInstance(this);
			}
		}
	}

	public void instantiateAnalysers() throws Exception {
		// create log processors and add to the list
		String[] classes = props.getProperty(ANALYSER_CLASSES).split(",");
		System.out.print("ANALYSER_CLASSES =" +classes);
		for (String cls : classes) {
			cls = cls.trim();
			if (!cls.equals("")) {
				Class c = Class.forName(cls, false, this.getClass()
						.getClassLoader());
				
				System.out.println("c.toString()"+c.toString());
				analyserClassString=analyserClassString+""+c.toString();

				Analyser a = (Analyser)c.getConstructor(new Class[] { StatisticsManager.class })
						.newInstance(this);
				analysers.add(a);
			}
		}
	}

	public void generateSystemParameters()
	{
		this.readLocalCost=this.readLocalTimeTotal/(this.readLocalCount*1000000.0);
		this.readRemoteCost=this.readRemoteTimeTotal/(this.readRemotecount*1000000.0);
		this.updateRemoteCost=this.updateRemoteTimeTotal/(this.updateRemoteCount*1000000.0);
		this.putLocalCost=this.putLocalTimeTotal/(this.putLocalCount*1000000.0);
		this.putRemoteCost=this.putRemoteTimeTotal/(this.putRemoteCount*1000000.0);
		
		
		
		Analyser.log.info("readLocalCost= " +this.readLocalCost +" readLocalCount="+this.readLocalCount);
		Analyser.log.info("readRemoteCost= " +this.readRemoteCost+" readRemotecount="+this.readRemotecount);
		Analyser.log.info("updateRemoteCost= " +this.updateRemoteCost+" updateRemoteCount="+this.updateRemoteCount);
		Analyser.log.info("putLocalCost= " +this.putLocalCost+" putLocalCount="+this.putLocalCount);
		Analyser.log.info("putRemoteCost= " +this.putRemoteCost+" putRemoteCount="+this.putRemoteCount);
	}
	
	public void registerProcessor(LogProcessor lp) {
		synchronized (processors) {
			if (!processors.contains(lp)) {
				processors.add(lp);
				log.debug("Log Processor add:" + lp);
			}
		}
	}

	public void deRegisterProcessor(LogProcessor lp) {
		synchronized (processors) {
			processors.remove(lp);
			log.debug("Log Processor removed:" + lp);
		}
	}

	public synchronized void processLog(LoggingEvent le) {
	
		if(acceptLogs)
		{
			for (LogProcessor lp : processors) {
				lp.processLog(le);
			}
		}
		//printStats();

	}
	
	
	
	public void addInterval()
	{
		//this.acceptLogs=false;
		System.out.print("add1");
		String ser="";
		Iterator it=servers.entrySet().iterator();
		while (it.hasNext())
		{
			
			 Map.Entry pairs = (Map.Entry)it.next();
			ser=(String) pairs.getKey();
			CacheLogList cl = (CacheLogList) servers.get(ser).getStruct("cacheLogGroup");
			cl.addNewCacheSegment();
			
			HttpLogList hl = (HttpLogList) servers.get(ser).getStruct("reqLogGroup");
			hl.addNewHttpInterval();
			
			
			RequestToResourcesMap rm= (RequestToResourcesMap) servers.get(ser).getStruct("reqObjMap");
			rm.addNewMap();
			
			ResourceToRequestMap rq= (ResourceToRequestMap) servers.get(ser).getStruct("objReqMap");
			rq.addNewMap();
			
			System.out.print("serv"+ser);
			
			
		}
		manager.globalCacheMap.clear();
		manager.globalObjectToRequestMap.clear();
		manager.globalRequestMap.clear();
		manager.globalRequestToObjectMap.clear();
		manager.gloabalRequestKeyCounter=0;
		manager.gloabalRequestKeyCounterIncremental=0;
		manager.gloabalCacheKeyCounter=0;
		
		
	//	this.acceptLogs=true;
		
		/*
		CacheLogList cl = (CacheLogList) servers.get("db-node-03.CS.McGill.CA").getStruct("cacheLogGroup");
		cl.addNewCacheSegment();
		
		HttpLogList hl = (HttpLogList) servers.get("db-node-03.CS.McGill.CA").getStruct("reqLogGroup");
		hl.addNewHttpInterval();
		*/
	}
	
	public void addIntervalOri()
	{
		//this.acceptLogs=false;
		System.out.print("add1");
		String ser="";
		Iterator it=servers.entrySet().iterator();
		while (it.hasNext())
		{
			
			 Map.Entry pairs = (Map.Entry)it.next();
			ser=(String) pairs.getKey();
			CacheLogList cl = (CacheLogList) servers.get(ser).getStruct("cacheLogGroup");
			cl.addNewCacheSegment();
			
			HttpLogList hl = (HttpLogList) servers.get(ser).getStruct("reqLogGroup");
			hl.addNewHttpInterval();
			
			
			RequestToResourcesMap rm= (RequestToResourcesMap) servers.get(ser).getStruct("reqObjMap");
			rm.addNewMap();
			
			ResourceToRequestMap rq= (ResourceToRequestMap) servers.get(ser).getStruct("objReqMap");
			rq.addNewMap();
			
			System.out.print("serv"+ser);
			
			
		}
		
	//	this.acceptLogs=true;
		
		/*
		CacheLogList cl = (CacheLogList) servers.get("db-node-03.CS.McGill.CA").getStruct("cacheLogGroup");
		cl.addNewCacheSegment();
		
		HttpLogList hl = (HttpLogList) servers.get("db-node-03.CS.McGill.CA").getStruct("reqLogGroup");
		hl.addNewHttpInterval();
		*/
	}

	public void runAnalysers() {
		for (Analyser analyser : analysers) {
			try{
				
			//	this.acceptLogs = false;
				analyser.analyse();
			//	this.acceptLogs = true;
			//	CacheLogProcessor cl= (CacheLogProcessor) processors.get(0);
			//	HttpRequestProcessor hl= (HttpRequestProcessor) processors.get(1);
				
				//addInterval();
				//hl.
				
				
			}catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}

	public void printStats() {
		for (ASServer serv : servers.values()) {

			System.out.println("==============BEGIN STATS===================");

			System.out.println("AS Server: " + serv.getServerId());

			Map<String, Object> structs = serv.getStructs();

			for (String structId : structs.keySet()) {

				System.out.println("Structure Name: " + structId);

				System.out.println("************Detail************\n"	+ structs.get(structId));

			}

			//System.out.println("============END STATS=====================");
		}
	}

	public ASServer getASServer(String serverId) {
		return servers.get(serverId);
	}
	
	public ASServer getASServer(int servNo) {
		ASServer as = null;
		
		Collection<ASServer> ascon = servers.values();
		Iterator<ASServer> itas = ascon.iterator();
		
		while(itas.hasNext()){
		
			as = itas.next();
			if(as.serverNo == servNo)
				return as;
		}
		
		return null;
	}


	public Map<String, ASServer> getASServers() {
		return servers;
	}
	public void run() {
		long analyseInterval = Long.parseLong(props.getProperty(ANALYSIS_INTERVAL));

		
		
			try {
				
				//Thread.sleep(analyseInterval);
				
			//	this.acceptLogs = false;
				Analyser.log.info("new run analyser ********* ");
				
				System.out.println("************tempCnt************\n"	+ tempCnt++);
				Analyser.log.info("tempCnt= " +tempCnt);
				if (tempCnt<5)
					runAnalysers();
				else 
					Analyser.log.info("tempCntELSE= " +tempCnt);
				
			//	this.acceptLogs = true;

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		
	}
	/*
	@Override
	public void run2() {
		long analyseInterval = Long.parseLong(props.getProperty(ANALYSIS_INTERVAL));

		while (true) {

			try {
				
				Thread.sleep(analyseInterval);
				
			//	this.acceptLogs = false;
				Analyser.log.info("new run analyser ********* ");
				
				System.out.println("************tempCnt************\n"	+ tempCnt++);
				Analyser.log.info("tempCnt= " +tempCnt);
				if (tempCnt<3)
					runAnalysers();
				else 
					Analyser.log.info("tempCntELSE= " +tempCnt);
				
			//	this.acceptLogs = true;

			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
		
	}
*/
}
