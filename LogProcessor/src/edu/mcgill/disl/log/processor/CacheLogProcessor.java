package edu.mcgill.disl.log.processor;

import edu.mcgill.disl.analytics.ASServer;
import edu.mcgill.disl.analytics.Analyser;
import edu.mcgill.disl.analytics.CacheLogList;
import edu.mcgill.disl.analytics.CacheObject;
import edu.mcgill.disl.analytics.policy.*;

import java.security.Policy;
import java.util.HashMap;

import org.apache.log4j.spi.LoggingEvent;

import edu.mcgill.disl.log.StatisticsManager;

public class CacheLogProcessor extends LogProcessor {
	
	//public HashMap<String, CacheObject> cacheMap;
	
	public static final String OBJ_LOG_GROUP = "cacheLogGroup";
	public StatisticsManager sm;
	 public boolean suspendFlag;
	 public int logType; //0:local get 1: remote get
		
	public CacheLogProcessor(StatisticsManager sm){
		super(sm);
		//register instance
		initializeStructures();
		getManager().registerProcessor(this);
		
		//cacheMap = new HashMap<String, CacheObject>();
	}
	
	

	@Override
	public synchronized void processLog(LoggingEvent le) {
		
		//System.out.println("Inside CacheLogProcessor: processLog");
		
		//System.out.println("l: " + le.getMessage() +"OriMessage= "+le.timeStamp+"CurrentTime " +getUID(le));
		
		
		//System.out.println("CacheprocessLog: " + le.getMessage());
		if(getLogType(le).equals(ACCESS_LOG))
			return;
		
		//System.out.println("le.getLocationInformation(): " + getLogServer(le).getServerId());
		
	//System.out.println("CacheprocessLog: " + le.getMessage());
		
		//ASServer serv = getLogServer(le);
		
		
		ASServer serv = manager.getASServer(0);
		
			
	//	System.out.println("ASServer: " + serv);
		
		String[] strarr = ((String) (le.getMessage())).split("::");
		
		//Analyser.log.info("UpdatNew="+le.getMessage());	
		
	//added by Omar	
		
	//	String[] strarr = ((String) (le.getMessage())).split("\\s+");
		
		String resTime= strarr[2];
		String cacheKey = strarr[1]; 
		String getput = strarr[0];
		String size = "0"; // 0 size for get otherwise for put
		
		if(getput.contains("put"))
		{
		//	size = strarr[2]; edited by Omar because it gives outOfBoundary error
			size = "1"; 
			
		}
		
		
		
		////REM by Omar	
		CacheLogList cl = (CacheLogList) serv.getStruct(OBJ_LOG_GROUP);
		
		CacheObject co = new CacheObject();
    	co.cacheKey = cacheKey;
    	co.size = Integer.parseInt(size);
    	co.totalExecTime=Long.parseLong(resTime);
    	
    	
    	
    	sm.totalExecTime+=Double.parseDouble(resTime);
    	if(getput.contains("put"))
		{
		//	size = strarr[2]; edited by Omar because it gives outOfBoundary error
			//co.last_put_time = le.timeStamp;
			co.last_put_time=System.currentTimeMillis();
		}
//    	if(getput.contains("get"))
//    	{
//    		co.first_get_time = le.timeStamp;
//    		co.last_get_time = le.timeStamp;
//    		co.getCount++;
//    		
//    	}
//    	else if(getput.contains("put"))
//    	{
//    		co.first_put_time = le.timeStamp;
//    		co.last_put_time = le.timeStamp;
//    		co.putCount++;
//    	}
    	
    	//if(!getput.contains("put"))
		//{ 
    //	co.getCount++; // how many times this object has been accessed by this AS
	//	}
    	
    	////REM by Omar	
    	//co.accessTimes.add();
    	
    	//System.out.println("null? " + manager.getASServer(getLogServer(le).getServerId()).oldObjList==null);
    	
    	
    	if (getput.contains("getLocal"))
    		logType=0; // for local hit
    	else if (getput.contains("getRemote"))
    	{
    		logType=1; //for remote hit
    		//System.out.println("Remote cacheObject"+co.cacheKey +"server="+getLogServer(le).getServerId());
    		/*
    		if (manager.getASServer(getLogServer(le).getServerId()).oldObjList!=null)
    		{
        		if (manager.getASServer(getLogServer(le).getServerId()).oldObjList.contains(cacheKey))
        		{
        			System.out.println("ERROR ... Remote cacheObject"+co.cacheKey +"server="+getLogServer(le).getServerId());
        		}

    		}
    		*/
    	}
    	
    	else if (getput.contains("UpdatNew"))
    	{
    		logType=2;
    		
    	}
    	else if (getput.contains("getLocLock"))
    		logType=3;
    	else if (getput.contains("getRemLock"))
    		logType=4;
    	else if (getput.contains("updtLock"))
    		logType=5;
    	else if (getput.contains("update"))
    		logType=6;
    	else if (getput.contains("putLocal"))
    		logType=7;	
    	else if (getput.contains("putRemote"))
    		logType=8;
    	
    	
    	
    	//if (logType>1)
    		//return;
    	
    	/*
    	if (manager.getASServer(getLogServer(le).getServerId()).oldObjList.contains(cacheKey))
    		logType=0; // for local hit
    	else
    	{
    		logType=1; //for remote hit
    		System.out.println("Remote cacheObject"+co.cacheKey +"server="+getLogServer(le).getServerId());
    	}
    	
    	
    	*/
    	
    	cl.addToCacheLogInterval(co, getput,le.timeStamp, logType);

	/*	
        if(cacheMap.containsKey(cacheKey))
        {
        	CacheObject ctemp = (CacheObject) cacheMap.get(cacheKey);
        	if(strarr[0].contains("get"))
        	{
        		ctemp.last_get_time = le.timeStamp;
        		ctemp.getCount++;
        	}
        	else if(strarr[1].contains("put"))
        	{
        		ctemp.last_put_time = le.timeStamp;
        		ctemp.putCount++;
        	}
        		        		
        }
        else
        {
        	CacheObject co = new CacheObject();
        	co.cacheKey = cacheKey;
        	if(strarr[0].contains("get"))
        	{
        		co.first_get_time = le.timeStamp;
        		co.last_get_time = le.timeStamp;
        		co.getCount++;
        	}
        	else if(strarr[0].contains("put"))
        	{
        		co.first_put_time = le.timeStamp;
        		co.last_put_time = le.timeStamp;
        		co.putCount++;
        	}
        	cacheMap.put(co.cacheKey, co);
        } */
	}
	
	public void initializeStructures(){
		for(ASServer serv : getManager().getASServers().values()){
			//add all structures here for each server that we will maintain
			
			//activity counter
			//serv.setStruct("cacheMap", new HashMap<String, CacheObject>());
			serv.setStruct(OBJ_LOG_GROUP, new CacheLogList(manager));
			
		}
	}
}