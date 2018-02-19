package edu.mcgill.disl.log.processor;

import edu.mcgill.disl.analytics.ASServer;
import edu.mcgill.disl.analytics.Analyser;
import edu.mcgill.disl.analytics.CacheLogList;
import edu.mcgill.disl.analytics.CacheObject;
import edu.mcgill.disl.analytics.CacheObjectReplicationCandidate;
import edu.mcgill.disl.analytics.HttpLogList;
import edu.mcgill.disl.analytics.HttpRequestObject;
import edu.mcgill.disl.analytics.MaintainingObjectReplication;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.spi.LoggingEvent;

import edu.mcgill.disl.log.StatisticsManager;

public class HttpRequestProcessor extends LogProcessor implements Runnable {
	Thread t = new Thread(this);
	Thread t2 = new Thread(this);
	Thread t3 = new Thread(this);
	Thread t4 = new Thread(this);
	Thread t5 = new Thread(this);
	
//	public HashMap<String, HttpRequestObject> reqMap;
	
	public static final String REQ_LOG_GROUP = "reqLogGroup";
	public StatisticsManager sm;
	 public boolean suspendFlag;
	 public List<String> updateTxn= new ArrayList<String>();
	
	
	
	public HttpRequestProcessor(StatisticsManager sm){
		super(sm);
		this.sm=sm;
		suspendFlag=true;
		
		initializeStructures();
		//register instance
		getManager().registerProcessor(this);
		updateTxn.add("putBidAuth");
		updateTxn.add("buyNowAuth");
		updateTxn.add("SellItemForm");
		updateTxn.add("Register");
		updateTxn.add("update");
		updateTxn.add("update");
		updateTxn.add("Store");
		
		
	}
	
	public boolean checkUpdateTxn(String txn)
	{
		for (int s=0;s<updateTxn.size();s++)
		{
			if (txn.contains(updateTxn.get(s)))
			 return true;
		}
		return false;
	}
	@Override
	public void processLog(LoggingEvent le) {  // what is the struct of LoggingEcvent?
		// TODO Auto-g
		//out.println("l: " + le.getMessage() +"OriMessage= "+le.timeStamp+"CurrentTime " +getUID(le));
		
		if(getLogType(le).equals(CACHE_LOG))
			return;
		
		
		//System.out.println("RequestprocessLog: " + le.getMessage());

		
		HttpRequestObject ho = new HttpRequestObject();
		long k=0;
		String s="";
		s= le.getMessage().toString();
		s=(String) s.subSequence(0, 13);
		k= Long.valueOf(s);
		
		//ho.first_date_time = k; ori
		ho.first_date_time = le.timeStamp;
		
		
		
		
		//System.out.println("AccessLogProcessor: " + le.getMessage() +"OriMessage= "+le.timeStamp+"CurrentTime "+ ho.first_date_time +"le.getStartTime()"+k);
		
		//ASServer serv = getLogServer(le);
		
		ASServer serv = manager.getASServer(0);
		
		//timestamp httpMethod+url+qs returnCode timetakenMillis
		
		String[] strarr = ((String) (le.getMessage())).split(" ");
		
		String url = strarr[1];
		
		
		if (checkUpdateTxn(url))
			return;
		
		sm.reqCounter++;
		//if (url.contains("RegisterItem"))
		//	return;
		
		int resp_code = Integer.parseInt(strarr[2]);
		long timetaken = Long.parseLong(strarr[3]);
		
	//	System.out.println("url: " +url + timetaken);
		
			
	//	System.out.println("serv: " + serv);
		HttpLogList hl = (HttpLogList) serv.getStruct(REQ_LOG_GROUP);
        
		//servIndex.get(0).oldLB
		
		if (url.contains("runAnalyser"))
		{
			System.out.print("runAnalyser");
			t.start();
			return;
		}
		else if (url.contains("run2Analyser"))
		{
			System.out.print("run2Analyser");
			t2.start();
			return;
		} 
		
		else if (url.contains("run3Analyser"))
		{
			System.out.print("run3Analyser");
			t3.start();
			return;
		} 
		
		else if (url.contains("run4Analyser"))
		{
			System.out.print("run4Analyser");
			t4.start();
			return;
		} 
		
		else if (url.contains("run5Analyser"))
		{
			System.out.print("run5Analyser");
			t5.start();
			return;
		} 
		
		else if (url.contains("addIntrv"))
		{
			System.out.print("addIntrv");
			sm.addInterval();
			return;
		}
			
		
    	//ho.first_date_time = le.timeStamp;
		
		//ho.first_date_time = System.currentTimeMillis();
    	ho.last_date_time = le.timeStamp;
    	ho.candidate=serv;
    	ho.resp_code = resp_code;
    	ho.url = url;// templatizeUrl(url);
    	if(resp_code == 200 || resp_code == 304)
    		ho.success_count ++;
    	else
    		ho.failure_count ++;
    	ho.counter++;
    	
    	//ho.execution_time = ((ho.execution_time * (ho.counter-1)) + timetaken)/ ho.counter ;
    	
    	hl.addtoHttpLogInterval(ho, timetaken);
        
//        if(reqMap.containsKey(url))
//        {
//        	HttpRequestObject htemp = (HttpRequestObject) reqMap.get(url);
//        	htemp.last_date_time = le.timeStamp;
//        	htemp.resp_code = resp_code;
//        	if(resp_code == 200 || resp_code == 304)
//        	{
//        		htemp.success_count ++;
//        	}
//        	else{
//        		htemp.failure_count ++;
//        	}
//        	htemp.counter++;
//        	htemp.execution_time = ((htemp.execution_time * (htemp.counter-1)) + timetaken)/ htemp.counter ;
//        }
//        else
//        {
//        	HttpRequestObject ho = new HttpRequestObject();
//        	ho.first_date_time = le.timeStamp;
//        	ho.last_date_time = le.timeStamp;
//        	ho.resp_code = resp_code;
//        	ho.url = url;
//        	if(resp_code == 200 || resp_code == 304)
//        		ho.success_count ++;
//        	else
//        		ho.failure_count ++;
//        	ho.counter++;
//        	
//        }
	}
	
	public void initializeStructures(){
		for(ASServer serv : getManager().getASServers().values()){
			//add all structures here for each server that we will maintain
			
			//activity counter
//			serv.setStruct("reqMap", new HashMap<String, HttpRequestObject>());
		
			serv.setStruct(REQ_LOG_GROUP, new HttpLogList(manager));
			
			
		}
	}

	
	public String templatizeUrl(String url){
		
		StringBuffer sb = new StringBuffer(url.length());
		boolean isVal=false;
		for(int i=0;i<url.length();i++){
			char ch = url.charAt(i);
			if(ch=='='){
				isVal = true;
			}else if(isVal){
				if(ch=='&'){
					isVal = false;
				}else{
					continue;
				}
			}
			sb.append(ch);
		}
		return sb.toString();
	}


	
	public void runOri()
	{
		// TODO Auto-generated method stub
		
		
		System.out.print(" HttpRequestProcessor -- run analyser 000 ");
		
		
		
		try {
			//Thread.sleep(150000);
			//Thread.sleep(120000);
			Thread.sleep(90000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.print(" HttpRequestProcessor -- run analyser 111");
		
		sm.runAnalysers();
		sm.addInterval();
		//Thread.sleep(18000);
		
	}

	//runTHISISWorkingWithReplication()
	public void runTHISISWorkingWithReplication()
	{
		long windowTime = Long.parseLong(sm.props.getProperty(sm.ANALYSIS_INTERVAL));
		int iterator = Integer.parseInt(sm.props.getProperty(sm.ANALYSIS_ITERATOR));
		// TODO Auto-generated method stub
		//while(sm.counter<30)
		while(sm.counter<iterator)
		{
		Analyser.log.info(" HttpRequestProcessor OK--+ run analyser  "+ sm.counter + "Analysis Interval" +windowTime );
		
		if ( sm.props.getProperty(sm.replication_strategy).equals("regression") || sm.props.getProperty(sm.replication_strategy).equals("dynamic") && sm.counter==1 ) // if we do regression add 15 seconds to the replicatoin interval
		{
			sm.acceptLogs=false;
				Analyser.log.info("  sm.replication_strategy.equals(\"regression\") ... sleep for extra 15 seconds... " );
				try {
					Thread.sleep(15000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//sleepFor+=5000;
				sm.acceptLogs=true;
		}	
		
		//sm.acceptLogs=false;
		
		
		try {
			sm.currentIntervalStartTime=System.currentTimeMillis();
			Thread.sleep(windowTime);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//System.out.print(" HttpRequestProcessor -- run analyser 111");
		
		
		sm.acceptLogs=false;
		Analyser.log.info(" sm.acceptLogs=false");
		if (sm.counter==0)
			sm.generateSystemParameters();
		sm.runAnalysers();
	//	if (sm.counter<1)
			sm.addInterval();
		
		
		Analyser.log.info(" sm.acceptLogs=true");
		sm.acceptLogs=true;
		
		sm.counter++;
		
		
	}		
	}
	
	
	public int checkWrkldChange()
	{
		
		System.out.println("=================================================================================");
		
		boolean toReset=false;
		// -1: no change 1:cache hit difference 2: read/write ratio difference
		int WrkldChange=sm.NO_WORKLOAD_CHANGE;
		
		final double WrkldChange_LocalHit=20.0; //// this is a percentage 
		
		//final double WrkldChange_ReadWriteRatio=20.0; // this is a percentage THIS WAS  
		
		final double WrkldChange_ReadWriteRatio=5.0; // this is a percentage -- change it to suit RUBiS
		
		// this portion is for cache hit change
		double totalHits =sm.readLocalCount+sm.readRemotecount;
		double curLocalHitRatio=(sm.readLocalCount/totalHits)*100;
		
		double avgLatency=(sm.readLocalCount * sm.readLocalCost + sm.readRemotecount * sm.readRemoteCost) / totalHits;
		
		if (curLocalHitRatio>=sm.maxLocalHit)
		{	
			sm.maxLocalHit=curLocalHitRatio;		
		}
		else if (curLocalHitRatio<sm.maxLocalHit)
		{
			if (Math.abs(curLocalHitRatio-sm.maxLocalHit)>WrkldChange_LocalHit)
			{
				WrkldChange=sm.LOCAL_HIT_WORKLOAD_CHANGE;
				toReset=true;	
			}
			
			// now reset values...
			
		//	sm.readLocalCount=0;
		//	sm.readRemotecount=0;
			
			                                                                                                                                                                                                                           
		}
		
		
		// this portion is for read/write ratio change::
		double totalRreadWriteHits =sm.updateRemoteCount+totalHits;
		
		System.out.println("sm.updateRemoteCount="+sm.updateRemoteCount +"sm.lastReadWriteRatio="+sm.lastReadWriteRatio);
		//double curWriteRatio=(sm.updateRemoteCount/totalRreadWriteHits)*100;
		double curWriteRatio=(sm.updateRemoteCount/(totalRreadWriteHits- sm.updateRemoteCount))*100.0; // the idea of mul by 4 is to reduce the number of read hit involved in write txn
		// The reason for subtracting is to avoid the hit due to the update from the total
		
		if (Math.abs(curWriteRatio-sm.lastReadWriteRatio)>WrkldChange_ReadWriteRatio)
		{
			WrkldChange=sm.READ_WRITE_RATIO_WORKLOAD_CHANGE;
			sm.readWriteRatioMap.put(0,curWriteRatio);
			System.out.println("WrkldChange=sm.READ_WRITE_RATIO_WORKLOAD_CHANGE" + "sm.updateRemoteCount"+ sm.updateRemoteCount + "totalHits="+totalHits );
			//sm.lastReadWriteRatio=Math.abs(curWriteRatio-sm.lastReadWriteRatio);
		}
		
		System.out.println(Math.abs(curWriteRatio-sm.lastReadWriteRatio)+"sm.maxLocalHit="+sm.maxLocalHit +" curLocalHitRatio="+curLocalHitRatio +"*** sm.previousReadWriteRatio="+sm.lastReadWriteRatio+" curWriteRatio="+curWriteRatio);
		System.out.println("INSIDE HTTP"+sm.readLocalCount + "/"+ sm.readRemotecount+ "/"+totalHits + "/"+sm.updateRemoteCount+"/"+totalRreadWriteHits+ "curWriteRatio"+curWriteRatio);
		System.out.println(toReset +""+ Math.abs(curLocalHitRatio-sm.maxLocalHit));
		if (toReset && WrkldChange!=sm.READ_WRITE_RATIO_WORKLOAD_CHANGE)
		{
			sm.readLocalCount=0;
			sm.readRemotecount=0;
			sm.updateRemoteCount=0;
		}
		
		sm.readLocalCount=0;
		sm.readRemotecount=0;
		sm.updateRemoteCount=0;
		sm.totalExecTime=0;
		
		sm.readLocalTimeTotal=0;
		sm.readRemoteTimeTotal=0;
		
		return WrkldChange;
		
	}
	
	public int checkWrkldChangeNEWWFORRW()
	{
		
		
		boolean toReset=false;
		// -1: no change 1:cache hit difference 2: read/write ratio difference
		int WrkldChange=sm.NO_WORKLOAD_CHANGE;
		
		final double WrkldChange_LocalHit=20.0; //// this is a percentage 
		final double WrkldChange_ReadWriteRatio=20.0; // this is a percentage
		
		final double WrkldChange_avgLatency=0.45; //// this is a percentage
		
		
		
		// this portion is for cache hit change
		double totalHits =0; double curLocalHitRatio=0;
		totalHits=sm.readLocalCount+sm.readRemotecount;
		 curLocalHitRatio=(sm.readLocalCount/totalHits)*100;
		
		
		
		//double curAvgLatency=(sm.readLocalCount * sm.readLocalCost + sm.readRemotecount * sm.readRemoteCost) / totalHits;
		
		double curAvgLatency=sm.totalExecTime/(totalHits*1000000);
		
		double avgLoc=sm.readLocalTimeTotal/(sm.readLocalCount*1000000);
		double avgRem=sm.readRemoteTimeTotal/(sm.readRemotecount*1000000);
		
		curAvgLatency=((avgLoc *   sm.readLocalCount) + (avgRem * sm.readRemotecount))/(sm.readLocalCount+sm.readRemotecount);
		
		Analyser.log.info("sm.readLocalCount="+sm.readLocalCount + "sm.readRemotecount="+sm.readRemotecount);
		Analyser.log.info("sm.readLocalTimeTotal="+sm.readLocalTimeTotal + "sm.readRemoteTimeTotal="+sm.readRemoteTimeTotal);
		Analyser.log.info("avgLoc="+avgLoc + "avgRem="+avgRem);
		
		
		
		Analyser.log.info("curAvgLatency="+curAvgLatency + "sm.bestLatency="+sm.bestLatency);
		
		
		
		if (curAvgLatency<=sm.bestLatency)
		{	
			sm.bestLatency=curAvgLatency;		
		}
		else if (curAvgLatency>sm.bestLatency)
		{
			if ((curAvgLatency-sm.bestLatency)>(sm.bestLatency*WrkldChange_avgLatency))
				WrkldChange=sm.LOCAL_HIT_WORKLOAD_CHANGE;
			
			// now reset values...
			toReset=true;
		//	sm.readLocalCount=0;
		//	sm.readRemotecount=0;
			
			Analyser.log.info("Diff ="+(curAvgLatency-sm.bestLatency) + "sm.bestLatency*WrkldChange_avgLatency="+sm.bestLatency*WrkldChange_avgLatency);
			
			
			
			                                                                                                                                                                                                                           
		}
		
		
		// this portion is for read/write ratio change::
		double totalRreadWriteHits =sm.updateRemoteCount+totalHits;
		//double curWriteRatio=(sm.updateRemoteCount/totalRreadWriteHits)*100;
		double curWriteRatio=(sm.updateRemoteCount/(totalRreadWriteHits- sm.updateRemoteCount))*100.0; // the idea of mul by 4 is to reduce the number of read hit involved in write txn
		// The reason for subtracting is to avoid the hit due to the update from the total
		
		if (Math.abs(curWriteRatio-sm.lastReadWriteRatio)>WrkldChange_ReadWriteRatio)
		{
			WrkldChange=sm.READ_WRITE_RATIO_WORKLOAD_CHANGE;
			sm.readWriteRatioMap.put(0,curWriteRatio);
			Analyser.log.info("WrkldChange=sm.READ_WRITE_RATIO_WORKLOAD_CHANGE" + "sm.updateRemoteCount"+ sm.updateRemoteCount + "totalHits="+totalHits );
			//sm.lastReadWriteRatio=Math.abs(curWriteRatio-sm.lastReadWriteRatio);
		}
		
		Analyser.log.info(Math.abs(curWriteRatio-sm.lastReadWriteRatio)+"sm.maxLocalHit="+sm.maxLocalHit +" curLocalHitRatio="+curLocalHitRatio +"*** sm.previousReadWriteRatio="+sm.lastReadWriteRatio+" curWriteRatio="+curWriteRatio);
		Analyser.log.info("INSIDE HTTP/"+sm.readLocalCount + "/"+ sm.readRemotecount+ "/"+totalHits + "/"+sm.updateRemoteCount+"/"+totalRreadWriteHits+ "curWriteRatio"+curWriteRatio);
		
		//if (toReset && WrkldChange!=sm.READ_WRITE_RATIO_WORKLOAD_CHANGE)
		//{
		
			sm.readLocalCount=0;
			sm.readRemotecount=0;
			sm.updateRemoteCount=0;
			sm.totalExecTime=0;
			
			sm.readLocalTimeTotal=0;
			sm.readRemoteTimeTotal=0;
			
			Analyser.log.info(sm.readLocalCount +"asdaf"+ sm.readRemotecount +""+ sm.updateRemoteCount +""+sm.totalExecTime);
			
			if (WrkldChange==sm.NO_WORKLOAD_CHANGE)
				sm.addInterval();
		//}
		return WrkldChange;
		
	}
	
	
	
	//runNewForReplication()
	public void run()
	{
		Analyser anss= sm.analysers.get(0);
		MaintainingObjectReplication MOR= new MaintainingObjectReplication(sm, anss, false, 1);
		
		String repStrategy=sm.props.getProperty(sm.replication_strategy);
		Analyser.log.info("repStrategy="+repStrategy );
		Analyser.log.info((repStrategy.equals("schism") || repStrategy.equals("dynamic-no-parameters-extraction") || repStrategy.equals("distributed")|| repStrategy.equals("popular")|| repStrategy.equals("none")|| repStrategy.equals("low-update")&& sm.counter==0));
		
		//long  shortWindowTime=10000;
		long  shortWindowTime=20000;

		long windowTime = Long.parseLong(sm.props.getProperty(sm.ANALYSIS_INTERVAL));
		
		int iterator = Integer.parseInt(sm.props.getProperty(sm.ANALYSIS_ITERATOR));
		// TODO Auto-generated method stub
		//while(sm.counter<30)
		while(sm.counter<iterator)
		{
		Analyser.log.info("  ------------------------------"+sm.counter + "Analysis Interval" +windowTime+"----------------------------------------");
		//Analyser.log.info(" HttpRequestProcessor OK-- OMMARRR+ run analyser  "+ sm.counter + "Analysis Interval" +windowTime );
		
		if ( repStrategy.equals("regression") || sm.props.getProperty(sm.replication_strategy).equals("dynamic") && sm.counter==1 ) // if we do regression add 15 seconds to the replicatoin interval
		{
			Analyser.log.info(sm.props.getProperty(sm.replication_strategy) + ""+  sm.props.getProperty(sm.replication_strategy));
			sm.acceptLogs=false;
				Analyser.log.info("  sm.replication_strategy.equals(\"regression\") ... sleep for extra 15 seconds... " );
				try {
					Thread.sleep(15000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//sleepFor+=5000;
				sm.acceptLogs=true;
		}	
		else if (sm.counter==0 && (repStrategy.equals("schism") || repStrategy.equals("dynamic-no-parameters-extraction") || repStrategy.equals("distributed") || repStrategy.equals("none") || repStrategy.equals("popular")|| repStrategy.equals("low-update") ))
		{
			Analyser.log.info("BBB"+sm.props.getProperty(sm.replication_strategy) + ""+  sm.props.getProperty(sm.replication_strategy));
			try {
				sm.currentIntervalStartTime=System.currentTimeMillis();
				Thread.sleep(windowTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
			
			sm.acceptLogs=false;
			Analyser.log.info(" sm.acceptLogs=false");
			if (sm.counter==0)
				sm.generateSystemParameters();
			sm.runAnalysers();
			sm.addInterval();
			
			double totalHits =sm.readLocalCount+sm.readRemotecount;
			double totalRreadWriteHits =sm.updateRemoteCount+totalHits;
			sm.lastReadWriteRatio=(sm.updateRemoteCount/totalRreadWriteHits)*100;
			
			
			sm.readLocalCount=0;
			sm.readRemotecount=0;
			sm.updateRemoteCount=0;
			
			Analyser.log.info(" sm.acceptLogs=true");
			sm.acceptLogs=true;
			sm.counter++;
		}
		
		else if (sm.counter>0 && (repStrategy.equals("dynamic-no-parameters-extraction") || repStrategy.equals("distributed") || repStrategy.equals("none")) )
		{
			Analyser.log.info("CCC"+sm.props.getProperty(sm.replication_strategy) + ""+  sm.props.getProperty(sm.replication_strategy));
			try {
				sm.currentIntervalStartTime=System.currentTimeMillis();
				Thread.sleep(shortWindowTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}			
			
			sm.acceptLogs=false;
			
			
			sm.WORKLOAD_CHANGE=this.checkWrkldChange();
			
			Analyser.log.info(" sm.acceptLogs=false" +sm.WORKLOAD_CHANGE);
			
			if (sm.WORKLOAD_CHANGE==sm.READ_WRITE_RATIO_WORKLOAD_CHANGE)
			{
				// here we have to check if it is a start of read/write ratio change or an end of it ... 
				
				sm.currentReadWriteChangePhase=true;
				// here we call another another method to change the object replication on object basis
				
				
				//sm.runAnalysers();
				
				
				sm.addInterval();
				
				MOR.startNewMonitoringInterval(sm.counter);
				try {
					MOR.montiroAndChangeObjectReplication(shortWindowTime);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				MOR.stopCurrentMonitoringInterval();
				
				
			}
			else if (sm.WORKLOAD_CHANGE==sm.LOCAL_HIT_WORKLOAD_CHANGE)
			{
				System.out.println("Before="+sm.globalRequestMap.size() +"AAA"+ sm.globalRequestToObjectMap.size() +""+ sm.globalCacheMap.size());;
				Analyser.log.info("Before="+sm.globalRequestMap.size() +"AAA"+ sm.globalRequestToObjectMap.size() +""+ sm.globalCacheMap.size());;
				sm.distributedObject.clear();
				sm.replicateObjectCounter.clear();
				
				sm.acceptLogs=true;
				try {
					sm.currentIntervalStartTime=System.currentTimeMillis();
					long sleepFor=(long) (shortWindowTime*2.8);
					Thread.sleep(shortWindowTime);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}	
				sm.acceptLogs=false;
				
				Analyser.log.info("After="+sm.globalRequestMap.size() +"AAA"+ sm.globalRequestToObjectMap.size() +""+ sm.globalCacheMap.size());;
				
				sm.runAnalysers();
				
				
				sm.addInterval();
				sm.bestLatency=99999;
			}
			
			sm.readLocalCount=0;
			sm.readRemotecount=0;
			sm.updateRemoteCount=0;
			sm.totalExecTime=0;
			
			//sm.runAnalysers();
			//sm.addInterval();
			//double thpt=sm.reqCounter/(windowTime/1000);
			//Analyser.log.info(" sm.acceptLogs=false"+"sm.counter="+sm.counter+ "thpt="+thpt + "totalReq="+sm.reqCounter);
			Analyser.log.info(" sm.acceptLogs=true");
			sm.acceptLogs=true;
			
			sm.counter++;
		}
		
		
		
		
		//sm.acceptLogs=false;
		
		
		
		
		
	}		
	}
	
	public void runFixedInterval()
	{
		long windowTime = Long.parseLong(sm.props.getProperty(sm.ANALYSIS_INTERVAL));
		int iterator = Integer.parseInt(sm.props.getProperty(sm.ANALYSIS_ITERATOR));
		// TODO Auto-generated method stub
		//while(sm.counter<30)
		long procTime=0;
		long before=0;
		long after=0;
		while(sm.counter<iterator)
		{
		Analyser.log.info(" HttpRequestProcessor run analyser  "+ sm.counter + "Analysis Interval" +windowTime );
		
		long sleepFor=windowTime-procTime;
		//sm.acceptLogs=false;
		try {
			//Thread.sleep(900000000);
			//Thread.sleep(90000);
			
			sm.currentIntervalStartTime=System.currentTimeMillis();
			
			 
			 
				Analyser.log.info(" SLEEP FOR  "+ (sleepFor));
			Thread.sleep(sleepFor);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//System.out.print(" HttpRequestProcessor -- run analyser 111");
		
		sm.acceptLogs=false;
		long thpt=sm.reqCounter/sleepFor;
		Analyser.log.info(" sm.acceptLogs=false"+ sm.reqCounter + "thpt="+thpt);
		before=System.currentTimeMillis();
		sm.runAnalysers();
	//	if (sm.counter<1)
			sm.addInterval();
			after=System.currentTimeMillis();
			procTime=after-before;
		Analyser.log.info(" sm.acceptLogs=true");
		sm.acceptLogs=true;
		
		sm.counter++;
		
		
	}		
	}


	//@Override
	public void runFlexibleInterval()
	{
		long windowTime = Long.parseLong(sm.props.getProperty(sm.ANALYSIS_INTERVAL));
		int iterator = Integer.parseInt(sm.props.getProperty(sm.ANALYSIS_ITERATOR));
		// TODO Auto-generated method stub
		//while(sm.counter<30)
		long procTime=0;
		long before=0;
		long after=0;
		while(sm.counter<iterator)
		{
		Analyser.log.info(" HttpRequestProcessor run analyser  "+ sm.counter + "Analysis Interval" +windowTime );
		
		long sleepFor=windowTime-procTime;
		
		/*
		if (sm.counter==1 && sm.replication_strategy.equals("regression") ) // if we do regression add 5 seconds to the replicatoin interval
		{
			Analyser.log.info(" sm.counter==1 && sm.replication_strategy.equals(\"regression\") add 5 seconds... " );
			sleepFor+=5000;
		}
		*/
		//sm.acceptLogs=false;
		try {
			//Thread.sleep(900000000);
			//Thread.sleep(90000);
			
			sm.currentIntervalStartTime=System.currentTimeMillis();
			
			 
			 
				Analyser.log.info(" SLEEP FOR  "+ (sleepFor));
				if (sleepFor<10000)
					sleepFor=60000;
				//ori 
				//sleepFor=10000;
			Thread.sleep(sleepFor);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//System.out.print(" HttpRequestProcessor -- run analyser 111");
		
		sm.acceptLogs=false;
		double thpt=sm.reqCounter/(sleepFor/1000);
		Analyser.log.info(" sm.acceptLogs=false"+"sm.counter="+sm.counter+ "thpt="+thpt + "totalReq="+sm.reqCounter);
		before=System.currentTimeMillis();
		/*
		if (thpt>=sm.maxThpt && sm.counter>0)
		{
			sm.runAnalysers();
			sm.maxThpt=0;
			sm.addInterval();
		}
		*/
		
		if (thpt>=sm.maxThpt && sm.counter>0)
		{
			sm.maxThpt=thpt;
			sm.addInterval();
			
		}
		
		else 
		{
			
			if (sm.counter==0)
			{
				/*
					try {
						Thread.sleep(60000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					*/
					//ori
					//sm.runAnalysers();
					//sm.maxThpt=0;
					sm.runAnalysers();
					sm.maxThpt=0;
					
					/*
					sm.addInterval();
					sm.maxThpt=100000;
					*/
					
				
				
			}
			
			else 
			{
				double toCheck=(sm.maxThpt-thpt)/(sm.maxThpt);
				Analyser.log.info("toCheck="+toCheck);
				
				sm.runAnalysers();
				sm.maxThpt=0;
				
				/* ORI
				if (toCheck>=0.10)
				{
					sm.runAnalysers();
					sm.maxThpt=0;
				}*/
				
			}
	//	if (sm.counter<1)
			
			
			sm.addInterval();
			
		}
			after=System.currentTimeMillis();
			procTime=after-before;
		sm.reqCounter=0;
		Analyser.log.info(" sm.acceptLogs=true");
		sm.acceptLogs=true;
		
		sm.counter++;
		
		
	}		
	}

	
}
