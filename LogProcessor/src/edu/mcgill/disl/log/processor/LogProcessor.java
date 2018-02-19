package edu.mcgill.disl.log.processor;

import org.apache.log4j.spi.LoggingEvent;

import edu.mcgill.disl.analytics.ASServer;
import edu.mcgill.disl.log.StatisticsManager;

public abstract class LogProcessor {
	
	static final String ACCESS_LOG = "CSMonitor.Access";
	static final String CACHE_LOG = "CSMonitor";
	static final String INSERT_LOG = "CSMonitor.Insert";
	
	protected StatisticsManager manager;
	
	public LogProcessor(StatisticsManager manager){
		this.manager = manager;
	}
	
	public StatisticsManager getManager(){
		return manager;
	}
	
	public ASServer getLogServer(LoggingEvent le){
		String host = (String) le.getMDC("host");
	//	System.out.println("host = " + host);
		if(host!=null){
			return manager.getASServer(host);
		}
		return null;
	}
	
	public String getUID(LoggingEvent le){
		return (String) le.getMDC("uuid");
	}
	
	public String getLogType(LoggingEvent le){
		return le.getLoggerName();
	}
		
	public abstract void processLog(LoggingEvent le);

}
