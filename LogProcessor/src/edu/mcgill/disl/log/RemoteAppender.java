package edu.mcgill.disl.log;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;


import edu.mcgill.disl.log.KTrace;

public class RemoteAppender extends AppenderSkeleton{
	
	StatisticsManager sm;
	KTrace kt=new KTrace();
	public RemoteAppender() {
		super();
		sm = StatisticsManager.getInstance();
		
	}

	@Override
	public synchronized void append(LoggingEvent le) {
		sm.processLog(le);
		//System.out.print("Starts Append");
		
		
	}

	@Override
	public synchronized void close() {
		
	}

	@Override
	public boolean requiresLayout() {
		return false;
	}
}
