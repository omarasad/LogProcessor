package edu.mcgill.disl.analytics;

import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.log.StatisticsManager;

public abstract class RequestBasedAnalyserStrategy {
	
	public RequestBasedAnalyser analyser;
	public LoadBalancerPolicy oldLB; 
	// for convenience
	public StatisticsManager manager;
	
	public RequestBasedAnalyserStrategy(RequestBasedAnalyser analyser){
		this.analyser = analyser;
		this.manager = this.analyser.manager;
	}
	
	public abstract boolean generatePolicies()throws Exception;

}
