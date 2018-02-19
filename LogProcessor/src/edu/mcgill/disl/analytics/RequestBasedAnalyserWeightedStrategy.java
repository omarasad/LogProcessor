package edu.mcgill.disl.analytics;

import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.log.StatisticsManager;

public abstract class RequestBasedAnalyserWeightedStrategy {
	
	public RequestBasedAnalyserWeighted analyser;
	public LoadBalancerPolicy oldLB; 
	// for convenience
	public StatisticsManager manager;
	
	public RequestBasedAnalyserWeightedStrategy(RequestBasedAnalyserWeighted analyser){
		this.analyser = analyser;
		this.manager = this.analyser.manager;
	}
	
	public abstract boolean generatePolicies()throws Exception;

}
