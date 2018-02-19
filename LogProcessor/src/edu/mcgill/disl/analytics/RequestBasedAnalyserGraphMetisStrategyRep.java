package edu.mcgill.disl.analytics;

import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;
import edu.mcgill.disl.log.StatisticsManager;

public abstract class RequestBasedAnalyserGraphMetisStrategyRep {
	
	public RequestBasedAnalyserGraphMetisRep analyser;
	public LoadBalancerPolicy oldLB; 
	// for convenience
	public StatisticsManager manager;
	
	//public RequestBasedAnalyserGraph currentAnalysisPhase;
	
	public RequestBasedAnalyserGraphMetisStrategyRep(RequestBasedAnalyserGraphMetisRep analyser){
		this.analyser = analyser;
		this.manager = this.analyser.manager;
		
	//	this.currentAnalysisPhase=this.analyser.currentAnalysisPhase;
		//this.analyser=this.analyser.currentAnalysisPhase;
		
	}
	
	public abstract boolean generatePolicies()throws Exception;

}
