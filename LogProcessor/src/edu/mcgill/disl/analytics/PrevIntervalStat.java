package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.mcgill.disl.analytics.policy.ListKey;
import edu.mcgill.disl.analytics.policy.ObjectKey;

public class PrevIntervalStat {
	
	public ArrayList<RequestBasedAnalyserGraph> prevData;
	
	
	public PrevIntervalStat()
	{
		prevData= new ArrayList<RequestBasedAnalyserGraph>();
	}
	
//	public void assignBitSet(RequestBasedAnalyserGraph currentAnalysisPhase)
//	{
//		Analyser.log.info("bit Set");
//		for(ASServer serv : currentAnalysisPhase.servIndex.values())
//		{
//			serv.curObjBits.clear();
//			Analyser.log.info("serv=" +serv.serverId);
//			for(ObjectKey key : serv.tmpPolicy.policyMap.keySet())
//			{
//				ListKey lk = (ListKey)key;
//				serv.curObjBits.set(currentAnalysisPhase.globalCacheMap.get(lk.key).index);
//			}
//			Analyser.log.info("prevStat serv card.=" +serv.curObjBits.cardinality());
//			
//		}
//		
//	}
	
	

}
