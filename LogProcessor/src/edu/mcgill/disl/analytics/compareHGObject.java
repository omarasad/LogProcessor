package edu.mcgill.disl.analytics;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;

import edu.mcgill.disl.log.StatisticsManager;

public class compareHGObject {
	
	public long totEdgeWgtOld=0;
	public long totEdgeWgtNew=0;
	public long totVrtxWgtOld=0;
	public long totVrtxWgtNew=0;
	public StatisticsManager manager;
	
	
	public compareHGObject(long totEdgeWgtOld2, long totEdgeWgt, long totVtxWgtOld, long totVtxWgt, StatisticsManager m)
	{
		Analyser.log.info(totEdgeWgtOld2 +"-"+totEdgeWgt +"-"+totVtxWgtOld+"-"+totVtxWgt);
		

		this.totEdgeWgtOld=totEdgeWgtOld2;
		this.totEdgeWgtNew=totEdgeWgt;
		this.totVrtxWgtOld=totVtxWgtOld;
		this.totVrtxWgtNew=totVtxWgt;
   	    this.manager=m;
	}
	
	
	 public void printValidateNewEdgesWeight()
	    {
	    	Analyser.log.info("================  STARTING Hper-GRAPH COMPARISON ================  ");
	    	 Analyser.log.info("Two graphs have " + Math.round(calulateVeterxDiff()*1000.0)/1000.0  + " of vertex diff");
			  Analyser.log.info("Two graphs have " +Math.round(calulateHyperEdgeDiff()*1000.0)/1000.0  + " of edge diff");
			 // Analyser.log.info("edgeWithBothVrtxAreNewCounter " + edgeWithBothVrtxAreNewCounter + "edgeWithBothVrtxAreOldCounter " + edgeWithBothVrtxAreOldCounter+ " edgeWithOneVrtxIsNewCounter" + edgeWithOneVrtxIsNewCounter);
			  //Analyser.log.info("edgeWithBothVrtxAreNew " + getRepectiveEdgeWeight(edgeWithBothVrtxAreNew, counterjustInNew) +"edgeWithBothVrtxAreOld " +  getRepectiveEdgeWeight(edgeWithBothVrtxAreOld, counterjustInNew) + "edgeWithOneVrtxIsNew" + getRepectiveEdgeWeight(edgeWithOneVrtxIsNew, counterjustInNew));
			  //Analyser.log.info("% common vrtx " + IncidentOldVertexTotWeight  + "% new vrtx" + IncidentNewVertexTotWeight );
			  Analyser.log.info("================  END Hyper-GRAPH COMPARISON ================  ");
	    }
	
	public double calculateCommonHyperEdgeDiff(String req)
	{
		//he1>>new he2>>old
		double counterCommon=0.0;
		double counterjustInNew=0.0;
		double counterjustInOld=0.0;
		
		double totalCommon=0;
		double totaljustInNew=0;
		double totaljustInOld=0;
		
		double totW1=0.0;
		double totW2=0.0;
		double wDiff=0.0;
		double total=0.0;
		
		
		
	
		if (manager.globalRequestMap.get(req).objectFreq!=null)	
		{
			for (Integer freq:manager.globalRequestMap.get(req).objectFreq.values())
			{
				totW1+=(double)freq;
			}
		}
		else
		{
			Analyser.log.info("new ??" +req+ " "+manager.globalRequestToObjectMap.get(req).size());
		}
		
		if (manager.globalRequestMapOld.get(req).objectFreq!=null)	
		{
			for (Integer freq:manager.globalRequestMapOld.get(req).objectFreq.values())
			{
				totW2+=(double)freq;
			}
		}
		else
		{
			Analyser.log.info("old ??" +req+ " "+manager.globalRequestToObjectMap.get(req).size());
		}
		
		
		
		
		for (String Obj:manager.globalRequestMap.get(req).objectFreq.keySet()) 
		{
			double tempW1= (double)manager.globalRequestMap.get(req).objectFreq.get(Obj)/totW1;
			tempW1=((double)manager.globalRequestMap.get(req).counter/(double)totEdgeWgtNew)*tempW1;
			if (manager.globalRequestMapOld.get(req).objectFreq.containsKey(Obj)) // AN OBJECT IS COMMON 
			{
				
				double tempW2= (double)manager.globalRequestMapOld.get(req).objectFreq.get(Obj)/totW2;
				tempW2=((double)manager.globalRequestMapOld.get(req).counter/(double)totEdgeWgtOld)*tempW2;
				wDiff=tempW1-tempW2;
				if (wDiff<0)
					wDiff=wDiff*-1;
				total+=wDiff;
				
				counterCommon++;
				totalCommon+=wDiff;
			}
			else // AN OBJECT IS ONLY IN THE NEW
			{
				wDiff=tempW1;
				total+=wDiff;
				
				counterjustInNew++;
				totaljustInNew+=wDiff;
			}
		}
		for (String Obj:manager.globalRequestMapOld.get(req).objectFreq.keySet())
		{
			double tempW2= (double)manager.globalRequestMapOld.get(req).objectFreq.get(Obj)/totW2;
			tempW2=((double)manager.globalRequestMapOld.get(req).counter/(double)totEdgeWgtOld)*tempW2;
			if (!manager.globalRequestMap.get(req).objectFreq.containsKey(Obj))
			{
				wDiff=tempW2;
				total+=wDiff;
				counterjustInOld++;
				totaljustInOld+=wDiff;
			}
		}
		
		
		double resultCommon=0.0;
		double resultInNew=0.0;
		double resultInOld=0.0;
		double totalCounters=counterCommon+counterjustInNew+counterjustInOld;
		
		
	//	Analyser.log.info("#HEC##"+counterCommon +"#HEN##"+counterjustInNew+"#HEO##"+counterjustInOld);
	//	Analyser.log.info("###HE"+totalCommon +"###HE"+totaljustInNew+"###HE"+totaljustInOld);
		
		if (!(counterCommon==0))
			resultCommon=totalCommon*counterCommon/totalCounters;
		if (!(counterjustInNew==0))
			resultInNew=totaljustInNew*counterjustInNew/totalCounters;
		if (!(counterjustInOld==0))
			resultInOld=totaljustInOld*counterjustInOld/totalCounters;
		
	//	Analyser.log.info("HE#C##"+resultCommon +"#HEN##"+resultInNew+"#HEO##"+resultInOld);
		
	double resAll=resultCommon + resultInNew + resultInOld;
		
	/*
		if (resultInNew>0)
			Analyser.log.info("resultInNew>0 ??"+req);
		if (resultInOld>0)
			Analyser.log.info("resultInOld>0 ??"+req);
			
			*/
		return resAll;
	}
	
	public double calulateHyperEdgeDiff()
	{
	
		int counterCommon=0;
		int counterjustInNew=0;
		int counterjustInOld=0;
	
		double totalCommon=0.0;
		double totaljustInNew=0.0;
		double totaljustInOld=0.0;
		Analyser.log.info("gOlde"+manager.globalRequestToObjectMapOld.size());
		Analyser.log.info("gNewe"+manager.globalRequestToObjectMap.size());
		
		for (String req1:manager.globalRequestToObjectMap.keySet())
		{ 
			double wDiff= 0.0;
			double wReq1=((double)manager.globalRequestMap.get(req1).counter)/(double)totEdgeWgtNew;
			if (manager.globalRequestToObjectMapOld.containsKey(req1)) // TAKE COMMON ELEMENTS
			{
				counterCommon++;
				
				
			
				 wDiff=calculateCommonHyperEdgeDiff(req1);
				//double wNew=((double)manager.globalCacheMap.get(v1).getCount)/totVrtxWgtNew;	
				//wDiff= wNew-wOld;
			//	if (wDiff<0)
				//	wDiff=wDiff*-1;
				
				totalCommon+=wDiff;
				//Analyser.log.info("#wDiff totalCommon##"+wDiff);
			}
			else // TAKE ELEMENTS IN THE NEW BUT NOT IN THE OLD
			{
				counterjustInNew++;
				wDiff=wReq1;
				totaljustInNew+=wDiff;
				//Analyser.log.info("#wDiff totaljustInNew##"+wDiff);
			}
		
		}
		
		for (String req1:manager.globalRequestToObjectMapOld.keySet()) // TAKE ELEMENTS IN THE OLD BUT NOT IN THE NEW
		{ 
			double wDiff= 0.0;
			double wReq1=((double)manager.globalRequestMapOld.get(req1).counter)/(double)totEdgeWgtOld;
			if (!manager.globalRequestMap.containsKey(req1)) 
			{
				counterjustInOld++;
				wDiff= wReq1;				
				totaljustInOld+=wDiff;
				//Analyser.log.info("#wDiff totaljustInOld##"+wDiff);
				
			}
		}
		
		
		
		
		double resultCommon=0.0;
		double resultInNew=0.0;
		double resultInOld=0.0;
		double totalCounters=counterCommon+counterjustInNew+counterjustInOld;
		
		Analyser.log.info("#EC##"+counterCommon +"#EN##"+counterjustInNew+"#EO##"+counterjustInOld);
		Analyser.log.info("###E"+totalCommon +"###"+totaljustInNew+"###"+totaljustInOld);
		
		if (!(counterCommon==0))
			resultCommon=totalCommon*counterCommon/totalCounters;
		if (!(counterjustInNew==0))
			resultInNew=totaljustInNew*counterjustInNew/totalCounters;
		if (!(counterjustInOld==0))
			resultInOld=totaljustInOld*counterjustInOld/totalCounters;
		
		Analyser.log.info("E###"+resultCommon +"E###"+resultInNew+"E###"+resultInOld);
		
		double resAll=resultCommon + resultInNew + resultInOld;
		
		
		return resAll;
		
		
	}
	
	public double calulateVeterxDiff()
	{
		int counterCommon=0;
		int counterjustInNew=0;
		int counterjustInOld=0;
		
		double totalCommon=0;
		double totaljustInNew=0;
		double totaljustInOld=0;
		Analyser.log.info("gOldv"+manager.globalCacheMapOld.size());
		Analyser.log.info("gNewv"+manager.globalCacheMap.size());
		
		double totalDiff=0.0;
		
		
			for (String v1:manager.globalCacheMap.keySet())
			{
				double wDiff= 0.0;
				double wNew=((double)manager.globalCacheMap.get(v1).getCount)/totVrtxWgtNew;
				//Analyser.log.info("wNew="+wNew+"weight="+manager.globalRequestMap.get(v1).weight*1000+"==="+v1);
				
				if (manager.globalCacheMapOld.containsKey(v1)) // TAKE COMMON ELEMENTS
				{
					counterCommon++;
					double wOld=((double)manager.globalCacheMapOld.get(v1).getCount)/totVrtxWgtOld;	
					wDiff= wNew-wOld;
					if (wDiff<0)
						wDiff=wDiff*-1;
					
					totalCommon+=wDiff;
				}
				else // TAKE ELEMENTS IN THE NEW BUT NOT IN THE OLD
				{
					counterjustInNew++;
					wDiff=wNew;
					totaljustInNew+=wDiff;
				}
				totalDiff+=wDiff;
				
			//	Analyser.log.info("totalDiffAXA="+totalDiff);
			}
			
	//		Analyser.log.info("totalDiffC="+totalDiff); 
			for (String v1:manager.globalCacheMapOld.keySet()) // TAKE ELEMENTS IN THE OLD BUT NOT IN THE NEW
			{
				double wDiff= 0.0;
				double wOld=((double)manager.globalCacheMapOld.get(v1).getCount)/totVrtxWgtOld;
				if (!manager.globalCacheMap.containsKey(v1))
				{
					counterjustInOld++;
					wDiff=wOld;
					totaljustInOld+=wDiff;
				}
				totalDiff+=wDiff;
			}
			//Analyser.log.info("totalDiffD="+totalDiff); 
		
				
		Analyser.log.info("#VC##"+counterCommon +"#VN##"+counterjustInNew+"#VO##"+counterjustInOld);
		Analyser.log.info("V###"+totalCommon +"###"+totaljustInNew+"###"+totaljustInOld);
		
		double resultCommon=0.0;
		double resultInNew=0.0;
		double resultInOld=0.0;
		double totalCounters=counterCommon+counterjustInNew+counterjustInOld;
		
		if (!(counterCommon==0))
			resultCommon=totalCommon*counterCommon/totalCounters;
		if (!(counterjustInNew==0))
			resultInNew=totaljustInNew*counterjustInNew/totalCounters;
		if (!(counterjustInOld==0))
			resultInOld=totaljustInOld*counterjustInOld/totalCounters;
		
		Analyser.log.info("V###"+resultCommon +"###"+resultInNew+"###"+resultInOld);
		
	double resAll=resultCommon + resultInNew + resultInOld;
		
		
		//totVrtxCount=counterCommon+counterjustInNew+counterjustInOld;
		//return totalDiff/totVrtxCount;
		return resAll;
		
	}
	

}
