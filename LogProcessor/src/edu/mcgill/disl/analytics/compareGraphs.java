package edu.mcgill.disl.analytics;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.alg.NeighborIndex;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleGraph;

import edu.mcgill.disl.log.StatisticsManager;

public class compareGraphs {
	
	
	
	public long totEdgeWgtOld=0;
	public long totEdgeWgtNew=0;
	public long totVrtxWgtOld=0;
	public long totVrtxWgtNew=0;
	StatisticsManager manager;
	
	public int totEdgeCount=0;
	public int totVrtxCount=0;
	
	public double edgeWithBothVrtxAreNew=0.0;
	public double edgeWithOneVrtxIsNew=0.0;
	public double edgeWithBothVrtxAreOld=0.0;

	
	public int edgeWithBothVrtxAreNewCounter=0;
	public int edgeWithBothVrtxAreOldCounter=0;
	public int edgeWithOneVrtxIsNewCounter=0;
	
	public int counterCommon=0;
	public int counterjustInNew=0;
	public int counterjustInOld=0;
	
	
	public double totalCountersEdges=0.0;
	
	
	public double resultInNewEdge=0.0;
	public double resultCommonEdge=0.0;
	public double resultInOldEdge=0.0;
	
	public double IncidentOldVertexTotWeight=0.0;
	public double IncidentNewVertexTotWeight=0.0;
	
	public Graph<String, DefaultEdge> graphOld =
		    new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
	
    public Graph<String, DefaultEdge> graphNew =
		      new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
	
    
    public double getRepectiveEdgeWeight(double partWeight, int counterjustInNew)
    {
    	double res=partWeight * ((double)counterjustInNew/totalCountersEdges);
    	return (res *100.0);
    }
    
    public void printValidateNewEdgesWeight()
    {
    	Analyser.log.info("================  STARTING GRAPH COMPARISON ================  ");
   	 Analyser.log.info("Two graphs have " + Math.round(calulateVeterxDiff()*1000.0)/1000.0  + " of vertex diff");
		  Analyser.log.info("Two graphs have " +Math.round(calulateEdgeDiff()*1000.0)/1000.0  + " of edge diff");
		  Analyser.log.info("edgeWithBothVrtxAreNewCounter " + edgeWithBothVrtxAreNewCounter + "edgeWithBothVrtxAreOldCounter " + edgeWithBothVrtxAreOldCounter+ " edgeWithOneVrtxIsNewCounter" + edgeWithOneVrtxIsNewCounter);
		  Analyser.log.info("edgeWithBothVrtxAreNew " + getRepectiveEdgeWeight(edgeWithBothVrtxAreNew, counterjustInNew) +"edgeWithBothVrtxAreOld " +  getRepectiveEdgeWeight(edgeWithBothVrtxAreOld, counterjustInNew) + "edgeWithOneVrtxIsNew" + getRepectiveEdgeWeight(edgeWithOneVrtxIsNew, counterjustInNew));
		  Analyser.log.info("% common vrtx " + IncidentOldVertexTotWeight  + "% new vrtx" + IncidentNewVertexTotWeight );
		  Analyser.log.info("================  END GRAPH COMPARISON ================  ");
    }
    
    public void validateOldEdgesWeight(String src, String trgt, double edgeWgt)
    {

    	if (!manager.globalRequestMap.containsKey(src) && !manager.globalRequestMap.containsKey(trgt) ) // both are old (outdated)
    	{
			

    		edgeWithBothVrtxAreOld+=edgeWgt;
    		edgeWithBothVrtxAreOldCounter++;
    		
    		//edgeWithBothVrtxAreNew++;
    	}

    	
    	else if (manager.globalRequestMap.containsKey(src) && manager.globalRequestMap.containsKey(trgt) ) // both are new
    	{
			

    		edgeWithBothVrtxAreNew+=edgeWgt;
    		edgeWithBothVrtxAreNewCounter++;
    		
    		//edgeWithBothVrtxAreNew++;
    	}
    	
    	else if (manager.globalRequestMap.containsKey(src) || manager.globalRequestMap.containsKey(trgt) ) // one is old other is common
    	{
    		edgeWithOneVrtxIsNew+=edgeWgt;
    		edgeWithOneVrtxIsNewCounter++;
    		    		
    		if (manager.globalRequestMapOld.containsKey(src) && manager.globalRequestMap.containsKey(src)) // src is common /trgt is old
        	{
        		IncidentOldVertexTotWeight+=(double) manager.globalRequestMapOld.get(src).weight/totVrtxWgtOld;
        		IncidentNewVertexTotWeight+=(double) manager.globalRequestMapOld.get(trgt).weight/totVrtxWgtOld;

        	}
        	else if (manager.globalRequestMapOld.containsKey(trgt) && manager.globalRequestMap.containsKey(trgt)) // trgt is common/ src is old
        	{
        		IncidentOldVertexTotWeight+=(double) manager.globalRequestMapOld.get(trgt).weight/totVrtxWgtOld;
        		IncidentNewVertexTotWeight+=(double) manager.globalRequestMapOld.get(src).weight/totVrtxWgtOld;

        	}
    		
    		
    		
    	}
    }
	

    // wait of common vertices
    // wait of vertices in new only
    // wait of vertices in old only
    public void validateNewEdgesWeight(String src, String trgt, double edgeWgt)
    {

    	if (manager.globalRequestMapOld.containsKey(src) && manager.globalRequestMapOld.containsKey(trgt) ) // both are old
    	{
			

    		edgeWithBothVrtxAreOld+=edgeWgt;
    		edgeWithBothVrtxAreOldCounter++;
    		
    		//edgeWithBothVrtxAreNew++;
    	}

    	
    	else if (!manager.globalRequestMapOld.containsKey(src) && !manager.globalRequestMapOld.containsKey(trgt) ) // both are new
    	{
			

    		edgeWithBothVrtxAreNew+=edgeWgt;
    		edgeWithBothVrtxAreNewCounter++;
    		
    		//edgeWithBothVrtxAreNew++;
    	}
    	
    	else if (manager.globalRequestMapOld.containsKey(src) || manager.globalRequestMapOld.containsKey(trgt) ) // one is old
    	{
    		edgeWithOneVrtxIsNew+=edgeWgt;
    		edgeWithOneVrtxIsNewCounter++;
    		    		
    		if (manager.globalRequestMapOld.containsKey(src) && manager.globalRequestMap.containsKey(src))
        	{
        		IncidentOldVertexTotWeight+=(double) manager.globalRequestMap.get(src).weight/totVrtxWgtNew;
        		IncidentNewVertexTotWeight+=(double) manager.globalRequestMap.get(trgt).weight/totVrtxWgtNew;

        	}
        	else if (manager.globalRequestMapOld.containsKey(trgt) && manager.globalRequestMap.containsKey(trgt))
        	{
        		IncidentOldVertexTotWeight+=(double) manager.globalRequestMap.get(trgt).weight/totVrtxWgtNew;
        		IncidentNewVertexTotWeight+=(double) manager.globalRequestMap.get(src).weight/totVrtxWgtNew;

        	}
    		
    		
    		
    	}

    	
    	/*
    	if (manager.globalRequestMapOld.containsKey(src))
    	{
    		IncidentOldVertexTotWeight+=(double) manager.globalRequestMapOld.get(src).weight/totVrtxWgtOld;
    	}
    	else if (manager.globalRequestMap.containsKey(src))
    	{
    		IncidentNewVertexTotWeight+=(double) manager.globalRequestMap.get(src).weight/totVrtxWgtNew;
    	}
    	
    	if (manager.globalRequestMapOld.containsKey(trgt))
    	{
    		IncidentOldVertexTotWeight+=(double) manager.globalRequestMapOld.get(trgt).weight/totVrtxWgtOld;
    	}
    	else if (manager.globalRequestMap.containsKey(trgt))
    	{
    		IncidentNewVertexTotWeight+=(double) manager.globalRequestMap.get(trgt).weight/totVrtxWgtNew;
    	}
    	
    	*/
    }
	
	public compareGraphs(Graph GraphOld, Graph GraphNew, long totEdgeWgtOld2, long totEdgeWgt, long totVtxWgtOld, long totVtxWgt, StatisticsManager m)
	{
		Analyser.log.info(totEdgeWgtOld2 +"-"+totEdgeWgt +"-"+totVtxWgtOld+"-"+totVtxWgt);
		
		this .graphOld=GraphOld;
		this.graphNew=GraphNew;
		this.totEdgeWgtOld=totEdgeWgtOld2;
		this.totEdgeWgtNew=totEdgeWgt;
		this.totVrtxWgtOld=totVtxWgtOld;
		this.totVrtxWgtNew=totVtxWgt;
   	    this.manager=m;
	}
	
	public int getEdgeWeight(String r1, String r2, String whichGraph)
	{
		if (whichGraph.equals("old"))
		{
			return manager.globalRequestMapOld.get(r1).counter+manager.globalRequestMapOld.get(r2).counter;
			
		}
		else
		{
			return manager.globalRequestMap.get(r1).counter+manager.globalRequestMap.get(r2).counter;
			
		}
	}
	
	public double calulateEdgeDiff()
	{
		
		
		double totalCommon=0;
		double totaljustInNew=0;
		double totaljustInOld=0;
		
		Set<DefaultEdge>gOlde=graphOld.edgeSet();
		Set<DefaultEdge>gNewe=graphNew.edgeSet();
		
		Analyser.log.info("gOlde"+gOlde.size());
		Analyser.log.info("gNewe"+gNewe.size());
		
		double totalDiff=0.0;		
		if (gOlde.size()>=gNewe.size())
		{
			Iterator<DefaultEdge> ir=gOlde.iterator();
			while (ir.hasNext())
			{
				DefaultEdge e1=ir.next();
				double wDiff= 0.0;
				String sourceV=graphOld.getEdgeSource(e1);
				String targetV=graphOld.getEdgeTarget(e1);
				double wOld=(double)getEdgeWeight(sourceV, targetV, "old")/totEdgeWgtOld;
				if (graphNew.containsEdge(sourceV, targetV)) 
				{
					counterCommon++;
					double wNew=(double)getEdgeWeight(sourceV, targetV, "new")/totEdgeWgtNew;
					wDiff= wNew-wOld;
					if (wDiff<0)
						wDiff=wDiff*-1;
					
					totalCommon+=wDiff;
				}
				else
				{
					counterjustInOld++;
					wDiff=wOld;
					totaljustInOld+=wDiff;
				//	validateOldEdgesWeight(sourceV, targetV,wDiff);
					
				}
				totalDiff+=wDiff;
			}
			Iterator<DefaultEdge> irr=gNewe.iterator();
			while (irr.hasNext())
			{
				DefaultEdge e1=irr.next();
				String sourceV=graphNew.getEdgeSource(e1);
				String targetV=graphNew.getEdgeTarget(e1);
				
				
				double wDiff= 0.0;
				if (!graphOld.containsEdge(sourceV, targetV))
				{
					counterjustInNew++;
					
					double wNew=(double)getEdgeWeight(sourceV, targetV, "new")/totEdgeWgtNew;
					wDiff=wNew;
					totaljustInNew+=wDiff;
					validateNewEdgesWeight(sourceV, targetV, wDiff);
				}
				totalDiff+=wDiff;
			}
		}	
		
		else if (gOlde.size() < gNewe.size())
		{
			Iterator<DefaultEdge> ir=gNewe.iterator();
			while (ir.hasNext())
			{
				DefaultEdge e1=ir.next();
				double wDiff= 0.0;
				String sourceV=graphOld.getEdgeSource(e1);
				String targetV=graphOld.getEdgeTarget(e1);
				double wNew=(double)getEdgeWeight(sourceV, targetV, "new")/totEdgeWgtNew;
				if (graphOld.containsEdge(sourceV, targetV))
				{
					counterCommon++;
					double wOld=(double)getEdgeWeight(sourceV, targetV, "old")/totEdgeWgtOld;
					wDiff= wNew-wOld;
					if (wDiff<0)
						wDiff=wDiff*-1;
					
					totalCommon+=wDiff;
				}
				else
				{
					counterjustInNew++;
					wDiff=wNew;
					totaljustInNew+=wDiff;
					validateNewEdgesWeight(sourceV, targetV,wDiff);
				}
				totalDiff+=wDiff;
			}
			Iterator<DefaultEdge> irr=gOlde.iterator();
			while (irr.hasNext())
			{
				DefaultEdge e1=irr.next();
				String sourceV=graphOld.getEdgeSource(e1);
				String targetV=graphOld.getEdgeTarget(e1);
				double wDiff= 0.0;
				if (!graphNew.containsEdge(sourceV, targetV))
				{
					counterjustInOld++;
					
					double wOld=(double)getEdgeWeight(sourceV, targetV, "old")/totEdgeWgtOld;
					wDiff=wOld;
					totaljustInOld+=wDiff;
					//validateOldEdgesWeight(sourceV, targetV,wOld);
				}
				totalDiff+=wDiff;
			}
		}	
		
		double resultCommon=0.0;
		double resultInNew=0.0;
		double resultInOld=0.0;
		double totalCounters=counterCommon+counterjustInNew+counterjustInOld;
		
		double resultAll=(totalCommon +totaljustInNew+totaljustInOld)/2.0;
		
		this.totalCountersEdges=totalCounters;
		
		Analyser.log.info("###EC"+counterCommon +"###"+counterjustInNew+"###"+counterjustInOld);
		Analyser.log.info("###E"+totalCommon +"###"+totaljustInNew+"###"+totaljustInOld);
		
		if (!(counterCommon==0))
			resultCommon=totalCommon*counterCommon/totalCounters;
		if (!(counterjustInNew==0))
			resultInNew=totaljustInNew*counterjustInNew/totalCounters;
		if (!(counterjustInOld==0))
			resultInOld=totaljustInOld*counterjustInOld/totalCounters;		
	double resAll=resultCommon + resultInNew + resultInOld;
	
	Analyser.log.info("###Edges Weights Distribution (Common)"+resultCommon*100 +"###(New)"+resultInNew*100+"###(Old)"+resultInOld*100);
		
		this.resultInNewEdge= resultInNew*100;
		this.resultCommonEdge=resultCommon*100;
		this.resultInOldEdge=resultInOld*100;
		
		//totVrtxCount=counterCommon+counterjustInNew+counterjustInOld;
		
		//return totalDiff/totVrtxCount;
		//return resAll*100;
		return resultAll;
		
	}
	
	public double calulateVeterxDiff()
	{
		int counterCommon=0;
		int counterjustInNew=0;
		int counterjustInOld=0;
		
		double totalCommon=0;
		double totaljustInNew=0;
		double totaljustInOld=0;
		
		Set<String>gOldv=graphOld.vertexSet();
		Set<String>gNewv=graphNew.vertexSet();
		
		Analyser.log.info("gOldv"+gOldv.size());
		Analyser.log.info("gNewv"+gNewv.size());
		
		double totalDiff=0.0;
		
		
		
			Iterator<String> ir=gNewv.iterator();
			while (ir.hasNext())
			{
				String v1=ir.next();
				double wDiff= 0.0;
				double wNew=((double)manager.globalRequestMap.get(v1).weight)/totVrtxWgtNew;
				//Analyser.log.info("wNew="+wNew+"weight="+manager.globalRequestMap.get(v1).weight*1000+"==="+v1);
				
				if (gOldv.contains(v1))
				{
					counterCommon++;
					double wOld=((double)manager.globalRequestMapOld.get(v1).weight)/totVrtxWgtOld;	
					wDiff= wNew-wOld;
					if (wDiff<0)
						wDiff=wDiff*-1;
					
					totalCommon+=wDiff;
				}
				else
				{
					counterjustInNew++;
					wDiff=wNew;
					totaljustInNew+=wDiff;
				}
				
			//	if (wNew<0)
			//		Analyser.log.info("totaljustInNew<0="+v1);
				totalDiff+=wDiff;
				
			//	Analyser.log.info("totalDiffAXA="+totalDiff);
			}
			
	//		Analyser.log.info("totalDiffC="+totalDiff); 
			Iterator<String> irr=gOldv.iterator();
			while (irr.hasNext())
			{
				String v1=irr.next();
				double wDiff= 0.0;
				double wOld=((double)manager.globalRequestMapOld.get(v1).weight)/totVrtxWgtOld;
				if (!gNewv.contains(v1))
				{
					counterjustInOld++;
					wDiff=wOld;
					totaljustInOld+=wDiff;
				}
				totalDiff+=wDiff;
			//	if (wOld<0)
				//	Analyser.log.info("totaljustInOld<0="+v1);
			}
			//Analyser.log.info("totalDiffD="+totalDiff); 
		
				
		Analyser.log.info("###VC"+counterCommon +"###"+counterjustInNew+"###"+counterjustInOld);
		Analyser.log.info("###V"+totalCommon +"###"+totaljustInNew+"###"+totaljustInOld);
		double resultAll=(totalCommon +totaljustInNew+totaljustInOld)/2.0;

		
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
		
		//Analyser.log.info("###"+resultCommon +"###"+resultInNew+"###"+resultInOld);
		
	double resAll=resultCommon + resultInNew + resultInOld;
		
		
		//totVrtxCount=counterCommon+counterjustInNew+counterjustInOld;
		//return totalDiff/totVrtxCount;
		//return (resAll*100);
	
	return resultAll;
		
	}
	
	
        
	
}
