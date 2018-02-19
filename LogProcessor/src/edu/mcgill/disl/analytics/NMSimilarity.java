package edu.mcgill.disl.analytics;

import java.lang.reflect.Array;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;


import org.jgrapht.Graph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.NeighborIndex;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleGraph;



/**
 * Created by Sashika on 8/26/2014.
 */
public class NMSimilarity {
	
	
	
	
	public Graph<String, DefaultEdge> graphA =
		    new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
	
public Graph<String, DefaultEdge> graphB =
		      new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
	
	
	public NeighborIndex<String, DefaultWeightedEdge> NGA;
	public NeighborIndex<String, DefaultWeightedEdge> NGB;
	
	
   // private Graph graphA;
  //  private Graph graphB;
    private List<List<Integer>> nodeListA;
    private List<List<Integer>> nodeListB;

    private Double[][] nodeSimilarity;
    private Double epsilon;
    private int graphSizeA;
    private int graphSizeB;
    
    Map <Integer,String> graphIndexA= new HashMap<Integer, String>();
    Map <String,Integer> graphIndexStringA= new HashMap<String, Integer>();
    
    
    
    Map <Integer,String> graphIndexB= new HashMap<Integer, String>();
    Map <String,Integer> graphIndexStringB= new HashMap<String, Integer>();

   
    
    public void makeSure()
    {
    	for (String req:graphIndexStringA.keySet())
    	{
    		if (!graphA.containsVertex(req))
    		{
    			Analyser.log.info("ERRRR GraphA" +req);
    		}
    	}
    	
    	for (String req:graphIndexStringB.keySet())
    	{
    		if (!graphB.containsVertex(req))
    		{
    			Analyser.log.info("ERRRR GraphB" +req);
    		}
    	}
    	
    	for (String req:graphIndexStringA.keySet())
    	{
    		NGA.neighborListOf(req);
    	}
    	
    	for (String req:graphIndexStringB.keySet())
    	{
    		NGB.neighborListOf(req);
    	}
    	
    	
    }
    
    public void createVertexIndexing(Graph  GraphA, Graph GraphB)
    {
    	Set<String> requests=GraphA.vertexSet();
    	Iterator<String> ir=requests.iterator();
    	int k=0;
    	while (ir.hasNext())
    	{
    		String req=ir.next();
    		graphIndexA.put(k, req);
    		graphIndexStringA.put(req, k);
    		k++;		
    	} 	    	
    	
    	
    	Set<String> requestsB=GraphB.vertexSet();
    	Iterator<String> irr=requestsB.iterator();
    	int kk=0;
    	while (irr.hasNext())
    	{
    		String req=irr.next();
    		graphIndexB.put(kk, req);
    		graphIndexStringB.put(req, kk);
    		kk++;		
    	} 	   
    	
    	 NGA = new NeighborIndex<String, DefaultWeightedEdge>(GraphA);
    	 NGB = new NeighborIndex<String, DefaultWeightedEdge>(GraphB);
    	 
    	 

    	 
    }
    
    
    
   

    
    
    public NMSimilarity(Graph GraphA, Graph GraphB, Double epsilon) {
        try {
        	
        	Set<String> requests=GraphA.vertexSet();
        	Iterator<String> ir=requests.iterator();
        	int k=0;
        	while (ir.hasNext())
        	{
        		String req=ir.next();
        		graphIndexA.put(k, req);
        		graphIndexStringA.put(req, k);
        		k++;		
        	} 	    	
        	
        	
        	Set<String> requestsB=GraphB.vertexSet();
        	Iterator<String> irr=requestsB.iterator();
        	int kk=0;
        	while (irr.hasNext())
        	{
        		String req=irr.next();
        		graphIndexB.put(kk, req);
        		graphIndexStringB.put(req, kk);
        		kk++;		
        	} 	   
        	
        	 NGA = new NeighborIndex<String, DefaultWeightedEdge>(GraphA);
        	 NGB = new NeighborIndex<String, DefaultWeightedEdge>(GraphB);
        	
        	
     		Analyser.log.info("GraphA.vertexSet().size() "+ GraphA.vertexSet().size());
            this.graphA = GraphA;
            this.graphB = GraphB;
            this.epsilon = epsilon;
            Analyser.log.info("graphA.vertexSet().size() "+ graphA.vertexSet().size());
            this.graphSizeA=GraphA.vertexSet().size();
            this.graphSizeB=GraphB.vertexSet().size();
            
            
            this.nodeSimilarity = new Double[graphSizeA][graphSizeB];
            
            Analyser.log.info("graphSizeA =" +this.graphSizeA);
            Analyser.log.info("graphSizeB =" +this.graphSizeB);
            
            makeSure();
            initializeSimilarityMatrices();
            
            

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void initializeSimilarityMatrices()
    {
        for (int i = 0; i < graphSizeA; i++)
        {
        	String reqI=graphIndexA.get(i);
        	//System.out.print("NGA-reqI="+reqI);
        //	System.out.print("NGA"+NGA.neighborListOf(reqI).size());
        	
            for (int j = 0; j < graphSizeB; j++) 
            {
            	String reqJ=graphIndexB.get(j);
            //	System.out.print("NGB-reqJ="+reqJ);
            	//System.out.print("NGB"+NGB.neighborListOf(reqJ).size());
            

            	
            		
                Double maxDegree = Double.valueOf(Math.max(NGA.neighborListOf(reqI).size(), NGB.neighborListOf(reqJ).size()));
                if (maxDegree != 0) 
                {
                    nodeSimilarity[i][j] = ((Math.min(NGA.neighborListOf(reqI).size(), NGB.neighborListOf(reqJ).size())) / (maxDegree));
                } 
                else 
                {
                    nodeSimilarity[i][j] = Double.valueOf(0);
                }
                
                
               
            }
        }

       
        for (int i = 0; i < graphSizeA; i++) {
            for (int j = 0; j < graphSizeB; j++) {
              //  System.out.print(nodeSimilarity[i][j]+" ");
            }
          //  System.out.println();
        }

    }

    public void measureSimilarity() {
        double maxDifference = 0.0;
        boolean terminate = false;

        while (!terminate) {
            maxDifference = 0.0;
            for (int i = 0; i < graphSizeA; i++)
            {
            	String reqI=graphIndexA.get(i);
                for (int j = 0; j < graphSizeB; j++) 
                {
                	String reqJ=graphIndexB.get(j);
                    //calculate in-degree similarities
                    double similaritySum = 0.0;
                    Double maxDegree = Double.valueOf(Math.max(NGA.neighborListOf(reqI).size(), NGB.neighborListOf(reqJ).size()));
                    int minDegree = Math.min(NGA.neighborListOf(reqI).size(), NGB.neighborListOf(reqJ).size());
                    if (minDegree == NGA.neighborListOf(reqI).size()) {
                        similaritySum = enumerationFunction(NGA.neighborListOf(reqI), NGB.neighborListOf(reqJ), 0);
                    } else {
                        similaritySum = enumerationFunction(NGB.neighborListOf(reqJ), NGA.neighborListOf(reqI), 1);
                    }
                    if (maxDegree == 0.0 && similaritySum == 0.0) {
                        nodeSimilarity[i][j] = 1.0;
                    } else if (maxDegree == 0.0) {
                        nodeSimilarity[i][j] = 0.0;
                    } else {
                        nodeSimilarity[i][j] = similaritySum / maxDegree;
                    }

                    //calculate out-degree similarities
                   

                }
            }

            for (int i = 0; i < graphSizeA; i++) {
                for (int j = 0; j < graphSizeB; j++) {
                    double temp = nodeSimilarity[i][j];
                    if (Math.abs(nodeSimilarity[i][j] - temp) > maxDifference) 
                    {
                        maxDifference = Math.abs(nodeSimilarity[i][j] - temp);
                    }
                    nodeSimilarity[i][j] = temp;
                }
            }

            if (maxDifference < epsilon) 
            {
                terminate = true;
            }
        }
        DecimalFormat f = new DecimalFormat("0.000");

        for (int i = 0; i < graphSizeA; i++) {
            for (int j = 0; j < graphSizeB; j++) {
                nodeSimilarity[i][j] = Double.valueOf(f.format(nodeSimilarity[i][j]));
              //  System.out.print(nodeSimilarity[i][j] + " ");
            }
           // System.out.println("");
        }
    }

    public double enumerationFunction(List<String> neighborListMin, List<String> neighborListMax, int graph) {
        double similaritySum = 0.0;
        Map<Integer, Double> valueMap = new HashMap<Integer, Double>();
        if (graph == 0) {
            for (int i = 0; i < neighborListMin.size(); i++) 
            {
                String nodeString = neighborListMin.get(i);
                int node=graphIndexStringA.get(nodeString);
                double max = 0.0;
                int maxIndex = -1;
                for (int j = 0; j < neighborListMax.size(); j++) 
                {
                    String keyString = neighborListMax.get(j);
                    
                    if (!graphB.containsVertex(keyString))
                    {
                    	Analyser.log.info("graphB null="+keyString);
                    	continue;
                    }
                    
                    if (!graphIndexStringB.containsKey(keyString))
                    	Analyser.log.info("graphIndexStringB null="+keyString);
                    
                    int key=graphIndexStringB.get(keyString);
                    if (!valueMap.containsKey(key)) 
                    {
                        if (max < nodeSimilarity[node][key])
                        {
                            max = nodeSimilarity[node][key];
                            maxIndex = key;
                        }
                    }
                }
                valueMap.put(maxIndex, max);
            }
        } else {
            for (int i = 0; i < neighborListMin.size(); i++) {
                String nodeString = neighborListMin.get(i);
                
                if (!graphB.containsVertex(nodeString))
                {
                	Analyser.log.info("graphBB null="+nodeString);
                	continue;
                }
                
                if (!graphIndexStringB.containsKey(nodeString))
                	Analyser.log.info("graphIndexStringBB null="+nodeString);
                
               
                
                int node=graphIndexStringB.get(nodeString);
                
                
                double max = 0.0;
                int maxIndex = -1;
                for (int j = 0; j < neighborListMax.size(); j++) 
                {
                	

                	 String keyString = neighborListMax.get(j);
                     int key=graphIndexStringA.get(keyString);
                   //  Analyser.log.info("key "+key);
                    // Analyser.log.info("node "+node);
                     
                    if (!valueMap.containsKey(key)) 
                    {
                        if (max < nodeSimilarity[key][node]) {
                            max = nodeSimilarity[key][node];
                            maxIndex = key;
                        }
                    }
                }
                valueMap.put(maxIndex, max);
            }
        }

        for (double value : valueMap.values()) {
            similaritySum += value;
        }
       // Analyser.log.info("similaritySum =" +similaritySum);
        return similaritySum;
    }

    public Double getGraphSimilarity() {
        Double finalGraphSimilarity = 0.0;
        DecimalFormat f = new DecimalFormat("0.000");
        measureSimilarity();
        List<String> vertexListA = new ArrayList <String>();
        List<String> vertexListB= new ArrayList <String>();
        
        vertexListA.addAll(graphA.vertexSet());
        vertexListB.addAll(graphB.vertexSet());
        
        

        if (graphA.vertexSet().size() < graphB.vertexSet().size()) 
        {
            finalGraphSimilarity = enumerationFunction(vertexListA, vertexListB, 0) / graphA.vertexSet().size();
        } 
        else 
        {
            finalGraphSimilarity = enumerationFunction(vertexListB, vertexListA, 1) / graphB.vertexSet().size();
        }
        finalGraphSimilarity = Double.valueOf(f.format(finalGraphSimilarity*100));
        return finalGraphSimilarity;
    }

}
