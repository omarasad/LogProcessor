package edu.mcgill.disl.analytics;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

public class Tester {
	
	public static void main(String args[])
	{
		Graph<String, DefaultEdge> graph1 =
			    new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
		
	     Graph<String, DefaultEdge> graph2 =
			      new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
	     
	     graph1.addVertex("a1");
	     graph1.addVertex("b1");
	     graph1.addVertex("c1");
	     
	     DefaultEdge de=graph1.addEdge("a1", "b1");
	     System.out.println(""+graph1.containsEdge(de));
	     
	}

}
