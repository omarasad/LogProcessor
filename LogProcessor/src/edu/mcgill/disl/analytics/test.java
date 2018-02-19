package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jgrapht.*;
import org.jgrapht.graph.*;
//import org.jgrapht.alg.KruskalMinimumSpanningTree;

class student
{
	String name="";
	public student(String n)
	{
		this.name=n;
	}
}

public class test {
	
	public static  void printPermutations(int[] n, int[] Nr, int idx) {
	    if (idx == n.length) { 
	    	//permPartServ.add(Arrays.toString(n));
	    	//stop condition for the recursion [base clause]
	    	Analyser.log.info("first "+Arrays.toString(n));
	        return;
	    }
	    for (int i = 0; i <= Nr[idx]; i++) { 
	        n[idx] = Nr[idx];
	        printPermutations(n, Nr, idx+1); //recursive invokation, for next elements
	    }
	}
	
	

	
	public void analyzeUrl(String url, int [] serv)
	{
		String r=null;
		int cat=0;
		int start=0;
		int end =0;
		int size=4000;
		int partSize=size/5;
		
		start=url.indexOf("category=");
		end=url.indexOf("&categoryName=");
		
		r=(String) url.subSequence(start+9, end);
		cat=Integer.parseInt(r);
		
	
		if (cat >0 && cat <= partSize)
			serv[0]++;
		else if (cat >partSize && cat < 2*partSize)
			serv[1]++;
		else if (cat >2*partSize && cat < 3*partSize)
			serv[2]++;
		else if (cat >3*partSize && cat < 4*partSize)
			serv[3]++;
		else if (4*cat >partSize && cat < 5*partSize)
			serv[4]++;

		//r=s.split(s.substring(4).toString());
		System.out.println(r);
		
	}
	public void printServReq(int [] serverReq)
	{
		System.out.println("Server"+ serverReq.toString() );
		for (int i=0;i<serverReq.length; i++)
		{
			System.out.println(serverReq[i]);
		}
	}
	
	public static void main(String []args)
	{
		int [] server1Req= new int [5];
		int [] server2Req= new int [5];
		int [] server3Req= new int [5];
		Arrays.fill(server1Req, 0);
		Arrays.fill(server2Req, 0);
		Arrays.fill(server3Req, 0);
		test t = new test();
		
		
		String s="/rubis/servlet/SearchItemsByCategory?TransactionType=browseItemsInCategory&category=4999&categoryName=ccc&page=0&nbOfItems=20";
		t.analyzeUrl(s, server1Req);
		String s2="/rubis/servlet/SearchItemsByCategory?TransactionType=browseItemsInCategory&category=49&categoryName=ccc&page=0&nbOfItems=20";
		t.analyzeUrl(s2, server2Req);
		t.printServReq(server1Req); 
		t.printServReq(server2Req); 
		
		 List<String> s1 = Arrays.asList("a","b","c");
		 List<String> s3 = Arrays.asList("c","d","e");
		    List<Integer> nums2 = Arrays.asList(4,5,6,1);
		    Set<String> sss=new HashSet<String>();
		    
		    List<Integer> allNums = new ArrayList<Integer>();
		    sss.addAll(s1);
		    allNums.addAll(nums2);
		    System.out.println(sss);
		    sss.addAll(s3);
		    allNums.remove(1);
		    
		    System.out.println(allNums);
		    System.out.println(sss);
		     
		    SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> g = new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);
		    g.addVertex("aaa");
		    SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> gC = new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>(DefaultWeightedEdge.class);

		    g.addVertex("bbb");
		    g.addVertex("ccc");
		    DefaultWeightedEdge e1 = g.addEdge("aaa", "bbb");
		    g.setEdgeWeight(e1,99);
		    DefaultWeightedEdge e2 = g.addEdge("bbb", "ccc");
		    g.setEdgeWeight(e2,111);
		   
		   
		    System.out.println(g);
		    
		    
		    System.out.println(g.vertexSet().size());
		    System.out.println( g.getEdge("aaa","bbb"));
		    
		   
		    System.out.println(g.getEdgeWeight(e2));
		  
		    
		    gC=(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>) g.clone();
		    g.removeVertex("aaa");
		    DefaultWeightedEdge e111 = g.getEdge("aaa", "bbb");

		    System.out.println(g.getEdgeWeight(e111));
		    System.out.println(gC);
		    System.out.println(g);
		   
		    
		 
		    
		    
		    //UndirectedGraph<String, DefaultEdge> stringGraph = createStringGraph();

	        // note undirected edges are printed as: {<v1>,<v2>}
	      //  System.out.println(stringGraph.toString());

		    int[] ser=new int [3] ;
			int[] NS=new int [3] ;
			NS[0]=0;
			NS[1]=1;
			NS[2]=2;
			
		
			printPermutations(ser, NS, 0);;
			
			
			WeightedGraph<String, String> requestsGraph = new WeightedGraph<String, String>() {
				
				@Override
				public Set<String> vertexSet() {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public boolean removeVertex(String arg0) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public String removeEdge(String arg0, String arg1) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public boolean removeEdge(String arg0) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public boolean removeAllVertices(Collection<? extends String> arg0) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public Set<String> removeAllEdges(String arg0, String arg1) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public boolean removeAllEdges(Collection<? extends String> arg0) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public double getEdgeWeight(String arg0) {
					// TODO Auto-generated method stub
					return 0;
				}
				
				@Override
				public String getEdgeTarget(String arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public String getEdgeSource(String arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public EdgeFactory<String, String> getEdgeFactory() {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public String getEdge(String arg0, String arg1) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public Set<String> getAllEdges(String arg0, String arg1) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public Set<String> edgesOf(String arg0) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public Set<String> edgeSet() {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public boolean containsVertex(String arg0) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public boolean containsEdge(String arg0, String arg1) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public boolean containsEdge(String arg0) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public boolean addVertex(String arg0) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public boolean addEdge(String arg0, String arg1, String arg2) {
					// TODO Auto-generated method stub
					return false;
				}
				
				@Override
				public String addEdge(String arg0, String arg1) {
					// TODO Auto-generated method stub
					return null;
				}
				
				@Override
				public void setEdgeWeight(String arg0, double arg1) {
					// TODO Auto-generated method stub
					
				}
			};
			
			System.out.println("ggg");
					HttpRequestObject r1 = null;
					HttpRequestObject r2 = null;
					student sa= new student("a");
					student sb= new student("b");
					
					
					requestsGraph.addVertex(sa.name);
					requestsGraph.addVertex(sb.name);
					String Edge=requestsGraph.addEdge(sa.name, sb.name);
					System.out.println("edge="+Edge);
					requestsGraph.setEdgeWeight(Edge, 3);
					requestsGraph.setEdgeWeight(Edge, 3);
					System.out.println("ggg");
					String e=requestsGraph.getEdge(sa.name, sb.name);
					System.out.println("edge="+e);
				double d=	requestsGraph.getEdgeWeight(e);
				System.out.println("ggg"+d);
					
				
				UndirectedGraph<String, DefaultEdge> graph =
					      new SimpleGraph<String, DefaultEdge>(DefaultEdge.class);
				
				
				String v1 = "v1";
			    String v2 = "v2";
			    String v3 = "v3";
			    String v4 = "v4";

			    graph.addVertex(v1);
			    graph.addVertex(v2);
			    graph.addVertex(v3);
			    graph.addVertex(v4);

			    graph.addEdge(v1, v2);
			    graph.addEdge(v2, v3);
			    graph.addEdge(v3, v4);
			    graph.addEdge(v4, v1);
			    
			  //  graph.addEdge(v2, v1);
			    Set<DefaultEdge> ProcessedEdges = new HashSet<DefaultEdge>();
			    

			    if( graph.getEdge(v1, v2) != null) { System.out.println("SUCCESS"); }
			    if (graph.getEdge(v2,v4) == null)  { System.out.println("SUCCESS"); }
			    
			    Set<DefaultEdge> edges=graph.edgesOf(v1);
			    
			    for (DefaultEdge edge:edges)
				 {
			    	System.out.println("EDGE" + edge);
			    	if (!ProcessedEdges.contains(edge))
					 {
						 
						 String SubV=graph.getEdgeTarget(edge);
						 ProcessedEdges.add(edge);						  
						 
						
								 
					 }
			    	else
			    	{
			    		System.out.println("proccessed before");
			    	}
				 }
			    
			    
Set<DefaultEdge> edges2=graph.edgesOf(v4);
			    
			    for (DefaultEdge edge:edges2)
				 {
			    	System.out.println("EDGE" + edge);
			    	if (!ProcessedEdges.contains(edge))
					 {
						 
						 String SubV=graph.getEdgeTarget(edge);
						 ProcessedEdges.add(edge);						  
						 
						
								 
					 }
			    	else
			    	{
			    		System.out.println("proccessed before");
			    	}
				 }
			    
			
			    
			 //   System.out.println(graph.getEdge(v1, v2));
			    

		    
		
		
		
	}

}
;