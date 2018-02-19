package edu.mcgill.disl.analytics;


import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeMap;

import edu.mcgill.disl.log.StatisticsManager;

public class ReplicaPermutation {
	
	public double maxPermGlobal=-100000000.0;
	public String bestRepPermGlobal="";
	double updateCount=0.0;
	 Map<String, Double> map= new HashMap<String, Double>(); //partitionsRemote
	//ArrayList<String> ListremoteReplicaList = new ArrayList<String>();
	public StatisticsManager manager;
	public String defaultPartition;
	
	public ReplicaPermutation(double uc,HashMap <String, Double> partitionsRemote)
	{
		updateCount=uc;
		map=partitionsRemote;
		
		
		
		
		
	}
	
	public ReplicaPermutation(double uc,HashMap <String, Double> partitionsAll, StatisticsManager sm, String defaultReplica)
	{
		updateCount=uc;
		manager=sm;
		
		
		defaultPartition=defaultReplica;

		 map = sortByComparator(partitionsAll, false);
		
	}
			
	public String getBestRep()
	{
		return bestRepPermGlobal;
	}
	
	public double getmaxPermGlobal()
	{
		return maxPermGlobal;
	}
	/*
	public void generateReplicasOptions()
	{
		
		
		for  (int k=2;k<=ListremoteReplicaList.size();k++)
		{
			System.out.println("======== k="+k);
			generateOptionsNewNew(k);
		}
		
	}*/
	
	public void generateOptions(int noOfReplicas, ArrayList <String> remoteReplicas)
	{
		 
		for (int i=0;i<remoteReplicas.size();i++)
		{
			String perm=remoteReplicas.get(i);
			
			for (int j=i+1;j<remoteReplicas.size();j++)
			{
				int k=1;
				 perm=remoteReplicas.get(i);
				while (k<noOfReplicas)
				{
					perm=perm+"/"+remoteReplicas.get(j);
					k++;
				}
				
				System.out.println(perm);
			}
			
			
		}
	}
	
	public void generateOptionsNew(int noOfReplicas, ArrayList <String> remoteReplicas)
	{
		 
		for (int i=0;i<remoteReplicas.size();i++)
		{
			String perm=remoteReplicas.get(i);
			int k=1; int j=i+1;
			while (k<noOfReplicas && j<remoteReplicas.size())
			{
				perm=perm+"/"+remoteReplicas.get(j);
				k++;
				j++;
			}
			
			System.out.println(perm);
			
			
			
			
			
			
		}
	}
	
	public double getBestRep(String curRep, double maxRepFactor,HashMap<String, Double> partitionsRemote)
	{
		int tempCounter = 0;
		
		String []s=curRep.split("/");
		double sum=0;
		for (int j=0;j<s.length;j++)
		{
			sum+=partitionsRemote.get(s[j]);
			System.out.println(sum);
		}
		return sum;
		
		
		
	}
	
	/*
	public void generateOptionsNewNew(int noOfReplicas)
	{
		String best="";
		double maxRepFactor=0;
		//System.out.println("noOfReplicas="+noOfReplicas +"remoteReplicas.size()="+remoteReplicas.size());
		for (int i=0;i<ListremoteReplicaList.size();i++)
		{
			//String perm=remoteReplicas.get(i);
			int k=1; int j=i+1;
			//while (j<=remoteReplicas.size() && !(j>i+1 && k==1))
			//while (j<remoteReplicas.size() && (i+noOfReplicas<=remoteReplicas.size()))
			while (j<ListremoteReplicaList.size() && (i+noOfReplicas<=ListremoteReplicaList.size()) && (j+noOfReplicas-1<=ListremoteReplicaList.size()) )
			{
				String perm=ListremoteReplicaList.get(i);
				while (k<noOfReplicas && (j+k-1<ListremoteReplicaList.size()))
				{
					perm=perm+"/"+ListremoteReplicaList.get(j+k-1);
					k++;
					//System.out.println(perm);
					
				}
				j++;
				k=1;
				System.out.println(perm);
				double tempRepFactor=getBestRep(perm,20,map);
				if (tempRepFactor>=maxRepFactor)
				{
					maxRepFactor=tempRepFactor;
					best=perm;
				}
				
				
			}
			
			
			
			
			
			
			
			
			
			
		}
		double updateCost=manager.replicateObjectCost.get(noOfReplicas)*updateCount;
		System.out.println("best perm="+best +"maxRepFactor="+maxRepFactor);
		if (maxRepFactor-updateCost>maxPermGlobal)
		{
			maxPermGlobal=maxRepFactor-updateCost;
			bestRepPermGlobal=best;
			
			
		}
	}
	
	*/
	public boolean evaluateReplicaPossiblity(HashMap <String, Double>map, int curReplicaNo)
	{
		//System.out.println("totalGain="+map.size());
		boolean toReturn =false;
		int c=0;
		double totalGain=0.0;
		
		Iterator ir=map.entrySet().iterator();
		while (ir.hasNext() && c<curReplicaNo)
		{
			Entry <String,Double> cur=(Entry<String, Double>) ir.next();
			totalGain+=cur.getValue();
			c++;
			
		}
		
		
		double updateCost=manager.replicateObjectCost.get(curReplicaNo)*updateCount;
		double totGain=totalGain- updateCost;
		
		if (totGain > 0)
			toReturn=true;
		
		System.out.println("totalGain="+totalGain +"totGain="+totGain);
		return toReturn;
		
		
	}
	
	public double evaluateReplicaPossiblityNew(HashMap <String, Double>map, int curReplicaNo)
	{
		//System.out.println("totalGain="+map.size());
		double totGain =0;
		int c=0;
		double totalGain=0.0;
		
		Iterator ir=map.entrySet().iterator();
		while (ir.hasNext() && c<curReplicaNo)
		{
			Entry <String,Double> cur=(Entry<String, Double>) ir.next();
			totalGain+=cur.getValue();
			c++;
			
		}
		
		
		double updateCost=manager.replicateObjectCost.get(curReplicaNo)*updateCount;
		 totGain=totalGain- updateCost;
		
		//if (totGain > 0)
		//	toReturn=true;
		
		System.out.println("totalGain="+totalGain +"totGain="+totGain);
		return totGain;
		
		
	}
	
	public void getBestReplicaSet()
	{
		Analyser.log.info("map.size()=================="+map.size());
		HashMap<String, Double> sortedMap = new HashMap<String, Double>();
		for (Entry<String, Double> entry : map.entrySet())
		{
		    sortedMap.put((String)entry.getKey(),(Double) entry.getValue());
		}
		
		Iterator ir=sortedMap.entrySet().iterator();
		
		while (ir.hasNext())
		{
			Entry <String,Double> p=(Entry<String, Double>) ir.next();
			System.out.println("key="+p.getKey() +"value="+p.getValue());	
		}
		
		
		
		boolean toContinue=true;
		int counter=1;
		while (toContinue && counter<=sortedMap.size())
		{
			if (!evaluateReplicaPossiblity(sortedMap,counter))
			{
				toContinue=false;
			}
			counter++;
		}
		
		int temp=0;
		
		Iterator ir1=sortedMap.entrySet().iterator();
		
		while (ir1.hasNext() && temp<counter)
		{
			Entry <String,Double> cur=(Entry<String, Double>) ir1.next();
			bestRepPermGlobal=bestRepPermGlobal+"/"+cur.getKey();
			System.out.println("cur.getKey()="+cur.getKey());
			temp++;
			
		}
		
		
		System.out.println("bestRepPermGlobal="+bestRepPermGlobal);
	}
	
	public void getBestReplicaSetNewOriWRK()
	{
		double bestGain=-10000000.0;
		int bestMaxCounter=1;
		//Analyser.log.info("map.size()NEW=================="+map.size());
		HashMap<String, Double> sortedMap = new HashMap<String, Double>();
		for (Entry<String, Double> entry : map.entrySet())
		{
		    sortedMap.put((String)entry.getKey(),(Double) entry.getValue());
		}
		
		Iterator ir=sortedMap.entrySet().iterator();
		
		while (ir.hasNext())
		{
			Entry <String,Double> p=(Entry<String, Double>) ir.next();
			Analyser.log.info("key="+p.getKey() +"value="+p.getValue());	
		}
		
		
		
		
		int counter=1;
		while (counter<=sortedMap.size())
		{
			Double tempCurGain=evaluateReplicaPossiblityNew(sortedMap,counter);
			if (tempCurGain>=bestGain)
			{
				bestGain=tempCurGain;
				bestMaxCounter=counter;
				
			}
			counter++;
		}
		
		int temp=0;
		
		Iterator ir1=sortedMap.entrySet().iterator();
		
		
			
		
		while (ir1.hasNext() && temp<counter)
		{
			Entry <String,Double> cur=(Entry<String, Double>) ir1.next();
			bestRepPermGlobal=bestRepPermGlobal+"/"+cur.getKey();
			System.out.println("cur.getKey()="+cur.getKey());
			temp++;
			
		}
		
		if (bestGain<0)
			bestRepPermGlobal=defaultPartition;
		
		System.out.println("bestRepPermGlobalNEWNEW="+bestRepPermGlobal);
	}
	
	
	// this is the one in the paper
	public String getBestReplicaSetNew()
	{
		bestRepPermGlobal="";
		Set <String> replicationPolicy=new HashSet<String>();
		Set <String> replicationPolicyProposed= new HashSet<String>();
		//Analyser.log.info("map.size()NEW=================="+map.size());
		replicationPolicy.add(defaultPartition);
		
		for (Entry<String, Double> entry : map.entrySet())
		{
			//Analyser.log.info("keyNNN="+entry.getKey() +"valueNNN="+entry.getValue());	
			String curPartition=entry.getKey();

			replicationPolicyProposed.add(curPartition);
			
			if (replicationPolicyProposed.size()==1) 
				continue; // there is only one partition which is the default.. no need to compare
			
			
			if (getRepGain(replicationPolicy,replicationPolicyProposed)>0)
			{
				replicationPolicy.clear();
				replicationPolicy.addAll(replicationPolicyProposed);
			}
		}
		
		for (String partition:replicationPolicy)
		{
			if (bestRepPermGlobal=="")
				bestRepPermGlobal=bestRepPermGlobal+partition;
			else
				bestRepPermGlobal=bestRepPermGlobal+"/"+partition;
			
		}
		
		//Analyser.log.info("bestRepPermGlobal="+bestRepPermGlobal);	
		
		return bestRepPermGlobal;
		
	}
	
	// this is the one in the paper
	public double getRepGain(Set<String> policyCurrent, Set<String> policyProposed )
	{
		double repGain=0.0, remoteAccess=0.0;
		
		for (String partition: policyProposed)
		{
			if (!policyCurrent.contains(partition))
			{
				remoteAccess+=map.get(partition);
			}
		}
		double updateCostTotal=updateCount*(manager.replicateObjectCost.get(policyProposed.size())-manager.replicateObjectCost.get(policyCurrent.size()));
		
		repGain=remoteAccess*(manager.readRemoteCost-manager.readLocalCost) -updateCostTotal; 
		
		Analyser.log.info("policyCurrent="+policyCurrent.toString() +"policyProposed="+policyProposed.toString() +"repGain="+repGain);	
		return repGain;
	}
	
	
	
	private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap, final boolean order)
    {

        List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(unsortMap.entrySet());

        // Sorting the list based on values
        Collections.sort(list, new Comparator<Entry<String, Double>>()
        {
            public int compare(Entry<String, Double> o1,
                    Entry<String, Double> o2)
            {
                if (order)
                {
                    return o1.getValue().compareTo(o2.getValue());
                }
                else
                {
                    return o2.getValue().compareTo(o1.getValue());

                }
            }
        });

        // Maintaining insertion order with the help of LinkedList
        Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
        for (Entry<String, Double> entry : list)
        {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
	
	public static void main(String args[])
	{
		
		
		
		HashMap <String, Double> partitionsRemoteNew= new HashMap<String, Double>();
		
		partitionsRemoteNew.put("a", 15.0);
		partitionsRemoteNew.put("b", 13.0);
		partitionsRemoteNew.put("c", 14.0);
		partitionsRemoteNew.put("d", 6.0);
	//	partitionsRemoteNew.put("e", 9);
		//partitionsRemoteNew.put("f", 2);
		
		/*
		ReplicaPermutation rp= new ReplicaPermutation(2.0,partitionsRemoteNew);
		rp.generateReplicasOptions();
		System.out.println(rp.getBestRep() +""+rp.getmaxPermGlobal());
		*/
		
		ReplicaPermutation rp= new ReplicaPermutation(2.0,partitionsRemoteNew);
		rp.getBestReplicaSet();
		
		
	}
	
	

}
