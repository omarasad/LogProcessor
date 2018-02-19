package edu.mcgill.disl.log;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.mcgill.disl.analytics.Analyser;
 
public class tester{
 
   public static void main(String[] args) {
 
	//initial a Map
	   ArrayList<Long> accessTimes;
	   accessTimes=new ArrayList<Long>();
	   accessTimes.add((long) 29);
	   accessTimes.add((long) 34);
	   accessTimes.add((long) 35);
	   accessTimes.add((long) 33);
	   accessTimes.add((long) 37);
	   
	Map<String,Integer> map = new HashMap<String,Integer>();
	map.put("1", 4);
	map.put("2", 5);
	map.put("3", 6);
	map.put("4", 7);
	map.put("5", 6);
	map.put("6", 22);
	map.put("6", 23);
	map.put("6", 24);
 
	//Map -> Set -> Iterator -> Map.Entry -> troublesome
        Iterator iterator=map.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry mapEntry=(Map.Entry)iterator.next();
            System.out.println("The key is: "+mapEntry.getKey()
            		+ ",value is :"+mapEntry.getValue());
        }
 
        //more elegant way
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
        	System.out.println("Key : " + entry.getKey() 
       			+ " Value : " + entry.getValue());
        }
 
        //weired way, but work anyway
        for (Object key: map.keySet()) {
        	System.out.println("Key : " + key.toString() 
       			+ " Value : " + map.get(key));
        }
        accessTimes.add((long) 99);
        accessTimes.remove(0);
        accessTimes.add((long) 66);
        System.out.println(accessTimes.get(0));
        
        Iterator<Long> itrMaxAccess = accessTimes.iterator();
		while (itrMaxAccess.hasNext())
		{
			System.out.println(itrMaxAccess.next());
		}
   }
 
}
