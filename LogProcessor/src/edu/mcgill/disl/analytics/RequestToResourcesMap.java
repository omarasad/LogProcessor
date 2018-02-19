package edu.mcgill.disl.analytics;

import java.util.*;
import java.util.Map.Entry;

import edu.mcgill.disl.log.StatisticsManager;

public class RequestToResourcesMap {
	
	Map<String,HashSet<String>> map;
	StatisticsManager sm = null;
	
	Map<String, Map<String, Integer>> mapNew;
	
	
	public RequestToResourcesMap(StatisticsManager manager)
	{
		sm = manager;
		map = new HashMap<String, HashSet<String>>(100);
		mapNew = new HashMap<String, Map<String,Integer>>(100);
	}
	
	
	public void addResource(String url, String res){
		String tempURL = url;//templatizeUrl(url);
		HashSet<String> resList = map.get(tempURL);
		
		if(resList == null){
			resList = new HashSet<String>();
			map.put(tempURL, resList);
		}
		resList.add(res);
	}
	
	public void addNewMap()
	{
		if ((map !=null))
			this.map.clear();
		
		if ((mapNew !=null))
			this.mapNew.clear();
		
		System.out.println("Remove Req to Res");
	}
	
	public void addResources(String url, Collection<String> resources){
		
		String tempURL = url;//templatizeUrl(url);
		
		HashSet<String> resList = sm.globalRequestToObjectMap.get(tempURL);		
		if(resList == null){
			resList = new HashSet<String>();
			sm.globalRequestToObjectMap.put(tempURL, resList);
		}
		
		//for(String res:resources){
		resList.addAll(resources);
		//}
		
		//System.out.println("sm.globalRequestToObjectMap.size()---"+sm.globalRequestToObjectMap.size());
	}
	
public void addResourcesOri(String url, Collection<String> resources){
		
		String tempURL = url;//templatizeUrl(url);
		
		HashSet<String> resList = map.get(tempURL);
		
		if(resList == null){
			resList = new HashSet<String>();
			map.put(tempURL, resList);
		}
		
		//for(String res:resources){
		resList.addAll(resources);
		//}
	}
	
	
public void addReqObjectFreq(String url, Collection<String> resources)
{
		
	String tempURL = url;//templatizeUrl(url);
	
	Map<String,Integer> resList = mapNew.get(tempURL);
	
	if(resList == null)
	{
		resList = new HashMap<String,Integer>();
		for (String obj:resources)
		{
			resList.put(obj,1);
		}
		
	}
	else
	{
		for (String obj:resources)
		{
			if (resList.containsKey(obj))
			{
			int objFreq=resList.get(obj);
			objFreq++;
			resList.put(obj, objFreq);
			}
			else
			{
				resList.put(obj,1);
			}
			
			
			
		}
	}
	mapNew.put(tempURL, resList);

}


public void addReqObjectFreqOri(String url, Collection<String> resources)
{
		
	String tempURL = url;//templatizeUrl(url);
	
	Map<String,Integer> resList = mapNew.get(tempURL);
	
	if(resList == null)
	{
		resList = new HashMap<String,Integer>();
		for (String obj:resources)
		{
			resList.put(obj,1);
		}
		
	}
	else
	{
		for (String obj:resources)
		{
			if (resList.containsKey(obj))
			{
			int objFreq=resList.get(obj);
			objFreq++;
			resList.put(obj, objFreq);
			}
			else
			{
				resList.put(obj,1);
			}
			
			
			
		}
	}
	mapNew.put(tempURL, resList);

}
	
	public String templatizeUrl(String url){
		
		StringBuffer sb = new StringBuffer(url.length());
		boolean isVal=false;
		for(int i=0;i<url.length();i++){
			char ch = url.charAt(i);
			if(ch=='='){
				isVal = true;
			}else if(isVal){
				if(ch=='&'){
					isVal = false;
				}else{
					continue;
				}
			}
			sb.append(ch);
		}
		return sb.toString();
	}
	/*
	public static void main(String[] args){
		RequestToResourcesMap t = new RequestToResourcesMap();
		
		String url = "/rubis/user?a=w5&b=asdf+fsdf&cd=hello1";
		
		System.out.println(t.templatizeUrl(url));
	}
	*/
	public String toString(){
		return map.toString();
	}


	public RequestToResourcesMap cloneReqResMap() 
	{
		RequestToResourcesMap reqresmap = new RequestToResourcesMap(sm) ;
		Map<String,HashSet<String>> reqMapCopied = new HashMap<String, HashSet<String>>();
		try
		{
		if (this.map!=null)
		{
			for(Entry<String, HashSet<String>> entry : this.map.entrySet())
			{
				String s= entry.getKey();
				HashSet<String> hs=(HashSet<String>) entry.getValue().clone();
				reqMapCopied.put(s, hs);			
			}		
		}
		reqresmap.map=reqMapCopied;
		
		return reqresmap;
		} 
		catch (Exception e) {
			System.out.println("error RequestResourceMappingLogProcessor"+ e.toString());
			return null;
		}
	}
}
