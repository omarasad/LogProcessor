package edu.mcgill.disl.analytics;

import java.util.*;
import java.util.Map.Entry;

import edu.mcgill.disl.log.StatisticsManager;

public class ResourceToRequestMap {
	
	public Map<String,HashSet<String>> map = new HashMap<String, HashSet<String>>(100);
	StatisticsManager sm = null;
	public ResourceToRequestMap(StatisticsManager manager)
	{
		sm = manager;
		map = new HashMap<String, HashSet<String>>(100);
	}
	public void mapResourceToUrl(String url, String res){
		
		String tempURL = url;//templatizeUrl(url);
		//String resEntity = getEntityType(res);
		
//		HashSet<String> urlList = map.get(resEntity);
		
		String tk = removeSizefromKey(res);
		
		HashSet<String> urlList = map.get(tk);
		
		if(urlList == null){
			urlList = new HashSet<String>();
//			map.put(resEntity, urlList);
			map.put(tk, urlList);
		}
		urlList.add(tempURL);
	}
	
	
public void mapResourcesToUrl(String url, Collection<String> resources){
		
		String tempURL = url;//templatizeUrl(url);
		
		for(String res : resources)
		{
			
//			String resEntity = getEntityType(res);
	
//			HashSet<String> urlList = map.get(resEntity);
			String tk = removeSizefromKey(res);
			
		
			HashSet<String> urlList = sm.globalObjectToRequestMap.get(tk);
			
			if(urlList == null){
				urlList = new HashSet<String>();
//				map.put(resEntity, urlList);
				sm.globalObjectToRequestMap.put(tk, urlList);
			}
			
			urlList.add(tempURL);
		
		}
		
	//	System.out.println("sm.globalObjectToRequestMap.size()---"+sm.globalObjectToRequestMap.size() );

	}
	
	public void mapResourcesToUrlOri(String url, Collection<String> resources){
		
		String tempURL = url;//templatizeUrl(url);
		
		for(String res : resources){
			
//			String resEntity = getEntityType(res);
	
//			HashSet<String> urlList = map.get(resEntity);
			String tk = removeSizefromKey(res);
			HashSet<String> urlList = map.get(tk);
			
			if(urlList == null){
				urlList = new HashSet<String>();
//				map.put(resEntity, urlList);
				map.put(tk, urlList);
			}
			
			urlList.add(tempURL);
		
		}
	}
	
	public ResourceToRequestMap cloneResReqMap() 
	{
		ResourceToRequestMap resreqmap= new ResourceToRequestMap(sm);
		Map<String,HashSet<String>> resMapCopied = new HashMap<String, HashSet<String>>();
		try
		{
		if (this.map!=null)
		{
			for(Entry<String, HashSet<String>> entry : this.map.entrySet())
			{
				String s= entry.getKey();
				HashSet<String> hs=(HashSet<String>) entry.getValue().clone();
				resMapCopied.put(s, hs);			
			}		
		}
		resreqmap.map=resMapCopied;
		
		return resreqmap;
		} 
		catch (Exception e) {
			System.out.println("error RequestResourceMappingLogProcessor"+ e.toString());
			return null;
		}
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
	
	public static String getEntityType(String key){
		return key.substring(0,key.indexOf("#"));
	}
	
	public String getEntityKey(String key){
		return key.substring(key.indexOf("#")+1);
	}
	
	public String removeSizefromKey(String key){
		String arr[] = key.split("::");
		return arr[0];
	}
	
	public String toString(){
		return map.toString();
	}


	public void addNewMap()
	{
		this.map.clear();
		System.out.println("Remove Res to Req");
	}
}
