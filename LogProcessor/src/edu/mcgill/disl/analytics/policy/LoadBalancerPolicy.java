package edu.mcgill.disl.analytics.policy;

import java.io.Serializable;
import java.util.*;

@SuppressWarnings("serial")
public class LoadBalancerPolicy implements Serializable,Cloneable{
	
	public Map<String,ArrayList<ServerInfo>> policyMap = new HashMap<String,ArrayList<ServerInfo>>();
	
	
	public LoadBalancerPolicy() {
		
	}
	
	public void mapUrlToServers(String url, String... servers){
		
		String tempURL = url;//templatizeUrl(url);
		
		ArrayList<ServerInfo> servList = policyMap.get(tempURL);
		
		if(servList==null){
			servList = new ArrayList<ServerInfo>();
			policyMap.put(tempURL, servList);
		}
		
		for(String server:servers){
			ServerInfo info = parseServerString(server);
			if(!servList.contains(info))
				servList.add(info);
		}
		
	}
	
	public ServerInfo parseServerString(String server){
		
		return new ServerInfo(server,"8080");
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

	
	public static class ServerInfo implements Serializable{
		
		public ServerInfo(String host,String port) {
			this.host = host;
			this.port = port;
		}
		
		
		public String host;
		public String port;
		
		public String toString(){
			String ret = new String();
			
			ret = "Host: " + this.host + ", " + "Port: " + this.port;
			
			return ret;
		}
		
		public boolean equals(Object o){
			
			if(o instanceof ServerInfo){
				return(this.host.equals(((ServerInfo)o).host));
		
			}
			
			return false;
		}
	}
	
	public LoadBalancerPolicy clone() {
		try
		{
		return (LoadBalancerPolicy) super.clone();
		}
		catch(Exception e){ return null; }
		}
	
	public String toString(){
		
		return policyMap.toString();
		
	}

}