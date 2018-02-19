package edu.mcgill.disl.analytics.policy;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.mcgill.disl.analytics.ASServer;


@SuppressWarnings("serial")
public class AppServerPolicy implements Serializable,Cloneable{
	
    public static final long serialVersionUID = 1L;	
	public Map<ObjectKey, RuleList> policyMap = new HashMap<ObjectKey,RuleList>();
	public static final String IGNORE = "IGNORE";
	public static final String LOCAL_STORE = "LOCAL_STORE";
	public String notFoundRule = IGNORE;
	
	public AppServerPolicy()
	{
		
	}
	
	public void addNewPolicy(ObjectKey ok, RuleList rl){ //map each object key with a role
		policyMap.put(ok, rl);
	}
		
	public RuleList getRuleList(ObjectKey ok){
		return policyMap.get(ok);
	}
	
	public RuleList getRuleListForObject(Object key){
		
		for(ObjectKey k : policyMap.keySet()){
			if(k instanceof ListKey){
				ListKey lk = (ListKey)k;
				if(lk.key.equals(key)){
					return policyMap.get(k);
				}
			}
		}
		return null;
	}
	
	public void removeKey(String cacheKey)
	{
		ListKey lk= new ListKey();
		lk.addtoListKey(cacheKey);
	}
	
	public AppServerPolicy clone() {
		try
		{
		return (AppServerPolicy) super.clone();
		}
		catch(Exception e){ return null; }
		}
	
	
	
	public String toString(){
		String ret = new String();
		
		ret = policyMap.toString() + "\n";
		
		return ret;
	}
	
	public  <K1, K2, V> Map<K1, Map<K2, V>> deepCopy(
		    Map<K1, Map<K2, V>> original){

		    Map<K1, Map<K2, V>> copy = new HashMap<K1, Map<K2, V>>();
		    for(Entry<K1, Map<K2, V>> entry : original.entrySet()){
		        copy.put(entry.getKey(), new HashMap<K2, V>(entry.getValue()));
		    }
		    return copy;
		}
	
	public ObjectKey getKey(String cacheKey)
	{
		for (Entry e:policyMap.entrySet())
		{
			ListKey key=(ListKey) e.getKey();
			if (key.key.contains(cacheKey))
				return (ObjectKey) e.getKey();
		}
		return null;
	}
	
}
