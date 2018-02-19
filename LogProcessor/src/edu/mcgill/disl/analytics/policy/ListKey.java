package edu.mcgill.disl.analytics.policy;

import java.io.Serializable;
import java.util.HashSet;

public class ListKey extends ObjectKey implements Serializable{
	
    public static final long serialVersionUID = 2L;	

	public ListKey(){
		//keyList = new HashSet<String>();
	}

	//public HashSet<String> keyList;
	public String key;
	
	public void addtoListKey(String key){
		//keyList.add(key);
		this.key = key;
	}
	
	public boolean equals(Object obj){
		if(obj instanceof ListKey){
			return this.key.equals(((ListKey)obj).key);
		}
		return false;
	}
	
	public int hashCode(){
		return key.hashCode();
	}
	
	public String toString(){
		return key;
	}
	
	
	
}
