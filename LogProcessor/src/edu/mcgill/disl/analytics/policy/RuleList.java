package edu.mcgill.disl.analytics.policy;

import java.io.Serializable;
import java.util.ArrayList;

public class RuleList  implements Serializable{
	
    public static final long serialVersionUID = 3L;	

	
	public static final int REPLICATE = 0;
	static public final int MOVE = 1; // to move the the object to AS listed in Destination_Server_List [This is replication]
	static public final int NONE = 2; // to repliacte the object to the all caches for replicating popular
	static public final int REPLICATE_ALL = 3; // to repliacte the object to the all caches for replicating popular
	
	static public final int STABLE_TTL = 180*60*100; // greatest TTL value
	static public final int LONG_TTL = 120*60*100;
	static public final int SHORT_TTL = 60*60*100; // Obj default value
	
	public String ASName;
	public int ruleType;
	public ArrayList<String> serverList;
	public Integer ttl;
	
	public RuleList(){
		
	}
	
	public RuleList(String AppServer, int ruleT, int ttl_type, ArrayList<String> serList){
		ASName = new String(AppServer);
		ruleType = ruleT;
		ttl = ttl_type;
		serverList = serList; 
	}
	
	public String toString(){
	
		String ret = new String();
		
		ret = "AS Name: " + this.ASName + ", Rule Type: " + this.ruleType + ", Server List: " + this.serverList + ", TTL: " + this.ttl; 
		
		return ret;
		
	}

}
