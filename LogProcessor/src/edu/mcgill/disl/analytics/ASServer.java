package edu.mcgill.disl.analytics;

import java.util.*;

import org.apache.log4j.Logger;

import edu.mcgill.disl.analytics.policy.AppServerPolicy;
import edu.mcgill.disl.analytics.policy.LoadBalancerPolicy;

public class ASServer implements Cloneable {
	
	public AppServerPolicy currentPolicy = null; 
	public AppServerPolicy lastPolicy = null;
	public AppServerPolicy tmpPolicy = null;
	
	public AppServerPolicy tempPolicytoDelete = null;
	
	public List<HttpRequestObject> curReqList = new ArrayList<HttpRequestObject>();
	
	public List<String> curGiantReqListString = new ArrayList<String>();


	public Set<String> curObjList = new HashSet<String>();
	public Set<String> tmpObjList = new HashSet<String>();
	public Set<String> oldObjList = new HashSet<String>();
	
	
	
	public HashMap<String,Integer> partReq = new  HashMap<String, Integer>(100);
	
	public List<String> curReqListString = new ArrayList<String>();
	public List<String> tmpReqListString = new ArrayList<String>();
	public ASServer toServer = null; 
	public static LoadBalancerPolicy oldLB = null;
	public int weight=0;
	public int recNo=0;
	public int objNo=0;
	public boolean assinged=false;
	public BitSet curObjBits = new BitSet(10000000);
	public BitSet tmpObjBits = new BitSet(10000000);
	
	public int localAccess=0;
	public int remoteAccess=0;
	public int fromRemoteAccess=0;
	public int totalLoad=0;
	public int totalObjectCapacity=0; // this is for replicating popular object
	
	
	static final Logger log = Logger.getLogger(ASServer.class.getName());
	
	String serverId;
	
	long policyTimeStamp;
	
	Map<String,Object> structs;
	
	public int serverNo; 
	
	
	
	public ASServer(String id, int servNo) {
		serverId = id;
		serverNo = servNo; 
		structs = new HashMap<String,Object>();
		log.debug("Server:" + serverId);
	}
	
	public Object getStruct(String name){
		return structs.get(name);
	}
	
	public Map<String,Object> getStructs(){
		return structs;
	}

	
	public void setStruct(String name, Object struct){
		structs.put(name, struct);
	}
	
	public String getServerId(){
		return serverId;
	}
	
	public ASServer clone() {
		try
		{
		return (ASServer) super.clone();
		}
		catch(Exception e){ return null; }
		}
	
	
}
