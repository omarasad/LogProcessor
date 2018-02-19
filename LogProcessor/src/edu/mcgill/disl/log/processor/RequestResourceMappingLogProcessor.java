package edu.mcgill.disl.log.processor;

import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

import edu.mcgill.disl.analytics.ASServer;
import edu.mcgill.disl.analytics.Analyser;
import edu.mcgill.disl.analytics.RequestToResourcesMap;
import edu.mcgill.disl.analytics.ResourceToRequestMap;
import edu.mcgill.disl.log.StatisticsManager;

public class RequestResourceMappingLogProcessor extends LogProcessor{
	
	static final Logger log = Logger.getLogger(RequestResourceMappingLogProcessor.class.getCanonicalName());
	
	public static final String ACTIVITY_COUNTER = "activityCounter";
	public static final String REQ_OBJ_MAP = "reqObjMap";
	public static final String OBJ_REQ_MAP = "objReqMap";
	 public List<String> updateTxn= new ArrayList<String>();
	//public StatisticsManager sm;
	
	Map<String,HashSet<String>> reqMap = new HashMap<String, HashSet<String>>();
	Map<String,String> objectType = new HashMap<String, String>();
	
	public RequestResourceMappingLogProcessor(StatisticsManager manager) {
		super(manager);
		//this.sm=manager;
		initializeStructures();
		getManager().registerProcessor(this);
		updateTxn.add("putBidAuth");
		updateTxn.add("buyNowAuth");
		updateTxn.add("SellItemForm");
		updateTxn.add("Register");
		updateTxn.add("update");
	}
	


double RoundTo2Decimals(double val) {
            DecimalFormat df2 = new DecimalFormat("###.##");
        return Double.valueOf(df2.format(val));
}


	
	public int getReplicNo(String obj)
	{
		
			
			for(Entry<Integer, String> val:getManager().updatedObjects.entrySet())
			{
				if (val.getValue().equals(obj))
					return val.getKey();
			}
		return -1;
	}
	
	
	public double getOperationCost(String op)
	{
		if (op.equals("getLocal"))
			return getManager().readLocalCost;
		else if (op.equals("getRemote"))
			return getManager().readRemoteCost;
		else if (op.equals("putLocal"))
			return getManager().putLocalCost;
		else if (op.equals("putRemote"))
			return getManager().putRemoteCost;
		
		
		
		else return 0.0;
	}
	
	public void initializeStructures(){
		for(ASServer serv : getManager().getASServers().values()){
			//add all structures here for each server that we will maintain
			
			//activity counter
			serv.setStruct(ACTIVITY_COUNTER, new Long(0));
			serv.setStruct(REQ_OBJ_MAP, new RequestToResourcesMap(manager));
			serv.setStruct(OBJ_REQ_MAP, new ResourceToRequestMap(manager));
			
		}
	}
	
	public boolean checkUpdateTxn(String txn)
	{
		for (int s=0;s<updateTxn.size();s++)
		{
			if (txn.contains(updateTxn.get(s)))
			 return true;
		}
		return false;
	}
	
	@Override
	public void processLog(LoggingEvent le) {
		
		
		
	//	ASServer serv = getLogServer(le);
		
		ASServer serv = manager.getASServer(0);
		
		// processing ACTIVITY_COUNTER
		Long counter = ((Long) serv.getStruct(ACTIVITY_COUNTER)) + 1;
		serv.setStruct(ACTIVITY_COUNTER, counter);
		//log.info(serv.getServerId() + ":activityCounter:" + counter);
		
		// processing REQ_OBJ_MAP & OBJ_REQ_MAP
		String guid = getUID(le);
	//	System.out.println("Omar======getLogType="+getLogType(le));
	//	System.out.println("Omar====== guid= " +guid);
		
		
		if(getLogType(le).equals(CACHE_LOG))
		{
		//	System.out.println("1111");
			HashSet<String> res = reqMap.get(guid);
			if(res==null)
			{
				res=new HashSet<String>();
				reqMap.put(guid, res); // Now the resource is already existed on the reqMap
				objectType.put(guid, getCacheObjectType(le));
			}
			String cacheRes = getCacheResource(le);
			res.add(cacheRes);
			
			// for object update semantic
			
			if ((getManager().updatedObjects.containsValue(cacheRes)  || cacheRes.contains("defaultCache")) && getManager().counter==1)
			{
			getManager().updatedObjectsGUID.put(cacheRes, guid);
			
			String cacheOperation=getCacheOperationType(le);
			ArrayList<String> cacheOperations= getManager().updatedObjectsOperations.get(guid);
			if(cacheOperations==null)
			{
				cacheOperations=new ArrayList<String>();
				getManager().updatedObjectsOperations.put(guid, cacheOperations); // Now the resource is already existed on the reqMap
				objectType.put(guid, getCacheObjectType(le));
			}
			//System.out.println(getCacheObjectType(le)+"cacheOperation="+cacheOperation);
			cacheOperations.add(cacheOperation);
			
			
			
				//String updatedObjString=getManager().updatedObjects.get(cacheRes);
				
				
				
			}
			
		}
		else if(getLogType(le).equals(ACCESS_LOG))
		{
			//System.out.println("2222");
			HashSet<String> res = reqMap.get(guid);
			String cacheType = objectType.get(guid);
			if(res==null || res.size()==0 ){
				//no object access in this log.. discarding now
				//System.out.println("no object access in this log"+le.getMessage());
				return;
			}
			
			/*
			if (checkUpdateTxn(le.toString()))
				return;
			*/
			String[] toks = ((String)le.getMessage()).split(" ");
			String url = toks[1];
			//System.out.println("le.toString()="+le.toString());
			if (checkUpdateTxn(url))
			{
				if (getManager().counter==1 && getManager().updatedObjects.size()>0)
				{
					HashSet<String> objects=reqMap.remove(guid);
					
					//System.out.println("objects.toString()="+objects.toString());
				ArrayList<String> cacheOperations=getManager().updatedObjectsOperations.get(guid);
				String replicatedObects="";
				Set <String>toRemove = new HashSet<String>();
				boolean contains=false;
				//System.out.println("getManager().updatedObjects.size()="+getManager().updatedObjects.size() );	
				for(String obj:getManager().updatedObjects.values())
				{
					if (objects.contains(obj))
					{
						//System.out.println("obj"+obj);
						contains=true;
						
						ArrayList<String> cacheOp= getManager().updatedObjectsOperations.get(guid);
						//System.out.println("=============objjj="+obj +"cacheOp size"+cacheOp.size() +"cacheOp.toString()="+cacheOp.toString());
						
						double totCost=0;
						int replicaNo=-1;
						for(int x=0;x<cacheOp.size();x++)
						{
							String obb1= cacheOp.get(x);
							//System.out.println("TrcacheOp="+obb1);
							totCost+=getOperationCost(obb1);
							System.out.println("TrcacheOp="+obb1 +"cost="+RoundTo2Decimals(getOperationCost(obb1)));
						}

						int c=getManager().replicateObjectCounter.get(obj);
						
						
						double newTotCost=RoundTo2Decimals(totCost);

						double currTotCost=getManager().replicateObjectCost.get(getReplicNo(obj));
						currTotCost=currTotCost+newTotCost;
						
						c++;
						
						getManager().replicateObjectCounter.put(obj, c);
						getManager().replicateObjectCost.put(getReplicNo(obj), currTotCost);
						System.out.println("getReplicNo(obj)="+getReplicNo(obj) +"newTotCost"+newTotCost + "currTotCost="+currTotCost+" c="+c );
						
						/* here we need to update the operation counter every time we add a new update cost
						 * if the counter is less than X predefined number of operations we simply continue 
						 * if not we divide the total number of times for that update by X and update the cost 
						 * 		and we remove that object from the list
						 */
						
						
						
						if (c>=4) // here after c iterations we average out the time
						{
							 currTotCost=currTotCost/(double)c;
							//currTotCost=currTotCost+newTotCost;
							getManager().replicateObjectCost.put(getReplicNo(obj), currTotCost);
							toRemove.add(obj);
							System.out.println("Final getReplicNo(obj)="+getReplicNo(obj) +"FinalTotCost="+currTotCost);
						}
						
						
						/*
						Iterator<String> ir1=cacheOp.iterator();
						while (ir1!=null)
						{
							String obb1=(String) ir1.next();
							System.out.println("cacheOp="+obb1);
						}
						**/
						
						
						
						
						
						
						
					}
					
				}	
				
			if (contains)
			{
				Iterator<String> irrr=toRemove.iterator();
				while (irrr.hasNext())
				{
					String obb=(String) irrr.next();
					System.out.println("obb="+obb);
					if (getManager().updatedObjects.containsValue(obb))
					{
						int keyToDelete=-1;
						for (Entry<Integer, String> entry : getManager().updatedObjects.entrySet()) 
						{
				            if (entry.getValue().equals(obb)) 
				            {
				            	keyToDelete=entry.getKey();
				                //System.out.println(entry.getKey());
				            }
				        }
						
						getManager().updatedObjects.remove(keyToDelete);
						//System.out.println("obb="+getManager().updatedObjects.size() +""+getManager().updatedObjects.containsKey(keyToDelete));						
					}
				}
			}
				return;
				}
			}
			
			else if (cacheType.contains("insert") || cacheType.contains("update"))
			{
				System.out.println("insert/update"+le.getMessage());

					reqMap.remove(guid);
					return;
				
			}
			else{
			//	System.out.println("3333");
				//get url+querystring
				//timestamp httpMethod url returnCode timetakenMillis
				
				//String[] toks = ((String)le.getMessage()).split(" ");
				//String url = toks[1];
				
				if (url.contains("RegisterItem") || url.contains("Store"))
					return;
				
				RequestToResourcesMap reqToRes = (RequestToResourcesMap) serv.getStruct(REQ_OBJ_MAP);
				
				Collection<String> ress = reqMap.remove(guid);
				//copy/transfer all resources associated with request url
				//and delete this request node (guid) from our map. remove returns our collection
				reqToRes.addResources(url, ress);
				
				
				reqToRes.addReqObjectFreq(url, ress); // by OMAR
				
				ResourceToRequestMap resToReq = (ResourceToRequestMap) serv.getStruct(OBJ_REQ_MAP);
				resToReq.mapResourcesToUrl(url, ress);
				
				//NEW DIRECTIONS-OMAR
				// SOMETHING HAVE TO BE ADDED HERE
				
			}
			
		}
		
	}
	

	
	//we assume le is from cache
	private String getCacheResource(LoggingEvent le){
		String resStr = (String)le.getMessage();
		String[] strs = resStr.split("::");
		resStr = strs[1];
		return resStr;
	}
	
	private String getCacheOperationType(LoggingEvent le){
		String resStr = (String)le.getMessage();
		String[] strs = resStr.split("::");
		resStr = strs[0];
		return resStr;
	}
	
	private String getCacheObjectType(LoggingEvent le){
		String resStr = (String)le.getMessage();
		String[] strs = resStr.split("::");
		resStr = strs[0];
		return resStr;
	}
	
}
