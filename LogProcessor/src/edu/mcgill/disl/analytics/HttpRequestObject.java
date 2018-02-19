package edu.mcgill.disl.analytics;

import com.nearinfinity.bloomfilter.*;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;

public class HttpRequestObject implements Cloneable{
	
	public HttpRequestObject()
	{
		url = new String(); // unique id
		first_date_time = 0;
		last_date_time = 0;
		resp_code = 0;
		accessTimes=new ArrayList<Long>();
		type = new String();
		success_count = 0;
		failure_count = 0;
		execution_time = 0;
		counter = 0; // how many times this req has arrived at this 
		prcsd=false;		//particular server
		weight=0;
		objectDrift = false;
		visitedReq=false;
		objectFreq=new HashMap<String, Integer>();
	     driftRate=0;
		 ObjNo=0;
			tempreture=-1;

		  index=0;
		  indexIncremental=-1;

		assignedbefore=-1;
		candidate=null;
		recDone=false;
		//items= new BitSet (1000000);
		 bloomFilter = new BloomFilter<String>(0.01, 50, new ToBytes<String>() {
			private static final long serialVersionUID = -2257818636984044019L;
			@Override
			public byte[] toBytes(String key) {
				return key.getBytes();
			}
		});
		
		 toReplicate=false;
		
	}
	
	public String url;
	public ArrayList<Long> accessTimes;
	public long first_date_time;
	public long last_date_time;
	public long execution_time;
	public int resp_code;
	public String type;
	public int success_count;
	public int failure_count;
	public int counter;
	public int weight;
	public int assignedbefore;
	public ASServer candidate;
	public boolean recDone;
	public BitSet items;
	public BloomFilter<String> bloomFilter;
	public boolean objectDrift;
	public int index;
	public int indexIncremental;
	public boolean prcsd;
	//public BloomFilter<String> bloomFilterOld;
	public boolean visitedReq;
	public float driftRate;
	public float ObjNo;
	public Map <String,Integer> objectFreq;
	
	public double tempreture;
	
	public boolean toReplicate;

	

	
	public Object clone(){
		HttpRequestObject newObj = new HttpRequestObject();
		newObj.url = this.url;
		newObj.first_date_time = this.first_date_time;
		newObj.last_date_time = this.last_date_time;
		newObj.execution_time = this.execution_time;
		newObj.resp_code = this.resp_code;
		newObj.type = this.type;
		newObj.success_count = this.success_count;
		newObj.failure_count = this.failure_count;
		newObj.counter = this.counter;
		newObj.accessTimes=this.accessTimes;
		newObj.weight=this.weight;
		newObj.assignedbefore=this.assignedbefore;
		newObj.candidate=this.candidate.clone();
		newObj.recDone=this.recDone;
		newObj.candidate=this.candidate;
		newObj.items=this.items;
		newObj.bloomFilter=this.bloomFilter;
		newObj.objectDrift=this.objectDrift;
		newObj.index=this.index;
		newObj.indexIncremental=this.indexIncremental;
		newObj.prcsd=this.prcsd;
		newObj.visitedReq=this.visitedReq;
		newObj.objectFreq=this.objectFreq;
	//	newObj.bloomFilterOld=this.bloomFilterOld.cloneCo();
		newObj.objectFreq=this.objectFreq;
		newObj.driftRate=this.driftRate;
		newObj.ObjNo=this.ObjNo;
		newObj.toReplicate=this.toReplicate;
		
		return newObj;
	}
	
	public String toString()
	{
		String result = new String();
		result = url+ "\n" + first_date_time + "\n" + last_date_time + "\n" + type + "\n" + resp_code + "\n" + success_count + "\n" + failure_count; 
		return result;
	}

	public HttpRequestObject cloneCO() {
		try
		{
		return (HttpRequestObject) super.clone();
		}
		catch(Exception e){ return null; }
		}
}
