package edu.mcgill.disl.analytics.policy;

import java.io.Serializable;

public class RangeKey extends ObjectKey  implements Serializable{

	String keyType;
	int begin_range;
	int end_range;
	
	public RangeKey(){
		
	}
	
	public RangeKey(String kt, int begin, int end){
		keyType = new String(kt);
		begin_range = begin;
		end_range = end;
	}
	
	public String toString(){
		String ret = new String();
		ret = "KeyType:" + this.keyType + ", Start index:" + this.begin_range + ", End index:" + this.end_range;
		return ret;
	
	}
	
}
