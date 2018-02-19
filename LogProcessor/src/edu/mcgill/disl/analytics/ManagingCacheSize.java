package edu.mcgill.disl.analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


// This class manages the cache space in case of limited size...
public class ManagingCacheSize {
	
	public int cacheSize;
	public List<CacheObject> policyMap = new ArrayList<CacheObject>();
	public List<CacheObject> toRemove = new ArrayList<CacheObject>();

	
	public ManagingCacheSize(int cs, List curMap)
	{
		cacheSize=cs;
		policyMap=curMap;
		
	}
	
	public void removeObjects()
	{
		//1. sort
		sortCacheObjectListByUpdateCount(true);
		
		//2. pick cs top popular objects
		trimCacheSpace();
		
		//3. return the result
	}
	
	
	private void sortCacheObjectListByUpdateCount(final boolean desc)
	{
		Collections.sort(policyMap, new Comparator<CacheObject>(){

			@Override
			public int compare(CacheObject co1, CacheObject co2) 
			{
				//return  desc? (int)co2.updateCount- (int)co1.updateCount : (int)co1.updateCount - (int)co2.updateCount;
				return  desc? (int)co2.tempPopulariyt- (int)co1.tempPopulariyt : (int)co1.tempPopulariyt- (int)co2.tempPopulariyt;
			}
			
		});
		
	}
	
	private void trimCacheSpace()
	{
		int currentSize=policyMap.size();
		for (int x=cacheSize;x<currentSize;x++)
		{
			toRemove.add(policyMap.get(x));
			//policyMap.remove(x);
			
		}
		
	}
}
