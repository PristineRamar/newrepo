package com.pristine.util.offermgmt;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.List;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRItemDTO;
//import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class PRCacheManager {
	static Logger		logger	= Logger.getLogger(PRCacheManager.class);
	static CacheManager manager = null;
	static Ehcache cache = null;
	
	public static void initialize()
	{
		String configPath = PropertyManager.getProperty("EHCACHE_CONFIG");
		manager = CacheManager.create(configPath);
		if(manager != null)
			cache = manager.getEhcache("pricingEngineCache");
		//logger.info("Cache " + cache.getName());
		int size = cache.getSize();
		//logger.info("Size of cache after creation : " + size);
		//logger.info("getMaxBytesLocalHeap():" + cache.getCacheConfiguration().getMaxBytesLocalHeap() +
				//",getMaxBytesLocalDisk():" + cache.getCacheConfiguration().getMaxBytesLocalDisk() +
				//",getMaxEntriesLocalHeap():" + cache.getCacheConfiguration().getMaxEntriesLocalHeap() +
				//",getMaxEntriesLocalDisk():" + cache.getCacheConfiguration().getMaxEntriesLocalDisk());
	}
	
	public static void shutdown()
	{
		manager.shutdown();
	}
	
	public static void putCache(String key, ArrayList<PRItemDTO> itemList){
		//long size = cache.calculateInMemorySize();
		//logger.info("Size of cache before addition : " + size);
		cache.put(new Element(key, itemList));
		//size = cache.calculateInMemorySize();
		//logger.info("Size of cache after addition : " + size);
		//logger.info("Number of items in list : " + itemList.size());
		
		logger.info("getKeys().size():" + cache.getKeys().size());
		logger.info("getKeys().toString():" + cache.getKeys().toString());
		logger.info("getSize():" + cache.getSize());
		
		// Write objects to a file
		String writeCacheToFile = PropertyManager.getProperty("EHCACHE_WRITE_TO_FILE", "FALSE");
		if(PRConstants.TRUE.equalsIgnoreCase(writeCacheToFile)){
			String cacheFilePath = PropertyManager.getProperty("EHCACHE_WRITE_FILE_PATH");
			FileWriter fw = null;
			BufferedWriter bw = null;
			try{
				fw = new FileWriter(cacheFilePath + "\\cache.csv");
				bw = new BufferedWriter(fw);
		        for(PRItemDTO item : itemList){
		        	if(item.getItemCode() < 920000){
		        		bw.write(item.toString());
			        	bw.write("\n");
		        	}
		        }
			}catch(IOException ioException){
				logger.error("Error when writing cache data into file " + ioException);
			}finally{
				try{
					bw.flush();
					fw.flush();
					bw.close();
					fw.close();
				}catch(IOException ioException){
					logger.error("Error when closing writers " + ioException);
				}
			}
		}
	}
	
	public static void putCacheStore(String key, PRItemDTO item){
		long size = cache.calculateInMemorySize();
		//logger.info("Size of cache before addition : " + size);
		cache.put(new Element(key, item));
		size = cache.calculateInMemorySize();
		//logger.info("Size of cache after addition : " + size);
		
		// Write objects to a file
		String writeCacheToFile = PropertyManager.getProperty("EHCACHE_WRITE_TO_FILE", "FALSE");
		if(PRConstants.TRUE.equalsIgnoreCase(writeCacheToFile)){
			String cacheFilePath = PropertyManager.getProperty("EHCACHE_WRITE_FILE_PATH");
			FileWriter fw = null;
			BufferedWriter bw = null;
			try{
				fw = new FileWriter(cacheFilePath + "\\cache.csv");
				bw = new BufferedWriter(fw);
		        if(item.getItemCode() < 920000){
		        	bw.write(item.toString());
			       	bw.write("\n");
		        }
			}catch(IOException ioException){
				logger.error("Error when writing cache data into file " + ioException);
			}finally{
				try{
					bw.flush();
					fw.flush();
					bw.close();
					fw.close();
				}catch(IOException ioException){
					logger.error("Error when closing writers " + ioException);
				}
			}
		}
	}
	
	public static ArrayList<PRItemDTO> getCache(String key){
		ArrayList<PRItemDTO> itemList = null;
		Element cacheElement = cache.get(key);
		if(cacheElement != null && cacheElement.getObjectValue() != null){
			itemList = (ArrayList<PRItemDTO>)cacheElement.getObjectValue();
		}
		return itemList;		
	}
	
	public static PRItemDTO getCacheStore(String key){
		PRItemDTO item = null;
		Element cacheElement = cache.get(key);
		if(cacheElement != null && cacheElement.getObjectValue() != null){
			item = (PRItemDTO)cacheElement.getObjectValue();
		}
		return item;		
	}
}
