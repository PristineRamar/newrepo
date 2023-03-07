package com.pristine.test.offermgmt;

import java.util.HashMap;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.service.offermgmt.ItemKey;

public class MemoryTest {
	private static Logger logger = Logger.getLogger("PricingEngineWS");
	
	public static void main(String[] args) {
		MemoryTest memoryTest = new MemoryTest();
		memoryTest.logMemoryUsage();
	}
	
	
	private void logMemoryUsage() {
		Runtime runtime = Runtime.getRuntime();  
		writeMemoryUsage("1", runtime);
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		for(int i=0; i<10000; i++)  {
			PRItemDTO prItemDTO = new PRItemDTO();
			ItemKey itemKey = new ItemKey(i, 0);
			itemDataMap.put(itemKey, prItemDTO);
		}
		writeMemoryUsage("2", runtime);
		System.gc();
		writeMemoryUsage("3", runtime);
		itemDataMap.clear();
		itemDataMap = null;
		System.gc();
		writeMemoryUsage("4", runtime);
		
	}
	public void writeMemoryUsage(String msg, Runtime runTime) {
		logger.debug("***" + msg + "***");
		logger.debug("Total Memory: " + humanReadableByteCount(runTime.totalMemory()));
		logger.debug("Free Memory: " + humanReadableByteCount(runTime.freeMemory()));
		logger.debug("Used Memory: " + humanReadableByteCount(runTime.totalMemory() - runTime.freeMemory()));
		logger.debug("******");
	}
	
	public String humanReadableByteCount(long bytes) {
	    int unit = 1000;
	    if (bytes < unit) return bytes + " B";
	    int exp = (int) (Math.log(bytes) / Math.log(unit));
	    String pre = ("kMGTPE").charAt(exp-1) + ("");
	    return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}
}