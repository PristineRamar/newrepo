package com.pristine.util;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.pristine.dao.PriceTestDAO;
import com.pristine.exception.GeneralException;

public class PriceTestUtil {
	private static Logger logger = Logger.getLogger("PriceTestUtil");
	

	String zoneNum = "";

	public String getZoneId(Connection conn, List<Integer> storeList) throws GeneralException {
		logger.debug("getZoneId() :- Process started ..");
		try {
			HashMap<String, Integer> zoneStrCount = new PriceTestDAO().setZoneCount(conn, storeList);
			logger.info("zoneStrCount Map : -"+ zoneStrCount.size());
			HashMap<Integer, Integer> zoneAndItsStoreCount = new PriceTestDAO().getZoneStrcount(conn);
			logger.info("zoneAndItsStoreCount Map: -"+ zoneStrCount.size());
			int maxCount = 0;
			int zoneId=0;
			for (Entry<String, Integer> zoneCount : zoneStrCount.entrySet()) {
				zoneId=Integer.parseInt(zoneCount.getKey().split(";")[0]);
				if (zoneCount.getValue() > maxCount) {
					maxCount = zoneCount.getValue();
					zoneNum = zoneCount.getKey();
				} 
				// If there is tie in no of stores,take zone which has max stores
				else if (maxCount == zoneCount.getValue()) {

					int tempZone1 = 0;
					int tempZone2 = 0;
					if (zoneAndItsStoreCount.containsKey(zoneId)) {
						tempZone1 = zoneAndItsStoreCount.get(zoneId);
					}
					if (zoneAndItsStoreCount.containsKey(Integer.parseInt(zoneNum.split(";")[0]))) {
						tempZone2 = zoneAndItsStoreCount.get(Integer.parseInt(zoneNum.split(";")[0]));
					}
					if (tempZone1 > tempZone2) {
						maxCount = tempZone1;
						zoneNum = zoneCount.getKey();
					} else {
						maxCount = tempZone2;
					}
				}
			}
		} catch (Exception e) {
			logger.info("exception in setzoneid: " + e);
		}
		return zoneNum;
	}

}