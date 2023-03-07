package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;

public class PriceMatch {
	private static Logger  logger = Logger.getLogger("PriceMatching");
	private int numberofMatches = 0;
	private int numberofNoMatches = 0;
	private ProductGroupDAO pgdao = new ProductGroupDAO();
	
	public void doPriceMatching(Connection conn, int chainId, int checkListId, 
			String startDate, String endDate) throws GeneralException {
		logger.info("Performing Item price matching ....");
		try {
			
			ScheduleDAO schDao = new ScheduleDAO();
			CompetitiveDataDAO compDataDao = new CompetitiveDataDAO (conn);
			
			ArrayList<ScheduleInfoDTO> schList = schDao.getSchedulesByChain(conn, chainId, checkListId, startDate, endDate);
			Iterator <ScheduleInfoDTO> itr = schList.iterator();
			int count = 0;
			
			while (itr.hasNext()){
				ScheduleInfoDTO schInfo = itr.next();
				logger.info("doing price matching for schedule - " + schInfo.getScheduleId());
				//Get the competitiveData items
				ArrayList<CompetitiveDataDTO> compDataList = compDataDao.getCompData(conn,schInfo.getScheduleId(), -1,-1, true);
				logger.info("# of items - " + compDataList.size());
				
				performPriceMatchforCheck(conn, chainId, compDataList, schInfo);
				count++;
				//if (count == 1) break;
			}
			logger.info("Number of schedule checked - " + count);
			logger.info("Number of price matching pairs - " + numberofMatches);
			logger.info("Number of price mis-matching pairs - " + numberofNoMatches);
		
		}catch(SQLException sqlce){
			throw new GeneralException ( "Cached Row access exception ", sqlce);
		}
	}
	
	
	public void doPriceMatchingV2(Connection conn, int chainId, int checkListId, 
			String startDate, String endDate) throws GeneralException {
		logger.info("Performing Item price matching V2....");
		try {
			
			ScheduleDAO schDao = new ScheduleDAO();
			ArrayList<ScheduleInfoDTO> schList = schDao.getSchedulesByChain(conn, chainId, checkListId, startDate, endDate);
			Iterator <ScheduleInfoDTO> itr = schList.iterator();
			int count = 0;
			
			
			StringBuffer sb = new StringBuffer();
			while (itr.hasNext()){
				ScheduleInfoDTO schInfo = itr.next();
				count++;
				sb.append(schInfo.getScheduleId());
				if ( count < schList.size())
					sb.append(",");
			}
			String csSchedules = sb.toString();
			logger.info("doing price matching for schedules - " + csSchedules);
			count = 0;
			CachedRowSet crs = pgdao.getPotentialItems(conn, chainId);
			while ( crs.next()){
				int baseItem = crs.getInt("BASE_ITEM_CODE");
				int relatedItem = crs.getInt("RELATED_ITEM_CODE");
				
				CachedRowSet priceMatchcrs = pgdao.getPriceMatchData( conn, csSchedules, baseItem, relatedItem);
				priceMatchcrs.next();
				int priceMatchCount = priceMatchcrs.getInt("PRICE_MATCH_CT");
				int priceNotMatchCount = priceMatchcrs.getInt("PRICE_NOT_MATCH_CT");
				count++;
				if( count%10000 == 0)
					logger.info(count + " item pair proccessed");
				if( priceMatchCount > 0 ){
					pgdao.incrementMatchCount(conn, chainId, baseItem,relatedItem);
					numberofMatches++;
				}
				if (priceNotMatchCount > 0){
					pgdao.incrementUnMatchCount(conn, chainId, baseItem,relatedItem);
					numberofNoMatches++;
				}
				
			}
			
			//Get the item list from potential items
					
			logger.info("Number of item pairs checked - " + count);
			logger.info("Number of price matching pairs - " + numberofMatches);
			logger.info("Number of price mis-matching pairs - " + numberofNoMatches);
		
		}catch(SQLException sqlce){
			throw new GeneralException ( "Cached Row access exception ", sqlce);
		}
	}
	
	private void performPriceMatchforCheck(Connection conn, 
			int chainId, ArrayList<CompetitiveDataDTO> compDataList, ScheduleInfoDTO schInfo) 
						throws GeneralException, SQLException {
		
		HashMap<Integer, CompetitiveDataDTO> checkDataMap = new HashMap<Integer, CompetitiveDataDTO> ();
		Iterator <CompetitiveDataDTO> dataListItr = compDataList.iterator();
		while ( dataListItr.hasNext()){
			CompetitiveDataDTO compData = dataListItr.next();
			checkDataMap.put(compData.itemcode, compData);
		}
		dataListItr = compDataList.iterator();
		int itemCount=0;
		while ( dataListItr.hasNext()){
			CompetitiveDataDTO baseItemData = dataListItr.next();
			if( baseItemData.itemNotFound.equals("Y")) continue;
			itemCount++;
			//if( itemCount > 50) break;
			CachedRowSet relatedItemCrs = pgdao.getPotentialRelatedItem(conn, chainId, schInfo.getScheduleId(), baseItemData.itemcode);
			while( relatedItemCrs.next()){
				
				int relatedItemCode = relatedItemCrs.getInt("RELATED_ITEM_CODE");
				//if( checkDataMap.containsKey(relatedItemCode)){
					CompetitiveDataDTO relatedItemData = checkDataMap.get(relatedItemCode);
					if( relatedItemData.itemNotFound.equals("Y")) continue;
					
					if( Math.abs(baseItemData.regPrice - relatedItemData.regPrice) < 0.001 ){
						pgdao.incrementMatchCount(conn, chainId, baseItemData.itemcode,relatedItemData.itemcode);
						numberofMatches++;
					}
					else{
						pgdao.incrementUnMatchCount(conn, chainId, baseItemData.itemcode,relatedItemData.itemcode);
						numberofNoMatches++;
					}
						
				//}
			}
		}
	}

}
