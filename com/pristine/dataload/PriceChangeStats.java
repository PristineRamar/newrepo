package com.pristine.dataload;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.PerformanceAnalysisDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.PriceCheckStatsDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PriceChangeStats {
	private static Logger  logger = Logger.getLogger("PerformanceStats");
	private CompetitiveDataDAO compDataDao;
	private HashMap <Integer, CompetitiveDataDTO> lastCheckMap = new HashMap <Integer, CompetitiveDataDTO>();
	
	public static void main(String[] args) {
		if (args.length == 1 && args[0].equals("-h")){
			logger.debug("Insufficient Arguments - PriceChangeStats scheduleId");
			System.exit(1);
		}
		PropertyConfigurator.configure("log4j-cost-retail.properties");
		int scheduleId = -1; 
		if( args.length == 1)
			scheduleId = Integer.parseInt(args[0]);

		PropertyManager.initialize("analysis.properties");
		
		Connection conn = null;
		try{
			conn = DBManager.getConnection();
		
			PriceChangeStats pcs = new PriceChangeStats();
			
			if( scheduleId > 0 ){
				pcs.calculatePriceChangeStats(conn, scheduleId, 0);
			}
			else{
				int chainId = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID","-1"));
				if( chainId <= 0){
					logger.info( "Invalid Chain Id, Subscriber Chain Id needs to be set" );
					System.exit(-1);
				}
				String startDate = DateUtil.getWeekStartDate(0);
				String endDate  = DateUtil.getWeekEndDate();
				ScheduleDAO schDAO = new ScheduleDAO();
				logger.info( "Processing for Chain = " + chainId + ", StartDate/EndDate = " + startDate + "-" + endDate);

				ArrayList <ScheduleInfoDTO> schList = schDAO.getSchedulesByChain(conn,chainId, -1, startDate, endDate);
				logger.info( "# of Schedules Processed = " +schList.size());
				//do the processing in a loop
				for( ScheduleInfoDTO schedule:schList){
					pcs.calculatePriceChangeStats(conn, schedule.getScheduleId(), 0 );
					PristineDBUtil.commitTransaction(conn, "Comp Data LIG");
				}
			}
				

			
		}catch(GeneralException ge){
			logger.error("Error in Price change Stats Calcs", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}
		catch(Exception e){
			logger.error("Java Exception ", e);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}
		

		try{
			if( PropertyManager.getProperty("DATALOAD.COMMIT", "").equalsIgnoreCase("TRUE")){
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "Price change Stats");
			}
			else{
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "Price change Stats");
			}
		}catch(GeneralException ge){
			logger.error("Error in commit", ge);
			System.exit(1);
		}finally{
			PristineDBUtil.close(conn);
		}
	}
	
	public void calculatePriceChangeStats( Connection conn, int scheduleId, int plItemCount) throws GeneralException {
		
		logger.info("Calculating Price change stats for " + scheduleId);
		
			
		//Find the list of schedules for the Previous 6 months
		ScheduleDAO schDao = new ScheduleDAO ();
		ArrayList<Integer> prevScheduleList = schDao.getPreviousSchedules(conn, scheduleId);
		boolean historyPresent = true;
		if( prevScheduleList.isEmpty()){
			logger.info("No Previous schedule for the current list");
			//return;
			historyPresent = false;
		}
		//Create a CSV schedule list
		String prevSchedules = "";
		for ( int prevSchId : prevScheduleList){
			prevSchedules = prevSchedules +  prevSchId + ",";
		}
		compDataDao = new CompetitiveDataDAO (conn);
		
		//Removes the extra comma at the end
		if ( historyPresent){
			prevSchedules = prevSchedules.substring(0, prevSchedules.length()-1);
			logger.debug("Previous Schedules = " + prevSchedules);
			setupLastCheckData(conn, prevScheduleList.get(0));
		}
		
		
	
		//Get items in the current in the current schedule
		ArrayList<CompetitiveDataDTO> compDataList= compDataDao.getCompData(conn, scheduleId, -1, -1, true);
		int count = 0;
		int skipcount = 0;
		logger.info("No of Items to Analyze:" +compDataList.size());
		try{
			compDataDao.updatePriceChangeStatsAllItems(conn, scheduleId, Constants.PRICE_NO_CHANGE);
			for ( CompetitiveDataDTO compData : compDataList){
			
				boolean update = true;
				//For each item in the current schedule 
				//get the historical price
				compData.chgDirection = Constants.PRICE_NA;
				compData.saleChgDirection = Constants.PRICE_NA;
				if( compData.itemNotFound.equals("N") && compData.priceNotFound.equals("N")&& historyPresent ){
					
					if( skipUpdate(compData)){
						update = false;
					}else if( historyPresent && checkLastSchedule(compData)){
						;//nothing to be done
					}else if ( historyPresent){
						ArrayList prevPricesList  = compDataDao.getPreviousItemPrices(conn, prevSchedules, compData.itemcode);
						ArrayList <Double> prevRegPrices = (ArrayList <Double>)prevPricesList.get(0);
						ArrayList <Double> prevSalePrices = (ArrayList <Double>)prevPricesList.get(1);
						//Find change direction
						if( prevRegPrices.size() > 0){
							double lastPrice = prevRegPrices.get(0);
							if( Math.abs(compData.regPrice-lastPrice)<0.01d)
								compData.chgDirection = Constants.PRICE_NO_CHANGE;
							else if ( compData.regPrice > lastPrice)
								compData.chgDirection = Constants.PRICE_WENT_UP;
							else
								compData.chgDirection = Constants.PRICE_WENT_DOWN;
						}
						
						if( prevSalePrices.size() > 0 && compData.fSalePrice > 0){
							double lastSalePrice = prevSalePrices.get(0);
							if( Math.abs(compData.fSalePrice-lastSalePrice)<0.01d)
								compData.saleChgDirection = Constants.PRICE_NO_CHANGE;
							else if ( compData.fSalePrice > lastSalePrice)
								compData.saleChgDirection = Constants.PRICE_WENT_UP;
							else
								compData.saleChgDirection = Constants.PRICE_WENT_DOWN;
						}
					}
					
				}
				count++;
				if( update){
					//check if the change direction values are different from the default. If not, skip update.
					if((compData.chgDirection == Constants.PRICE_NO_CHANGE) &&(compData.saleChgDirection == Constants.PRICE_NA))
						update = false;
				}
				
				//Update compData
				
				if( update){
					compDataDao.updatePriceChangeStats(conn, compData);
				}
				else
					skipcount++;
				if( count> 0 && count%10000 == 0 )
					logger.info("No of Items Processed:" +count + ", Skipped items = " + skipcount);
				//if( count == 1000 ) break;
				
				
				
				//if (count > 3)
				//break;
				//TO DO in the future - Handle Item not found condition if required
				//Not found X times (X being 3)
			}
			updatePerformanceStats(conn, scheduleId, plItemCount);
			logger.info("Price change Stats successfully completed for " + scheduleId);
		}catch(GeneralException e){
			logger.error("Error occured in calculatePriceChangeStats",e);
			e.printStackTrace();
			throw new GeneralException("Error occured in calculatePriceChangeStats",e);
		}
		
		
	}
	
	private void setupLastCheckData(Connection conn, int lastCheckScheduleId)  throws GeneralException {
		ArrayList<CompetitiveDataDTO> lastCheckCompDataList= compDataDao.getCompData(conn, lastCheckScheduleId, -1, -1, true);
		
		for ( CompetitiveDataDTO compData : lastCheckCompDataList){
			lastCheckMap.put(compData.itemcode, compData);
		}
			
	}
		
	private boolean checkLastSchedule(CompetitiveDataDTO compData) {
		boolean retVal = false;
		
		if( lastCheckMap.containsKey(compData.itemcode)){
			retVal = true;
			CompetitiveDataDTO lastCheckCompData = lastCheckMap.get(compData.itemcode);
			if ( compData.saleInd.equals("Y") && lastCheckCompData.saleInd.equals("Y")){
				//set the Sale direction
				double lastSalePrice = lastCheckCompData.fSalePrice; 
				if( Math.abs(compData.fSalePrice-lastSalePrice)<0.01d)
					compData.saleChgDirection = Constants.PRICE_NO_CHANGE;
				else if ( compData.fSalePrice > lastSalePrice)
					compData.saleChgDirection = Constants.PRICE_WENT_UP;
				else
					compData.saleChgDirection = Constants.PRICE_WENT_DOWN;
			}
			else if (compData.saleInd.equals("Y")){
				retVal = false;
			}
			double lastPrice = lastCheckCompData.regPrice;
			if( Math.abs(compData.regPrice-lastPrice)<0.01d)
				compData.chgDirection = Constants.PRICE_NO_CHANGE;
			else if ( compData.regPrice > lastPrice)
				compData.chgDirection = Constants.PRICE_WENT_UP;
			else
				compData.chgDirection = Constants.PRICE_WENT_DOWN;
			
		}
	
		return retVal;
	}

	private boolean skipUpdate(CompetitiveDataDTO compData) throws GeneralException {
		boolean retVal = false;
		if ( !compData.saleInd.equals("Y")){
			if( compData.effRegRetailStartDate != null && !compData.effRegRetailStartDate.isEmpty() ){
				Date effRegRetailStartDate = DateUtil.toDate(compData.effRegRetailStartDate);
				Date currentWeekStartDate = DateUtil.toDate(compData.weekStartDate);
				if( DateUtil.getDateDiff(currentWeekStartDate, effRegRetailStartDate) > 8 )
					retVal = true;
			}
		}
		
		return retVal;
	}
	
	private void updatePerformanceStats(Connection conn, int scheduleId, int plItemCount) throws GeneralException{
		PriceCheckStatsDTO priceCheckStatsDto = new PriceCheckStatsDTO ();
		
		PerformanceAnalysisDAO performanceDao = new PerformanceAnalysisDAO ();
		
		int existingPLCount = performanceDao.getPLCount(conn, scheduleId);
		priceCheckStatsDto.setPlItemsCount(plItemCount + existingPLCount);
		priceCheckStatsDto.setScheduleId(scheduleId);
		
		int itemCount =  performanceDao.getSummaryCompData(conn, scheduleId, null);
		priceCheckStatsDto.setItemCT(itemCount);
		priceCheckStatsDto.setItemsCheckedCT(itemCount);
		
		int itemsNotFoundCT = performanceDao.getSummaryCompData(conn, 
									scheduleId, " AND ITEM_NOT_FOUND_FLG = 'Y'");
		priceCheckStatsDto.setItemsNotFoundCT(itemsNotFoundCT);

		int priceNotFoundCT = performanceDao.getSummaryCompData(conn, 
				scheduleId, " AND PRICE_NOT_FOUND_FLG = 'Y'");
		priceCheckStatsDto.setPriceNotFoundCT(priceNotFoundCT);

		int  promotionCount = performanceDao.getSummaryCompData(conn, 
				scheduleId, " AND PROMOTION_FLG = 'Y'");
		priceCheckStatsDto.setOnSaleCT(promotionCount);

		int  wentUpCount = performanceDao.getSummaryCompData(conn, 
				scheduleId, " AND CHANGE_DIRECTION = " + Constants.PRICE_WENT_UP);
		priceCheckStatsDto.setWentUpCT(wentUpCount);

		int  wentDownCount = performanceDao.getSummaryCompData(conn, 
				scheduleId, " AND CHANGE_DIRECTION = " + Constants.PRICE_WENT_DOWN);
		priceCheckStatsDto.setWentDownCT(wentDownCount);
		
		int  noChangeCount = performanceDao.getSummaryCompData(conn, 
				scheduleId, " AND CHANGE_DIRECTION = " + Constants.PRICE_NO_CHANGE);
		priceCheckStatsDto.setNoChangeCT(noChangeCount);
		
		performanceDao.updatePriceCheckStat(conn, priceCheckStatsDto);
		
	}
}
