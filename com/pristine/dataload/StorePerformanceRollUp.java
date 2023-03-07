package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dao.DBManager;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.StorePerformanceRollupDAO;
import com.pristine.dao.StorePerformanceRollupDAO.LEVEL_TYPE_ENUM;
import com.pristine.dao.StorePerformanceRollupDAO.PRODUCT_LEVEL_ENUM;
import com.pristine.dto.StorePerformanceRollupSummaryDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class StorePerformanceRollUp
{
	private static Logger logger = Logger.getLogger("StorePerformanceRollUp");
	private String _weekEndDate;
	final private int VALUE_PROCESS_LOG = 1000;
	
	public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-Store-Performance-rollup.properties");
		PropertyManager.initialize("analysis.properties");
		
		String weekendDate = "";
		// to get weekend date and run for that week
		if (args.length > 0) {
			//logger.debug("Insufficient Arguments - PriceIndexRollUp weekend date");
			weekendDate = args[0].toString();
		} else {
			logger.warn("Insufficient Input : Week End Date missing");
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DAY_OF_MONTH, -7);
			weekendDate = DateUtil.getWeekEndDate(cal.getTime());
		} 
		logger.info("Week End Date " + weekendDate);

		Connection conn = null;
		try {

			conn = DBManager.getConnection();
			conn.setAutoCommit(true);

			StorePerformanceRollUp storePerformanceRollUp = new StorePerformanceRollUp();
			storePerformanceRollUp._weekEndDate = weekendDate;
			
			//district roll up
			logger.info("Processing District Summary Roll up");
			storePerformanceRollUp.performRollUp(conn);
			
			//region roll up
			logger.info("Processing Region Summary Roll up");
			storePerformanceRollUp.performSummaryWeeklyRollUp(conn,LEVEL_TYPE_ENUM.REGION, LEVEL_TYPE_ENUM.DISTRICT);
			
			//banner roll up
			logger.info("Processing Division Summary Roll up");
			storePerformanceRollUp.performSummaryWeeklyRollUp(conn,LEVEL_TYPE_ENUM.BANNER,LEVEL_TYPE_ENUM.REGION);
			
			//chain roll up
			logger.info("Processing Chain Summary Roll up");
			storePerformanceRollUp.performSummaryWeeklyRollUpForChain(conn,LEVEL_TYPE_ENUM.CHAIN,LEVEL_TYPE_ENUM.BANNER);

		} catch (GeneralException ge) {
			logger.error("Error in district roll up", ge);
			//PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		} catch (SQLException se) {
			logger.error("Error in district roll up", se);
			//PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}
		finally {
			//Close DB connection
			logger.info("Closing DB connection");
			PristineDBUtil.close(conn);
		}
	}

	public void performRollUp(Connection conn) throws GeneralException {
		//logger.debug("Start : Region Roll up ");
		StorePerformanceRollupDAO performanceDataDao = new StorePerformanceRollupDAO(conn);
		
		//Get district Data
		ArrayList<StorePerformanceRollupSummaryDTO> objDistList = performanceDataDao.getDistrictID(conn, 0);
		//logger.debug("Total Districts : " + districtList.size());

		if (objDistList.size() > 0) {
			//Run loop for each District
			for (StorePerformanceRollupSummaryDTO distDataDTO : objDistList) {
				int targetLevelId = distDataDTO.getLevelID();
				logger.info("Processing For District "
						+ String.valueOf(targetLevelId));

				//Get store data for the current processing District
				String storeList = performanceDataDao.getStoreID(conn,
						String.valueOf(targetLevelId));
				//logger.info("Store List " + storeList);

				if (storeList.length() > 0) {
					//Get the summary data based on list of stores
					ArrayList<StorePerformanceRollupSummaryDTO> summaryList = performanceDataDao
							.getSummaryInformation(conn, storeList, 
							_weekEndDate, PRODUCT_LEVEL_ENUM.PRODUCT_STORE_LEVEL);
					logger.info("Roll up data count " + String.valueOf(summaryList.size()));
					//Check for performance data existence 
					if (summaryList.size() > 0) {

						//Delete the existing record 
						performanceDataDao.deleteSummaryWeeklyRollup(conn,
						_weekEndDate, LEVEL_TYPE_ENUM.DISTRICT,	targetLevelId);

						//Run loop for each summary record
						int proRecSize = 0;
						for (StorePerformanceRollupSummaryDTO performanceSummaryData : summaryList) {
							proRecSize = proRecSize + 1;
							
							performanceSummaryData.setLevelID(targetLevelId);
							//Insert summary data
							performanceDataDao.insertSummaryWeeklyRollup(conn,
									_weekEndDate, performanceSummaryData);

							//Calculate and write processing log
							if ((proRecSize % VALUE_PROCESS_LOG) == 0) {
								logger.info("Records processed....."
										+ String.valueOf(proRecSize));
							}

						}
					} else {
						logger.info("No Summary Data found");
					}
				} 
				else {
					logger.info("No Store Data Found");
				}
			}
		}
		else {
			logger.info("No District Found");
		}
		
		//logger.debug("End : Region Roll up ");
	}

	public void performSummaryWeeklyRollUp(Connection conn, LEVEL_TYPE_ENUM LevelTypeID, LEVEL_TYPE_ENUM SourceLevelTypeID) throws GeneralException {

		//logger.info("Start : Region Roll up ");

		StorePerformanceRollupDAO performanceDataDao = new StorePerformanceRollupDAO(conn);
		ArrayList<StorePerformanceRollupSummaryDTO> regionList = null;

		//if the level type is region then select the available regions		
		if (LevelTypeID == LEVEL_TYPE_ENUM.REGION) {
			regionList = performanceDataDao.getRegions(conn, 0);
			//logger.debug("Total Region : " + regionList.size());
		}
		//if the level type is banner then select the available banner
		else if (LevelTypeID == LEVEL_TYPE_ENUM.BANNER) {
			regionList = performanceDataDao.getBanner(conn);
			//logger.debug("Total Division : " + regionList.size());
		}
		else return; 

		if (regionList.size() > 0) {
			//Run loop each Region / Division 
			for (StorePerformanceRollupSummaryDTO regionData : regionList) {
				int targetLevelID = regionData.getLevelID();
				if (LevelTypeID == LEVEL_TYPE_ENUM.REGION) {
					logger.info("Processing for Region " + 
										String.valueOf(targetLevelID));
				} else if (LevelTypeID == LEVEL_TYPE_ENUM.BANNER) {
					logger.info("Processing for Division " + 
										String.valueOf(targetLevelID));
				}
				
				
				ArrayList<StorePerformanceRollupSummaryDTO> rollupList = null;

				//if the type is regions then select the districts under that region
				if (LevelTypeID == LEVEL_TYPE_ENUM.REGION) {
					rollupList = performanceDataDao.getDistrictID(conn, 
																targetLevelID);
				}
				//if the type is banner then select the regions under that banner
				else if (LevelTypeID == LEVEL_TYPE_ENUM.BANNER) {
					rollupList = performanceDataDao.getRegions(conn,
																targetLevelID);
				}

				//Check the existence of Region / Division
				if (rollupList.size() > 0) {
					StringBuilder sbIDList = new StringBuilder();

					for (StorePerformanceRollupSummaryDTO rollupDTO : rollupList) {
						sbIDList.append(rollupDTO.getLevelID());
						sbIDList.append(",");
					}

					sbIDList.deleteCharAt(sbIDList.length() - 1);
					//logger.info("District / Region List " + sbIDList.toString());

					ArrayList<StorePerformanceRollupSummaryDTO> summaryList = 
						performanceDataDao.getSummaryRollupInformation(conn,
						SourceLevelTypeID, sbIDList.toString(), _weekEndDate,
										PRODUCT_LEVEL_ENUM.PRODUCT_STORE_LEVEL);

					logger.info("Roll up data count " + String.valueOf(summaryList.size()));

					if (summaryList.size() > 0) {

						//delete the data related to the specify level for the week end date
						performanceDataDao.deleteSummaryWeeklyRollup(conn,
										_weekEndDate, LevelTypeID, targetLevelID);

						int proRecSize = 0;
						for (StorePerformanceRollupSummaryDTO performanceSummaryData : summaryList) {
							proRecSize = proRecSize + 1;

							performanceSummaryData.setLevelTypeID(LevelTypeID.index());
							performanceSummaryData.setLevelID(regionData.getLevelID());
							
							//Insert the summary data
							performanceDataDao.insertSummaryWeeklyRollup(conn,
							_weekEndDate, performanceSummaryData);

							//Calculate and write processing log
							if ((proRecSize % VALUE_PROCESS_LOG) == 0) {
								logger.info("Records processed....." + 
												String.valueOf(proRecSize));
							}
						}
					}
					else {
						logger.info("No Summary Data Found");
					}
				} 
				else {
					logger.info("No District / Region Data Found");
				}
			}
		} 
		else {
			logger.info("No Region / Division Data Found");			
		}
		//logger.info("End : Region Roll up ");
	}


	public void performSummaryWeeklyRollUpForChain(Connection conn, LEVEL_TYPE_ENUM LevelTypeID, LEVEL_TYPE_ENUM SourceLevelTypeID) throws GeneralException {

		StorePerformanceRollupDAO performanceDataDao = new StorePerformanceRollupDAO(conn);
		
		int targetChainId = -1;
		
		//Get chain id
		StoreDAO objStoreDAO = new StoreDAO();
		targetChainId = objStoreDAO.getSubscriberCompetitorChain(conn); 
		logger.info("Processing For Chain " + String.valueOf(targetChainId));			
		
		//If chain id exist then proceed further
		if (targetChainId > 0) {
			
			//get Division id for chain
			ArrayList<StorePerformanceRollupSummaryDTO> divisionList = null;
			divisionList = performanceDataDao.getBanner(conn);
			
			//If division data exist then proceed next
			if (divisionList.size() > 0) {
				
				StringBuilder sbDivisionIDList = new StringBuilder();
				for (StorePerformanceRollupSummaryDTO divisionData : divisionList) {
					sbDivisionIDList.append(divisionData.getLevelID());
					sbDivisionIDList.append(",");
				}
				sbDivisionIDList.deleteCharAt(sbDivisionIDList.length() - 1);
				//logger.info("Division  List " + sbDivisionIDList.toString());
		
				//Get the summary data based on division
				ArrayList<StorePerformanceRollupSummaryDTO> summaryList = 
						performanceDataDao.getSummaryRollupInformation(conn, 
						SourceLevelTypeID, sbDivisionIDList.toString(), 
						_weekEndDate, PRODUCT_LEVEL_ENUM.PRODUCT_STORE_LEVEL);
				StorePerformanceRollupSummaryDTO chainData = new StorePerformanceRollupSummaryDTO();
				chainData.setLevelID(targetChainId);
				
				logger.info("Summary Record count " + String.valueOf(summaryList.size()));
				
				//Check the existence for Summary data
				if (summaryList.size() > 0) {
					//delete the old chain data for the week end date
					performanceDataDao.deleteSummaryWeeklyRollup (conn, _weekEndDate, LevelTypeID, targetChainId);
					
					int proRecSize = 0;
					
					for (StorePerformanceRollupSummaryDTO performanceSummaryData : summaryList) {
						
						proRecSize = proRecSize + 1;
						
						performanceSummaryData.setLevelTypeID(LevelTypeID.index());
						performanceSummaryData.setLevelID(targetChainId);
						performanceDataDao.insertSummaryWeeklyRollup (conn,_weekEndDate, performanceSummaryData);
						
						//Calculate and write processing log
						if ( (proRecSize % VALUE_PROCESS_LOG) == 0 ) {
							logger.info( "Records processed....." + String.valueOf(proRecSize));
						}
					}
				}			
				else {
					logger.info("No summary data Found");
				}
				
			}
			else {
				logger.info("No Banner data Found");
			}
		}
		else {
			logger.info("Chain Id Not Found");
		}
	}
}
