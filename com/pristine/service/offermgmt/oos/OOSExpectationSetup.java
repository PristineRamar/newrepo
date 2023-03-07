package com.pristine.service.offermgmt.oos;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.oos.OOSExpectationDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.oos.OOSExpectationDTO;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class OOSExpectationSetup {

	private static Logger logger = Logger.getLogger("OOSExpectationSetup");
	private Connection conn = null;
	private OOSExpectationDAO oosExpDao = new OOSExpectationDAO();
	private int HIGH_MOVER_CUTOFF = 0;

	private int chainId;
	int storeTrxCount;
	String prevCalendarIds = "";
	String inputDate = "";
	
	public OOSExpectationSetup() {
		PropertyManager.initialize("recommendation.properties");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
	}

	public static void main(String[] args) {

		PropertyConfigurator.configure("log4j-oos-exp-setup.properties");

		String storeNo = null;
		boolean purgeData = false;
		String inputDate = "";
		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith("STORE_NO")) {
					storeNo = arg.substring("STORE_NO=".length());
				} else if (arg.startsWith("PURGE_DATA")) {
					String doPurge = arg.substring("PURGE_DATA=".length());
					if (doPurge.equalsIgnoreCase("TRUE"))
						purgeData = true;
				} else if (arg.startsWith("DATE")) {
					inputDate = arg.substring("DATE=".length());
				}
			}
		}

		if (storeNo == null) {
			logger.error("Mandatory parameter STORE_NO not passed");
			System.exit(1);
		}

		OOSExpectationSetup expSetup = new OOSExpectationSetup();
		expSetup.buildExpectation(storeNo, purgeData, inputDate);
		logger.info("Program completed...");
	}

	private void buildExpectation(String storeNo, boolean purgeData, String inputDate) {

		try {
			// Get the Store Id
			int storeId = getStoreId(storeNo);
			List<RetailCalendarDTO> calendarList = getCalendarInfo(inputDate);
			// Get the calendar Information
			int weekStartDayCalendarId = calendarList.get(0).getCalendarId();
			int currentDayCalendarId = calendarList.get(calendarList.size() - 1).getCalendarId();

			HIGH_MOVER_CUTOFF = Integer.parseInt(PropertyManager.getProperty("HIGH_MOVER_CUTOFF", "0"));
			if(HIGH_MOVER_CUTOFF == 0){
				logger.error("HIGH_MOVER_CUTOFF configuration is missing. Program quits...");
				return;
			}
			logger.debug(" Week start Calendar Id = " + weekStartDayCalendarId);
			logger.debug(" Current Day Calendar Id = " + currentDayCalendarId);
			// Build a list of Calendar Ids for previous days
			// create 2 functions
			List<Integer> prevCalendarIdList = new ArrayList<Integer>();
			for (int i = 0; i < calendarList.size() - 1; i++) {
				prevCalendarIdList.add(calendarList.get(i).getCalendarId());
			}
			logger.debug(" # of Days used to build Expectation " + prevCalendarIdList.size());

			// Purge all Data
			// Purge Data for today.
			purgeExpectations(storeId, purgeData, currentDayCalendarId);

			List<OOSItemDTO> oosItemMonitorList = getOOSCandidateItems(storeId, weekStartDayCalendarId);
			setupItemLevelExpectations(storeId, currentDayCalendarId, prevCalendarIdList, oosItemMonitorList);
			setupLIRLevelExpectations(storeId, currentDayCalendarId, oosItemMonitorList);

			PristineDBUtil.commitTransaction(conn, "OOSExpectation");

		} catch (Exception e) {
			logger.error("Unknown Java Exception ", e);
			PristineDBUtil.rollbackTransaction(conn, "OOSExpectation");
		} catch (GeneralException ge) {
			logger.error("Pristine General Exception ", ge);
			PristineDBUtil.rollbackTransaction(conn, "OOSExpectation");

		} finally {
			PristineDBUtil.close(conn);
		}

	}

	private void setupLIRLevelExpectations(int storeId, int currentDayCalendarId, List<OOSItemDTO> oosItemMonitorList) throws GeneralException {
		// create a mapping for LIR and Items
		HashMap<Integer, List<Integer>> lirItemMap = new HashMap<Integer, List<Integer>>();

		for (OOSItemDTO itemDTO : oosItemMonitorList) {
			int lirID = itemDTO.getRetLirId();
			if (lirID > 0) {
				// Check if exists in the map
				if (lirItemMap.containsKey(lirID)) {
					List<Integer> itemList = lirItemMap.get(lirID);
					itemList.add(itemDTO.getProductId());
				} else {
					List<Integer> itemList = new ArrayList<Integer>();
					itemList.add(itemDTO.getProductId());
					lirItemMap.put(lirID, itemList);
				}
			}
		}
		// Previous calendar ids as string and Store transaction count are
		// available in the previous method

		// Build csv of Lir Ids and Item Codes
		// Now Get LIR level stats

		// Create and setup a DTO
		ArrayList<OOSExpectationDTO> lirLevelTrxList = new ArrayList<OOSExpectationDTO>();

		Set<Integer> lirSet = lirItemMap.keySet();
		StringBuffer itemListStr = new StringBuffer();
		StringBuffer lirListStr = new StringBuffer();
		int count = 0;
		boolean appendSeperator = false;
		for (int lirID : lirSet) {
			if (appendSeperator)
				lirListStr.append(",");
			else
				appendSeperator = true;

			lirListStr.append(lirID);
			count++;
			List<Integer> itemList = lirItemMap.get(lirID);
			for (int item : itemList) {
				if (itemListStr.length() > 0)
					itemListStr.append(",");
				itemListStr.append(item);
			}

			if (count % 20 == 0) {
				logger.debug("items: " + itemListStr.toString());
				logger.debug("lir: " + lirListStr.toString());
				ArrayList<OOSExpectationDTO> lirInfoList = oosExpDao.getLirLevelTrxStats(conn, storeId, prevCalendarIds, storeTrxCount,
						lirListStr.toString(), itemListStr.toString());

				// add it to the list
				lirLevelTrxList.addAll(lirInfoList);
				appendSeperator = false;
				count = 0;
				lirListStr.setLength(0);
				itemListStr.setLength(0);
			}

		}

		if (lirListStr.length() > 0) {
			logger.debug("items: " + itemListStr.toString());
			logger.debug("lir: " + lirListStr.toString());
			ArrayList<OOSExpectationDTO> lirInfoList = oosExpDao.getLirLevelTrxStats(conn, storeId, prevCalendarIds, storeTrxCount,
					lirListStr.toString(), itemListStr.toString());
			lirLevelTrxList.addAll(lirInfoList);

		}
		logger.debug("Expection LIR Info: " + lirLevelTrxList.size());

		// Insert Array list
		oosExpDao.insertExpectationInfo(conn, currentDayCalendarId, lirLevelTrxList);
	}

	private void setupItemLevelExpectations(int storeId, int currentDayCalendarId, List<Integer> prevCalendarIdList,
			List<OOSItemDTO> oosItemMonitorList) throws GeneralException {

		// Get the store level Transaction count
		for (int calId : prevCalendarIdList) {
			if (prevCalendarIds.length() > 0)
				prevCalendarIds += ",";
			prevCalendarIds += calId;
		}
		logger.debug("Previous Days Calendar Ids = " + prevCalendarIds);
		storeTrxCount = oosExpDao.getStoreLevelTrxCount(conn, storeId, prevCalendarIds);
		logger.debug("Store Level Transaction Count = " + storeTrxCount);

		// Create and setup a DTO
		ArrayList<OOSExpectationDTO> itemLevelTrxList = new ArrayList<OOSExpectationDTO>();

		StringBuffer itemList = new StringBuffer();
		// Get Item Level expectation and Trx count
		int count = 0;
		boolean appendSeperator = false;
		for (OOSItemDTO itemDTO : oosItemMonitorList) {

			if (appendSeperator)
				itemList.append(",");
			else
				appendSeperator = true;
			itemList.append(itemDTO.getProductId());
			count++;

			if (count % 200 == 0) {
				// Get Transaction count and movement count by item
				// logger.debug("items: " + itemList.toString());
				ArrayList<OOSExpectationDTO> itemInfoList = oosExpDao.getItemLevelTrxStats(conn, storeId, prevCalendarIds, storeTrxCount,
						itemList.toString());

				// logger.debug("Expection Info: " + itemInfoList.size());
				itemLevelTrxList.addAll(itemInfoList);
				itemList.setLength(0);
				appendSeperator = false;
			}

		}
		// Add DTO to Array List
		if (itemList.length() > 0) {
			// logger.debug("items: " + itemList.toString());
			ArrayList<OOSExpectationDTO> itemInfoList = oosExpDao.getItemLevelTrxStats(conn, storeId, prevCalendarIds, storeTrxCount,
					itemList.toString());
			// logger.debug("Expection Info: " + itemInfoList.size());
			itemLevelTrxList.addAll(itemInfoList);
		}

		// Insert Array list
		oosExpDao.insertExpectationInfo(conn, currentDayCalendarId, itemLevelTrxList);
	}

	private List<OOSItemDTO> getOOSCandidateItems(int storeId, int weekStartDayCalendarId) throws GeneralException {

		String weekStartDate = DateUtil.getWeekStartDate(0);
		RetailCalendarDAO calendarDao = new RetailCalendarDAO();
		int weekCalendarId = calendarDao.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK).getCalendarId();
		OOSCandidateItemImpl oosCandidateItemImpl = new OOSCandidateItemImpl();
		//List<PRItemDTO> finalItemList = new ArrayList<PRItemDTO>();
		List<OOSItemDTO> finalItemList = oosCandidateItemImpl.getOOSCandidateItems(conn, Constants.STORE_LEVEL_ID, storeId, weekCalendarId);
		//finalItemList = oosCandidateItemImpl.getAuthorizedItemOfWeeklyAd(conn, 0, 0, Constants.STORE_LEVEL_ID, storeId, weekCalendarId, chainId);
		logger.debug("# of items in Promotion : " + finalItemList.size());

		List<OOSItemDTO> highMoverList = oosExpDao.getHighMoverItems(conn, storeId, weekStartDayCalendarId, HIGH_MOVER_CUTOFF);
		logger.debug("# of Highmover items : " + highMoverList.size());

		/* Create the finalItem List */
		Set<Integer> promotionItemSet = new HashSet<Integer>();

		for (OOSItemDTO promotionItem : finalItemList) {
			promotionItemSet.add(promotionItem.getProductId());
		}

		/*
		 * Add only the High mover items to promotionalList provided they are
		 * not present
		 */
		for (OOSItemDTO highMoverItem : highMoverList) {
			if (!promotionItemSet.contains(highMoverItem.getProductId())) {
				finalItemList.add(highMoverItem);
			}
		}

		logger.debug("# of items to Build Expectation: " + finalItemList.size());

		return finalItemList;
	}

	private int getStoreId(String storeNo) throws GeneralException {
		CompStoreDAO compStrdao = new CompStoreDAO();
		String[] storeArr = new String[1];
		storeArr[0] = storeNo;
		// Gets the subsriber Chain Id
		chainId = compStrdao.getCompChainId(conn);
		logger.debug("Subsriber chain id = " + chainId);
		HashMap<String, Integer> strIdMap = compStrdao.getCompStoreData(conn, chainId, storeArr);
		int storeId = -1;
		if (strIdMap.containsKey(storeNo)) {
			storeId = strIdMap.get(storeNo);
		} else {
			throw new GeneralException("Not able to find Store Id, perhaps store no is incorrect");
		}
		logger.debug("Expectation for Store Id = " + storeId);
		return storeId;
	}

	private List<RetailCalendarDTO> getCalendarInfo(String inputDate) throws GeneralException {
		String weekStartDate = DateUtil.getWeekStartDate(0);
		String currentDate = "";
		if(inputDate == "")
			currentDate = DateUtil.dateToString(new Date(), Constants.APP_DATE_FORMAT);
		else
			currentDate = inputDate;
		
		logger.debug("Week Start = " + weekStartDate + ", Current Date = " + currentDate);

		RetailCalendarDAO calendarDao = new RetailCalendarDAO();
		List<RetailCalendarDTO> calendarList = calendarDao.dayCalendarList(conn, DateUtil.toDate(weekStartDate), DateUtil.toDate(currentDate), "D");
		logger.debug(" Calendar list size = " + calendarList.size());

		if (calendarList.size() < 2) {
			throw new GeneralException("Cannot build Expectation, today should be Monday or greater");
		}

		return calendarList;
	}

	private void purgeExpectations(int storeId, boolean purgeAllData, int currentDayCalendarId) throws GeneralException {

		if (purgeAllData)
			currentDayCalendarId = -1;
		oosExpDao.clearOOSExpectation(conn, storeId, currentDayCalendarId);

	}

}

/*
 * Table definition
 * 
 * 
 **** Allocate Table space DROP TABLE OOS_TRX_BASED_EXPECTATION; create table
 * OOS_TRX_BASED_EXPECTATION ( calendar_id number (5) NOT NULL, STORE_ID number
 * (5) NOT NULL, product_LEVEL_ID number (2) NOT NULL, product_id number (6) NOT
 * NULL, STORE_level_trx_cnt number (5) NOT NULL, item_level_trx_cnt number (5)
 * NOT NULL, ITEM_LEVEL_unitS number (5) NOT NULL, constraint oos_exp_pk PRIMARY
 * KEY (calendar_id, store_id, product_level_id, product_id) );
 * 
 */