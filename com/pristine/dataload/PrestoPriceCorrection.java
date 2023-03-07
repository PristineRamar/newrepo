package com.pristine.dataload;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class PrestoPriceCorrection {

	private static Logger  logger = Logger.getLogger("PrestoPriceCorrection");
	private CompetitiveDataDAO compDataDao;
	
	private HashMap <Integer, ArrayList<Float>> regPriceItemMap = new HashMap <Integer, ArrayList<Float>>();
	private HashMap <Integer, ArrayList<Float>> regPriceLirIdMap = new HashMap <Integer, ArrayList<Float>>();
	
	private HashMap <Integer, ArrayList<Float>> salePriceItemMap = new HashMap <Integer, ArrayList<Float>>();
	private HashMap <Integer, ArrayList<Float>> salePriceLirIdMap = new HashMap <Integer, ArrayList<Float>>();
	
	private HashMap <Integer, ArrayList<Float>> csRegPriceItemMap= new HashMap <Integer, ArrayList<Float>>();
	private HashMap <Integer, ArrayList<Float>> csRegPriceLirIdMap= new HashMap <Integer, ArrayList<Float>>();
	
	private HashMap <Integer, ArrayList<Float>> csSalePriceItemMap= new HashMap <Integer, ArrayList<Float>>();
	private HashMap<Integer, ArrayList<Float>> csSalePriceLirIdMap = new HashMap <Integer, ArrayList<Float>>();
	
	private HashMap <Integer, CompetitiveDataDTO> controlStoreItemMap= new HashMap <Integer, CompetitiveDataDTO>();
	private HashMap <Integer, CompetitiveDataDTO> controlStoreLirIdMap= new HashMap <Integer, CompetitiveDataDTO>();
	
	private HashMap <Integer, ArrayList<Integer>> catItemMap= new HashMap <Integer, ArrayList<Integer>> ();
	private HashMap <Integer, ArrayList<Integer>> catLirIdMap= new HashMap <Integer, ArrayList<Integer>> ();

	private HashMap <Integer, Float> catChangePctMap= new HashMap <Integer, Float> ();
	private HashMap <Integer, ArrayList<Float>> catChangePctItemListMap= new HashMap <Integer, ArrayList<Float>> ();
	
	private HashMap <Integer, Float> changePctItemMap= new HashMap <Integer, Float> ();
	private HashMap <Integer, Float> changePctLirIdMap= new HashMap <Integer, Float> ();
	
	private HashMap <Integer, CompetitiveDataDTO> scrapeStoreItemMap= new HashMap <Integer, CompetitiveDataDTO>();
	private HashMap <Integer, CompetitiveDataDTO> scrapeStoreLirIdMap= new HashMap <Integer, CompetitiveDataDTO>();

	private static final String CONTROL_STORE_NA = "NA";
	private static final String CONTROL_STORE_SALE = "S";
	private static final String CONTROL_STORE_REGULAR = "R";
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		int scheduleId = -1;
		String scrapeStoreNum = "";
		if( args.length == 2){
			scheduleId = Integer.parseInt(args[0]);
			scrapeStoreNum = args[1];
		}
		else if ( args.length >1)
			logger.error("Insufficient Arguments - PrestoPriceCorrection [ScheduleId ScrapeStoreNum]");
		PropertyConfigurator.configure("log4j.properties");

		PropertyManager.initialize("analysis.properties");
		
		Connection conn = null;
		try{
			conn = DBManager.getConnection();
			PrestoPriceCorrection priceCorrector = new PrestoPriceCorrection (); 
			if( scheduleId > 0 ){
				priceCorrector.correctPrice(conn, scheduleId, scrapeStoreNum );
				PristineDBUtil.commitTransaction(conn, "PriceCorrector");
			}
			else{
				ScheduleDAO schDAO = new ScheduleDAO();
				
				//Get the list of Stores to be corrected.
				
				
				//For each store get the schedule for the store for current week
				//Log the store num and schedule Id
				//Correct the price for Schedule and Store Id.
				PristineDBUtil.commitTransaction(conn, "PriceCorrector");
			}
		}catch(GeneralException ge){
			logger.error("Error in Price change Stats Calcs", ge);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}
		catch(Exception e){
			e.printStackTrace();
			logger.error("Java Exception ", e);
			PristineDBUtil.rollbackTransaction(conn, "Data Load");
			System.exit(1);
		}

	}

	private void correctPrice(Connection conn, int scheduleId, String scrapeStoreNum) throws GeneralException {
		compDataDao = new CompetitiveDataDAO(conn);
		String controlStoreNum = PropertyManager.getProperty(  scrapeStoreNum + "_CONTROL_STORENUM","");;
		String baseStoreNum = PropertyManager.getProperty(  scrapeStoreNum + "_BASE_STORENUM","");
		
		if( baseStoreNum.equals("")){
			throw new GeneralException ( "XX_BASE_STORENUM cannot be empty");
		}
		
		ScheduleDAO schDao = new ScheduleDAO ();
		Date schStartDate = null;
		String schStartDateStr = null;
		ScheduleInfoDTO schedule = schDao.getScheduleInfo(conn, scheduleId);
		if(schedule != null){
			schStartDateStr = schedule.getStartDate();
			schStartDate = DateUtil.toDate(schStartDateStr, Constants.APP_DATE_FORMAT);
		}else{
			logger.error("Invalid Schedule");
			System.exit(-1);
		}
		
		logger.debug("Start Date - " + schStartDate);
		
		String baseStoreStartDate = DateUtil.getWeekStartDate(schStartDate, 26);
		String controlStoreStartDate = DateUtil.getWeekStartDate(schStartDate, 26);
		
		//Get the base store data
		ArrayList<ScheduleInfoDTO> schInfoList  = schDao.getScheduleListForStore(conn, baseStoreNum, -1, baseStoreStartDate, 182);
		
		for ( ScheduleInfoDTO schInfo : schInfoList ){
			/** Imp -- Change the API to get the category_id as well and populate it **/
			ArrayList<CompetitiveDataDTO> compDataList= compDataDao.getCompData(conn, schInfo.getScheduleId(), -1, -1, true);
			setupPrevHistory(compDataList);
		}
		logger.info("Previous History Setup Complete");
		
		//Populate Control Store Data
		//Control Store information is optional.
		if( !controlStoreNum.equals("")){
			ArrayList<ScheduleInfoDTO> controlStoreSchInfoList  = schDao.getScheduleListForStore(conn, controlStoreNum, -1, controlStoreStartDate, 189);
			for ( ScheduleInfoDTO schInfo : controlStoreSchInfoList ){
				ArrayList<CompetitiveDataDTO> compDataList= compDataDao.getCompData(conn, schInfo.getScheduleId(), -1, -1, true);
				setupControlStoreHistory(compDataList);
			}
			
			ScheduleInfoDTO csCurrSch = schDao.getSchedulesForStore(conn, controlStoreNum, 67, schStartDateStr);
			if(csCurrSch != null){
				ArrayList<CompetitiveDataDTO> compDataList= compDataDao.getCompData(conn, csCurrSch.getScheduleId(), -1, -1, true);
				setupControlStoreData(compDataList);
			}
		}
		logger.info("Control Store Data Setup Complete");
		
		//Get the scrape store data
		ArrayList<CompetitiveDataDTO> scrapeStoreDataList= compDataDao.getCompData(conn, scheduleId, -1, -1, true);
		setupScrapeStoreData(scrapeStoreDataList);
		logger.info("Scrape Store Data Setup Complete");
		
		//Find the change percentage for the category.
		setupMinimumChangePercentage(scrapeStoreDataList);
		
		//Correct the prices for Scrape store.
		ArrayList<CompetitiveDataDTO> toBeUpdatedList = applyCorrectionRules(scrapeStoreDataList);
		
		//Update corrected data
		compDataDao.updateCompetitiveData(toBeUpdatedList);
		compDataDao.updateCompetitiveDataComments(scrapeStoreDataList);
	}

	private void setupControlStoreData(ArrayList<CompetitiveDataDTO> compDataList) {
		HashMap <Integer, CompetitiveDataDTO> controlStoreMap = null;
		int key = 0;
		for ( CompetitiveDataDTO compData : compDataList){
			if ( compData.lirId > 0){
				controlStoreMap = controlStoreLirIdMap;
				key = compData.lirId; 
			}else{
				controlStoreMap = controlStoreItemMap;
				key = compData.itemcode;
			}
			if( !controlStoreMap.containsKey(key)){
				controlStoreMap.put(key, compData);
			}
			
			if ( compData.lirId > 0){
				controlStoreLirIdMap = controlStoreMap;
			}else{
				controlStoreItemMap = controlStoreMap;
			}
		}
		
	}

	private void setupScrapeStoreData(ArrayList<CompetitiveDataDTO> compDataList) {
		HashMap <Integer, CompetitiveDataDTO> scrapeStoreMap = null;
		int key = 0;
		for ( CompetitiveDataDTO compData : compDataList){
			if ( compData.lirId > 0){
				scrapeStoreMap = scrapeStoreLirIdMap;
				key = compData.lirId; 
			}else{
				scrapeStoreMap = scrapeStoreItemMap;
				key = compData.itemcode;
			}
			if( !scrapeStoreMap.containsKey(key)){
				scrapeStoreMap.put(key, compData);
			}
			
			if ( compData.lirId > 0){
				scrapeStoreLirIdMap = scrapeStoreMap;
			}else{
				scrapeStoreItemMap = scrapeStoreMap;
			}
		}
		
	}
	
	private void setupPrevHistory(ArrayList<CompetitiveDataDTO> compDataList) {
		
		HashMap <Integer, ArrayList<Float>> regPriceMap = null;
		HashMap <Integer, ArrayList<Float>> salePriceMap = null;
		HashMap <Integer, ArrayList<Integer>> catItemMap = null;
		int key = 0;
		int categoryKey = 0;
	    
		for ( CompetitiveDataDTO compData : compDataList){
			if ( compData.lirId > 0){
				regPriceMap = regPriceLirIdMap;
				salePriceMap = salePriceLirIdMap;
				catItemMap = this.catLirIdMap;
				key = compData.lirId; 
			} else{
				regPriceMap = regPriceItemMap;
				salePriceMap = salePriceItemMap;
				catItemMap = this.catItemMap;
				key = compData.itemcode;
			}
			categoryKey = compData.categoryId;
			
			//Populate the regular price if there is a change in price compared to the last observation
			if( !compData.itemNotFound.equals("Y")){
				ArrayList <Float> regItemPriceList= null;
				if( regPriceMap.containsKey(key) ) {
					regItemPriceList = regPriceMap.get(key);
				}else{
					regItemPriceList = new ArrayList <Float> ();
				}
				regPriceMap.put( key, regItemPriceList);
				boolean addPrice = true; 
				/*int priceCount = regItemPriceList.size();
				if( priceCount > 0){
					float lastPrice = regItemPriceList.get(priceCount-1);
					if( Math.abs(lastPrice - compData.regPrice) < 0.01)
						addPrice = false;
				}*/
				if ( addPrice)
					regItemPriceList.add(compData.regPrice);
			}

			//Populate the Sale price map- if the price point already exists, do not populate
			if( !compData.itemNotFound.equals("Y")){
				ArrayList <Float> saleItemPriceList= null;
				if( salePriceMap.containsKey(key) ) {
					saleItemPriceList = salePriceMap.get(key);
				}else{
					saleItemPriceList = new ArrayList <Float> ();
				}
				salePriceMap.put( key, saleItemPriceList);
				boolean addPrice = true;
				/*for (float salePrice : saleItemPriceList){
					if( Math.abs(salePrice - compData.fSalePrice) < 0.01)
						addPrice = false;
				}*/
				if ( addPrice)
					saleItemPriceList.add(compData.fSalePrice);
			}
			
			ArrayList <Integer> catItemList= null;
			if( catItemMap.containsKey(categoryKey) ){
			    catItemList = catItemMap.get(categoryKey);
			    if(!catItemList.contains(key))
			    	catItemList.add(key);
			}else{
				catItemList = new ArrayList<Integer>();
			}
			catItemMap.put(categoryKey, catItemList);
			
			if ( compData.lirId > 0){
				regPriceLirIdMap = regPriceMap;
				salePriceLirIdMap = salePriceMap;
				catLirIdMap = catItemMap;
			} else{
				regPriceItemMap = regPriceMap;
				salePriceItemMap = salePriceMap;
				this.catItemMap = catItemMap;
			}
		}
	}

	private void setupControlStoreHistory(ArrayList<CompetitiveDataDTO> compDataList) {
		
		HashMap <Integer, ArrayList<Float>> regPriceMap = null;
		HashMap <Integer, ArrayList<Float>> salePriceMap = null;
		int key = 0;
		int categoryKey = 0;
	    
		for ( CompetitiveDataDTO compData : compDataList){
			if ( compData.lirId > 0){
				regPriceMap = csRegPriceLirIdMap;
				salePriceMap = csSalePriceLirIdMap;
				key = compData.lirId; 
			} else{
				regPriceMap = csRegPriceItemMap;
				salePriceMap = csSalePriceItemMap;
				key = compData.itemcode;
			}
			
			//Populate the regular price 
			if( !compData.itemNotFound.equals("Y")){
				ArrayList <Float> regItemPriceList= null;
				if( regPriceMap.containsKey(key) ) {
					regItemPriceList = regPriceMap.get(key);
				}else{
					regItemPriceList = new ArrayList <Float> ();
				}
				regPriceMap.put( key, regItemPriceList);
				boolean addPrice = true; 
				if ( addPrice)
					regItemPriceList.add(compData.regPrice);
			}

			//Populate the Sale price map
			if( !compData.itemNotFound.equals("Y")){
				ArrayList <Float> saleItemPriceList= null;
				if( salePriceMap.containsKey(key) ) {
					saleItemPriceList = salePriceMap.get(key);
				}else{
					saleItemPriceList = new ArrayList <Float> ();
				}
				salePriceMap.put( key, saleItemPriceList);
				boolean addPrice = true;
				if ( addPrice)
					saleItemPriceList.add(compData.fSalePrice);
			}
			
			if ( compData.lirId > 0){
				csRegPriceLirIdMap = regPriceMap;
				csSalePriceLirIdMap = salePriceMap;
			} else{
				csRegPriceItemMap = regPriceMap;
				csSalePriceItemMap = salePriceMap;
			}
		}
	}

	
		
	private void setupMinimumChangePercentage(ArrayList<CompetitiveDataDTO> scrapeStoreDataList){
		HashMap <Integer, Float> chgPctMap = null;
		for(CompetitiveDataDTO compDataDTO : scrapeStoreDataList){
			int key = 0;
			ArrayList<Float> regPriceList = null;
			ArrayList<Float> salePriceList = null;
			if(compDataDTO.lirId > 0){
				key = compDataDTO.lirId;
				regPriceList = regPriceLirIdMap.get(key);
				salePriceList = salePriceLirIdMap.get(key);
				chgPctMap = changePctLirIdMap;
			}else{
				key = compDataDTO.itemcode;
				regPriceList = regPriceItemMap.get(key);
				salePriceList = salePriceItemMap.get(key);
				chgPctMap = changePctItemMap;
			}
			
			if(regPriceList == null || salePriceList == null){
				continue;
			}
			
			float minChangePct = 0;
			float chgPct = 0;
			for(int i = 0; i < regPriceList.size() ; i++){
				float regPrice = regPriceList.get(i);
				if(salePriceList != null && salePriceList.size() > 0){
				float salePrice = salePriceList.get(i);
					// Find Minimum Change Percentage only when an item was on sale
					if(regPrice > 0 && salePrice > 0){
						chgPct = (regPrice - salePrice)/regPrice * 100;
						if((minChangePct == 0 && chgPct > minChangePct) || chgPct < minChangePct)
							minChangePct = chgPct;
					}
				}
			}
			
			if(minChangePct != 0){
				if(chgPctMap.containsKey(key)){
					float chgPctInMap =  chgPctMap.get(key);
					if(minChangePct < chgPctInMap)
						chgPctMap.put(key, minChangePct);
				}else{
					chgPctMap.put(key, minChangePct);
				}
			}
			
			if(compDataDTO.lirId > 0){
				changePctLirIdMap = chgPctMap;
			}else{
				changePctItemMap = chgPctMap;
			}
		}
	}
	
	private ArrayList<CompetitiveDataDTO> applyCorrectionRules(ArrayList<CompetitiveDataDTO> scrapeStoreDataList){
		
		logger.info("No of records to be corrected - " + scrapeStoreDataList.size());
		ArrayList<CompetitiveDataDTO> toBeUpdateList = new ArrayList<CompetitiveDataDTO>();
		int counter = 0;
		for(CompetitiveDataDTO scrapeStoreData : scrapeStoreDataList){
			counter++;
			boolean updateReqd = false;
			ArrayList<Float> regPriceList = null;
			ArrayList<Float> salePriceList = null;
			ArrayList<Float> csRegPriceList = null;
			ArrayList<Float> csSalePriceList = null;
			
			int key = 0;
			if(scrapeStoreData.lirId > 0){
				key = scrapeStoreData.lirId;
				regPriceList = regPriceLirIdMap.get(scrapeStoreData.lirId);
				salePriceList = salePriceLirIdMap.get(scrapeStoreData.lirId);
				csRegPriceList = csRegPriceLirIdMap.get(scrapeStoreData.lirId);
				csSalePriceList = csSalePriceLirIdMap.get(scrapeStoreData.lirId);
			}else{
				key = scrapeStoreData.itemcode;
				regPriceList = regPriceItemMap.get(scrapeStoreData.itemcode);
				salePriceList = salePriceItemMap.get(scrapeStoreData.itemcode);
				csRegPriceList = csRegPriceItemMap.get(scrapeStoreData.itemcode);
				csSalePriceList = csSalePriceItemMap.get(scrapeStoreData.itemcode);
			}
			
			if(regPriceList == null && salePriceList == null && csRegPriceList == null && csSalePriceList == null){
				scrapeStoreData.comment = "No History SUSPECT";
				continue;
			}
			
			boolean isCorrected = false;
			float scrapeStorePrice = scrapeStoreData.regPrice;
			String saleInd = scrapeStoreData.saleInd;
			boolean saleFlag = (saleInd.equals(String.valueOf(Constants.YES))) ? true : false;
			
			StringBuffer saleHistory = new StringBuffer("SPH(");
			saleHistory.append(getPriceHistory(salePriceList));
			saleHistory.append(")");
			
			StringBuffer regHistory = new StringBuffer("RPH(");
			regHistory.append(getPriceHistory(regPriceList));
			regHistory.append(")");
			
			StringBuffer csSaleHistory = new StringBuffer("CSSPH(");
			csSaleHistory.append(getPriceHistory(csSalePriceList));
			csSaleHistory.append(")");
			
			StringBuffer csRegHistory = new StringBuffer("CSRPH(");
			csRegHistory.append(getPriceHistory(csRegPriceList));
			csRegHistory.append(")");
			
			/* Rule 1 - If the current price is one of the prev sale prices for item/lir id, make the current price as Sale Price and reg price as  prev reg price */
			if(!isCorrected && !saleFlag && salePriceList != null && salePriceList.contains(scrapeStorePrice)){
				for(int i = 0 ; i < salePriceList.size() ; i++){
					if(scrapeStorePrice == salePriceList.get(i)){
						scrapeStoreData.fSalePrice = scrapeStorePrice;
						scrapeStoreData.fSaleMPrice = scrapeStoreData.regMPrice;
						scrapeStoreData.saleMPack = scrapeStoreData.regMPack;
						scrapeStoreData.saleInd = String.valueOf(Constants.YES);
						float regPriceHistorical = regPriceList.get(i);
						float regPriceLatest = getLatestPrice(regPriceList);
						
						if(regPriceHistorical > regPriceLatest)
							scrapeStoreData.regPrice = regPriceHistorical;
						else
							scrapeStoreData.regPrice = regPriceLatest;
						
						scrapeStoreData.regMPack = 0;
						scrapeStoreData.regMPrice = 0;
						String controlStoreStatus = null;
						String itemStatus = getItemStatusInControlStore(scrapeStoreData);
						
						if(CONTROL_STORE_SALE.equals(itemStatus)){
							if(scrapeStoreData.regPrice == getLatestRegPriceInControlStore(scrapeStoreData)){
								controlStoreStatus = "P";
							}else{
								controlStoreStatus = "S-P";
							}
							controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
							scrapeStoreData.comment = "C Rule 1(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							updateReqd = true;
						}else if(CONTROL_STORE_REGULAR.equals(itemStatus)){
							controlStoreStatus = "S-F";
							controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
							CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData);
							if(scrapeStorePrice >= controlStoreData.regPrice){
								scrapeStoreData.comment = "Rule 1(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
								updateReqd = false;
							}else{
								scrapeStoreData.comment = "C Rule 1(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
								updateReqd = true;
							}
						}else{
							controlStoreStatus = "NA";
							scrapeStoreData.comment = "C Rule 1(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							updateReqd = true;
						}
						
						isCorrected = true;
						logger.info("Correction applied for " + key + " " + scrapeStoreData.comment);
						break;
					}
				}
			}
			
			/* Rule 2 - If the current price is same as prev reg price, send it as reg price */
			if(!isCorrected && !saleFlag && regPriceList != null && getLatestPrice(regPriceList) == scrapeStorePrice){
				String controlStoreStatus = null;
				
				String itemStatus = getItemStatusInControlStore(scrapeStoreData);
				
				if(CONTROL_STORE_REGULAR.equals(itemStatus)){
					if(scrapeStoreData.regPrice == getLatestRegPriceInControlStore(scrapeStoreData)){
						controlStoreStatus = "P";
					}else{
						controlStoreStatus = "R-P";
					}
					controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
					scrapeStoreData.comment = "Rule 2(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
				}else if(CONTROL_STORE_SALE.equals(itemStatus)){
					controlStoreStatus = "R-F";
					controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
					CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData); 
					if(scrapeStorePrice <= controlStoreData.fSalePrice){
						scrapeStoreData.fSalePrice = scrapeStorePrice;
						scrapeStoreData.fSaleMPrice = 0;						
						scrapeStoreData.saleInd = String.valueOf(Constants.YES);
						scrapeStoreData.regPrice = controlStoreData.regPrice;
						scrapeStoreData.comment = "C Rule 2(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						updateReqd = true;
					}else{
						scrapeStoreData.comment = "Rule 2(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
					}
				}else{
					controlStoreStatus = "NA";
					scrapeStoreData.comment = "Rule 2(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
				}
				
				isCorrected = true;
				logger.info("Correction applied for " + key + " " + scrapeStoreData.comment);
			}
			
			/* Rule 3 - If the current price is greater than prev reg price, send it as reg price */
			if(!isCorrected && !saleFlag && regPriceList != null){
				if(scrapeStorePrice > getLatestPrice(regPriceList)){
					String controlStoreStatus = null;
					
					String itemStatus = getItemStatusInControlStore(scrapeStoreData);
					if(CONTROL_STORE_REGULAR.equals(itemStatus)){
						controlStoreStatus = "R-P";
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
						scrapeStoreData.comment = "Rule 3(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
					}else if(CONTROL_STORE_SALE.equals(itemStatus)){
						controlStoreStatus = "R-F";
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
						CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData); 
						if(scrapeStorePrice <= controlStoreData.fSalePrice){
							scrapeStoreData.fSalePrice = scrapeStorePrice;
							scrapeStoreData.fSaleMPrice = 0;						
							scrapeStoreData.saleInd = String.valueOf(Constants.YES);
							scrapeStoreData.regPrice = controlStoreData.regPrice;
							scrapeStoreData.comment = "C Rule 3(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							updateReqd = true;
						}else{
							scrapeStoreData.comment = "Rule 3(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						}
					}else{
						controlStoreStatus = "NA";
						scrapeStoreData.comment = "Rule 3(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
					}
					
					isCorrected = true;
					logger.info("Correction applied for " + key + " " + scrapeStoreData.comment);
				}
			}
			
			/* Rule 7 - If the published price is less than previous price, and if the published price falls in the range of historic 
			   regular price or one of the regular prices in the past, then send it as regular price. */ 
			if(!isCorrected){
				if(regPriceList != null && !saleFlag && scrapeStorePrice < getLatestPrice(regPriceList)){
					float maxRegPrice = 0;
					float minRegPrice = 0;
					for(int i = 0 ; i < regPriceList.size() ; i++){
						float regPrice = regPriceList.get(i);
						if(regPrice != 0){
							if( i == 0 ){
								maxRegPrice = regPrice;
								minRegPrice = regPrice;
							}else{
								if( regPrice > maxRegPrice )
									maxRegPrice = regPrice;
								
								if( regPrice < minRegPrice )
									minRegPrice = regPrice;
							}
						}
					}
					
					if(scrapeStorePrice <= maxRegPrice || (regPriceList != null && regPriceList.contains(scrapeStorePrice))){
						String controlStoreStatus = null;
						String itemStatus = getItemStatusInControlStore(scrapeStoreData);
						CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData); 
						if(CONTROL_STORE_REGULAR.equals(itemStatus)){
							controlStoreStatus = "R-P";
							controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
							if(minRegPrice !=0 && scrapeStorePrice >= minRegPrice && scrapeStorePrice <= maxRegPrice)
								scrapeStoreData.comment = "Rule 7(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							else if(scrapeStorePrice >= controlStoreData.regPrice)
								scrapeStoreData.comment = "Rule 7(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							else
								scrapeStoreData.comment = "Rule 7 SUSPECT (CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						}else if(CONTROL_STORE_SALE.equals(itemStatus)){
							controlStoreStatus = "R-F";
							controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
							if(scrapeStorePrice <= controlStoreData.fSalePrice){
								scrapeStoreData.fSalePrice = scrapeStorePrice;
								scrapeStoreData.fSaleMPrice = 0;						
								scrapeStoreData.saleInd = String.valueOf(Constants.YES);
								scrapeStoreData.regPrice = controlStoreData.regPrice;
								scrapeStoreData.comment = "C Rule 7(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
								updateReqd = true;
							}else{
								if(minRegPrice !=0 && scrapeStorePrice >= minRegPrice && scrapeStorePrice <= maxRegPrice)
									scrapeStoreData.comment = "Rule 7(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
								else
									scrapeStoreData.comment = "Rule 7 SUSPECT (CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							}
							
						}else{
							controlStoreStatus = "NA";
							if(minRegPrice !=0 && scrapeStorePrice >= minRegPrice && scrapeStorePrice <= maxRegPrice)
								scrapeStoreData.comment = "Rule 7(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							else
								scrapeStoreData.comment = "Rule 7 SUSPECT (CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						}
						
						isCorrected = true;
						logger.info("Correction applied for " + key + " " + scrapeStoreData.comment);
					}
				}
			}
			
			/* Rule 4 - If the published price does not match a ‘sale’ price for that item in the last 26 weeks, but it’s within the range of 
			 the ‘sale’ prices for that items within the last 26 weeks, retain the previous ‘regular’ price as the current ‘regular’ price.*/
			if(!isCorrected && !saleFlag && salePriceList != null){
				float maxSalePrice = 0;
				for(int i = 0 ; i < salePriceList.size() ; i++){
					float salePrice = salePriceList.get(i);
					if( i == 0 ){
						maxSalePrice = salePrice;
					}else{
						if( salePrice > maxSalePrice )
							maxSalePrice = salePrice;
					}
				}
				if(scrapeStorePrice <= maxSalePrice){
					scrapeStoreData.fSalePrice = scrapeStorePrice;
					scrapeStoreData.fSaleMPrice = scrapeStoreData.regMPrice;
					scrapeStoreData.saleMPack = scrapeStoreData.regMPack;
					scrapeStoreData.saleInd = String.valueOf(Constants.YES);
					scrapeStoreData.regPrice = getLatestPrice(regPriceList);
					scrapeStoreData.regMPack = 0;
					scrapeStoreData.regMPrice = 0;
					String controlStoreStatus = null;
					String itemStatus = getItemStatusInControlStore(scrapeStoreData);
					
					if(CONTROL_STORE_SALE.equals(itemStatus)){
						if(scrapeStoreData.regPrice == getLatestRegPriceInControlStore(scrapeStoreData)){
							controlStoreStatus = "P";
						}else{
							controlStoreStatus = "S-P";
						}
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
						scrapeStoreData.comment = "C Rule 4(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						updateReqd = true;
					}else if(CONTROL_STORE_REGULAR.equals(itemStatus)){
						controlStoreStatus = "S-F";
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
						CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData);
						if(scrapeStorePrice >= controlStoreData.regPrice){
							scrapeStoreData.comment = "Rule 4(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							updateReqd = false;
						}else{
							scrapeStoreData.comment = "C Rule 4(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							updateReqd = true;
						}
					}else{
						controlStoreStatus = "NA";
						scrapeStoreData.comment = "C Rule 4(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						updateReqd = true;
					}
					
					isCorrected = true;
					logger.info("Correction applied for " + key + " " + scrapeStoreData.comment);
				}
			}
			
			/* Rule 5 - If the published price is less than last regular price by at least the minimum % difference between ‘sale’ prices and corresponding 
			 ‘regular’ prices within last 26 weeks, mark published price as ‘sale’ price and last ‘regular’ price as current ‘regular’ price.*/
			if(!isCorrected && !saleFlag && regPriceList != null){
				float lastRegPrice = getLatestPrice(regPriceList);
				float chgPct = (lastRegPrice - scrapeStorePrice)/lastRegPrice * 100;
				
				float minChgPct = -999;
				int tempKey = 0;
				if(scrapeStoreData.lirId > 0){
					tempKey = scrapeStoreData.lirId;
					if(changePctLirIdMap != null && changePctLirIdMap.containsKey(tempKey)){
						minChgPct = changePctLirIdMap.get(tempKey);
					}
				}else{
					tempKey = scrapeStoreData.itemcode;
					if(changePctItemMap != null && changePctItemMap.containsKey(tempKey)){
						minChgPct = changePctItemMap.get(tempKey);
					}
				}
				
				if(minChgPct != -999 && chgPct > minChgPct){
					scrapeStoreData.fSalePrice = scrapeStorePrice;
					scrapeStoreData.fSaleMPrice = scrapeStoreData.regMPrice;
					scrapeStoreData.saleMPack = scrapeStoreData.regMPack;
					scrapeStoreData.saleInd = String.valueOf(Constants.YES);
					scrapeStoreData.regPrice = getLatestPrice(regPriceList);
					scrapeStoreData.regMPack = 0;
					scrapeStoreData.regMPrice = 0;
					String controlStoreStatus = null;
					String itemStatus = getItemStatusInControlStore(scrapeStoreData);
					
					if(CONTROL_STORE_SALE.equals(itemStatus)){
						if(scrapeStoreData.regPrice == getLatestRegPriceInControlStore(scrapeStoreData)){
							controlStoreStatus = "P";
						}else{
							controlStoreStatus = "S-P";
						}
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
						scrapeStoreData.comment = "C Rule 5(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						updateReqd = true;
					}else if(CONTROL_STORE_REGULAR.equals(itemStatus)){
						controlStoreStatus = "S-F";
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
						CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData);
						if(scrapeStorePrice >= controlStoreData.regPrice){
							scrapeStoreData.comment = "Rule 5(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							updateReqd = false;
						}else{
							scrapeStoreData.comment = "C Rule 5(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
							updateReqd = true;
						}
					}else{
						controlStoreStatus = "NA";
						scrapeStoreData.comment = "C Rule 5(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						updateReqd = true;
					}
					
					isCorrected = true;
					logger.info("Correction applied for " + key + " " + scrapeStoreData.comment);
				}
			}
			
			/* Rule 6 - If the published prices is a multi price and its lower than last regular price, the published prices is likely to be a ‘sale’ price */
			if(!isCorrected && !saleFlag && scrapeStoreData.regMPack > 1){
				if(regPriceList != null && scrapeStorePrice < getLatestPrice(regPriceList)){
					scrapeStoreData.fSaleMPrice = scrapeStorePrice;
					scrapeStoreData.fSalePrice = scrapeStoreData.regPrice;
					scrapeStoreData.saleMPack = scrapeStoreData.regMPack;
					scrapeStoreData.saleInd = String.valueOf(Constants.YES);
					scrapeStoreData.regMPrice = getLatestPrice(regPriceList);
					scrapeStoreData.regPrice = 0;
					scrapeStoreData.regMPack = 0;
					String controlStoreStatus = null;
					String itemStatus = getItemStatusInControlStore(scrapeStoreData);
					
					if(CONTROL_STORE_SALE.equals(itemStatus)){
						if(scrapeStoreData.regPrice == getLatestRegPriceInControlStore(scrapeStoreData)){
							controlStoreStatus = "P";
						}else{
							CompetitiveDataDTO compDataDto = getControlStoreData(scrapeStoreData);
							if(compDataDto.getSalePack() > 1)
								controlStoreStatus = "S-M-P";
							else
								controlStoreStatus = "S-P";
						}
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
					}else if(CONTROL_STORE_REGULAR.equals(itemStatus)){
						controlStoreStatus = "S-F";
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
					}else{
						controlStoreStatus = "NA";
					}
					scrapeStoreData.comment = "C Rule 6(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
					isCorrected = true;
					updateReqd = true;
					logger.info("Correction applied for " + key + " " + scrapeStoreData.comment);
				}
			}
			
			/* Rule 8 - If published price has a sale indicator, update reg price with the prev reg price */
			if(saleFlag && regPriceList != null){
				String controlStoreStatus = null;
				String itemStatus = getItemStatusInControlStore(scrapeStoreData);
				CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData);
				
				if(CONTROL_STORE_SALE.equals(itemStatus)){
					controlStoreStatus = "S-P";
					controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
				}else if(CONTROL_STORE_REGULAR.equals(itemStatus)){
					controlStoreStatus = "S-F";
					controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
				}else{
					controlStoreStatus = "NA";
				}
				
				float maxRegPrice = 0;
				float minRegPrice = 0;
				for(int i = 0 ; i < regPriceList.size() ; i++){
					float regPrice = regPriceList.get(i);
					if(regPrice != 0){
						if( i == 0 ){
							maxRegPrice = regPrice;
							minRegPrice = regPrice;
						}else{
							if( regPrice > maxRegPrice )
								maxRegPrice = regPrice;
							
							if( regPrice < minRegPrice )
								minRegPrice = regPrice;
						}
					}
				}
				
				// Update only if published price is not one of the regular prices in the past
				if(regPriceList != null ){
					if(regPriceList.contains(scrapeStorePrice)){
						scrapeStoreData.comment = "Rule 8(CS - " + controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString();
						isCorrected = true;
					}
					
					// Update only if published price is less than the range of regular price 
					if(!isCorrected){
						if((minRegPrice != 0 && scrapeStorePrice >= minRegPrice) && (maxRegPrice != 0 && scrapeStorePrice <= maxRegPrice)){
							scrapeStoreData.comment = "Rule 8(CS - " + controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString();
							isCorrected = true;
						}
					}
					
					if(isCorrected == true){
						if(controlStoreData != null && scrapeStorePrice < controlStoreData.regPrice){
							scrapeStoreData.regMPack = 0;
							scrapeStoreData.regPrice = 0;
							if(scrapeStoreData.saleMPack > 0){
								scrapeStoreData.fSaleMPrice = scrapeStoreData.fSalePrice * scrapeStoreData.saleMPack;
								scrapeStoreData.fSalePrice = 0;
							}else{
								scrapeStoreData.fSaleMPrice = 0;
							}
							scrapeStoreData.comment = "C Rule 8(CS - " + controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString();
							updateReqd = true;
						}
					}
				}
				
				if(!isCorrected && minRegPrice != 0 && scrapeStorePrice < minRegPrice){
					
					scrapeStoreData.regPrice = getLatestPrice(regPriceList);
					scrapeStoreData.regMPack = 0;
					scrapeStoreData.regMPrice = 0;
					if(scrapeStoreData.saleMPack > 0){
						scrapeStoreData.fSaleMPrice = scrapeStoreData.fSalePrice * scrapeStoreData.saleMPack;
						scrapeStoreData.fSalePrice = 0;
					}else{
						scrapeStoreData.fSaleMPrice = 0;
					}
						
					scrapeStoreData.comment = "C Rule 8(CS - "+ controlStoreStatus +")(PP - " + scrapeStorePrice + ")" + regHistory.toString();
					if(controlStoreData != null && scrapeStoreData.regPrice < controlStoreData.regPrice){
						scrapeStoreData.regMPack = 0;
						scrapeStoreData.regPrice = 0;
					}
					updateReqd = true;
				}
			}
			
			/* Rule 9 - If the item has no regular price history, send the price published as regular price only if
            Item is regular priced in the control store */
			if(!isCorrected && ((regPriceList == null) || (regPriceList == null && salePriceList == null))){
				CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData);
				String controlStoreStatus = null;
				String itemStatus = getItemStatusInControlStore(scrapeStoreData);
				if(controlStoreData != null){
					if(CONTROL_STORE_SALE.equals(itemStatus)){
						controlStoreStatus = "R-F";
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
						if(scrapeStorePrice <= controlStoreData.fSalePrice){
							scrapeStoreData.saleInd = "Y";
							scrapeStoreData.fSalePrice = scrapeStorePrice;
							scrapeStoreData.regPrice = controlStoreData.regPrice;
							scrapeStoreData.regMPrice = 0;
							updateReqd = true;
							isCorrected = true;
							scrapeStoreData.comment = "C Rule 9(CS - "+ controlStoreStatus + ")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						}else{
							scrapeStoreData.comment = "Rule 9 SUSPECT(CS - "+ controlStoreStatus + ")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						}
					}else if(CONTROL_STORE_REGULAR.equals(itemStatus)){
						controlStoreStatus = "R-P";
						controlStoreStatus = controlStoreStatus + getControlStoreComment(scrapeStoreData);
						if(scrapeStorePrice >= controlStoreData.regPrice){
							isCorrected = true;
							scrapeStoreData.comment = "Rule 9(CS - "+ controlStoreStatus + ")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						}else{
							scrapeStoreData.comment = "Rule 9 SUSPECT(CS - "+ controlStoreStatus + ")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
						}
					}
				}else{
					scrapeStoreData.comment = "Rule 9 SUSPECT(CS - NA)(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
				}
			}
			
			if(scrapeStoreData.comment == null){
				String itemStatus = getItemStatusInControlStore(scrapeStoreData);
				scrapeStoreData.comment = "SUSPECT(CS - " + itemStatus + ")(PP - " + scrapeStorePrice + ")" + regHistory.toString() + saleHistory.toString() + csRegHistory.toString() + csSaleHistory.toString();
			}
			
			if(scrapeStoreData.comment != null && scrapeStoreData.comment.length() > 300){
				scrapeStoreData.comment = scrapeStoreData.comment.substring(0, 299);
			}
			
			if(updateReqd)
				toBeUpdateList.add(scrapeStoreData);
			
			scrapeStoreData.newUOM = Constants.EMPTY;
		}
		logger.info("Counter " + counter);
		
		return toBeUpdateList;
	}
	
	/**
	 * This method returns latest price from the price list that is greater than 0
	 * @param priceList
	 * @return
	 */
	public float getLatestPrice(ArrayList<Float> priceList){
		for(float price : priceList){
			if(price != 0)
				return price;
		}
		return 0;
	}
	
	/*public String compareControlStorePrice(CompetitiveDataDTO scrapeStoreData){
		float controlStoreRegPrice = getLatestRegPriceInControlStore(scrapeStoreData);
		
		if(controlStoreRegPrice > 0){
			if(Math.abs(scrapeStoreData.regPrice - controlStoreRegPrice / scrapeStoreData.regPrice) > 5){
				return "P";
			}else{
				return "F";
			}
		}else{
			return "F";
		}
	}*/
	
	/**
	 * Sale Check Rule
	 * If the price provided (PP) is determined as regular price in the base store using level 1 rule, then check the control store. 
	 * If the item is on sale and the PP is less than or equal to control store sale price, 
	 * mark PP as sale price and use control store price as regular price.
	 */
	public boolean saleCheck(CompetitiveDataDTO scrapeStoreData, float priceProvided){
		boolean isItemOnSale = isItemOnSaleInControlStore(scrapeStoreData);
		if(isItemOnSale){
			CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData);
			if(priceProvided <= controlStoreData.fSalePrice){
				scrapeStoreData.fSalePrice = priceProvided;
				scrapeStoreData.fSaleMPrice = 0;						
				scrapeStoreData.saleInd = String.valueOf(Constants.YES);
				scrapeStoreData.regPrice = controlStoreData.regPrice;
				return true;
			}
			return false;
		}else{
			return false;
		}
	}
	
	/**
	 * Regular Check Rule
	 * If the price provided (PP) is determined as Sale price in the base store using level 1 rule, then check the control store. 
	 * If the item is regular priced in control store and the PP is greater than or equal to control store Reg price, mark PP as regular price.
	 */
	public boolean regularCheck(CompetitiveDataDTO scrapeStoreData, float priceProvided){
		boolean isItemRegularPriced = isItemRegularPricedInControlStore(scrapeStoreData);
		if(isItemRegularPriced){
			CompetitiveDataDTO controlStoreData = getControlStoreData(scrapeStoreData);
			if(priceProvided >= controlStoreData.regPrice){
				scrapeStoreData.regPrice = priceProvided;
				return true;
			}
			return false;
		}else{
			return false;
		}
	}

	/**
	 * This method returns control store data for the week for which correction is being applied
	 * @param scrapeStoreData
	 * @return
	 */
	public CompetitiveDataDTO getControlStoreData(CompetitiveDataDTO scrapeStoreData){
		CompetitiveDataDTO controlStoreData = null;
		int key = 0;
		if(scrapeStoreData.lirId > 0){
			key = scrapeStoreData.lirId;
			controlStoreData = controlStoreLirIdMap.get(key);
		}else{
			key = scrapeStoreData.itemcode;
			controlStoreData = controlStoreItemMap.get(key);
		}
		return controlStoreData;
	}
	
	/**
	 * This method returns if control store history is available
	 */
	public boolean isControlStoreHistoryExists(CompetitiveDataDTO scrapeStoreData){
	    int key = 0;
		ArrayList<Float> regPriceList = null;
        ArrayList<Float> salePriceList = null;
        
		if(scrapeStoreData.lirId > 0){
			key = scrapeStoreData.lirId;
			regPriceList = csRegPriceLirIdMap.get(key);
			salePriceList = csSalePriceLirIdMap.get(key);
		}else{
			key = scrapeStoreData.itemcode;
			regPriceList = csRegPriceItemMap.get(key);
			salePriceList = csSalePriceItemMap.get(key);
		}
		
		if((regPriceList != null && regPriceList.size() > 0) || (salePriceList != null && salePriceList.size() > 0))
			return true;
		else
			return false;
	}
	
	/**
	 * This method returns if the item is on regular/sale in control store for the current week in the 
	 * control store history
	 * Current week refers to the week for which correction is being applied
	 * @param scrapeStoreData
	 * @return
	 */
	public String getItemStatusInControlStore(CompetitiveDataDTO scrapeStoreData){
        CompetitiveDataDTO compDataDTO = getControlStoreData(scrapeStoreData);
		
		if(compDataDTO != null){
			if(compDataDTO.saleInd.equals("Y"))
				return CONTROL_STORE_SALE;
			else
				return CONTROL_STORE_REGULAR;
		}else
			return CONTROL_STORE_NA;
	}
	
	/**
	 * This method returns if the item is on sale in control store for the current week in the 
	 * control store history
	 * Current week refers to the week for which correction is being applied
	 * @param scrapeStoreData
	 * @return
	 */
	private boolean isItemOnSaleInControlStore(CompetitiveDataDTO scrapeStoreData) {
		String status = getItemStatusInControlStore(scrapeStoreData);
		if(status.equals(CONTROL_STORE_SALE))
			return true;
		else
			return false;
	}
	
	/**
	 * This method returns if the item is regular priced in control store for the current week in the 
	 * control store history
	 * Current week refers to the week for which correction is being applied
	 * @param scrapeStoreData
	 * @return
	 */
	private boolean isItemRegularPricedInControlStore(CompetitiveDataDTO scrapeStoreData) {
		String status = getItemStatusInControlStore(scrapeStoreData);
		if(status.equals(CONTROL_STORE_REGULAR))
			return true;
		else
			return false;
	}
	
	private String getControlStoreComment(CompetitiveDataDTO scrapeStoreData){
		StringBuffer comment = new StringBuffer("");
		
		CompetitiveDataDTO compDataDTO = getControlStoreData(scrapeStoreData);
		// comment.append(" ");
		// comment.append(getItemStatusInControlStore(scrapeStoreData));
		comment.append(" ");
		comment.append(compDataDTO.regPrice);
		comment.append("/");
		comment.append(compDataDTO.fSalePrice);
		
		return comment.toString();
	}
	
	/**
	 * This method returns latest regular price in control store
	 * @param scrapeStoreData
	 * @return
	 */
	public Float getLatestRegPriceInControlStore(CompetitiveDataDTO scrapeStoreData){
        int key = 0;
		
        ArrayList<Float> regPriceList = null;
        
		if(scrapeStoreData.lirId > 0){
			key = scrapeStoreData.lirId;
			regPriceList = csRegPriceLirIdMap.get(key);
	    }else{
			key = scrapeStoreData.itemcode;
			regPriceList = csRegPriceItemMap.get(key);
		}
		
		Float regPrice = 0.0f;
		
		if(regPriceList == null)
			return regPrice;
		
		regPrice = regPriceList.get(0) == 0 ? getLatestPrice(regPriceList) : regPriceList.get(0);
		
		return regPrice;
	}
	
	/**
	 * This method returns changes in price history
	 * @param priceList
	 * @return
	 */
	public String getPriceHistory(ArrayList<Float> priceList){
		StringBuffer priceHistory = new StringBuffer();
		if(priceList != null){
			int tempCnt = 0;
			ArrayList<Float> tempPriceList = new ArrayList<Float>();
			for(int i = 0; i < priceList.size(); i++){
				if(i == 0){
					tempPriceList.add(priceList.get(i));
					tempCnt++;
				}else{
					if(!(tempPriceList.get(tempCnt-1).floatValue() == priceList.get(i).floatValue())){
						tempPriceList.add(priceList.get(i));
						tempCnt++;
					}
				}
			}
			
			for(float regPrice : tempPriceList){
				priceHistory.append(regPrice);
				priceHistory.append(" ");
			}
		}
		return priceHistory.toString();
	}
	
	private void setupCategoryChangePercentage(int categoryId){
		if( !catChangePctMap.containsKey(categoryId) ){
			float minChangePct = 0;
			ArrayList<Float> chgPctList = new ArrayList<Float>();
			if( catItemMap.containsKey(categoryId) ){
				ArrayList<Integer> itemCodeList = catItemMap.get(categoryId);
				for(int itemCode : itemCodeList){
					ArrayList<Float> regPriceList = regPriceItemMap.get(itemCode);
					ArrayList<Float> excludeDupPrice = excludeDuplicatePrices(regPriceList);
					for(int i = 0 ; i < (excludeDupPrice.size() - 1) ; i++){
						float price1 = excludeDupPrice.get(i);
						float price2 = excludeDupPrice.get(i+1);
						float chgPct = (price1 - price2)/price1 * 100;
						chgPctList.add(chgPct);
						if((minChangePct == 0 && chgPct > minChangePct) || chgPct < minChangePct){
							minChangePct = chgPct;
						}
					}
				}
			}
			
			if( catLirIdMap.containsKey(categoryId) ){
				ArrayList<Integer> itemCodeList = catLirIdMap.get(categoryId);
				for(int itemCode : itemCodeList){
					ArrayList<Float> regPriceLirList = regPriceLirIdMap.get(itemCode);
					ArrayList<Float> excludeDupLirPrice = excludeDuplicatePrices(regPriceLirList);
					for(int i = 0 ; i < (excludeDupLirPrice.size() - 1) ; i++){
						float price1 = excludeDupLirPrice.get(i);
						float price2 = excludeDupLirPrice.get(i+1);
						float chgPct = (price1 - price2)/price1 * 100;
						chgPctList.add(chgPct);
						if((minChangePct == 0 && chgPct > minChangePct) || chgPct < minChangePct){
							minChangePct = chgPct;
						}
					}
				}
			}
			
			catChangePctItemListMap.put(categoryId, chgPctList);
			if(minChangePct != 0){
				catChangePctMap.put(categoryId, minChangePct);
			}
		}
	}
	
	private ArrayList<Float> excludeDuplicatePrices(ArrayList<Float> regPriceList){
		ArrayList<Float> excludeDupPrice = new ArrayList<Float>();
		for(float regPrice : regPriceList){
			if(!excludeDupPrice.contains(regPrice)){
				excludeDupPrice.add(regPrice);
			}
		}
		return excludeDupPrice;
	}

}
