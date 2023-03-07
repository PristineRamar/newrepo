package com.pristine.service.offermgmt.oos;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
//import java.util.HashSet;
import java.util.List;
import java.util.Map;
//import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
//import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
//import com.pristine.dto.PriceZoneDTO;
//import com.pristine.dto.RetailCalendarDTO;
//import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductKey;
//import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
//import com.pristine.service.RetailPriceServiceOptimized;
//import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.oos.OOSCandidateItemImpl;
import com.pristine.service.offermgmt.oos.OOSService;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
//import com.pristine.util.offermgmt.PRFormatHelper;


public class OOSPrediction {

	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String WEEK_START_DATE = "WEEK_START_DATE=";
	private static final String WEEK_TYPE = "WEEK_TYPE=";
	
	private static Logger  logger = Logger.getLogger("OOSPrediction");
	private int locationLevelId = 0;
	private int locationId = 0;
	private int inputProductLevelId = 0;
	private int inputProductId = 0;
	private int weekCalendarId = 0;
	String weekStartDate = null;
	Connection conn = null;
	
	public OOSPrediction(){
		try{
			conn = DBManager.getConnection();
		}
		catch(GeneralException ge){
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-oos-prediction.properties");
		PropertyManager.initialize("recommendation.properties");
		String location = null;
		String locationLevel = null;
		String product = null;
		String productLevel = null;
		String weekType = null;
		OOSPrediction saleItemPrediction = new OOSPrediction();
		for(String arg: args){
			if(arg.startsWith(LOCATION_ID)){
				location = arg.substring(LOCATION_ID.length());
			}
			else if(arg.startsWith(LOCATION_LEVEL_ID)){
				locationLevel = arg.substring(LOCATION_LEVEL_ID.length());
			}
			else if(arg.startsWith(PRODUCT_ID)){
				product = arg.substring(PRODUCT_ID.length());
			}
			else if(arg.startsWith(PRODUCT_LEVEL_ID)){
				productLevel = arg.substring(PRODUCT_LEVEL_ID.length());
			}
			else if(arg.startsWith(WEEK_START_DATE)){
				saleItemPrediction.weekStartDate = arg.substring(WEEK_START_DATE.length());
			}
			else if(arg.startsWith(WEEK_TYPE)){
				weekType = arg.substring(WEEK_TYPE.length());
			}
		}
		logger.info("**********************************************");
		saleItemPrediction.predictOOSItems(location, locationLevel, product, productLevel, saleItemPrediction.weekStartDate, weekType);
		logger.info("**********************************************");
		
	}
	
	private void predictOOSItems(String location, String locationLevel, String product, String productLevel,
			String weekStartDate, String weekType) {
		try {
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			int chainId = Integer.parseInt(retailPriceDAO.getChainId(conn));
			OOSCandidateItemImpl oosCandidateItemImpl = new OOSCandidateItemImpl();
//			PredictionService predictionService = new PredictionService();
			List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
			
			// Validate and get inputs
			validateInputsAndGetInputValues(location, locationLevel, product, productLevel, weekStartDate, weekType);

			logger.info("*** Prediction for location: " + locationLevel + "-" + location + 
					" for week calendar id: " + weekCalendarId + " is Started... ***");
			
			// Get candidate items
			logger.info("Getting OOS Candidate Items is Started...");
			List<PRItemDTO> finalItemList = oosCandidateItemImpl.getAuthorizedItemOfWeeklyAd(conn,
					inputProductLevelId, inputProductId, locationLevelId, locationId, weekCalendarId, chainId);
			logger.debug("Total OOS Candidate Items:" + finalItemList.size());
			logger.info("Getting OOS Candidate Items is Completed...");
			
			if(finalItemList.size() == 0){
				logger.info("OOS candidate items not found.");
				return;
			}
			// Group items by category level
			List<PredictionInputDTO> predictionInputList = groupByProductLevel(finalItemList,weekCalendarId, locationId, locationLevelId);
//
			// Call prediction for each product
			for (PredictionInputDTO predictionInputDTO : predictionInputList) {
				logger.info("Calling Prediction Engine for product id: " + predictionInputDTO.productId
						+ " is Started...");
				HashMap<Integer, List<PRItemDTO>> ligMap = (HashMap<Integer, List<PRItemDTO>>) finalItemList.stream()
						.filter(x -> x.getCategoryProductId() == predictionInputDTO.productId && x.getRetLirId() > 0)
						.collect(Collectors.groupingBy(PRItemDTO::getRetLirId));
				
				PredictionService predictionService = new PredictionService(null, null, ligMap);
				
				predictionService.predictMovement(conn, predictionInputDTO, executionTimeLogs, "PREDICTION_PRICE_REC",
						false);
				logger.info("Calling Prediction Engine for product id: " + predictionInputDTO.productId
						+ " is Completed...");
				
				findAndStoreLocationForecast(finalItemList, weekCalendarId, locationId, locationLevelId, Constants.CATEGORYLEVELID,
						predictionInputDTO.productId);
				PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
			}
			
			//Save forecast into table
			logger.info("Saving forecast is started...");
//			findAndStoreLocationForecast(finalItemList, weekCalendarId, locationId, locationLevelId);
			logger.info("Saving forecast is completed.");
			
			//Find out the % of contribution
			//findClientForecastAndUpdate(finalItemList, weekCalendarId, locationId, locationLevelId);
			
			// Commit the transaction
//			PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
			
			logger.info("*** Prediction for location: " + locationLevel + "-" + location + 
					" for week calendar id: " + weekCalendarId + " is completed... ***");
		} catch (Exception | GeneralException e) {
			e.printStackTrace();
			PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
			logger.error("Exception in Prediction and transaction is rollbacked " + e + e.getMessage());
		} finally {
			PristineDBUtil.close(conn);
		}
	}
	
	private void validateInputsAndGetInputValues(String location, String locationLevel, String product, String productLevel, String weekStartDate,
			String weekType) throws GeneralException, ParseException {

		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		if (location == null || locationLevel == null)
			throw new GeneralException("Location id and Location level id are mandatory.");
		locationId = Integer.parseInt(location);
		locationLevelId = Integer.parseInt(locationLevel);

		if (product != null)
			inputProductId = Integer.parseInt(product);
		if (productLevel == null)
			inputProductLevelId = Constants.CATEGORYLEVELID;
		else
			inputProductLevelId = Integer.parseInt(productLevel);

		if (weekStartDate == null && weekType == null) {
			throw new GeneralException("Specify either WEEK_TYPE or WEEK_START_DATE to proceed further");
		}

		if (weekStartDate != null) {
			weekCalendarId = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK).getCalendarId();
		} else {
			if (weekType.equalsIgnoreCase(Constants.CURRENT_WEEK)) {
				Date currentDate = new Date();
				String currDateStr = dateFormat.format(currentDate);
				weekCalendarId = retailCalendarDAO.getCalendarId(conn, currDateStr, Constants.CALENDAR_WEEK).getCalendarId();
			}
		}
	}
	
	private List<PredictionInputDTO> groupByProductLevel(List<PRItemDTO> finalItemList,
			int weekCalendarId, int locationId, int locationLevelId) {
		List<PredictionInputDTO> predictionInputList = new ArrayList<PredictionInputDTO>();
		HashMap<ProductKey, List<PRItemDTO>> itemMap = new HashMap<ProductKey, List<PRItemDTO>>();
		for (PRItemDTO prItemDTO : finalItemList) {
			
//			logger.debug("*********** - 1");			
//			logger.debug("ItemCode:" + prItemDTO.getItemCode() 
//					+ ",Reg Prices:" + prItemDTO.getRegMPack() + "-" + prItemDTO.getRegMPrice() + "-" +  
//					prItemDTO.getRegPrice() + ",Sale Price: " + prItemDTO.getSaleMPack() + "-" + 
//					prItemDTO.getSaleMPrice() + "-" + prItemDTO.getSalePrice()  + "-" + prItemDTO.getCategoryProductId());
//logger.debug("*********** - 1");					
					
			if (prItemDTO.getRegPrice() > 0 || prItemDTO.getRegMPrice() > 0) {
				ProductKey productKey = new ProductKey(Constants.CATEGORYLEVELID, prItemDTO.getCategoryProductId());
				if (itemMap.get(productKey) == null) {
					List<PRItemDTO> itemList = new ArrayList<PRItemDTO>();
					itemList.add(prItemDTO);
					itemMap.put(productKey, itemList);
				} else {
					List<PRItemDTO> itemList = itemMap.get(productKey);
					itemList.add(prItemDTO);
					itemMap.put(productKey, itemList);
				}
			}
		}
		for (Map.Entry<ProductKey, List<PRItemDTO>> entry : itemMap.entrySet()) {
			ProductKey productKey = entry.getKey();
			PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
			predictionInputDTO.locationId = locationId;
			predictionInputDTO.locationLevelId = locationLevelId;
			predictionInputDTO.productId = productKey.getProductId();
			predictionInputDTO.productLevelId = productKey.getProductLevelId();
			predictionInputDTO.startCalendarId = weekCalendarId;
			predictionInputDTO.endCalendarId = weekCalendarId;
			predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();
			List<PRItemDTO> prItemList = entry.getValue();
			for (PRItemDTO prItemDTO : prItemList) {
				PredictionItemDTO predictionItemDTO = new PredictionItemDTO();
				predictionItemDTO.lirInd = false;
				predictionItemDTO.itemCodeOrLirId = prItemDTO.getItemCode();
				predictionItemDTO.upc = prItemDTO.getUpc();
				predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();
				PricePointDTO pricePointDTO = new PricePointDTO();
				if (prItemDTO.getRegMPack() > 1) {
					pricePointDTO.setRegPrice(prItemDTO.getRegMPrice());
					pricePointDTO.setRegQuantity(prItemDTO.getRegMPack());
				} else {
					pricePointDTO.setRegPrice(prItemDTO.getRegPrice());
				}

				if (prItemDTO.getSaleMPack() > 1) {
					pricePointDTO.setSalePrice(prItemDTO.getSaleMPrice());
					pricePointDTO.setSaleQuantity(prItemDTO.getSaleMPack());
				} else {
					pricePointDTO.setSalePrice(prItemDTO.getSalePrice());
					pricePointDTO.setSaleQuantity(prItemDTO.getSaleMPack());
				}
				pricePointDTO.setAdPageNo(prItemDTO.getPageNumber());
				pricePointDTO.setAdBlockNo(prItemDTO.getBlockNumber());
				pricePointDTO.setPromoTypeId(prItemDTO.getPromoTypeId());
				pricePointDTO.setDisplayTypeId(prItemDTO.getDisplayTypeId());

				predictionItemDTO.pricePoints.add(pricePointDTO);
				predictionInputDTO.predictionItems.add(predictionItemDTO);
				
//				logger.debug("ItemCode:" + prItemDTO.getItemCode() 
//						+ ",Reg Prices:" + prItemDTO.getRegMPack() + "-" + prItemDTO.getRegMPrice() + "-" +  
//						prItemDTO.getRegPrice() + ",Sale Price: " + prItemDTO.getSaleMPack() + "-" + 
//						prItemDTO.getSaleMPrice() + "-" + prItemDTO.getSalePrice()  + "-" + prItemDTO.getCategoryProductId());
			}
			predictionInputList.add(predictionInputDTO);
		}
		return predictionInputList;
	}
	
	private void findAndStoreLocationForecast(List<PRItemDTO> itemList, int weekCalendarId, int locationId, int locationLevelId,
			int productLevelId, int productId) throws GeneralException {
		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
		OOSService oosService = new OOSService();
		List<OOSItemDTO> oosItems = new ArrayList<OOSItemDTO>();
		// 1. Delete if predictions are already there
		logger.info("Deleting existing forecast is started...");
		int deleteCount = oosAnalysisDAO.deleteForecastItem(conn, locationLevelId, locationId, productLevelId, productId, weekCalendarId);
		logger.info("# of rows deleted - " + deleteCount);
		fillOOSItemList(itemList, oosItems, weekCalendarId, locationId, locationLevelId, productLevelId, productId);
		// 2. Get already predicted forecast for items
		logger.info("Getting predicted movement is started...");
		oosService.getWeeklyPredictedMovement(conn, locationLevelId, locationId, weekCalendarId, oosItems);
		logger.info("Getting predicted movement is completed.");
		// 3. Read and write in to this table
		logger.info("Inserting forecast OOS item is started for " + oosItems.size() + " items...");
		logger.info("No Of Items:" + oosItems.size());
		oosAnalysisDAO.insertForecastItem(conn, oosItems);
		logger.info("Inserting forecast OOS item is completed.");
	}
	
	private void fillOOSItemList(List<PRItemDTO> itemList, List<OOSItemDTO> oosItems, int weekCalendarId, int locationId, int locationLevelId,
			int productLevelId, int productId) {
		// Convert to OOS item object
		for (PRItemDTO itemDTO : itemList) {
			if (productId == 0 || (productId > 0 && itemDTO.getCategoryProductId() == productId)) {
				OOSItemDTO oosItemDTO = new OOSItemDTO();
				oosItemDTO.setProductLevelId(Constants.ITEMLEVELID);
				oosItemDTO.setProductId(itemDTO.getItemCode());
				oosItemDTO.setLocationId(locationId);
				oosItemDTO.setLocationLevelId(locationLevelId);
				oosItemDTO.setCalendarId(weekCalendarId);
				oosItemDTO.setPromoTypeId(itemDTO.getPromoTypeId());
				oosItemDTO.setAdPageNo(itemDTO.getPageNumber());
				oosItemDTO.setBlockNo(itemDTO.getBlockNumber());
				oosItemDTO.setDisplayTypeId(itemDTO.getDisplayTypeId());
				oosItemDTO.setClientChainLevelWeeklyMov(itemDTO.getAdjustedUnits());
				MultiplePrice regPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());
				MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(itemDTO.getSaleMPack(), itemDTO.getSalePrice(), itemDTO.getSaleMPrice());
				oosItemDTO.setRegPrice(regPrice);
				oosItemDTO.setSalePrice(salePrice);
				oosItemDTO.setDistFlag(itemDTO.getDistFlag());
				oosItems.add(oosItemDTO);
			}
		}
	}
	
//	private void findClientForecastAndUpdate(List<PRItemDTO> itemList, int weekCalendarId, int locationId, int locationLevelId) throws GeneralException{
//		OOSAnalysisDAO oosAnalysisDAO = new OOSAnalysisDAO();
//		List<OOSItemDTO> oosItems = new ArrayList<OOSItemDTO>();
//		fillOOSItemList(itemList, oosItems, weekCalendarId, locationId, locationLevelId);
//		int noOfWeeks = Integer.parseInt(PropertyManager.getProperty("WEEKLY_AVG_MOV_HISTORY", "0"));
//		if(noOfWeeks == 0){
//			throw new GeneralException("Configuration WEEKLY_AVG_MOV_HISTORY is missing");
//		}
//		
//		
//		//Get the weekly ad location. 
//		int baseLocationLevelId = itemList.get(0).getWeeklyAdLocationLevelId();
//		int baseLocationId = itemList.get(0).getWeeklyAdLocationId();
//		
//		logger.info("Getting chain level movement average for last " + noOfWeeks + " is started...");
//		HashMap<ProductKey, OOSItemDTO> movementAvgMap = oosAnalysisDAO.getWeeklyMovAverageAtChain(conn, noOfWeeks,
//				baseLocationLevelId, baseLocationId, weekStartDate);
//		logger.info("Getting chain level movement average is completed.");
//
//		logger.info("Getting store level movement average for last " + noOfWeeks + " is started...");
//		oosAnalysisDAO.getWeeklyMovAverageForLocation(conn, noOfWeeks, locationLevelId, locationId, weekStartDate,
//				movementAvgMap);
//		logger.info("Getting store level movement average is completed.");
//		
//		
//		for(OOSItemDTO oosItemDTO: oosItems){
//			ProductKey productKey = new ProductKey(oosItemDTO.getProductLevelId(), oosItemDTO.getProductId());
//			if(movementAvgMap.get(productKey) != null){
//				OOSItemDTO movAvgObject = movementAvgMap.get(productKey);
//				//Chain level movement average.
//				long chainLevelMovAvg = movAvgObject.getxWeeksChainLevelAvgMov();
//				oosItemDTO.setxWeeksChainLevelAvgMov(chainLevelMovAvg);
//				//Store level movement average.
//				long storeLevelMovAvg = movAvgObject.getxWeeksStoreLevelAvgMov();
//				oosItemDTO.setxWeeksStoreLevelAvgMov(storeLevelMovAvg);
//				//Chain level client forecast.
//				long clientWeeklyMovAtChain = oosItemDTO.getClientChainLevelWeeklyMov();
//				//Identify the % of movement contribution of a store from chain level movement.
//				long clientWeeklyMovForLocation = 0;
//				if(chainLevelMovAvg > 0){
//					float x = (float) storeLevelMovAvg / chainLevelMovAvg;
//					float pctContributedByStr = x * 100;
//					oosItemDTO.setxWeeksStoreToChainPercent(PRFormatHelper
//							.roundToTwoDecimalDigitAsDouble((float) pctContributedByStr));
//					//Identify store level client movement.
//					clientWeeklyMovForLocation = (long) ((clientWeeklyMovAtChain * pctContributedByStr) / 100);
//					//Set store level client movement.
//					oosItemDTO.setClientWeeklyPredictedMovement(clientWeeklyMovForLocation);
//				}
//			}
//		}
//		logger.info("Updating client forecast is started...");
//		oosAnalysisDAO.updateStoreLevelClientForecast(conn, oosItems);
//		logger.info("Updating client forecast is completed.");
//	}
}
