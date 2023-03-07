package com.pristine.service.offermgmt.prediction;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
//import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.oos.OOSAnalysisDAO;
import com.pristine.dao.offermgmt.prediction.PredictionDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.oos.OOSItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.oos.OOSCandidateItemImpl;
import com.pristine.service.offermgmt.oos.OOSService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;

public class LocationLevelPrediction {
	Connection conn = null;
	private static Logger logger = Logger.getLogger("LocationLevelPrediction");
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String WEEK_START_DATE = "WEEK_START_DATE=";
	private static final String WEEK_TYPE = "WEEK_TYPE=";
	private static final String NO_OF_FUTURE_WEEKS = "NO_OF_FUTURE_WEEKS=";
	private static final String DELETE_PREDICTION_CACHE = "DELETE_PREDICTION_CACHE=";
	private int locationLevelId = 0, locationId = 0, inputProductLevelId = 0, inputProductId = 0;
	private List<Integer> weeksToProcess = new ArrayList<Integer>();
	private int noOfFutureWeeks = 0;
	String weekStartDate = null;
	String weekType = null;
	private Boolean deletePredictionCache = false;
	
	public LocationLevelPrediction() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j-location-prediction.properties");
		PropertyManager.initialize("recommendation.properties");
		LocationLevelPrediction locationLevelPrediction = new LocationLevelPrediction();
		
		for (String arg : args) {
			if (arg.startsWith(LOCATION_ID)) {
				locationLevelPrediction.locationId = Integer.parseInt(arg.substring(LOCATION_ID.length()));
			} else if (arg.startsWith(LOCATION_LEVEL_ID)) {
				locationLevelPrediction.locationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
			} else if (arg.startsWith(WEEK_START_DATE)) {
				locationLevelPrediction.weekStartDate = arg.substring(WEEK_START_DATE.length());
			} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
				locationLevelPrediction.inputProductLevelId = Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length()));
			} else if (arg.startsWith(PRODUCT_ID)) {
				locationLevelPrediction.inputProductId = Integer.parseInt(arg.substring(PRODUCT_ID.length()));
			} else if(arg.startsWith(WEEK_TYPE)){
				locationLevelPrediction.weekType = arg.substring(WEEK_TYPE.length());
			} else if(arg.startsWith(NO_OF_FUTURE_WEEKS)){
				locationLevelPrediction.noOfFutureWeeks = Integer.parseInt(arg.substring(NO_OF_FUTURE_WEEKS.length()));
			} else if(arg.startsWith(DELETE_PREDICTION_CACHE)){
				locationLevelPrediction.deletePredictionCache = Boolean.parseBoolean(arg.substring(DELETE_PREDICTION_CACHE.length()));
			}
		}

		logger.info("**********************************************");
		locationLevelPrediction.predictItem();
		logger.info("**********************************************");
	}

	private void predictItem() {
		try {
			List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
//			PredictionService predictionService = new PredictionService();
			OOSCandidateItemImpl oosCandidateItemImpl = new OOSCandidateItemImpl();
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			int chainId = Integer.parseInt(retailPriceDAO.getChainId(conn));
			initializeVariables();

			// Process each week
			for (Integer weekCalendarId : weeksToProcess) {
				logger.info("Running for calendar Id: " + weekCalendarId + " is started...");
				List<PRItemDTO> finalItemList = oosCandidateItemImpl.getAuthorizedItemOfWeeklyAd(conn, inputProductLevelId, inputProductId,
						locationLevelId, locationId, weekCalendarId, chainId);

				HashMap<Integer, List<PRItemDTO>> ligMap = (HashMap<Integer, List<PRItemDTO>>) finalItemList.stream().filter(x -> x.getRetLirId() > 0)
						.collect(Collectors.groupingBy(PRItemDTO::getRetLirId));
				
				PredictionService predictionService = new PredictionService(null, null, ligMap);
				
				// Convert to item
				if (finalItemList.size() == 0) {
					logger.warn("No item to predict");
				} else {
					// Group items by category level
					List<PredictionInputDTO> predictionInputList = groupByProductLevel(finalItemList, weekCalendarId, locationId, locationLevelId);
					logger.info("No of Categories to be processed: " + predictionInputList.size());
					int processedCategoryCnt = 1;
					// Call prediction for each product
					for (PredictionInputDTO predictionInputDTO : predictionInputList) {
						try {
							// NU:: 8th Aug 2016, when the prediciton runs for
							// long
							// time, connection getting closed
							// and other categories are also not running
							if (conn.isClosed()) {
								conn = DBManager.getConnection();
								logger.info("Opening conneciton again...");
							}

							// Clear future weeks prediction or force predict
							// it, as weekly ad or promo would have updated
							if (deletePredictionCache) {
								PredictionDAO predictionDAO = new PredictionDAO();
								List<Integer> items = new ArrayList<Integer>();
								for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
									items.add(predictionItemDTO.itemCodeOrLirId);
								}
								logger.info("Deletion of cache for product id: " + predictionInputDTO.productId + " is Started...");
								predictionDAO.deletePrediction(conn, locationLevelId, locationId, weekCalendarId, items);
								logger.info("Deletion of cache for product id: " + predictionInputDTO.productId + " is Completed...");
							}

							logger.info("Calling Prediction Engine for product id: " + predictionInputDTO.productId + " is Started...");
							predictionService.predictMovement(conn, predictionInputDTO, executionTimeLogs, "PREDICTION_PRICE_REC", false);
							logger.info("Calling Prediction Engine for product id: " + predictionInputDTO.productId + " is Completed...");

							logger.info("Saving forecast is started...");
							findAndStoreLocationForecast(finalItemList, weekCalendarId, locationId, locationLevelId, Constants.CATEGORYLEVELID,
									predictionInputDTO.productId);
							logger.info("Saving forecast is completed.");

							PristineDBUtil.commitTransaction(conn, "Transaction is Committed");
							logger.info("No of Categories processed: " + processedCategoryCnt + "out of (" + predictionInputList.size() + ")");
							processedCategoryCnt = processedCategoryCnt + 1;
						} catch (Exception | GeneralException ex) {
							if (conn.isClosed()) {
								logger.error("Unable to predict as connection is closed");
							} else {
								ex.printStackTrace();
								PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
								logger.error("Exception in predictItem and transaction is rollbacked " + ex.toString() + ex + ex.getMessage());
							}
						}
					}
				}
			}
		} catch (Exception | GeneralException ex) {
			ex.printStackTrace();
			PristineDBUtil.rollbackTransaction(conn, "Transaction is Rollbacked");
			logger.error("Exception in predictItem and transaction is rollbacked " + ex.toString() + ex + ex.getMessage());
		} finally {
			PristineDBUtil.close(conn);
		}
	}
	
	private void initializeVariables() throws ParseException, GeneralException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		SimpleDateFormat dateFormat = new SimpleDateFormat(Constants.APP_DATE_FORMAT);
		RetailCalendarDTO retailCalendarDTO = null;

		inputProductLevelId = inputProductLevelId == 0 ? Constants.CATEGORYLEVELID : inputProductLevelId;

		if (weekStartDate != null) {
			retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK);
			weeksToProcess.add(retailCalendarDTO.getCalendarId());
		} else {
			if (weekType.equalsIgnoreCase(Constants.CURRENT_WEEK)) {
				Date currentDate = new Date();
				String currDateStr = dateFormat.format(currentDate);
				retailCalendarDTO = retailCalendarDAO.getCalendarId(conn, currDateStr, Constants.CALENDAR_WEEK);
				weeksToProcess.add(retailCalendarDTO.getCalendarId());
				// If week type is currentweek, then only do future week prediction
				// Get all future weeks
				LinkedHashMap<Integer, RetailCalendarDTO> futureCalendars = retailCalendarDAO.getAllFutureWeeks(conn,
						retailCalendarDTO.getStartDate(), noOfFutureWeeks);
				for(RetailCalendarDTO retailCalendarDTO2 : futureCalendars.values()) {
					weeksToProcess.add(retailCalendarDTO2.getCalendarId());
				}
			}
		}
	}
	
	private List<PredictionInputDTO> groupByProductLevel(List<PRItemDTO> finalItemList, int weekCalendarId, int locationId,
			int locationLevelId) {
		List<PredictionInputDTO> predictionInputList = new ArrayList<PredictionInputDTO>();
		HashMap<ProductKey, List<PRItemDTO>> itemMap = new HashMap<ProductKey, List<PRItemDTO>>();
		for (PRItemDTO prItemDTO : finalItemList) {
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

//				logger.debug("ItemCode:" + prItemDTO.getItemCode() + ",Reg Prices:" + prItemDTO.getRegMPack() + "-" + prItemDTO.getRegMPrice() + "-"
//						+ prItemDTO.getRegPrice() + ",Sale Price: " + prItemDTO.getSaleMPack() + "-" + prItemDTO.getSaleMPrice() + "-"
//						+ prItemDTO.getSalePrice() + "-" + prItemDTO.getCategoryProductId());
			}
			predictionInputList.add(predictionInputDTO);
		}
		return predictionInputList;
	}
	
	private void findAndStoreLocationForecast(List<PRItemDTO> itemList, int weekCalendarId, int locationId, int locationLevelId,
			int productLevelId, int productId)
			throws GeneralException {
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
		oosAnalysisDAO.insertForecastItem(conn, oosItems);
		logger.info("Inserting forecast OOS item is completed.");
	}
	
	private void fillOOSItemList(List<PRItemDTO> itemList, List<OOSItemDTO> oosItems, int weekCalendarId,
			int locationId, int locationLevelId, int productLevelId, int productId) {
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
			MultiplePrice regPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(),
					itemDTO.getRegMPrice());
			MultiplePrice salePrice = PRCommonUtil.getMultiplePrice(itemDTO.getSaleMPack(), itemDTO.getSalePrice(),
					itemDTO.getSaleMPrice());
			oosItemDTO.setRegPrice(regPrice);
			oosItemDTO.setSalePrice(salePrice);
			oosItemDTO.setDistFlag(itemDTO.getDistFlag());
			oosItemDTO.setRetLirId(itemDTO.getRetLirId());
			oosItems.add(oosItemDTO);
			}
		}
	}
}
