package com.pristine.dataload.offermgmt.prediction;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.LocationGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.offermgmt.ItemDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemAdInfoDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRItemDisplayInfoDTO;
import com.pristine.dto.offermgmt.PRItemSaleInfoDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionOutputDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.RetailCalendarService;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.service.offermgmt.DisplayTypeLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionService;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.service.offermgmt.promotion.ItemLogService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class ItemPrediction {

	private static Logger logger = Logger.getLogger("ItemPrediction");

	private int noOfWeeksPriceHistory = 13;
	private int noOfWeeksCostHistory = 13;

	private static final String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static final String LOCATION_ID = "LOCATION_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	private static final String WEEK_START_DATE = "WEEK_START_DATE=";
	private static final String CATEGORY_ID = "CATEGORY_ID=";
	private static final String REG_MULTIPLE = "REG_MULTIPLE=";
	private static final String REG_PRICE = "REG_PRICE=";
	private static final String SALE_MULTIPLE = "SALE_MULTIPLE=";
	private static final String SALE_PRICE = "SALE_PRICE=";
	private static final String PROMO_TYPE_ID = "PROMO_TYPE_ID=";
	private static final String PAGE_NO = "PAGE_NO=";
	private static final String BLOCK_NO = "BLOCK_NO=";
	private static final String DISPLAY_TYPE_ID = "DISPLAY_TYPE_ID=";

	private int inputLocationLevelId = -1;
	private int inputLocationId = -1;
	private int inputProductLevelId = -1;
	private int inputProductId = -1;
	private String inputWeekStartDate = null;
	private int inputCategoryId = -1;
	private int inputRegMultiple = -1;
	private double inputRegPrice = -1;
	private int inputSaleMultiple = 0;
	private double inputSalePrice = 0;
	private int inputPromoTypeId = 0;
	private int inputPageNo = 0;
	private int inputBlockNo = 0;
	private int inputDisplayTypeId = 0;

	private Connection conn = null;

	List<RetailCalendarDTO> retailCalendarCache = new ArrayList<RetailCalendarDTO>();

	private String itemAnalysisLogPath = PropertyManager.getProperty("ITEM_PREDICTION_LOG_PATH");

	HashMap<ProductKey, PRItemDTO> authroizedItemMap = new HashMap<ProductKey, PRItemDTO>();

	HashMap<Integer, List<PRItemDTO>> ligAndItsMember = new HashMap<Integer, List<PRItemDTO>>();

	private int chainId = 0;

	public static void main(String[] args) {
		PropertyManager.initialize("recommendation.properties");
		PropertyConfigurator.configure("log4j.properties");
		
		ItemPrediction itemPrediction = new ItemPrediction();

		// Set input arguments
		itemPrediction.setArguments(args);

		// Call prediction
		itemPrediction.callPrediction();
	}

	private void setArguments(String[] args) {
		if (args.length > 0) {
			for (String arg : args) {
				if (arg.startsWith(LOCATION_LEVEL_ID)) {
					inputLocationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
				} else if (arg.startsWith(LOCATION_ID)) {
					inputLocationId = Integer.parseInt(arg.substring(LOCATION_ID.length()));
				} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
					inputProductLevelId = Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length()));
				} else if (arg.startsWith(PRODUCT_ID)) {
					inputProductId = Integer.parseInt(arg.substring(PRODUCT_ID.length()));
				} else if (arg.startsWith(WEEK_START_DATE)) {
					inputWeekStartDate = arg.substring(WEEK_START_DATE.length());
				} else if (arg.startsWith(CATEGORY_ID)) {
					inputCategoryId = Integer.parseInt(arg.substring(CATEGORY_ID.length()));
				} else if (arg.startsWith(REG_MULTIPLE)) {
					inputRegMultiple = Integer.parseInt(arg.substring(REG_MULTIPLE.length()));
				} else if (arg.startsWith(REG_PRICE)) {
					inputRegPrice = Double.parseDouble(arg.substring(REG_PRICE.length()));
				} else if (arg.startsWith(SALE_MULTIPLE)) {
					inputSaleMultiple = Integer.parseInt(arg.substring(SALE_MULTIPLE.length()));
				} else if (arg.startsWith(SALE_PRICE)) {
					inputSalePrice = Double.parseDouble(arg.substring(SALE_PRICE.length()));
				} else if (arg.startsWith(PROMO_TYPE_ID)) {
					inputPromoTypeId = Integer.parseInt(arg.substring(PROMO_TYPE_ID.length()));
				} else if (arg.startsWith(PAGE_NO)) {
					inputPageNo = Integer.parseInt(arg.substring(PAGE_NO.length()));
				} else if (arg.startsWith(BLOCK_NO)) {
					inputBlockNo = Integer.parseInt(arg.substring(BLOCK_NO.length()));
				} else if (arg.startsWith(DISPLAY_TYPE_ID)) {
					inputDisplayTypeId = Integer.parseInt(arg.substring(DISPLAY_TYPE_ID.length()));
				}
			}
		}
	}

	private void callPrediction() {
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();

		try {
			// initialize
			initialize();

			/*********************************************/
			ItemService itemService = new ItemService(executionTimeLogs);
			LocationGroupDAO locationGroupDAO = new LocationGroupDAO(getConnection());
			HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
			List<Integer> items = new ArrayList<Integer>();
			List<Integer> allStores = locationGroupDAO.getStoresOfLocation(inputLocationLevelId, inputLocationId);
			
			//
			if(inputProductLevelId == Constants.PRODUCT_LEVEL_ID_LIG) {
				ItemDAO itemDAO = new ItemDAO();
				List<PRItemDTO> itemList = itemDAO.getLigMembers(getConnection(), inputProductId);
				for(PRItemDTO itemDTO : itemList) {
					items.add(itemDTO.getItemCode());
				}
			} else {
				items.add(inputProductId);
			}
			
			List<PRItemDTO> allStoreItems = itemService.getAuthorizedItemsOfZoneAndStore(getConnection(), Constants.CATEGORYLEVELID, inputCategoryId,
					items, allStores);

			// Populating the authorized item map
			for (PRItemDTO itemDTO : allStoreItems) {
				authroizedItemMap.put(new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode()), itemDTO);
			}

			ligAndItsMember = populateLigDetailsInMap(authroizedItemMap);

			/*********************************************/

			/*********************************************/
			RetailCostServiceOptimized retailCostServiceOptimized = new RetailCostServiceOptimized(getConnection());
			PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();

			RetailCalendarDTO curCalDTO = new RetailCalendarService().getWeekCalendarDetail(retailCalendarCache, inputWeekStartDate);

			// Get all weekly calendar details
			HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new RetailCalendarService().getAllWeeks(retailCalendarCache);

			Set<Integer> itemSet = new HashSet<Integer>();
			for (Map.Entry<ProductKey, PRItemDTO> item : authroizedItemMap.entrySet()) {
				if (item.getKey().getProductLevelId() == Constants.ITEMLEVELID) {
					itemSet.add(item.getValue().getItemCode());
					itemDataMap.put(new ItemKey(item.getValue().getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR), item.getValue());
				}
			}

			List<String> priceAndStrategyZoneNos = new ArrayList<String>();
			priceAndStrategyZoneNos.add("18");

			PRStrategyDTO inputDTO = new PRStrategyDTO();
			inputDTO.setLocationId(0);

			logger.info("Getting price is started...");
			// Get price
			// TODO:: temporarily using Zone 18 price, actually need to take store list price
			pricingEngineDAO.getPriceDataOptimized(getConnection(), chainId, inputDTO, curCalDTO, null, noOfWeeksPriceHistory, itemDataMap, null,
					new HashMap<Integer, HashMap<ItemKey, PRItemDTO>>(), true, priceAndStrategyZoneNos, allStores);
			logger.info("Getting price is completed...");

			// Get cost
			// TODO:: temporarily using Zone 18 cost, actually need to take store list price
			// TODO:: 13 is hard coded for no of previous week history, 5 is hard coded for zone 18
			// Get non-cached item's zone and store cost history
			logger.info("Getting cost is started...");
			HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory = retailCostServiceOptimized.getCostHistory(chainId, curCalDTO,
					noOfWeeksCostHistory, allWeekCalendarDetails, itemSet, priceAndStrategyZoneNos, allStores);

			// Find latest cost of the item for zone and store
			retailCostServiceOptimized.getLatestCostOfZoneItems(itemCostHistory, itemSet, itemDataMap, chainId, 0, curCalDTO, noOfWeeksCostHistory,
					allWeekCalendarDetails);
			logger.info("Getting cost is completed...");

			List<PromoItemDTO> promoItems = new ArrayList<PromoItemDTO>();
			// update to authorizedMap and update lig level cost
			for (PRItemDTO itemDTO : itemDataMap.values()) {
				ProductKey productKey = new ProductKey(Constants.ITEMLEVELID, itemDTO.getItemCode());

				if (authroizedItemMap.get(productKey) != null) {
					PromoItemDTO promoItemDTO = new PromoItemDTO();
					PRItemDTO authorizedItem = authroizedItemMap.get(productKey);
					
					promoItemDTO.setProductKey(productKey);
					promoItemDTO.setUpc(authorizedItem.getUpc());
					promoItemDTO.setRetLirName(authorizedItem.getRetLirName());
					promoItemDTO.setItemName(authorizedItem.getItemName());
					promoItemDTO.setCategoryId(inputCategoryId);
					
					if(inputRegMultiple > 0 && inputRegPrice > 0) {
						promoItemDTO.setRegPrice(new MultiplePrice(inputRegMultiple, inputRegPrice));	
					} else {
						promoItemDTO.setRegPrice(PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice()));
					}
					
					promoItemDTO.setListCost(itemDTO.getListCost());
					promoItemDTO.setDealCost(itemDTO.getRecWeekDealCost());
					
					PRItemSaleInfoDTO saleInfo = new PRItemSaleInfoDTO();
					saleInfo.setSalePrice(new MultiplePrice(inputSaleMultiple, inputSalePrice));
					saleInfo.setPromoTypeId(inputPromoTypeId);
					promoItemDTO.setSaleInfo(saleInfo);
					
					PRItemAdInfoDTO adInfo = new PRItemAdInfoDTO();
					adInfo.setAdPageNo(inputPageNo);
					adInfo.setAdBlockNo(inputBlockNo);
					promoItemDTO.setAdInfo(adInfo);
					
					PRItemDisplayInfoDTO displayInfo = new PRItemDisplayInfoDTO();
					displayInfo.setDisplayTypeLookup(DisplayTypeLookup.get(inputDisplayTypeId));
					promoItemDTO.setDisplayInfo(displayInfo);
					
					promoItems.add(promoItemDTO);
				}
			}

			/*********************************************/

			callPredictionEngine(promoItems);
			
			// Commit transaction
			PristineDBUtil.commitTransaction(getConnection(), "Commit Recommendation");
			
		} catch (GeneralException | Exception | OfferManagementException e) {
			e.printStackTrace();
			logger.error(e.toString(), e);
		}
	}

	private void initialize() throws GeneralException, Exception {
		logger.debug("PromotionEngineService: initializing connection and properties");
		setConnection();

		retailCalendarCache = new RetailCalendarDAO().getFullRetailCalendar(getConnection());

		chainId = Integer.valueOf(new RetailPriceDAO().getChainId(getConnection()));

		itemAnalysisLogPath = itemAnalysisLogPath + "/" + inputLocationLevelId + "_" + inputLocationId + "_" + inputCategoryId + "_"
				+ inputProductLevelId + "_" + inputProductId + "_prediction.csv";
	}

	protected Connection getConnection() {
		return conn;
	}

	/**
	 * Sets database connection. Used when program runs in batch mode
	 * 
	 * @throws Exception
	 */
	protected void setConnection() throws Exception {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException e) {
				throw new Exception("Error in setConnection() - " + e);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	public HashMap<Integer, List<PRItemDTO>> populateLigDetailsInMap(HashMap<ProductKey, PRItemDTO> authorizedItems)
			throws OfferManagementException, GeneralException {
		HashMap<Integer, List<PRItemDTO>> retLirMap = new HashMap<Integer, List<PRItemDTO>>();

		try {
			logger.debug("populateLigDetailsInMap() : Start....");
			for (PRItemDTO prItem : authorizedItems.values()) {
				if (prItem.getRetLirId() > 0 && !prItem.isLir()) {
					List<PRItemDTO> tList = new ArrayList<PRItemDTO>();
					if (retLirMap.get(prItem.getRetLirId()) != null) {
						tList = retLirMap.get(prItem.getRetLirId());
					}
					tList.add(prItem);
					retLirMap.put(prItem.getRetLirId(), tList);
				}
			}
			// For debugging only
			/*
			 * for(Map.Entry<Integer, List<PRItemDTO>> items : retLirMap.entrySet()) { logger.debug("No of item in LIG:" +
			 * items.getKey() + "-" + items.getValue().size()); }
			 */
			// logger.debug("LIG Map : " + (retLirMap != null ? retLirMap.toString() : " NULL "));
			logger.debug("populateLigDetailsInMap() : End....");
		} catch (Exception ex) {
			logger.error("populateLigDetailsInMap() : Exception occurred : " + ex.getMessage());
			throw new GeneralException("Error in populateLigDetailsInMap() - " + ex.getMessage());
		}
		return retLirMap;
	}

	public void callPredictionEngine(List<PromoItemDTO> items) throws GeneralException, OfferManagementException {

		logger.debug("callPredictionEngine(): Start...");
		ItemLogService itemLogService = new ItemLogService();
		//PredictionService predictionService = new PredictionService();
		PredictionService predictionService = new PredictionService(null, null, ligAndItsMember);
		RetailCalendarDTO retailCalendarDTO = new RetailCalendarService().getWeekCalendarDetail(retailCalendarCache, inputWeekStartDate);

			// Form input for the prediction
		PredictionInputDTO predictionInputDTO = formPredictionInput(inputLocationLevelId, inputLocationId, Constants.CATEGORYLEVELID,
				inputCategoryId, retailCalendarDTO.getCalendarId(), items);

		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();

		// Call prediction engine
		PredictionOutputDTO predictionOutput = predictionService.predictMovement(conn, predictionInputDTO, executionTimeLogs, "PREDICTION_PROMOTION",
				false);

		logger.debug("predictionOutput:" + predictionOutput.toString());
		// Update predictions
		updatePrediction(inputLocationLevelId, inputLocationId, items, predictionOutput);

		// logger.debug(" groupByCategory : " + (groupByCategory != null ? groupByCategory.toString() : " NULL "));
		logger.debug("callPredictionEngine(): End...");

		/*
		 * logger.debug("PrestoItemCode,RetLirId,UPC,CategoryId,RegMultiple,RegPrice,SaleMultiple,SalePrice,PromoTypeId," +
		 * "AdPageNo,AdBlockNo,DisplayTypeId,ListCost,DealCost,PredictionStatus,PredictedMovement");
		 */

		List<PromoItemDTO> itemWithPredictions = new ArrayList<PromoItemDTO>();
		// For debugging alone
		for (PromoItemDTO promoItem : items) {
			promoItem.setAdditionalDetailForLog("Prediction");
			
			double movement = 0;
			movement = (promoItem.getPredStatus() == PredictionStatus.SUCCESS ? promoItem.getPredMov() : 0);
			
			Double cost = (promoItem.getDealCost() != null ? promoItem.getDealCost() : promoItem.getListCost());

//			logger.debug("cost:" + cost + ",movement:" + movement + ",sale price:" + promoItem.getSaleInfo().toString());
			
			Double margin = PRCommonUtil.getMarginDollar(promoItem.getSaleInfo().getSalePrice(), cost, movement);
			promoItem.setPredMar(margin != null ? margin : 0);
			
			Double sales = PRCommonUtil.getSalesDollar(promoItem.getSaleInfo().getSalePrice(), movement);
			promoItem.setPredRev(sales != null ? sales : 0);
			
			itemWithPredictions.add(promoItem);
		}
		
		itemLogService.writeToCSVFile(itemAnalysisLogPath, itemWithPredictions);
	}
	
	private PredictionInputDTO formPredictionInput(int locationLevelId, int locationId, int productLevelId, int productId, int recWeekCalendarId,
			List<PromoItemDTO> items) {

		logger.debug("formPredictionInput(): Start...");
		PredictionInputDTO predictionInputDTO = new PredictionInputDTO();
		predictionInputDTO.locationId = locationId;
		predictionInputDTO.locationLevelId = locationLevelId;
		predictionInputDTO.productId = productId;
		predictionInputDTO.productLevelId = productLevelId;
		predictionInputDTO.startCalendarId = recWeekCalendarId;
		predictionInputDTO.endCalendarId = recWeekCalendarId;
		predictionInputDTO.predictionItems = new ArrayList<PredictionItemDTO>();

		for (PromoItemDTO item : items) {
			PredictionItemDTO predictionItemDTO = formPredictionItemDTO(item);
			if (predictionItemDTO != null) {
				predictionInputDTO.predictionItems.add(predictionItemDTO);
			}
		}

		//logger.debug("predictionInputDTO values are : " + predictionInputDTO.toString());
		logger.debug("formPredictionInput(): End...");
		return predictionInputDTO;
	}

	private PredictionItemDTO formPredictionItemDTO(PromoItemDTO item) {
		logger.debug("formPredictionItemDTO(): Start...");
		PredictionItemDTO predictionItemDTO = null;

		predictionItemDTO = new PredictionItemDTO();
		predictionItemDTO.lirInd = false;
		predictionItemDTO.itemCodeOrLirId = item.getProductKey().getProductId();
		predictionItemDTO.upc = item.getUpc();
		predictionItemDTO.pricePoints = new ArrayList<PricePointDTO>();

		if (item.getRegPrice() != null) {
			PricePointDTO pricePointDTO = formPricePoint(item);
			predictionItemDTO.pricePoints.add(pricePointDTO);
		}

		logger.debug("formPredictionItemDTO(): End...");
		return predictionItemDTO;
	}

	private PricePointDTO formPricePoint(PromoItemDTO promoItemDTO) {
		PricePointDTO pricePointDTO = new PricePointDTO();

		pricePointDTO.setRegPrice(promoItemDTO.getRegPrice().price);
		pricePointDTO.setRegQuantity(promoItemDTO.getRegPrice().multiple);

		pricePointDTO.setSaleQuantity(promoItemDTO.getSaleInfo().getSalePrice().multiple);
		pricePointDTO.setSalePrice(promoItemDTO.getSaleInfo().getSalePrice().price);
		pricePointDTO.setPromoTypeId(promoItemDTO.getSaleInfo().getPromoTypeId());

		pricePointDTO.setAdPageNo(promoItemDTO.getAdInfo().getAdPageNo());
		pricePointDTO.setAdBlockNo(promoItemDTO.getAdInfo().getAdBlockNo());

		pricePointDTO.setDisplayTypeId(promoItemDTO.getDisplayInfo().getDisplayTypeLookup().getDisplayTypeId());
		return pricePointDTO;
	}

	private void updatePrediction(int locationLevelId, int locationId, List<PromoItemDTO> items, PredictionOutputDTO predictionOutput) {

		logger.debug("updatePrediction(): Start...");
		HashMap<Integer, List<PricePointDTO>> predictionOutputMap = convertPredictionOutputDTOToPredictionOutputMap(predictionOutput);
		if (predictionOutputMap != null && predictionOutputMap.size() > 0) {
			for (PromoItemDTO itemDTO : items) {
				List<PricePointDTO> pricePointListFromPrediction = predictionOutputMap.get(itemDTO.getProductKey().getProductId());
				if (pricePointListFromPrediction != null) {

					int itemCode = itemDTO.getProductKey().getProductId();
					MultiplePrice curRegPrice = itemDTO.getRegPrice();

					PredictionDetailKey pdk = new PredictionDetailKey(locationLevelId, locationId, itemCode,
							(curRegPrice != null ? curRegPrice.multiple : 0), (curRegPrice != null ? curRegPrice.price : 0),
							(itemDTO.getSaleInfo().getSalePrice() != null ? itemDTO.getSaleInfo().getSalePrice().multiple : 0),
							(itemDTO.getSaleInfo().getSalePrice() != null ? itemDTO.getSaleInfo().getSalePrice().price : 0),
							itemDTO.getAdInfo().getAdPageNo(), itemDTO.getAdInfo().getAdBlockNo(),
							(itemDTO.getSaleInfo() != null ? itemDTO.getSaleInfo().getPromoTypeId()
									: 0),
							(itemDTO.getDisplayInfo().getDisplayTypeLookup() != null
									? itemDTO.getDisplayInfo().getDisplayTypeLookup().getDisplayTypeId()
									: 0));

					logger.debug("itemCode:" + itemCode + ",pdk:" + pdk.toString());

					for (PricePointDTO pricePointFromPrediction : pricePointListFromPrediction) {
						PredictionDetailKey pdkPricePoint = new PredictionDetailKey(locationLevelId, locationId,
								itemDTO.getProductKey().getProductId(), pricePointFromPrediction.getRegQuantity(),
								pricePointFromPrediction.getRegPrice(), pricePointFromPrediction.getSaleQuantity(),
								pricePointFromPrediction.getSalePrice(), pricePointFromPrediction.getAdPageNo(),
								pricePointFromPrediction.getAdBlockNo(), pricePointFromPrediction.getPromoTypeId(),
								pricePointFromPrediction.getDisplayTypeId());

						logger.debug("itemCode:" + itemCode + ",pricePointFromPrediction:" + pricePointFromPrediction.toString());

						if (pdk.equals(pdkPricePoint)) {
							// update current sale prediction
							itemDTO.setPredMov(pricePointFromPrediction.getPredictedMovement());
							itemDTO.setPredStatus(pricePointFromPrediction.getPredictionStatus());
						}
					}
				}
			}
		}
		logger.debug("updatePrediction(): End...");
	}

	private HashMap<Integer, List<PricePointDTO>> convertPredictionOutputDTOToPredictionOutputMap(PredictionOutputDTO predictionOutput) {
		logger.debug("convertPredictionOutputDTOToPredictionOutputMap(): Start...");
		HashMap<Integer, List<PricePointDTO>> predictionOutputMap = null;
		if (predictionOutput.predictionItems != null) {
			predictionOutputMap = new HashMap<Integer, List<PricePointDTO>>();
			for (PredictionItemDTO pItem : predictionOutput.predictionItems) {
				predictionOutputMap.put(pItem.itemCodeOrLirId, pItem.pricePoints);
			}
		} else {
			logger.warn("No output from prediction service");
		}
		logger.debug("predictionOutputMap : " + (predictionOutputMap != null ? predictionOutputMap.toString() : " NULL "));
		logger.debug("convertPredictionOutputDTOToPredictionOutputMap(): End...");

		return predictionOutputMap;
	}

}
