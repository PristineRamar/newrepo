package com.pristine.service.offermgmt.statistics;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.PriceCheckListDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class GeneralStatistics {
	Connection conn = null;
	private static Logger logger = Logger.getLogger("GeneralStatistics");
	private static String LOCATION_LEVEL_ID = "LOCATION_LEVEL_ID=";
	private static String LOCATION_ID = "LOCATION_ID=";
	private static String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static String PRODUCT_ID = "PRODUCT_ID=";
	private static String STATISTICS_TYPE_ID = "STATISTICS_TYPE_ID=";
	private static final String WEEK_START_DATE = "WEEK_START_DATE=";
	private int locationLevelId = 0;
	private String locationIds = "";
	private int productLevelId = 0;
	private String productIds = "";
	private int statisticsTypeId = 0;
	String weekStartDate = null;
	private int NO_OF_ROUNDING_DIGITS_CROSSING = 2;

	public GeneralStatistics() {
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException ge) {
			logger.error("Unable to connect database", ge);
			System.exit(1);
		}
	}

	public static void main(String[] args) throws OfferManagementException {
		PropertyConfigurator.configure("log4j-general-statistics.properties");
		PropertyManager.initialize("recommendation.properties");

		GeneralStatistics generalStatistics = new GeneralStatistics();

		for (String arg : args) {
			if (arg.startsWith(LOCATION_ID)) {
				generalStatistics.locationIds = arg.substring(LOCATION_ID.length());
			} else if (arg.startsWith(LOCATION_LEVEL_ID)) {
				generalStatistics.locationLevelId = Integer.parseInt(arg.substring(LOCATION_LEVEL_ID.length()));
			} else if (arg.startsWith(WEEK_START_DATE)) {
				generalStatistics.weekStartDate = arg.substring(WEEK_START_DATE.length());
			} else if (arg.startsWith(PRODUCT_LEVEL_ID)) {
				generalStatistics.productLevelId = Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length()));
			} else if (arg.startsWith(PRODUCT_ID)) {
				generalStatistics.productIds = arg.substring(PRODUCT_ID.length());
			} else if (arg.startsWith(STATISTICS_TYPE_ID)) {
				generalStatistics.statisticsTypeId = Integer.parseInt(arg.substring(STATISTICS_TYPE_ID.length()));
			}
		}

		generalStatistics.doStatistics1();
	}

	private void doStatistics1() throws OfferManagementException {
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		PricingEngineDAO pricingeEngineDAO = new PricingEngineDAO();

		String[] products = productIds.split(",");
		String[] locations = locationIds.split(",");

		List<RecommendationStatistics> recommendationStatistics = new ArrayList<RecommendationStatistics>();
		try {
			int weekCalendarId = retailCalendarDAO.getCalendarId(conn, weekStartDate, Constants.CALENDAR_WEEK).getCalendarId();

			// Each category
			for (String productIdStr : products) {
				int productId = Integer.valueOf(productIdStr);

				// Each Zone
				for (String locationIdStr : locations) {
					int locationId = Integer.valueOf(locationIdStr);
					List<Long> runIds = new ArrayList<Long>();

					logger.info("********Running for product:" + productId + ",location:" + locationId + "is Started........");

					// Get latest run id for the week
					long runId = pricingeEngineDAO.getLatestRecommendationRunId(conn, locationLevelId, locationId, productLevelId, productId,
							weekCalendarId);
					runIds.add(runId);

					// Get recommendation details
					HashMap<Long, List<PRItemDTO>> recItemsMap = pricingeEngineDAO.getRecItemsOfRunIds(conn, runIds);

					// Find # of items whose price points follow rules
					if (recItemsMap != null && recItemsMap.size() > 0) {
						if (statisticsTypeId == 1) {
							findIfPricePointFollowRules(locationId, productId, recItemsMap.get(runId));
						} else if (statisticsTypeId == 2) {
							// Find no of items whose recommended price is higher than current price and crossed 2 rounding digits
							recommendationStatistics.add(geRecommendationStats(runId, locationId, productId, recItemsMap.get(runId)));
						}
					}

					logger.info("********Running for product:" + productId + ",location:" + locationId + "is completed........");
				}
			}

			if (statisticsTypeId == 2) {
				generateReport(recommendationStatistics);
			}

		} catch (GeneralException e) {
			e.printStackTrace();
			logger.error("Exception in doStatistics1:" + e.toString());
		} finally {
			PristineDBUtil.close(conn);
		}
	}

	private void findIfPricePointFollowRules(int locationId, int productId, List<PRItemDTO> recommendedItems) {
		int noOfItems = 0;
		int noOfPricePointsFollowRules = 0;
		int totalPricePoints = 0;
		PricingEngineService pricingEngineService = new PricingEngineService();

		for (PRItemDTO itemDTO : recommendedItems) {
			if (!itemDTO.isLir()) {
				PRExplainLog explainLog = itemDTO.getExplainLog();

				// get rounding digits
				List<Double> roundingDigits = null;
				if (explainLog != null) {
					for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
						if ((guidelineAndConstraintLog.getConstraintTypeId() == ConstraintTypeLookup.ROUNDING.getConstraintTypeId())) {
							roundingDigits = guidelineAndConstraintLog.getRoundingDigits();
						}
					}
				}

				if (roundingDigits != null && roundingDigits.size() > 0) {
					noOfItems++;

					// Loop each rounding digit
					for (Double pricePoint : roundingDigits) {
						boolean isItemInConflict = false;
						totalPricePoints++;
						itemDTO.setRecommendedRegPrice(new MultiplePrice(1, pricePoint));

						// Reset all conflicts
						itemDTO.setIsConflict(0);
						for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
							guidelineAndConstraintLog.setIsConflict(false);
						}

						// Mark item as conflict, if any of the guideline/constraint in conflict
						for (PRGuidelineAndConstraintLog guidelineAndConstraintLog : explainLog.getGuidelineAndConstraintLogs()) {
							boolean isConflict = pricingEngineService.isRecommendedPriceBreaksGuidelinesOrConstraints(itemDTO,
									guidelineAndConstraintLog);
							if (isConflict) {
								itemDTO.setIsConflict(1);
								isItemInConflict = true;
								break;
							}
						}
						if (!isItemInConflict) {
							itemDTO.setIsConflict(0);
						}

						if (itemDTO.getIsConflict() == 0) {
							noOfPricePointsFollowRules++;
						}
					}
				}
			}
		}

		logger.info("***********************************************************************");
		logger.info("Results for location:" + locationId + ",product:" + productId + ",noOfItems:" + noOfItems + ",totalPricePoints:"
				+ totalPricePoints + ",noOfPricePointsFollowRules:" + noOfPricePointsFollowRules + ",avgPricePointPerItem:"
				+ PRFormatHelper.doubleToTwoDigitString(Double.valueOf(noOfPricePointsFollowRules) / Double.valueOf(noOfItems)));
		logger.info("***********************************************************************");
	}

	private RecommendationStatistics geRecommendationStats(long runId, int locationId, int productId, List<PRItemDTO> recommendedItems)
			throws OfferManagementException {
		int totalItems = 0;
		int totalPriceChange = 0, totalNextDollar = 0, totalCrossingRounding = 0;
		RecommendationStatistics recommendationStatistics = new RecommendationStatistics();

		HashMap<Long, PRStrategyDTO> runAndItsCategoryLevelStrategy = new HashMap<Long, PRStrategyDTO>();
		HashMap<Long, PRStrategyDTO> strategyMap = new HashMap<Long, PRStrategyDTO>();
		HashMap<Integer, Integer> checkListCrossingRoundingDigits = recommendationStatistics.checkListCrossingRoundingDigits;
		HashMap<Integer, Integer> checkListCrossingNextDollar = recommendationStatistics.checkListCrossingNextDollar;
		HashMap<Integer, Integer> checkListPriceChange = recommendationStatistics.checkListPriceChange;
		Set<Integer> distinctPriceCheckList = new HashSet<Integer>();

		recommendationStatistics.runId = runId;
		recommendationStatistics.locationId = locationId;
		recommendationStatistics.productId = productId;

		for (PRItemDTO itemDTO : recommendedItems) {
			if (!itemDTO.isLir()) {
				totalItems = totalItems + 1;
			}

			if (!itemDTO.isLir() && itemDTO.getIsNewPriceRecommended() == 1) {

				int priceCheckListId = itemDTO.getPriceCheckListId() == null ? 0 : itemDTO.getPriceCheckListId();
				Double recUnitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRecommendedRegPrice(), true);
				Double curUnitPrice = PRCommonUtil.getUnitPrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice(), true);
				MultiplePrice curPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());

				getStrategyDefinition(conn, runId, itemDTO.getStrategyId(), runAndItsCategoryLevelStrategy, strategyMap);

				PRStrategyDTO strategyDTO = strategyMap.get(itemDTO.getStrategyId());

				distinctPriceCheckList.add(priceCheckListId);

				// If there is price increase
				if (itemDTO.getIsNewPriceRecommended() == 1) {
					totalPriceChange = totalPriceChange + 1;
					if (checkListPriceChange.get(priceCheckListId) != null) {
						int cnt = checkListPriceChange.get(priceCheckListId);
						cnt = cnt + 1;
						checkListPriceChange.put(priceCheckListId, cnt);
					} else {
						checkListPriceChange.put(priceCheckListId, 1);
					}
				}

				if (recUnitPrice > curUnitPrice) {

					// Get all rounding digits between current price and recommended price
					double[] roundingDigits = strategyDTO.getConstriants().getRoundingConstraint().getRange(curUnitPrice, recUnitPrice, "");

					// If there are more than 2 rounding digits, then mark it
					if (curUnitPrice > 0 && roundingDigits.length > (NO_OF_ROUNDING_DIGITS_CROSSING + 1)) {
						totalCrossingRounding = totalCrossingRounding + 1;
						logger.debug("Crossing rounding,ItemCode:" + itemDTO.getItemCode() + ",Current Price:" + curPrice + ",Rec Price:"
								+ itemDTO.getRecommendedRegPrice());

						if (checkListCrossingRoundingDigits.get(priceCheckListId) != null) {
							int cnt = checkListCrossingRoundingDigits.get(priceCheckListId);
							cnt = cnt + 1;
							checkListCrossingRoundingDigits.put(priceCheckListId, cnt);
						} else {
							checkListCrossingRoundingDigits.put(priceCheckListId, 1);
						}
					}

					// If the price increase to next dollar
					if ((long) Math.abs(recUnitPrice) > (long) Math.abs(curUnitPrice)) {
						totalNextDollar = totalNextDollar + 1;
						if (checkListCrossingNextDollar.get(priceCheckListId) != null) {
							int cnt = checkListCrossingNextDollar.get(priceCheckListId);
							cnt = cnt + 1;
							checkListCrossingNextDollar.put(priceCheckListId, cnt);
						} else {
							checkListCrossingNextDollar.put(priceCheckListId, 1);
						}
					}
				}
			}
		}

		recommendationStatistics.totalItems = totalItems;
		recommendationStatistics.totalCrossingRounding = totalCrossingRounding;
		recommendationStatistics.totalNextDollar = totalNextDollar;
		recommendationStatistics.totalPriceChange = totalPriceChange;
		recommendationStatistics.distinctPriceCheckList = distinctPriceCheckList;

		return recommendationStatistics;
	}

	private void getStrategyDefinition(Connection conn, Long runId, Long strategyId, HashMap<Long, PRStrategyDTO> runAndItsCategoryLevelStrategy,
			HashMap<Long, PRStrategyDTO> strategyMap) throws OfferManagementException {
		StrategyDAO strategyDAO = new StrategyDAO();
		PRStrategyDTO strategyDTO = null;
		if (strategyId != null && strategyId > 0) {
			if (strategyMap.get(strategyId) != null) {
				strategyDTO = strategyMap.get(strategyId);
			} else {
				strategyDTO = strategyDAO.getStrategyDefinition(conn, strategyId);
				strategyMap.put(strategyDTO.getStrategyId(), strategyDTO);
			}
		}
		runAndItsCategoryLevelStrategy.put(runId, strategyDTO);
	}

	private void generateReport(List<RecommendationStatistics> recommendationStatistics) throws GeneralException {
		Set<Integer> distinctPriceCheckList = new HashSet<Integer>();
		// Find distinct check list
		PriceCheckListDAO priceCheckListDAO = new PriceCheckListDAO();
		HashMap<Integer, PriceCheckListDTO> priceCheckListMap = priceCheckListDAO.getAllPriceCheckListInfo(conn);
		
		ProductGroupDAO productGroupDAO = new ProductGroupDAO();
		HashMap<ProductKey, ProductDTO> productMap = productGroupDAO.getAllCategories(conn);

		String distinctCheckLists = "";
		for (RecommendationStatistics recStats : recommendationStatistics) {
			distinctPriceCheckList.addAll(recStats.distinctPriceCheckList);
		}

		for (Integer checkListId : distinctPriceCheckList) {
			String checkListName = "";

			if (checkListId == 0) {
				checkListName = "Others";
			} else {
				checkListName = priceCheckListMap.get(checkListId).getPriceCheckListName();
			}

			distinctCheckLists = distinctCheckLists + checkListName + "\t";
		}

		logger.info("Location Id \t Product Id \t Category \t Run Id \t Total Items \t New Price->\t" + distinctCheckLists + "\t Next Dollar->\t"
				+ distinctCheckLists + "\t Crossing Rounding Digits->\t" + distinctCheckLists);

		for (RecommendationStatistics recStats : recommendationStatistics) {
			String checkListInfo = "";

			checkListInfo = String.valueOf(recStats.totalPriceChange);
			for (Integer checkListId : distinctPriceCheckList) {
				int cnt = recStats.checkListPriceChange.get(checkListId) != null ? recStats.checkListPriceChange.get(checkListId) : 0;
				checkListInfo = checkListInfo + "\t" + String.valueOf(cnt);
			}

			checkListInfo = checkListInfo + "\t\t" + recStats.totalNextDollar;
			for (Integer checkListId : distinctPriceCheckList) {
				int cnt = recStats.checkListCrossingNextDollar.get(checkListId) != null ? recStats.checkListCrossingNextDollar.get(checkListId) : 0;
				checkListInfo = checkListInfo + "\t" + String.valueOf(cnt);
			}

			checkListInfo = checkListInfo + "\t\t" + recStats.totalCrossingRounding;
			for (Integer checkListId : distinctPriceCheckList) {
				int cnt = recStats.checkListCrossingRoundingDigits.get(checkListId) != null
						? recStats.checkListCrossingRoundingDigits.get(checkListId)
						: 0;
				checkListInfo = checkListInfo + "\t" + String.valueOf(cnt);
			}

			logger.info(recStats.locationId + "\t" + recStats.productId + "\t"
					+ productMap.get(new ProductKey(Constants.CATEGORYLEVELID, recStats.productId)).getProductName() + "\t" +recStats.runId + "\t"
					+ recStats.totalItems + "\t" + checkListInfo);
		}

	}
}

class RecommendationStatistics {
	long runId;
	int productId;
	int locationId;
	int totalItems;
	int totalCrossingRounding;
	int totalPriceChange;
	int totalNextDollar;
	Set<Integer> distinctPriceCheckList = new HashSet<Integer>();
	public HashMap<Integer, Integer> checkListCrossingRoundingDigits = new HashMap<Integer, Integer>();
	public HashMap<Integer, Integer> checkListCrossingNextDollar = new HashMap<Integer, Integer>();
	public HashMap<Integer, Integer> checkListPriceChange = new HashMap<Integer, Integer>();
}
