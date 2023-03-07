package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.RecommendationAnalysisDAO;
import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.NotificationDetailInputDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRecItemAnalysis;
import com.pristine.dto.offermgmt.PRRecommendationCompDetail;
import com.pristine.dto.offermgmt.RecommendationAnalysisDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class RecommendationAnalysis {
	private static Logger logger = Logger.getLogger("RecommendationAnalysis");
	private static int PRED_ANALYSIS_2_X_WEEKS = 26;
	private static int PRED_ANALYSIS_3_X_WEEKS = 52;
	
	/**
	 * 
	 * @param connection
	 * @param runId
	 * @throws GeneralException
	 * @throws SQLException
	 */
	public void recommendationAnalysis(Connection connection, Long runId, HashMap<Long, List<PRItemDTO>> runAndItsRecommendedItems,
			HashMap<ItemKey, PRItemDTO> itemDataMap, LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails)
					throws GeneralException, SQLException {
		RecommendationAnalysisDAO recommendationAnlaysisDAO = new RecommendationAnalysisDAO();
		try {
			// Get all property for a given runId.
			// PRRecommendation getRecommendationDetails =
			// recommendationAnlaysisDAO.getRecommendationAnlaysis(connection,
			// runId);
			List<PRItemDTO> recommendedItems = runAndItsRecommendedItems.get(runId);
			// To check the values got any change and set values in
			// RecommendationAnalysisDTO
			RecommendationAnalysisDTO recommendationAnalysisDTO = doRecommendationAnalysis(runId, recommendedItems);
			// Delete the row for a particular runId values are present
			recommendationAnlaysisDAO.deleteFromRecommendedAnalysis(connection, recommendationAnalysisDTO);
			// insert the recommendation status in the database
			recommendationAnlaysisDAO.insertRecommendationDetails(connection, recommendationAnalysisDTO);
			logger.debug("recommendation analysis completed");
			// add notifications
			addNotifications(connection, recommendationAnalysisDTO);

			boolean doPredAnalysis = Boolean.parseBoolean(PropertyManager.getProperty("DO_PREDICTION_ANALYSIS", "TRUE"));
			// Prediction analysis
			if(doPredAnalysis) {
				doPredictionAnalysis(connection, recommendedItems, itemDataMap, previousCalendarDetails, runId);
			}
		} catch (GeneralException e) {
			logger.error("Error in recommendationAnalysis()" + e.toString(), e);
			throw new GeneralException("Error in recommendationAnalysis() " + e.toString(), e);
		}
	}

	/**
	 * 
	 * @param prRecommendation
	 * @return
	 * @throws GeneralException
	 */
	private RecommendationAnalysisDTO doRecommendationAnalysis(long runId, List<PRItemDTO> recommendedItems) throws GeneralException {
		RecommendationAnalysisDTO recommendationAnalysisDTO = new RecommendationAnalysisDTO();

		recommendationAnalysisDTO.setRunId(runId);
		// to check has any predictions values
		recommendationAnalysisDTO.setHasPredictions(isRecHasPredictions(recommendedItems));
		// to check at least one price recommendation values are present
		recommendationAnalysisDTO.setHasAtleastOneRecommendation(isRecHasAtleastOneRecommendation(recommendedItems));
		// to check it has new price
		recommendationAnalysisDTO.setHasAtleastOneNewPrice(isRecHasAtleastOneNewPrice(recommendedItems));

		return recommendationAnalysisDTO;
	}

	/**
	 * Check if even one of the recommended price has valid prediction
	 * 
	 * @param recommendedItems
	 * @return boolean
	 */
	private boolean isRecHasPredictions(List<PRItemDTO> recommendedItems) {
		boolean isRecHasPredictions = false;

		for (PRItemDTO recommendedItem : recommendedItems) {
			if (!recommendedItem.isLir()) {
				if (recommendedItem.getPredictionStatus() != null && recommendedItem.getPredictionStatus() == 0
						&& recommendedItem.getPredictedMovement() != null && recommendedItem.getPredictedMovement() > 0) {
					isRecHasPredictions = true;
					break;
				}
			}
		}
		return isRecHasPredictions;
	}

	/**
	 * To check at least one new recommended regular price
	 * 
	 * @param recommendedItems
	 * @return boolean
	 */
	private boolean isRecHasAtleastOneRecommendation(List<PRItemDTO> recommendedItems) {
		boolean isRecHasAtleastOneRecommendation = false;
		for (PRItemDTO recommendedItem : recommendedItems) {
			if (!recommendedItem.isLir()) {
				if (recommendedItem.getRecommendedRegPrice() != null) {
					isRecHasAtleastOneRecommendation = true;
					break;
				}
			}
		}
		return isRecHasAtleastOneRecommendation;
	}

	/**
	 * To check at least one difference in between current price and recommended
	 * price
	 * 
	 * @param recommendedItems
	 * @return boolean
	 */
	private boolean isRecHasAtleastOneNewPrice(List<PRItemDTO> recommendedItems) {
		boolean isRecHasAtleastOneNewPrice = false;

		for (PRItemDTO recommendedItem : recommendedItems) {
			if (!recommendedItem.isLir()) {
				MultiplePrice currentPrice = PRCommonUtil.getMultiplePrice(recommendedItem.getRegMPack(), recommendedItem.getRegPrice(),
						recommendedItem.getRegMPrice());
//				MultiplePrice recommendedPrice = new MultiplePrice(recommendedItem.getRecommendedRegMultiple(),
//						recommendedItem.getRecommendedRegPrice());
				MultiplePrice recommendedPrice = recommendedItem.getRecommendedRegPrice();
				if ((currentPrice != null && !currentPrice.equals(recommendedPrice)) || (currentPrice == null && recommendedPrice != null)) {
					isRecHasAtleastOneNewPrice = true;
					break;
				}
				/*if (!(currentPrice.equals(recommendedPrice))) {
					isRecHasAtleastOneNewPrice = true;
					break;
				}*/
			}
		}
		return isRecHasAtleastOneNewPrice;
	}

	private void addNotifications(Connection connection, RecommendationAnalysisDTO recommendationAnalysisDTO) throws SQLException, GeneralException {
		NotificationService notificationService = new NotificationService();
		List<NotificationDetailInputDTO> notificationDetailDTOs = new ArrayList<NotificationDetailInputDTO>();
		NotificationDetailInputDTO notificationDetailInputDTO;
		if (!recommendationAnalysisDTO.isHasPredictions()) {
			notificationDetailInputDTO = new NotificationDetailInputDTO();
			notificationDetailInputDTO.setModuleId(PRConstants.REC_MODULE_ID);
			notificationDetailInputDTO.setNotificationTypeId(PRConstants.NO_PREDICTIONS);
			notificationDetailInputDTO.setNotificationKey1(recommendationAnalysisDTO.getRunId());
			notificationDetailDTOs.add(notificationDetailInputDTO);
		}

		if (!recommendationAnalysisDTO.isHasAtleastOneNewPrice()) {
			notificationDetailInputDTO = new NotificationDetailInputDTO();
			notificationDetailInputDTO.setModuleId(PRConstants.REC_MODULE_ID);
			notificationDetailInputDTO.setNotificationTypeId(PRConstants.NO_NEW_PRICES);
			notificationDetailInputDTO.setNotificationKey1(recommendationAnalysisDTO.getRunId());
			notificationDetailDTOs.add(notificationDetailInputDTO);
		}

		if (!recommendationAnalysisDTO.isHasAtleastOneRecommendation()) {
			notificationDetailInputDTO = new NotificationDetailInputDTO();
			notificationDetailInputDTO.setModuleId(PRConstants.REC_MODULE_ID);
			notificationDetailInputDTO.setNotificationTypeId(PRConstants.NO_RECOMMENDATION);
			notificationDetailInputDTO.setNotificationKey1(recommendationAnalysisDTO.getRunId());
			notificationDetailDTOs.add(notificationDetailInputDTO);
		}
		for (NotificationDetailInputDTO notificationDetailInputDTO2 : notificationDetailDTOs) {
			logger.debug(notificationDetailInputDTO2.getNotificationTypeId());
			logger.debug(notificationDetailInputDTO2.getNotificationKey1());
		}
		notificationService.addNotificationsBatch(connection, notificationDetailDTOs, false);
	}

	//Find all distinct competitors of check list and non check list
	//of the recommendation and insert in to the table
	public void insertCompDetails(Connection conn, long runId, List<PRItemDTO> itemList) throws GeneralException {
		RecommendationAnalysisDAO recommendationAnlaysisDAO = new RecommendationAnalysisDAO();
		List<PRRecommendationCompDetail> compDetails = new ArrayList<PRRecommendationCompDetail>();

		// Process all items
		// Group by comp and its items
		HashMap<LocationKey, List<PRItemDTO>> groupedByComp = groupItemsByComp(itemList);

		// Process items
		processCompItems(runId, groupedByComp, compDetails, null);

		// Process all check list
		// Group by check list and its items
		HashMap<Integer, List<PRItemDTO>> groupedByCheckList = groupItemsByCheckList(itemList);

		for (Map.Entry<Integer, List<PRItemDTO>> entry : groupedByCheckList.entrySet()) {
			int priceCheckListId = entry.getKey();
			List<PRItemDTO> items = entry.getValue();
			// Group by comp and its items
			groupedByComp = groupItemsByComp(items);
			// Process items
			processCompItems(runId, groupedByComp, compDetails, priceCheckListId);
		}

		// Delete existing records
		recommendationAnlaysisDAO.deleteRecommendationCompDetail(conn, runId);

		// Insert the Data
		if (compDetails.size() > 0) {
			recommendationAnlaysisDAO.insertRecommendationCompDetail(conn, compDetails);
		}
	}

	private HashMap<LocationKey, List<PRItemDTO>> groupItemsByComp(List<PRItemDTO> itemList) {
		HashMap<LocationKey, List<PRItemDTO>> groupedByComp = new HashMap<LocationKey, List<PRItemDTO>>();
		for (PRItemDTO item : itemList) {
			if (item.getCompStrId() != null && item.getCompStrId().getLocationId() > 0) {
				List<PRItemDTO> items = new ArrayList<PRItemDTO>();
				if (groupedByComp.get(item.getCompStrId()) != null) {
					items = groupedByComp.get(item.getCompStrId());
				}
				items.add(item);
				groupedByComp.put(item.getCompStrId(), items);
			}
		}
		return groupedByComp;
	}

	private void processCompItems(long runId, HashMap<LocationKey, List<PRItemDTO>> groupedByComp, List<PRRecommendationCompDetail> compDetails,
			Integer priceCheckListId) {
		PriceIndexCalculation priceIndexCalculation = new PriceIndexCalculation();
		for (Map.Entry<LocationKey, List<PRItemDTO>> entry : groupedByComp.entrySet()) {
			PRRecommendationCompDetail recommendationCompDetail = new PRRecommendationCompDetail();
			LocationKey compKey = entry.getKey();
			List<PRItemDTO> compItems = entry.getValue();
			int totalItems = 0, totalDistinctItems = 0;
			Double futurePI = priceIndexCalculation.getFutureSimpleIndex(compItems, true);
			Double currentPI = priceIndexCalculation.getCurrentSimpleIndex(compItems);

			for (PRItemDTO item : compItems) {
				if (!item.isLir())
					totalItems++;
				if (item.isLir()) {
					totalDistinctItems++;
				} else if (!item.isLir() && item.getRetLirId() < 1) {
					totalDistinctItems++;
				}
			}

			recommendationCompDetail.setRunId(runId);
			recommendationCompDetail.setLocationLevelId(compKey.getLocationLevelId());
			recommendationCompDetail.setLocationId(compKey.getLocationId());
			recommendationCompDetail.setTotalItems(totalItems);
			recommendationCompDetail.setTotalDistinctItems(totalDistinctItems);
			recommendationCompDetail.setCurrentSimpleIndex(currentPI);
			recommendationCompDetail.setFutureSimpleIndex(futurePI);
			recommendationCompDetail.setPriceCheckListId(priceCheckListId);
			compDetails.add(recommendationCompDetail);
		}
	}

	private HashMap<Integer, List<PRItemDTO>> groupItemsByCheckList(List<PRItemDTO> itemList) {
		HashMap<Integer, List<PRItemDTO>> groupedByCheckList = new HashMap<Integer, List<PRItemDTO>>();
		for (PRItemDTO item : itemList) {
			if (item.getPriceCheckListId() != null) {
				List<PRItemDTO> items = new ArrayList<PRItemDTO>();
				if (groupedByCheckList.get(item.getPriceCheckListId()) != null) {
					items = groupedByCheckList.get(item.getPriceCheckListId());
				}
				items.add(item);
				groupedByCheckList.put(item.getPriceCheckListId(), items);
			}
		}
		return groupedByCheckList;
	}

	// NU:: 2nd Aug 2016, automated way to show questionable predictions
	public void doPredictionAnalysis(Connection conn, List<PRItemDTO> recommendedItems, HashMap<ItemKey, PRItemDTO> itemDataMap,
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails, long runId) throws GeneralException {
		RecommendationAnalysisDAO recommendationAnalysisDAO = new RecommendationAnalysisDAO();
		List<PRRecItemAnalysis> recAnalysisItems = new ArrayList<PRRecItemAnalysis>();
		HashMap<Integer, List<PRRecItemAnalysis>> ligMemberMap = new HashMap<Integer, List<PRRecItemAnalysis>>();

		// Config values
		//13 (units)
		int config1 = Integer.parseInt(PropertyManager.getProperty("PR_PRED_ANALYSIS_CONFIG1"));
		//10 (%)
		int config2 = Integer.parseInt(PropertyManager.getProperty("PR_PRED_ANALYSIS_CONFIG2"));
		//15 (%)
		int config3 = Integer.parseInt(PropertyManager.getProperty("PR_PRED_ANALYSIS_CONFIG3"));
		//5 (%)
		int config4 = Integer.parseInt(PropertyManager.getProperty("PR_PRED_ANALYSIS_CONFIG4"));

		// Loop through all non-lig and lig members
		for (PRItemDTO recommendedItem : recommendedItems) {
			// Ignore lig and non-predicted items
			if (recommendedItem.isLir() || recommendedItem.getPredictionStatus() == null
					|| recommendedItem.getPredictionStatus() != PredictionStatus.SUCCESS.getStatusCode()) {
				continue;
			}

			PRRecItemAnalysis recItemAnalysis = new PRRecItemAnalysis();
			ItemKey itemKey = PRCommonUtil.getItemKey(recommendedItem.getItemCode(), recommendedItem.isLir());
			PRItemDTO itemDTO = itemDataMap.get(itemKey);

			recItemAnalysis.setRecommendationId(recommendedItem.getRecommendationId());

			//1. Retail increased compared to Min retail of last week and forecast increased by > 10% 
			// compared to last week’s actual units and by > 13 units
			//2. Retail reduced compared to Min retail of last week and forecast reduced by > 10% 
			// compared to last week’s actual units and by > 13 units
			predictionAnalysis1(recItemAnalysis, itemDTO, recommendedItem, previousCalendarDetails, config1, config2);

			// 1. > maximum weekly movement in last 26 weeks at regular price points
			// 2. < minimum weekly movement in last 26 weeks at regular price points
			predictionAnalysis2(recItemAnalysis, itemDTO, recommendedItem, previousCalendarDetails, config1);

			// 1. At least 15% < minimum sold at such retail in last 52 weeks 
			// (consider units sold at retails within 5% of forecasted retail) and the difference > 13 units 
			// 2. At least 15% > maximum sold at such retail in last 52 weeks 
			//(consider units sold at retails within 5% of forecasted retail) and the difference is > 13 units
			predictionAnalysis3(recItemAnalysis, itemDTO, recommendedItem, previousCalendarDetails, config1, config3, config4);

			if (recItemAnalysis.isPassesAtleastOneCase()) {
				recAnalysisItems.add(recItemAnalysis);
				if(recommendedItem.getRetLirId() > 0) {
					List<PRRecItemAnalysis> tempList = new ArrayList<PRRecItemAnalysis>();
					if(ligMemberMap.get(recommendedItem.getRetLirId()) != null) {
						tempList = ligMemberMap.get(recommendedItem.getRetLirId());
					}
					tempList.add(recItemAnalysis);
					ligMemberMap.put(recommendedItem.getRetLirId(), tempList);
				}
			}
		}
		
		// Find lig level data
		updateLIGAnalysis(recommendedItems, ligMemberMap, recAnalysisItems);

		//Delete Data
		recommendationAnalysisDAO.deleteRecommendationItemAnalysis(conn, runId);
		// Insert data
		if (recAnalysisItems.size() > 0) {
			recommendationAnalysisDAO.insertRecommendationItemAnalysis(conn, recAnalysisItems);
		}
	}

	private void predictionAnalysis1(PRRecItemAnalysis recItemAnalysis, PRItemDTO itemDTO, PRItemDTO recommendedItem,
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails, int config1, int config2) {
		
		ProductMetricsDataDTO lastWeekIMS = null;
		// Last week IMS
		for (RetailCalendarDTO retailCalendarDTO : previousCalendarDetails.values()) {
			// First row will have last week calendar id
			lastWeekIMS = itemDTO.getLastXWeeksMovDetail().get(retailCalendarDTO.getCalendarId());
			break;
		}

		// Both the prices are present
//		if (lastWeekIMS != null && lastWeekIMS.getFinalPrice() > 0 && recommendedItem.getRecommendedRegPrice() != null
//				&& recommendedItem.getRecommendedRegPrice() > 0) {
		if (lastWeekIMS != null && lastWeekIMS.getFinalPrice() > 0 && recommendedItem.getRecommendedRegPrice() != null
				&& recommendedItem.getRecommendedRegPrice().price != null) {
//			double recUnitPrice = PRCommonUtil.getUnitPrice(recommendedItem.getRecommendedRegMultiple(), recommendedItem.getRecommendedRegPrice(),
//					null, true);
			double recUnitPrice = PRCommonUtil.getUnitPrice(recommendedItem.getRecommendedRegPrice(), true);
			double lastWeekMinUnitPrice = lastWeekIMS.getFinalPrice();
			
			long predictedMovement = roundPredictedMovement(recommendedItem.getPredictedMovement());
			long lastWeekActualMov = Math.round(lastWeekIMS.getTotalMovement());
			
			//1. Retail increased compared to Min retail of last week and forecast increased by > 10% 
			// compared to last week’s actual units and by > 13 units
			// Price increased and there is actual movement
			if (recUnitPrice > lastWeekMinUnitPrice) {
				long actualMov = increaseByPCT(lastWeekActualMov, config2);
				// Forecast is greater than x% of actual
				if (predictedMovement > actualMov && Math.abs(predictedMovement - actualMov) > config1) {
					recItemAnalysis.setIsForecastUpRetailUp(1);
					recItemAnalysis.setPassesAtleastOneCase(true);
				}
			}

			//2. Retail reduced compared to Min retail of last week and forecast reduced by > 10% 
			// compared to last week’s actual units and by > 13 units
			// Price reduced and there is actual movement
			if (recUnitPrice < lastWeekMinUnitPrice) {
				long actualMov = decreaseByPCT(lastWeekActualMov, config2);
				// Forecast is reduced by x% of actual
				if (predictedMovement < actualMov && Math.abs(predictedMovement - actualMov) > config1) {
					recItemAnalysis.setIsForecastDownRetailDown(1);
					recItemAnalysis.setPassesAtleastOneCase(true);
				}
			}

		}
	}

	private void predictionAnalysis2(PRRecItemAnalysis recItemAnalysis, PRItemDTO itemDTO, PRItemDTO recommendedItem,
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails, int config1) {
		int noOfWeeksBehind = PRED_ANALYSIS_2_X_WEEKS;
		int noOfWeeksCount = 1;
		boolean isSoldAtRegPrice = false;
		boolean isSoldAtSalePrice = false;
		List<Double> regMovements = new ArrayList<Double>();
		double minSold = 0, maxSold = 0;

		// Get all regular movements
		for (RetailCalendarDTO retailCalendarDTO : previousCalendarDetails.values()) {
			if (noOfWeeksCount > noOfWeeksBehind) {
				break;
			} else {
				ProductMetricsDataDTO productMetricsDataDTO = itemDTO.getLastXWeeksMovDetail().get(retailCalendarDTO.getCalendarId());
				if (productMetricsDataDTO != null) {
					if (productMetricsDataDTO.getSalePrice() > 0 || productMetricsDataDTO.getSaleMPrice() > 0) {
						isSoldAtSalePrice = !isSoldAtSalePrice ? true : isSoldAtSalePrice;
					} else {
						isSoldAtRegPrice = !isSoldAtRegPrice ? true : isSoldAtRegPrice;
						regMovements.add(productMetricsDataDTO.getTotalMovement());
					}
				}
			}
			noOfWeeksCount++;
		}

		if (regMovements.size() > 0) {
			minSold = Collections.min(regMovements);
			maxSold = Collections.max(regMovements);
		}

		long predictedMovement = roundPredictedMovement(recommendedItem.getPredictedMovement());
		// > maximum weekly movement in last 26 weeks at regular price points
		if (maxSold > 0) {
			if (predictedMovement > maxSold && Math.abs(predictedMovement - maxSold) > config1) {
				recItemAnalysis.setIsHigherThanXWeeksAvg(1);
				recItemAnalysis.setPassesAtleastOneCase(true);
			}
		}

		// < minimum regular weekly movement in last 26 weeks at regular price points
		if (minSold > 0) {
			if (predictedMovement < minSold && Math.abs(predictedMovement - minSold) > config1) {
				recItemAnalysis.setIsLowerThanXWeeksAvg(1);
				recItemAnalysis.setPassesAtleastOneCase(true);
			}
		}
	}

	private void predictionAnalysis3(PRRecItemAnalysis recItemAnalysis, PRItemDTO itemDTO, PRItemDTO recommendedItem,
			LinkedHashMap<Integer, RetailCalendarDTO> previousCalendarDetails, int config1, int config3, int config4) {
		long minUnitsSold = 0;
		long maxUnitsSold = 0;

//		Double recUnitPrice = PRCommonUtil.getUnitPrice(recommendedItem.getRecommendedRegMultiple(), recommendedItem.getRecommendedRegPrice(), null,
//				true);
		Double recUnitPrice = PRCommonUtil.getUnitPrice(recommendedItem.getRecommendedRegPrice(), true);
		if (recUnitPrice != null) {
			double minPriceRange = PRFormatHelper.roundToTwoDecimalDigitAsDouble(recUnitPrice - (recUnitPrice * (config4 / PRConstants.HUNDRED)));
			double maxPriceRange = PRFormatHelper.roundToTwoDecimalDigitAsDouble(recUnitPrice + (recUnitPrice * (config4 / PRConstants.HUNDRED)));
			int noOfWeeksCount = 1;
			int noOfWeeksBehind = PRED_ANALYSIS_3_X_WEEKS;

			// Loop last 52 weeks
			for (RetailCalendarDTO retailCalendarDTO : previousCalendarDetails.values()) {
				// Find min and max sold
				if (noOfWeeksCount > noOfWeeksBehind) {
					break;
				} else {
					ProductMetricsDataDTO productMetricsDataDTO = itemDTO.getLastXWeeksMovDetail().get(retailCalendarDTO.getCalendarId());
					if (productMetricsDataDTO != null
							&& (productMetricsDataDTO.getFinalPrice() >= minPriceRange && productMetricsDataDTO.getFinalPrice() <= maxPriceRange)) {
						long totalMov = Math.round(productMetricsDataDTO.getTotalMovement());
						if (minUnitsSold == 0 || totalMov < minUnitsSold) {
							minUnitsSold = totalMov;
						}

						if (maxUnitsSold == 0 || totalMov > maxUnitsSold) {
							maxUnitsSold = totalMov;
						}
					}
				}
				noOfWeeksCount++;
			}

			long predictedMovement = roundPredictedMovement(recommendedItem.getPredictedMovement());

			//  At least 15% < minimum sold at such retail in last 52 weeks 
			// (consider units sold at retails within 5% of forecasted retail) and the difference > 13 units 
			if (minUnitsSold > 0) {
				long minMov = decreaseByPCT(minUnitsSold, config3);
				if (predictedMovement < minMov && Math.abs(predictedMovement - minMov) > config1) {
					recItemAnalysis.setIsLowerThanMinSoldInXWeeks(1);
					recItemAnalysis.setPassesAtleastOneCase(true);
				}
			}

			// At least 15% > maximum sold at such retail in last 52 weeks 
			// (consider units sold at retails within 5% of forecasted retail) and the difference is > 13 units
			if (maxUnitsSold > 0) {
				long maxMov = increaseByPCT(maxUnitsSold, config3);
				if (predictedMovement > maxMov && Math.abs(predictedMovement - maxMov) > config1) {
					recItemAnalysis.setIsHigherThanMaxSoldInXWeeks(1);
					recItemAnalysis.setPassesAtleastOneCase(true);
				}
			}
		}
	}
	
	private void updateLIGAnalysis(List<PRItemDTO> recommendedItems, HashMap<Integer, List<PRRecItemAnalysis>> ligMemberMap,
			List<PRRecItemAnalysis> recAnalysisItems) {
		// Loop lig's
		for (PRItemDTO recommendedItem : recommendedItems) {
			if (recommendedItem.isLir()) {
				PRRecItemAnalysis recLigItemAnalysis = new PRRecItemAnalysis();
				recLigItemAnalysis.setRecommendationId(recommendedItem.getRecommendationId());

				if (ligMemberMap.get(recommendedItem.getItemCode()) != null) {
					for (PRRecItemAnalysis recItemAnalysis : ligMemberMap.get(recommendedItem.getItemCode())) {
						// Ignore if already set
						if (recLigItemAnalysis.getIsForecastUpRetailUp() == 0 && recItemAnalysis.getIsForecastUpRetailUp() == 1) {
							recLigItemAnalysis.setIsForecastUpRetailUp(recItemAnalysis.getIsForecastUpRetailUp());
							recLigItemAnalysis.setPassesAtleastOneCase(true);
						}

						if (recLigItemAnalysis.getIsForecastDownRetailDown() == 0 && recItemAnalysis.getIsForecastDownRetailDown() == 1) {
							recLigItemAnalysis.setIsForecastDownRetailDown(recItemAnalysis.getIsForecastDownRetailDown());
							recLigItemAnalysis.setPassesAtleastOneCase(true);
						}

						if (recLigItemAnalysis.getIsHigherThanXWeeksAvg() == 0 && recItemAnalysis.getIsHigherThanXWeeksAvg() == 1) {
							recLigItemAnalysis.setIsHigherThanXWeeksAvg(recItemAnalysis.getIsHigherThanXWeeksAvg());
							recLigItemAnalysis.setPassesAtleastOneCase(true);
						}

						if (recLigItemAnalysis.getIsLowerThanXWeeksAvg() == 0 && recItemAnalysis.getIsLowerThanXWeeksAvg() == 1) {
							recLigItemAnalysis.setIsLowerThanXWeeksAvg(recItemAnalysis.getIsLowerThanXWeeksAvg());
							recLigItemAnalysis.setPassesAtleastOneCase(true);
						}

						if (recLigItemAnalysis.getIsLowerThanMinSoldInXWeeks() == 0 && recItemAnalysis.getIsLowerThanMinSoldInXWeeks() == 1) {
							recLigItemAnalysis.setIsLowerThanMinSoldInXWeeks(recItemAnalysis.getIsLowerThanMinSoldInXWeeks());
							recLigItemAnalysis.setPassesAtleastOneCase(true);
						}

						if (recLigItemAnalysis.getIsHigherThanMaxSoldInXWeeks() == 0 && recItemAnalysis.getIsHigherThanMaxSoldInXWeeks() == 1) {
							recLigItemAnalysis.setIsHigherThanMaxSoldInXWeeks(recItemAnalysis.getIsHigherThanMaxSoldInXWeeks());
							recLigItemAnalysis.setPassesAtleastOneCase(true);
						}
					}
				}

				if (recLigItemAnalysis.isPassesAtleastOneCase()) {
					recAnalysisItems.add(recLigItemAnalysis);
				}
			}
		}
	}
	
	private long increaseByPCT(long inputNumber, int percentage){
		long output = Math.round((inputNumber + (inputNumber * (percentage / PRConstants.HUNDRED))));
		return output;
	}
	
	private long decreaseByPCT(long inputNumber, int percentage){
		long output = Math.round((inputNumber - (inputNumber * (percentage / PRConstants.HUNDRED))));
		return output;
	}
	
	private long roundPredictedMovement(Double predictedMovement) {
		return predictedMovement != null ? Math.round(predictedMovement) : 0;
	}
}
