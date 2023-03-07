package com.pristine.service.offermgmt.prediction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

import com.pristine.dto.ProductMetricsDataDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.dto.offermgmt.prediction.PricePointDTO;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;


public class PredictionAnalysis {
	
	static Logger logger = Logger.getLogger("PredictionAnalysis");
	
	private final String SEPARATOR = ",";
	private int newItemHistLessThanXWeeks;
	private int newPricePointUpperLimitPct;		
	private int newPricePointLowerLimitPct;
	private int criteriaWeekNewItems;
	private int observationLowerLimit;

	
	private List<Integer> newItemList = new ArrayList<Integer>();
	private HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory = 
			new HashMap<Integer, HashMap<String, List<RetailPriceDTO>>>();
	private HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData = 
			new HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>>();
	
	private static final String GET_NEW_ITEM_LIST = 
			"SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE ITEM_CODE IN " +			
					" ( " +													
					"%TOTAL_ITEMS_QUERY%" +							
					" ) " +
					" AND ACTIVE_INDICATOR = 'Y' " +
					" AND CREATE_TIMESTAMP >= TO_DATE(?, 'MM/dd/yyyy') AND LIR_IND <> 'Y' AND CREATE_TIMESTAMP IS NOT NULL";
	
	/**
	 * Default constructor
	 */
	public PredictionAnalysis(){
		//PropertyManager.initialize("recommendation.properties");
	}
	
	/**
	 * Parameterized constructor
	 * @param movementData
	 * @param itemPriceHistory
	 */
	public PredictionAnalysis(HashMap<Integer, HashMap<Integer, ProductMetricsDataDTO>> movementData, 
			HashMap<Integer, HashMap<String, List<RetailPriceDTO>>> itemPriceHistory) {
		this.movementData = movementData;
		this.itemPriceHistory = itemPriceHistory;
	}
	
	/**
	 * Main method to initiate prediction analysis
	 * 
	 * @param conn
	 * @param predictionInputDTOs
	 */
	public void initiate(Connection conn, List<PredictionInputDTO> predictionInputDTOs){
	
		logger.info("PredictionAnalysis :: Start of initiate()");
		
		if(conn!=null && predictionInputDTOs!=null && predictionInputDTOs.size() > 0){
			//proceed only when we have the history and movement data
			initializeProperties();
			newItemsList(conn, predictionInputDTOs);
			
			if(itemPriceHistory != null && movementData != null && itemPriceHistory.size() > 0 && movementData.size() > 0){
				// Loop PredictionItemDTO
				for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
					//logger.debug("Analyzing for product id : " + predictionInputDTO.productId);
					for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
						// Analyzing prediction item wise  
//						analyzePrediction(conn, predictionItemDTO, predictionInputDTOs); 
						analyzePredictionV2(predictionItemDTO);
					}
				}
//				verifyOutcome(predictionInputDTOs); //for verification purpose only
			}else{
				logger.debug("Item history or movement history data not available. Exiting...");
			}
		}else{
			logger.debug("Database connection or prediction input is not passed properly.");
		}
		logger.info("PredictionAnalysis :: End of initiate()");
	}
	
	/**
	 * Initializing properties
	 * 
	 */
	
	private void initializeProperties(){
		newItemHistLessThanXWeeks = Integer.parseInt(PropertyManager.getProperty("NEW_ITEM_HISTORY_LESS_THAN_X_WEEKS"));
		newPricePointUpperLimitPct = Integer.parseInt(PropertyManager.getProperty("NEW_PRICE_POINT_UPPER_LIMIT_PCT"));		
		newPricePointLowerLimitPct = Integer.parseInt(PropertyManager.getProperty("NEW_PRICE_POINT_LOWER_LIMIT_PCT"));
		criteriaWeekNewItems = Integer.parseInt(PropertyManager.getProperty("CRITERIA_WEEK_NEW_ITEMS"));
		observationLowerLimit = Integer.parseInt(PropertyManager.getProperty("OBSERVATION_LOWER_LIMIT"));
		
	}
	/**
	 * The method to analyze prediction for each Item for all 6 rules
	 * 
	 * @param conn
	 * @param predictionItemDTO
	 * @param predictionInputDTOs
	 */
	private void analyzePrediction(Connection conn, PredictionItemDTO predictionItemDTO,
			List<PredictionInputDTO> predictionInputDTOs ){
		
		logger.debug("analyzePrediction(): Start for product id : " + predictionItemDTO.itemCodeOrLirId);
		
		//1. New price point. 
		//a. If it’s > 105% of current price and predicted movement is not < current movement.
		//b. If it’s < 95% of current price and predicted movement is not > current movement.  
		predcitedMovementIrregular(predictionItemDTO);
		
		//2. New item, less than 6 weeks history for the item
		hasSufficientHistory(predictionItemDTO);
		
		//3. Less than 4 weekly observations for this price point over last 18 months
		hasLessObservations(predictionItemDTO);
		
		//method to remove last comma from final outcome
		formatQuestionablePredictionValue(predictionItemDTO);
		
		logger.debug("analyzePrediction(): End for product id : " + predictionItemDTO.itemCodeOrLirId);
		
	}
	private void analyzePredictionV2(PredictionItemDTO predictionItemDTO){
		
		PricePointDTO currentRegPricePointDTO = getCurrentPricePoint(predictionItemDTO);
		if(currentRegPricePointDTO!= null){
			
			//Clear Questionable prediction before process starts
			predictionItemDTO.pricePoints.forEach(pricePoint->{
				if(pricePoint.getQuestionablePrediction()!= null){
					pricePoint.setQuestionablePrediction(
							pricePoint.getQuestionablePrediction() + SEPARATOR);
				}
				
			});
			
		//  1.If the current price has more than <<10>> observations and predicted current value is outside the min /max range 
			checkCurrentPriceForecastRange(predictionItemDTO, currentRegPricePointDTO);
			//	2.If the trend of new forecast in relation to current forecast is incorrect. 
			//	Incorrect trend will get triggered if the difference in the unit > 1 and 
			//	more than 2% diff in relation to current forecast
			checkNewForecastInCorrentTrend(predictionItemDTO, currentRegPricePointDTO);
			//	3.If the trend is correct and difference is more than 1 but  movement difference between current and 
			//	new is greater than <<+/- 20%>>, mark it as questionable
			checkForecastDiffWithInRange(predictionItemDTO, currentRegPricePointDTO);
			//	4.If there is a valid prediction ( > 0) for items with less than 6 historical data points at item level 
			//	and not based on price point, mark it as questionable
			hasSufficientHistory(predictionItemDTO);
			
			//method to remove last comma from final outcome
			//formatQuestionablePredictionValue(predictionItemDTO);
		}
	}
	
	/**
	 * If the current price has more than <<10>> observations and predicted current value is outside the min /max range
	 */
	private void checkCurrentPriceForecastRange(PredictionItemDTO predictionItemDTO,PricePointDTO currentRegPricePointDTO){
		
		List<ProductMetricsDataDTO> currentPriceMovementDetails = new ArrayList<>();
		if (movementData.get(predictionItemDTO.itemCodeOrLirId) != null) {
			MultiplePrice currentRegPrice = new MultiplePrice(currentRegPricePointDTO.getRegQuantity(), currentRegPricePointDTO.getRegPrice());
			Double curUnitPrice = PRCommonUtil.getUnitPrice(currentRegPrice, true);
			//logger.info("Processing for Item code: "+predictionItemDTO.itemCodeOrLirId+" and Current unit Price:"+curUnitPrice);
			// Take all available list for the current price in a given item
			movementData.get(predictionItemDTO.itemCodeOrLirId).forEach((calendarId, productMetricsDataDTO) -> {
				
//				MultiplePrice regMultiplePrice = new MultiplePrice((int)productMetricsDataDTO.getRegularQuantity(), productMetricsDataDTO.getRegularPrice());
				if (curUnitPrice.equals(productMetricsDataDTO.getFinalPrice())) {
					currentPriceMovementDetails.add(productMetricsDataDTO);
				}
			});
			
			
			if (currentPriceMovementDetails.size()>= 10 && currentRegPricePointDTO.getPredictionStatus().getStatusCode() == 0) {
				Optional<ProductMetricsDataDTO> minValue = currentPriceMovementDetails.stream().filter(pdto-> pdto.getTotalMovement()>0)
						.min(Comparator.comparing(ProductMetricsDataDTO::getTotalMovement));
				
				Optional<ProductMetricsDataDTO> maxValue = currentPriceMovementDetails.stream().filter(pdto-> pdto.getTotalMovement()>0)
						.max(Comparator.comparing(ProductMetricsDataDTO::getTotalMovement));
				
				int curPredMov = (int) Math.round(currentRegPricePointDTO.getPredictedMovement());
				if (minValue.isPresent() && maxValue.isPresent()
						&& (curPredMov < minValue.get().getTotalMovement()
								|| curPredMov > maxValue.get().getTotalMovement())) {
					
					//logger.info("Movement Values in list: "+currentPriceMovementDetails.stream().map(pdto-> Double.toString(pdto.getTotalMovement())).collect(Collectors.joining(", ")));
					//logger.info("Current Predicted Mov: "+curPredMov+" Min Value"+minValue.get().getTotalMovement()
					//		+" Max Value: "+maxValue.get().getTotalMovement());
					// If current price has less than 10 observations, then highlight as questionable
					predictionItemDTO.pricePoints.forEach(pricePoint -> {
						pricePoint.setQuestionablePrediction(
								pricePoint.getQuestionablePrediction() + PRConstants.CURRENT_PRICE_FORECAST_MOV_NOT_WITHIN_RANGE + SEPARATOR);
					});
				} 
				
//				else if (!minValue.isPresent() || !maxValue.isPresent()) {
//					logger.error("Minimum/Maximum regular qty value is not found for the item: " + predictionItemDTO.itemCodeOrLirId);
//				}
			}
		}
	}
	
	/**
	 * Method to identify questionable prediction for price points with condition :
	 * 2.If the trend of new forecast in relation to current forecast is incorrect.
	 * Incorrect trend will get triggered if the difference in the unit > 1 and 
	 * more than 2% diff in relation to current forecast
	 */
	private void checkNewForecastInCorrentTrend(PredictionItemDTO predictionItemDTO, PricePointDTO currentRegPricePointDTO){
		
		// Check Trend of each Price points against Current price point
		int forecastRange = Integer.parseInt(PropertyManager.getProperty("QUES_PRED_UNIT_FORECAST_PCT_DIFF_LIMIT", "2"));
		int roundingDiffLimit = Integer.parseInt(PropertyManager.getProperty("QUES_PRED_FORECAST_ROUNDING_DIFF_LIMIT", "1"));
		
		MultiplePrice currentRegPrice = new MultiplePrice(currentRegPricePointDTO.getRegQuantity(), currentRegPricePointDTO.getRegPrice());
		Double curUnitPrice = PRCommonUtil.getUnitPrice(currentRegPrice, true);
//		logger.info("Processing for Item code: "+predictionItemDTO.itemCodeOrLirId);
		predictionItemDTO.pricePoints.forEach(pricePoint->{
			MultiplePrice regMultiplePrice = new MultiplePrice(pricePoint.getRegQuantity(), pricePoint.getRegPrice());
			Double regUnitPrice = PRCommonUtil.getUnitPrice(regMultiplePrice, true);
//			logger.info("Current price: "+curUnitPrice+",Reg Price : "+regUnitPrice+", Cur Mov: "+currentRegPricePointDTO.getPredictedMovement()
//			+", Reg Mov: "+pricePoint.getPredictedMovement());
			// Newly recommended price is lesser than current price but newly recommended forecast is higher than the current forecast
			if(curUnitPrice > regUnitPrice && currentRegPricePointDTO.getPredictedMovement() >  pricePoint.getPredictedMovement()
					&&!isForecastDiffDueToRounding(currentRegPricePointDTO, pricePoint, roundingDiffLimit)){
				if(!isForecastPercentageWithinRange(currentRegPricePointDTO, pricePoint, forecastRange)){
					pricePoint.setQuestionablePrediction(
							pricePoint.getQuestionablePrediction() + PRConstants.QUESTIONABLE_PREDICTED_MOVEMENT_B + SEPARATOR);
				}
			}
			
			// Newly recommended price is higher than current price but newly recommended forecast is lower than the current forecast
			else if(curUnitPrice < regUnitPrice && currentRegPricePointDTO.getPredictedMovement() <  pricePoint.getPredictedMovement()
					&& (!isForecastDiffDueToRounding(currentRegPricePointDTO, pricePoint, roundingDiffLimit))){
				if(!isForecastDiffDueToRounding(currentRegPricePointDTO, pricePoint, roundingDiffLimit)){
					pricePoint.setQuestionablePrediction(
							pricePoint.getQuestionablePrediction() + PRConstants.QUESTIONABLE_PREDICTED_MOVEMENT_A + SEPARATOR);
				}
			}
			
		});
	}

	/**
	 * 3.If the trend is correct and difference is more than 1 but  movement difference between current and
	 * new is greater than <<+/- 20%>>, mark it as questionable
	 */
	private void checkForecastDiffWithInRange(PredictionItemDTO predictionItemDTO, PricePointDTO currentRegPricePointDTO){
		int forecastRange = Integer.parseInt(PropertyManager.getProperty("QUESTIONABLE_PREDICTED_FORECAST_MAX_DIFF", "20"));
		
		MultiplePrice currentRegPrice = new MultiplePrice(currentRegPricePointDTO.getRegQuantity(), currentRegPricePointDTO.getRegPrice());
		Double curUnitPrice = PRCommonUtil.getUnitPrice(currentRegPrice, true);
		
		predictionItemDTO.pricePoints.forEach(pricePoint->{
			
			MultiplePrice regMultiplePrice = new MultiplePrice(pricePoint.getRegQuantity(), pricePoint.getRegPrice());
			Double regUnitPrice = PRCommonUtil.getUnitPrice(regMultiplePrice, true);
			
			// If trend is correct and forecast difference is not with the limit
			if(((curUnitPrice > regUnitPrice && currentRegPricePointDTO.getPredictedMovement() >  pricePoint.getPredictedMovement())
					||(curUnitPrice < regUnitPrice && currentRegPricePointDTO.getPredictedMovement() <  pricePoint.getPredictedMovement()))
					&& !isForecastPercentageWithinRange(currentRegPricePointDTO, pricePoint, forecastRange)){
				pricePoint.setQuestionablePrediction(
						pricePoint.getQuestionablePrediction() + PRConstants.NEW_FORECAST_DIFFERENCE_MORE_THAN_X_PERCENT + SEPARATOR);
			}
		});
		
	}
	/**
	 * Method to identify questionable prediction for price points with condition :
	 * 2. New item, less than 6 weeks history for the items
	 * 
	 * @param predictionItemDTO
	 */
	private void hasSufficientHistory(PredictionItemDTO predictionItemDTO){
		
		//logger.debug("hasSufficientHistory() : Start for Item code : " + predictionItemDTO.itemCodeOrLirId);
		try {
			if(newItemList.size()>0 && newItemList.contains(predictionItemDTO.itemCodeOrLirId) && predictionItemDTO.pricePoints != null){
				
				//logger.debug(" Item code : " + predictionItemDTO.itemCodeOrLirId + " is a new item ");	
				if((!(movementData.get(predictionItemDTO.itemCodeOrLirId) != null && 
						movementData.get(predictionItemDTO.itemCodeOrLirId).size() >= newItemHistLessThanXWeeks))){
					
					//logger.debug("Item has not enough history for valid prediction. Marking as questionable. Reason -2 .");
					//logger.debug("Pricepoint size : " + predictionItemDTO.pricePoints.size());
					for (PricePointDTO pricePoint : predictionItemDTO.pricePoints) {
						if (!pricePoint.getIsAlreadyPredicted()) {
							pricePoint.setQuestionablePrediction( pricePoint.getQuestionablePrediction() + 
									PRConstants.NEW_ITEM_LESS_HISTORY_THAN_X_WEEKS + SEPARATOR);
						}
					}
				}
			}else{
				//logger.debug(" Item code : " + predictionItemDTO.itemCodeOrLirId + " is  not  a new item OR does not have price points. ");	
			}
		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("hasSufficientHistory() : " + ex.toString(), ex);
		}
		//logger.debug("hasSufficientHistory() : End for item code " + predictionItemDTO.itemCodeOrLirId);	
		
	}

	/**
	 * Method to identify it is a :: 1. New price point. 
	 *	a. If it’s > 105% of current price and predicted movement is not < current movement.
	 *	b. If it’s < 95% of current price and predicted movement is not > current movement. 
	 * 
	 * @param predictionItemDTO
	 */
	private void predcitedMovementIrregular(PredictionItemDTO predictionItemDTO) {

		logger.debug("predcitedMovementIrregular() : Start for Item code : " + predictionItemDTO.itemCodeOrLirId);
		try {
			PricePointDTO currentRegPricePointDTO = getCurrentPricePoint(predictionItemDTO);

			if (currentRegPricePointDTO != null) {
				MultiplePrice currentRegPrice = new MultiplePrice(currentRegPricePointDTO.getRegQuantity(), currentRegPricePointDTO.getRegPrice());
				logger.debug("Current Reg Price is: " + currentRegPrice.toString());

				Double curRegUnitPrice = PRCommonUtil.getUnitPrice(currentRegPrice, true);

				// select the new pricePoints which are out of range
				// Calculating higher regPrice limit
				Double upperRegPriceLimit = curRegUnitPrice * (newPricePointUpperLimitPct / 100.0);

				// Calculating lower regPrice limit
				Double lowerRegPriceLimit = curRegUnitPrice * (newPricePointLowerLimitPct / 100.0);

				upperRegPriceLimit = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(upperRegPriceLimit));
				logger.debug("upperRegPriceLimit is : " + upperRegPriceLimit);

				lowerRegPriceLimit = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(lowerRegPriceLimit));
				logger.debug("lowerRegPriceLimit is : " + lowerRegPriceLimit);

				for (PricePointDTO pricePoint : predictionItemDTO.pricePoints) {
					if (!pricePoint.getIsAlreadyPredicted()) {
						MultiplePrice multiplePrice = new MultiplePrice(pricePoint.getRegQuantity(), pricePoint.getRegPrice());

						Double unitPrice = PRCommonUtil.getUnitPrice(multiplePrice, true);

						logger.debug("A new price point : " + multiplePrice.toString());

						// a. If it’s > 105% of current price and predicted movement is not < current movement.
						if ((unitPrice >= upperRegPriceLimit)
								&& (pricePoint.getPredictedMovement() >= currentRegPricePointDTO.getPredictedMovement())) {
							logger.debug("Marking as questionable - 1A");
							pricePoint.setQuestionablePrediction(
									pricePoint.getQuestionablePrediction() + PRConstants.QUESTIONABLE_PREDICTED_MOVEMENT_A + SEPARATOR);
						}

						// b. If it’s < 95% of current price and predicted movement is not > current movement.
						if ((pricePoint.getRegPrice() <= lowerRegPriceLimit)
								&& (pricePoint.getPredictedMovement() <= currentRegPricePointDTO.getPredictedMovement())) {
							logger.debug("Marking as questionable - 1B");
							pricePoint.setQuestionablePrediction(
									pricePoint.getQuestionablePrediction() + PRConstants.QUESTIONABLE_PREDICTED_MOVEMENT_B + SEPARATOR);
						}
					}

				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("predcitedMovementIrregular():: Exception : " + ex.toString(), ex);
		}
		logger.debug("predcitedMovementIrregular() : End for Item code : " + predictionItemDTO.itemCodeOrLirId);
	}
	

	/**
	 * Method to retrieve current price point for item
	 * 
	 * @param predictionItemDTO
	 * @return
	 */
	private PricePointDTO getCurrentPricePoint(PredictionItemDTO predictionItemDTO){
		PricePointDTO pricePointDTOOut = null; 
		if(predictionItemDTO.pricePoints != null)
		for (PricePointDTO pricePointDTO : predictionItemDTO.pricePoints) {
			//Initializing variables
			if (pricePointDTO.isCurrentPrice()){
				pricePointDTOOut = pricePointDTO;
			}
		}
		return pricePointDTOOut;
	}
	
	
	/**
	 * Retrieving list of new items 
	 * 
	 * @param conn
	 * @param predictionInputDTOs
	 * @return List of new Items
	 */
	private void newItemsList(Connection conn, List<PredictionInputDTO> predictionInputDTOs){
		
		//fetching new item from this list
		newItemList.clear();
		int QUERYLIMIT = 999;
		try{
			List<Integer> totalItemList = new ArrayList<Integer>();
			StringBuffer totalItemsQuery = new StringBuffer(""); 
			
			int querysize = 0;
			String totalItemsQuerys = "";
			//fetching total item list
			for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
				for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
					if (predictionItemDTO.lirInd != null && predictionItemDTO.lirInd == true) {
					} else {
						totalItemList.add(predictionItemDTO.itemCodeOrLirId);
						totalItemsQuery.append(predictionItemDTO.itemCodeOrLirId);
						totalItemsQuery.append(",");
						querysize++;
					}
					if(querysize == QUERYLIMIT){
						totalItemsQuerys = totalItemsQuery.toString();
						totalItemsQuerys = totalItemsQuerys.substring(0, totalItemsQuerys.length()-1);
//						logger.debug("totalItems list : " + totalItemsQuerys);
						fetchNewItemList(conn, totalItemsQuerys );
						totalItemsQuery = new StringBuffer(""); 
						totalItemsQuerys = "";
						querysize = 0;
					}
				}
			}
			
			if(querysize > 0){ //The last iteration
				totalItemsQuerys = totalItemsQuery.toString();
				totalItemsQuerys = totalItemsQuerys.substring(0, totalItemsQuerys.length()-1);
//				logger.debug("totalItems list : " + totalItemsQuerys);
				fetchNewItemList(conn,totalItemsQuerys );
			}
			
//			logger.debug(" New item list size is : " + newItemList.size());
			for(Integer item : newItemList){
				logger.debug("New Item in list is : " + item );
			}
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("newItemsList() : Exception occurred - " + ex.toString(), ex);
		}	
	}
	
	
	/**
	 * Method to fetch new items from datatbase
	 * 
	 * @param conn
	 * @param itemsList
	 */
	private void fetchNewItemList(Connection conn, String itemsList){
		
//		logger.debug("fetchNewItemList() : Start ");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		int counter = 0;
		try{
			String sql = null;
			sql = GET_NEW_ITEM_LIST;
			if(itemsList.length() >0 ){
				sql = sql.replaceAll("%TOTAL_ITEMS_QUERY%", itemsList);
				
				stmt = conn.prepareStatement(sql);
//				logger.debug("New Item List Query - " + sql);
				stmt.setString(++counter, DateUtil.getWeekStartDate(criteriaWeekNewItems));
				rs = stmt.executeQuery();
				while(rs.next()){
					logger.debug("New item found");
					newItemList.add(rs.getInt("ITEM_CODE"));
				}
			}else{
				logger.debug(" Empty item list.");
			}
		}catch(SQLException sqlEx){
			sqlEx.printStackTrace();
			logger.error("fetchNewItemList() : Error when retrieving new item list - " + sqlEx.getMessage());
		}catch(Exception ex){
			ex.printStackTrace();
			logger.error("fetchNewItemList() : Exception occurred - " + ex.toString(), ex);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
//			logger.debug("fetchNewItemList() : New item list size is : " + newItemList.size());
		}	
	}
	
	/**
	 * Method to extract new Price points for an item
	 * 
	 * @param predictionItemDTO
	 * @return
	 */
/*	private void getNewPricePoints(PredictionItemDTO predictionItemDTO){
		
		HashMap<String, List<RetailPriceDTO>> itemHistory = new HashMap<String, List<RetailPriceDTO>>();
		itemHistory = itemPriceHistory.get(predictionItemDTO.itemCodeOrLirId);
		
		for (PricePointDTO pricePoint : predictionItemDTO.pricePoints) {
			if(pricePoint.getSalePrice() == 0.0 && pricePoint.getSaleQuantity() == 0 &&
					pricePoint.getAdBlockNo() == 0 && pricePoint.getAdPageNo() == 0){
				boolean isNewPricePoint = true;
				for(String key : itemHistory.keySet()){
					List<RetailPriceDTO> lrd = itemHistory.get(key);
					for(RetailPriceDTO rpdto : lrd){
						//need to review if the regular price assumption is right
						 if(FormatFloatToTwoDecimalDigit(rpdto.getRegPrice()).equals(
								 PRFormatHelper.doubleToTwoDigitString(pricePoint.getRegPrice()))){
							 isNewPricePoint = false;
						 }
					}
				}
				if(isNewPricePoint){
					pricePoint.setNewPricePoint(isNewPricePoint);
				}
			}
		}
		
	}
*/	
	/**
	 * Formatting float value to two decimal digits
	 * 
	 * @param a
	 * @return
	 */
/*	private String FormatFloatToTwoDecimalDigit(float a){
		return (Float.toString(a).concat("000")).substring(0, Float.toString(a).lastIndexOf(".")+3);
	}

*/	
	
	//need to implement logic //prodc/startdate/regprice
	/**
	 * Method to get observations : 3. Less than 4 weekly observations 
	 * for this price point over last 52 weeks (18 months was changed)
	 * 
	 * @param conn
	 * @param predictionItemDTO
	 */
	private void hasLessObservations(PredictionItemDTO predictionItemDTO) {

		logger.debug("hasLessObservations() : Start for Item code : " + predictionItemDTO.itemCodeOrLirId);

		try {
			HashMap<Integer, ProductMetricsDataDTO> movementMap = new HashMap<Integer, ProductMetricsDataDTO>();
			HashMap<MultiplePrice, Integer> frequencyOfPricePoint = new HashMap<MultiplePrice, Integer>();

			if (movementData.get(predictionItemDTO.itemCodeOrLirId) != null && movementData.get(predictionItemDTO.itemCodeOrLirId).size() > 0) {
				logger.debug("Item has movement history ");
				movementMap = movementData.get(predictionItemDTO.itemCodeOrLirId);

				logger.debug("Calculating frequency of regular price point for item  " + predictionItemDTO.itemCodeOrLirId);
				for (Integer cal : movementMap.keySet()) {
					ProductMetricsDataDTO pmdto = movementMap.get(cal);
					MultiplePrice multiplePrice = new MultiplePrice((int) pmdto.getRegularQuantity(), Double.valueOf(pmdto.getRegularPrice()));
					
					if (frequencyOfPricePoint.get(multiplePrice) != null) {
						frequencyOfPricePoint.put(multiplePrice, frequencyOfPricePoint.get(multiplePrice) + 1);
					} else {
						frequencyOfPricePoint.put(multiplePrice, 1);
					}
				}

				logger.debug("Calculating frequency complete. Intitiating comparision with prediction price points");
				for (PricePointDTO pricePoint : predictionItemDTO.pricePoints) {
					if (!pricePoint.getIsAlreadyPredicted()) {
						MultiplePrice multiplePrice = new MultiplePrice(pricePoint.getRegQuantity(), pricePoint.getRegPrice());
						if (frequencyOfPricePoint.get(multiplePrice) == null || (frequencyOfPricePoint.get(multiplePrice) != null
								&& frequencyOfPricePoint.get(multiplePrice) < observationLowerLimit)) {

							logger.debug("Frequency of Observation at price point : " + pricePoint.getRegPrice() + " is "
									+ frequencyOfPricePoint.get(multiplePrice));
							logger.debug("There is no observation or less that 4 observations for this price point in movement history");
							logger.debug("Marking as questionable prediction - Reason- 3 - " + pricePoint.getRegPrice());
							pricePoint.setQuestionablePrediction(
									pricePoint.getQuestionablePrediction() + PRConstants.LESS_OBSERVATIONS_IN_LAST_X_WEEKS + SEPARATOR);
						} else {
							logger.debug("Frequency of Observation at price point : " + pricePoint.getRegPrice() + " is "
									+ frequencyOfPricePoint.get(multiplePrice));
						}
					}
				}
			} else {
				logger.debug("Item has no movement history");
				for (PricePointDTO pricePoint : predictionItemDTO.pricePoints) {
					if (!pricePoint.getIsAlreadyPredicted()) {
						pricePoint.setQuestionablePrediction(
								pricePoint.getQuestionablePrediction() + PRConstants.LESS_OBSERVATIONS_IN_LAST_X_WEEKS + SEPARATOR);
					}
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
			logger.error("hasLessObservations() : Exception : " + ex.toString(), ex);
		}
		logger.debug("hasLessObservations() : Start for Item code : " + predictionItemDTO.itemCodeOrLirId);

	}
	
	/**
	 * Method to format the output Questionable prediction reason string 
	 * 
	 * @param predictionItemDTO
	 */
	private void formatQuestionablePredictionValue(PredictionItemDTO predictionItemDTO){
		logger.debug("formatQuestionablePredictionValue() : Start " );
		for (PricePointDTO pricePoint : predictionItemDTO.pricePoints) {
			if(pricePoint.getQuestionablePrediction()!=null && pricePoint.getQuestionablePrediction().length() > 0 && !pricePoint.getIsAlreadyPredicted()){
				pricePoint.setQuestionablePrediction(pricePoint.getQuestionablePrediction().
						substring(0, pricePoint.getQuestionablePrediction().length()-1));
			}
		}
	}
	
	/**
	 * Check forecast difference is due to rounding digits
	 * @param item
	 * @return
	 */
	private boolean isForecastDiffDueToRounding(PricePointDTO curPricePoint, PricePointDTO regPricePoint, int roundingDiffLimit) {
		boolean forecastDiffDueToRounding = false;

		int maxDiffForecast = (int) (Math.round(curPricePoint.getPredictedMovement()) + roundingDiffLimit);
		int minDiffForecast = (int) (Math.round(curPricePoint.getPredictedMovement()) - roundingDiffLimit);
//		logger.debug(" Pred Mov: " + Math.round(regPricePoint.getPredictedMovement()) + " max diff mov: " + maxDiffForecast + " min diff mov: "
//				+ minDiffForecast);
		if (Math.round(regPricePoint.getPredictedMovement()) <= maxDiffForecast
				&& Math.round(regPricePoint.getPredictedMovement()) >= minDiffForecast) {
			forecastDiffDueToRounding = true;
		}
		return forecastDiffDueToRounding;
	}
	
	/**
	 * To check predicted unit forecast diff is within the given limit 
	 * @param item
	 * @return
	 */
	private boolean isForecastPercentageWithinRange(PricePointDTO curPricePoint, PricePointDTO regPricePoint, int forecastRange) {
		boolean forecastWithinRange = false;

			double forecastdiffLimit = (curPricePoint.getPredictedMovement() / 100) * forecastRange;

			int maxDiffForecast = (int) Math.round(curPricePoint.getPredictedMovement() + forecastdiffLimit);
			int minDiffForecast = (int) Math.round(curPricePoint.getPredictedMovement() - forecastdiffLimit);
//			logger.debug(" Pred Mov: " + Math.round(regPricePoint.getPredictedMovement()) + " max diff mov: "
//					+ maxDiffForecast + " min diff mov: " + minDiffForecast);
			if (Math.round(regPricePoint.getPredictedMovement()) <= maxDiffForecast && Math.round(regPricePoint.getPredictedMovement()) >= minDiffForecast) {
				forecastWithinRange = true;
			}
		return forecastWithinRange;
	}
	
	//remove this function
//	private void verifyOutcome(List<PredictionInputDTO> predictionInputDTOs){
//		for (PredictionInputDTO predictionInputDTO : predictionInputDTOs) {
//			for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
//				logger.debug("Test output for item : " + predictionItemDTO.itemCodeOrLirId);
//				for(PricePointDTO pd : predictionItemDTO.pricePoints){
//					logger.debug("verifyOutcome() :: Price Point : regPrice " + pd.getRegPrice() + " : isQuestionable : " + pd.getQuestionablePrediction());
//				}
//			}
//		}
//	}
//	

	//need to implement logic
	
//	private void lessPriceChanges(PredictionItemDTO predictionItemDTO){
	
//	}
	/*private void lessPriceChanges(PredictionItemDTO predictionItemDTO){
			
		boolean isLessPriceChanges = false;
		
		if(newItemList.size()>0 && newItemList.contains(predictionItemDTO.itemCodeOrLirId)){
			
		HashMap<String, List<RetailPriceDTO>> itemHistory = itemPriceHistory.get(predictionItemDTO.itemCodeOrLirId);
		
		float previousPrice = 0.0f;
		int princeChangeCount = -1;
			for(String startDate: itemHistory.keySet()){
				List<RetailPriceDTO> rdtoList = itemHistory.get(startDate);
				for(RetailPriceDTO rdto: rdtoList){
					if(Float.compare(rdto.getRegPrice() ,previousPrice) != 0){
						previousPrice = rdto.getRegPrice();
						princeChangeCount++;
						if(princeChangeCount > 4){
							isLessPriceChanges = false;
							return;
						}
					}
				}
			}
			
			if(!hasSufficientHist){
				for (PricePointDTO pricePoint : predictionItemDTO.pricePoints) {
						pricePoint.setQuestionablePrediction(
								pricePoint.getQuestionablePrediction() + 
								PropertyManager.getProperty("NEW_ITEM_LESS_HISTORY_THAN_X_WEEKS") + SEPARATOR);
				}
			}
		}
		
	}*/
	
	
	//need to implement logic
//	private boolean firstTimeInAd(){
//		return false;
//	}
	//need to implement logic
//	private boolean firstTimeOnPromo(){
//		return false;
//	}	
	
}
