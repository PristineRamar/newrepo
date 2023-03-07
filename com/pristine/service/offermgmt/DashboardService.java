package com.pristine.service.offermgmt;

import java.sql.Connection;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.StrategyDAO;
import com.pristine.dto.offermgmt.PRGuidelinePI;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRCommonUtil;

public class DashboardService {

	private static Logger logger = Logger.getLogger("DashboardService");

	public Double calculateValue(Connection conn, Integer strategyId, Double futurePI, List<PRItemDTO> itemList)
			throws GeneralException, OfferManagementException {
		Double value = null;
		//PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		PriceIndexCalculation priceIndexCalculation = new PriceIndexCalculation();
		double futureIndex = futurePI == null ? 0 : futurePI;
		double minIndexGoal = Constants.DEFAULT_NA, maxIndexGoal = Constants.DEFAULT_NA;
		
		//Get min and max price index goal
		if(strategyId != null){
			//PRStrategyDTO strategy = pricingEngineDAO.getStrategyDefinition(conn, new Long(strategyId));
			StrategyDAO strategyDAO = new StrategyDAO();
			PRStrategyDTO strategy = strategyDAO.getStrategyDefinition(conn, new Long(strategyId));
			if (strategy.getGuidelines() != null){
				PRGuidelinePI temp = new PRGuidelinePI();
				PRGuidelinePI categoryLevelPI =  temp.getCategoryLevelPIGuideline(strategy.getGuidelines().getPiGuideline());
				if(categoryLevelPI != null){
					minIndexGoal = categoryLevelPI.getMinValue() ;
					maxIndexGoal = categoryLevelPI.getMaxValue() ;
				}
			}
			
			//If there is future and goal index
			if (futureIndex > 0 && (minIndexGoal != Constants.DEFAULT_NA || maxIndexGoal != Constants.DEFAULT_NA)) {
				PRGuidelinePI temp = new PRGuidelinePI();
				PRGuidelinePI categoryLevelPI =  temp.getCategoryLevelPIGuideline(strategy.getGuidelines().getPiGuideline());
				//Check if it already meets the goal
				boolean isPIMeetsGoal = priceIndexCalculation.isPriceIndexMeetsGoal(futureIndex, categoryLevelPI);
				//If not meeting the goal
				if(!isPIMeetsGoal){
					value = getValue(itemList, minIndexGoal, maxIndexGoal, categoryLevelPI);
				}
			}
		}
		
		return value;
	}
	
	private Double getValue(List<PRItemDTO> itemList, double minIndexGoal, double maxIndexGoal, PRGuidelinePI categoryLevelPI) {
		PriceIndexCalculation priceIndexCalculation = new PriceIndexCalculation();
		Double value = 0d;
		logger.debug("Calculation of value started...");
		logger.debug("Min Goal / Max Goal : " + minIndexGoal + "/" + maxIndexGoal);
		for (PRItemDTO item : itemList) {
			Double basePrice = PRCommonUtil.getUnitPrice(item.getRegMPack(), item.getRegPrice(),
					item.getRegMPrice(), true);
			double compPrice = PRCommonUtil.getUnitPrice(item.getCompPrice(), true) ;
			double movement = item.getPredictedMovement() == null ? 0 : item.getPredictedMovement();
			double goalIndex = 0;
			
			if ((!item.isLir()) && basePrice > 0 && compPrice > 0) {
				double itemIndex = compPrice / basePrice;
				double itemValue = 0;
				boolean isPIMeetsGoal = priceIndexCalculation.isPriceIndexMeetsGoal(itemIndex, categoryLevelPI);
				
				if (minIndexGoal != Constants.DEFAULT_NA && maxIndexGoal != Constants.DEFAULT_NA) {
					if (itemIndex < minIndexGoal) {
						goalIndex = minIndexGoal;
					} else if (itemIndex > maxIndexGoal) {
						goalIndex = maxIndexGoal;
					} 
				} else if (minIndexGoal != Constants.DEFAULT_NA) {
					goalIndex = minIndexGoal;
				} else {
					goalIndex = maxIndexGoal;
				}
				
				// Formula to find the value -- (1/goal index - 1/future index) * Comp Price * Movement
				if (goalIndex > 0 && !isPIMeetsGoal) {
					goalIndex = goalIndex / 100;
					itemValue = ((1.0 / goalIndex - 1.0 / itemIndex) * compPrice * movement);
					value = value + itemValue;
				}

//				logger.debug("Item Code: " + item.getItemCode() + ",Base Price: " + basePrice + ",Comp Price: "
//						+ compPrice + ",Movement: " + movement + ",Index Goal: " + goalIndex + ",Item Index: "
//						+ itemIndex + ",Item Value: " + itemValue + ",Value: " + value);
			}
		}
		logger.debug("Calculation of value ended...");
		return value == 0 ? null : value;
	}
}
