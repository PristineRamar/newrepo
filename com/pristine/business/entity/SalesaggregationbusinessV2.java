package com.pristine.business.entity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.dto.salesanalysis.SpecialCriteriaDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

public class SalesaggregationbusinessV2 {

	static Logger logger = Logger.getLogger("Salesaggregationbusiness");

	final double  maxMarginPercentage = 999.99;
	final double  minMarginPercentage = -999.99;

	double _groupRegularQuantity = 0;
	double _groupRegularRevenue	 = 0;
	double _groupSaleQuantity    = 0;
	double _groupSaleRevenue     = 0;
	double _groupTotalRevenue    = 0;
	
	double _groupRegVolumeMovement    = 0;
	double _groupSaleVolumeMovement   = 0;
	double _groupIgRegVolumeRevenue   = 0;
	double _groupIgSaleVolumeRevenue  = 0;
	
	// add the visit count for product level
	double _groupVisitCount = 0;
	
	
	// variable added for product level margin
	double _groupRegularMargin = 0;
	double _groupSaleMargin = 0;

	double _groupPLRegularQuantity = 0;
	double _groupPLRegularRevenue	 = 0;
	double _groupPLSaleQuantity    = 0;
	double _groupPLSaleRevenue     = 0;
	double _groupPLTotalRevenue    = 0;
	
	double _groupPLRegVolumeMovement    = 0;
	double _groupPLSaleVolumeMovement   = 0;
	
	// add the visit count for product level
	double _groupPLVisitCount = 0;
	
	// variable added for product level margin
	double _groupPLRegularMargin = 0;
	double _groupPLSaleMargin = 0;

	
	//Presto subsription chain
	int prestoSubsriber = 0;

	//Constructor added
	public SalesaggregationbusinessV2(){
		prestoSubsriber = Integer.parseInt(PropertyManager.getProperty("PRESTO_SUBSCRIBER"));
	}
	
	/*
	 * ****************************************************************
	 * Method used to Iterate the movement Daily list 
	 * Argument 1: Movement data List
	 * Argument 2: Output productAggregation data map
	 * Argument 3: Store ID
	 * Argument 4: UOM data Map from DB
	 * Argument 5: List of possible UOMs
	 * Argument 6: Product Visit count Map
	 * Argument 7: Deal cost map
	 * Argument 8: Processing product level id
	 * Argument 9: GAS Item list
	 * Argument 10 : SpecialCriteriaDTO
	 * Return Output productAggregation data map
	 * @throws GeneralException
	 * ****************************************************************
	 */
	public void calculateProductSummary(
			List<MovementDailyAggregateDTO> movementList,
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			SummaryDataDTO objStoreDto, HashMap<String, String> uomMapDb,
			HashMap<String, String> uomMapPro,
			HashMap<String, Integer> productVisitMap,
			HashMap<String, Integer> plProductVisitMap,
			HashMap<String, Double> dealCostMap, int productLevelId, 
			HashMap<Integer, Integer> gasItems,
			SpecialCriteriaDTO objSpecialDto, boolean marginCalcReq) throws GeneralException {

		HashMap<String, SummaryDataDTO> leastProductMap = new HashMap<String, SummaryDataDTO>();

		for (int ii = 0; ii < movementList.size(); ii++) {

			// Get the movement DTO
			MovementDailyAggregateDTO objMoveDto = movementList.get(ii);

			if (productLevelId == Constants.POSDEPARTMENT){
				
				if (gasItems.size() >0 && 
						gasItems.containsKey(objMoveDto.getItemCode())){

					// call the movement by volume business....
					processMovementByVolume(uomMapDb, uomMapPro, objMoveDto);
					
					// Get state based GAS Revenue
					calculateStoreBasedGasRevenue(objMoveDto, 
							objSpecialDto.getstoreBasedGasValue(), 
							objStoreDto.getstoreState());
				}
			}
			
			String productId = null;
			
			if (productLevelId == Constants.POSDEPARTMENT)
			{
				productId = Integer.toString(objMoveDto.getPosDepartment());
			}
			else
			{
				HashMap<Integer, Integer> parentProductMap = objMoveDto.getProductData();
				productId = parentProductMap.get(productLevelId).toString();
			}
			
			// Get Cost 
			double itemCost = getCost(dealCostMap, objMoveDto);
			
			
			if (productId != null || productId != "") {
				if (leastProductMap.containsKey(productId)) {
					// Update Sub-category aggregation data into Map
					aggregateDatadto(objMoveDto, leastProductMap,
							"ADDPROCESS", productId, productLevelId,
							productVisitMap, plProductVisitMap, itemCost, marginCalcReq);
				} else {
					// Add Sub-category aggregation data into Map
					aggregateDatadto(objMoveDto, leastProductMap,
							"NEWPROCESS", productId, productLevelId,
							productVisitMap, plProductVisitMap, itemCost, marginCalcReq);
				}
			}
		}

		if (leastProductMap.size() > 0)
			productMap.put(productLevelId, leastProductMap);
	}
	
	public void calculateStoreSummary(
			List<MovementDailyAggregateDTO> movementList,
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			SummaryDataDTO objStoreDto, HashMap<String, String> uomMapDb,
			HashMap<String, String> uomMapPro, Double storeVisitCount,
			Double plStoreVisitCount,
			HashMap<String, Double> couponAggregationMap,
			HashMap<String, Double> itemCostMap, 
			HashMap<Integer, Integer> gasItems, 
			HashMap<String, Double> noCostItemMap, boolean marginCalcReq) 
												throws GeneralException {

		HashMap<String, SummaryDataDTO> storeMap = 
										new HashMap<String, SummaryDataDTO>();

		double totRegularMovement = 0;
		double totSaleMovement = 0;
		double totRegularRevenue = 0;
		double totSaleRevenue = 0;
		double totRegularMargin = 0;
		double totSaleMargin = 0;
		double totalSaleCost = 0;
		double totalRegularCost = 0;
		double totRegVolumeMovement = 0;
		double totSaleVolumeMovement = 0;
		double totIgRegVolumeRevenue = 0;
		double totIgSaleVolumeRevenue = 0;

		double plRegularMovement = 0;
		double plSaleMovement = 0;
		double plRegularRevenue = 0;
		double plSaleRevenue = 0;
		double plRegularMargin = 0;
		double plSaleMargin = 0;
		double plSaleCost = 0;
		double plRegularCost = 0;
		double plRegVolumeMovement = 0;
		double plSaleVolumeMovement = 0;
		
		SummaryDataDTO objSummarryDto = null;

		for (int ii = 0; ii < movementList.size(); ii++) {

			// Get the movement DTO
			MovementDailyAggregateDTO objMoveDto = movementList.get(ii);
			// logger.debug("Processing Item Code" + objMoveDto.getItemCode());
			if (gasItems.size() >0 && 
					gasItems.containsKey(objMoveDto.getItemCode())){
				logger.debug("GAS Item" + objMoveDto.getItemCode());
				continue;
			} 

			totRegularMovement += objMoveDto.getRegularQuantity();
			totSaleMovement += objMoveDto.getSaleQuantity();
			totRegularRevenue += objMoveDto.getRevenueRegular();
			totSaleRevenue += objMoveDto.getRevenueSale();
			
			if (objMoveDto.getPrivateLabel() ==1){
				plRegularMovement += objMoveDto.getRegularQuantity();
				plSaleMovement += objMoveDto.getSaleQuantity();
				plRegularRevenue += objMoveDto.getRevenueRegular();
				plSaleRevenue += objMoveDto.getRevenueSale();
			}
			
			if (marginCalcReq){
				// Get deal cost 
				double itemCost = getCost(itemCostMap, objMoveDto);

				// Get items which don't have cost
				if (itemCost == 0)
					getNoCostItemData(itemCostMap, objMoveDto, noCostItemMap);
				
				if (itemCost != 0){
					if (objMoveDto.getFlag().equalsIgnoreCase("Y")){
						totSaleMargin += (objMoveDto.getRevenueSale() - itemCost);
						totalSaleCost += itemCost;
						
						if (objMoveDto.getPrivateLabel() ==1){
							plSaleMargin += (objMoveDto.getRevenueSale() - itemCost);
							plSaleCost += itemCost;
						}
					}
					else{
						totRegularMargin += (objMoveDto.getRevenueRegular() - itemCost);
						totalRegularCost += itemCost;
						
						if (objMoveDto.getPrivateLabel() ==1){
							plRegularMargin += (objMoveDto.getRevenueRegular() - itemCost);
							plRegularCost += itemCost;
						}
					}
				}
			}
			
			// Calculate movement by volume
			processMovementByVolume(uomMapDb, uomMapPro, objMoveDto);
			
			// Calculate Movement By Volume
			totRegVolumeMovement += objMoveDto.getregMovementVolume();
			totSaleVolumeMovement += objMoveDto.getsaleMovementVolume();
			totIgRegVolumeRevenue += objMoveDto.getigRegVolumeRev();
			totIgSaleVolumeRevenue += objMoveDto.getigSaleVolumeRev();
			
			if (objMoveDto.getPrivateLabel() ==1){
				plRegVolumeMovement += objMoveDto.getregMovementVolume();
				plSaleVolumeMovement += objMoveDto.getsaleMovementVolume();
			}
		}

		double storeCouponRevenue = 0;
		
		if (couponAggregationMap.size() >0){

			// Subtract the coupon values from Store Revenue
			Object[] copArray = couponAggregationMap.values().toArray();
			for (int cop = 0; cop < copArray.length; cop++) {
				storeCouponRevenue += (Double) copArray[cop];
			}
			
			totSaleRevenue = totSaleRevenue - storeCouponRevenue;
			totSaleMargin = totSaleMargin - storeCouponRevenue;
		}
		
		objSummarryDto = new SummaryDataDTO();
		objSummarryDto.setRegularMovement(totRegularMovement);
		objSummarryDto.setSaleMovement(totSaleMovement);
		objSummarryDto.setSaleRevenue(totSaleRevenue);
		objSummarryDto.setRegularRevenue(totRegularRevenue);
		objSummarryDto.setProductId("0");
		objSummarryDto.setProductLevelId(0);
		objSummarryDto.setSaleDealCost(totalSaleCost);
		objSummarryDto.setRegularDealCost(totalRegularCost);
		objSummarryDto.setregMovementVolume(totRegVolumeMovement);
		objSummarryDto.setsaleMovementVolume(totSaleVolumeMovement);
		objSummarryDto.setigRegVolumeRev(totIgRegVolumeRevenue);
		objSummarryDto.setigSaleVolumeRev(totIgSaleVolumeRevenue);
		objSummarryDto.setTotalVisitCount(storeVisitCount);
		objSummarryDto.setRegularMargin(totRegularMargin);
		objSummarryDto.setSaleMargin(totSaleMargin);

		objSummarryDto.setPLRegularRevenue(plRegularRevenue);
		objSummarryDto.setPLSaleRevenue(plSaleRevenue);
		objSummarryDto.setPLRegularMovement(plRegularMovement);
		objSummarryDto.setPLSaleMovement(plSaleMovement);
		objSummarryDto.setPLregMovementVolume(plRegVolumeMovement);
		objSummarryDto.setPLsaleMovementVolume(plSaleVolumeMovement);
		objSummarryDto.setPLRegularMargin(plRegularMargin);
		objSummarryDto.setPLSaleMargin(plSaleMargin);
		objSummarryDto.setPLRegularDealCost(plRegularCost);
		objSummarryDto.setPLSaleDealCost(plSaleCost);
		objSummarryDto.setPLTotalVisitCount(plStoreVisitCount);

		// Calculate the other metrics info
		CalculateStoreMetrics(objSummarryDto, marginCalcReq);
		storeMap.put(String.valueOf(objStoreDto.getLocationId()),
				objSummarryDto);

		if (storeMap.size() > 0)
			productMap.put(Constants.LOCATIONLEVELID, storeMap);

	}
	
	
	
	/*
	 *  Sum the all product level aggregation and stored into map
	 *  Argument 1 : MovementDailyAggregateDTO
	 *  Argument 2 : HashMap<String, SummaryDataDTO>
	 *  Argument 3 : modeOfProcess
	 *  Argument 4 : productId
	 *  Argument 5 : productLevelId
	 *  Argument 6 : HashMap<String, Integer>
	 *  Argument 7 : couponRevenue
	 */
	private void aggregateDatadto(MovementDailyAggregateDTO movementDataDto,
			HashMap<String, SummaryDataDTO> byProductMap, String modeOfProcess,
			String productId, int productLevelId,
			HashMap<String, Integer> visitMap,  HashMap<String, Integer> plvisitMap,
			double itemCost, boolean marginCalcReq) throws GeneralException {

		SummaryDataDTO objSummaryDto = new SummaryDataDTO();

		// check the new for map or exist
		if (modeOfProcess.equalsIgnoreCase("NEWPROCESS")) {

			// Aggregation process for TotalRevenue
			objSummaryDto.setRegularMovement(movementDataDto
					.getRegularQuantity());
			objSummaryDto.setSaleMovement(movementDataDto.getSaleQuantity());
			objSummaryDto
					.setRegularRevenue(movementDataDto.getRevenueRegular());
			objSummaryDto.setSaleRevenue(movementDataDto.getRevenueSale());

			// Aggregation process for movement by volume
			objSummaryDto.setregMovementVolume(movementDataDto
					.getregMovementVolume());
			objSummaryDto.setsaleMovementVolume(movementDataDto
					.getsaleMovementVolume());
			objSummaryDto
					.setigRegVolumeRev(movementDataDto.getigRegVolumeRev());
			objSummaryDto.setigSaleVolumeRev(movementDataDto
					.getigSaleVolumeRev());
			
			if (itemCost != 0 ){
				if (movementDataDto.getFlag().equalsIgnoreCase("Y")){
					objSummaryDto.setSaleMargin(movementDataDto.getRevenueSale() - itemCost);
					objSummaryDto.setSaleDealCost(itemCost);
				}
				else {
					objSummaryDto.setRegularMargin(movementDataDto.getRevenueRegular() - itemCost);
					objSummaryDto.setRegularDealCost(itemCost);
				}
			}
			
			// Aggregation process for TotalRevenue at Private Label level
			if (movementDataDto.getPrivateLabel() == 1){
				objSummaryDto.setPLRegularRevenue(movementDataDto.getRevenueRegular());
				objSummaryDto.setPLSaleRevenue(movementDataDto.getRevenueSale());
				
				objSummaryDto.setPLRegularMovement(movementDataDto.getRegularQuantity());
				objSummaryDto.setPLSaleMovement(movementDataDto.getSaleQuantity());
	
				// Aggregation process for movement by volume
				objSummaryDto.setPLregMovementVolume(movementDataDto.getregMovementVolume());
				objSummaryDto.setPLsaleMovementVolume(movementDataDto.getsaleMovementVolume());
				
				if (itemCost != 0 ){
					if (movementDataDto.getFlag().equalsIgnoreCase("Y")){
						objSummaryDto.setPLSaleMargin(movementDataDto.getRevenueSale() - itemCost);
						objSummaryDto.setPLSaleDealCost(itemCost);
					}
					else {
						objSummaryDto.setPLRegularMargin(movementDataDto.getRevenueRegular() - itemCost);
						objSummaryDto.setPLRegularDealCost(itemCost);
					}
				}			
			}
			else //Set initial values
			{
				objSummaryDto.setPLRegularRevenue(0);
				objSummaryDto.setPLSaleRevenue(0);
				objSummaryDto.setPLRegularMovement(0);
				objSummaryDto.setPLSaleMovement(0);
				objSummaryDto.setPLregMovementVolume(0);
				objSummaryDto.setPLsaleMovementVolume(0);
				objSummaryDto.setPLSaleMargin(0);
				objSummaryDto.setPLSaleDealCost(0);
				objSummaryDto.setPLRegularMargin(0);
				objSummaryDto.setPLRegularDealCost(0);
			}
			
		} else if (modeOfProcess.equalsIgnoreCase("ADDPROCESS")) {

			SummaryDataDTO objExistingSummaryDto = byProductMap.get(productId);

			// Aggregation process for TotalRevenue
			objSummaryDto.setRegularMovement(movementDataDto
					.getRegularQuantity()
					+ objExistingSummaryDto.getRegularMovement());
			objSummaryDto.setSaleMovement(movementDataDto.getSaleQuantity()
					+ objExistingSummaryDto.getSaleMovement());
			objSummaryDto.setRegularRevenue(movementDataDto.getRevenueRegular()
					+ objExistingSummaryDto.getRegularRevenue());
			objSummaryDto.setSaleRevenue(movementDataDto.getRevenueSale()
					+ objExistingSummaryDto.getSaleRevenue());

			// Aggregation process for movement by volume
			objSummaryDto.setregMovementVolume(movementDataDto
					.getregMovementVolume()
					+ objExistingSummaryDto.getregMovementVolume());
			objSummaryDto.setsaleMovementVolume(movementDataDto
					.getsaleMovementVolume()
					+ objExistingSummaryDto.getsaleMovementVolume());
			objSummaryDto.setigRegVolumeRev(movementDataDto.getigRegVolumeRev()
					+ objExistingSummaryDto.getigRegVolumeRev());
			objSummaryDto.setigSaleVolumeRev(movementDataDto
					.getigSaleVolumeRev()
					+ objExistingSummaryDto.getigSaleVolumeRev());

			if (itemCost != 0){
				if (movementDataDto.getFlag().equalsIgnoreCase("Y")){
					objSummaryDto.setSaleDealCost(itemCost + objExistingSummaryDto.getSaleDealCost());
					objSummaryDto.setRegularDealCost(objExistingSummaryDto.getRegularDealCost());

					objSummaryDto.setRegularMargin(objExistingSummaryDto.getRegularMargin());
					objSummaryDto.setSaleMargin(objExistingSummaryDto.getSaleMargin() + (movementDataDto.getRevenueSale() - itemCost));
				}
				else {
					objSummaryDto.setRegularDealCost(itemCost + objExistingSummaryDto.getRegularDealCost());
					objSummaryDto.setSaleDealCost(objExistingSummaryDto.getSaleDealCost());

					objSummaryDto.setRegularMargin(objExistingSummaryDto.getRegularMargin() + (movementDataDto.getRevenueRegular() - itemCost) );
					objSummaryDto.setSaleMargin(objExistingSummaryDto.getSaleMargin());
				}				
			}
			else
			{
				objSummaryDto.setRegularDealCost(objExistingSummaryDto.getRegularDealCost());
				objSummaryDto.setSaleDealCost(objExistingSummaryDto.getSaleDealCost());
				
				objSummaryDto.setRegularMargin(objExistingSummaryDto.getRegularMargin());
				objSummaryDto.setSaleMargin(objExistingSummaryDto.getSaleMargin());
			}
		
			// Aggregation process for TotalRevenue at Private label level
			if (movementDataDto.getPrivateLabel() == 1){			
				objSummaryDto.setPLRegularMovement(movementDataDto.getRegularQuantity()	+ objExistingSummaryDto.getPLRegularMovement());
				objSummaryDto.setPLSaleMovement(movementDataDto.getSaleQuantity() +  objExistingSummaryDto.getPLSaleMovement());
				objSummaryDto.setPLRegularRevenue(movementDataDto.getRevenueRegular() + objExistingSummaryDto.getPLRegularRevenue());
				objSummaryDto.setPLSaleRevenue(movementDataDto.getRevenueSale()	+ objExistingSummaryDto.getPLSaleRevenue());
				objSummaryDto.setPLregMovementVolume(movementDataDto.getregMovementVolume() + objExistingSummaryDto.getPLregMovementVolume());
				objSummaryDto.setPLsaleMovementVolume(movementDataDto.getsaleMovementVolume() + objExistingSummaryDto.getPLsaleMovementVolume());

				if (itemCost != 0){
					if (movementDataDto.getFlag().equalsIgnoreCase("Y")){
						objSummaryDto.setPLSaleDealCost(itemCost + objExistingSummaryDto.getPLSaleDealCost());
						objSummaryDto.setPLRegularDealCost(objExistingSummaryDto.getPLRegularDealCost());
	
						objSummaryDto.setPLRegularMargin(objExistingSummaryDto.getPLRegularMargin());
						objSummaryDto.setPLSaleMargin(objExistingSummaryDto.getPLSaleMargin() + (movementDataDto.getRevenueSale() - itemCost));
					}
					else {
						objSummaryDto.setPLRegularDealCost(itemCost + objExistingSummaryDto.getPLRegularDealCost());
						objSummaryDto.setPLSaleDealCost(objExistingSummaryDto.getPLSaleDealCost());
	
						objSummaryDto.setPLRegularMargin(objExistingSummaryDto.getPLRegularMargin() + (movementDataDto.getRevenueRegular() - itemCost) );
						objSummaryDto.setPLSaleMargin(objExistingSummaryDto.getPLSaleMargin());
					}				
				}
				else
				{
					objSummaryDto.setPLRegularDealCost(objExistingSummaryDto.getPLRegularDealCost());
					objSummaryDto.setPLSaleDealCost(objExistingSummaryDto.getPLSaleDealCost());
					
					objSummaryDto.setPLRegularMargin(objExistingSummaryDto.getPLRegularMargin());
					objSummaryDto.setPLSaleMargin(objExistingSummaryDto.getPLSaleMargin());
				}
			}
			else{ //Retain existing values
				objSummaryDto.setPLRegularRevenue(objExistingSummaryDto.getPLRegularRevenue());
				objSummaryDto.setPLSaleRevenue(objExistingSummaryDto.getPLSaleRevenue());

				objSummaryDto.setPLRegularMovement(objExistingSummaryDto.getPLRegularMovement());
				objSummaryDto.setPLSaleMovement(objExistingSummaryDto.getPLSaleMovement());

				objSummaryDto.setPLregMovementVolume(objExistingSummaryDto.getPLregMovementVolume());
				objSummaryDto.setPLsaleMovementVolume(objExistingSummaryDto.getPLsaleMovementVolume());

				objSummaryDto.setPLRegularMargin(objExistingSummaryDto.getPLRegularMargin());
				objSummaryDto.setPLSaleMargin(objExistingSummaryDto.getPLSaleMargin());

				objSummaryDto.setPLSaleDealCost(objExistingSummaryDto.getPLSaleDealCost());
				objSummaryDto.setPLRegularDealCost(objExistingSummaryDto.getPLRegularDealCost());
			}
		}
		
		// add product-id and product-level-id
		objSummaryDto.setProductId(productId);
		objSummaryDto.setProductLevelId(productLevelId);

			// find the visit count from the map
			if (visitMap.containsKey(productId)) {
				objSummaryDto.setTotalVisitCount(visitMap.get(productId));
			} else {
				objSummaryDto.setTotalVisitCount(0);
			}

			// find the visit count from the map
			if (plvisitMap.containsKey(productId)) {
				objSummaryDto.setPLTotalVisitCount(plvisitMap.get(productId));
			} else {
				objSummaryDto.setPLTotalVisitCount(0);
			}			
			
			
		// Calculate the other Metrics info
		CalculateStoreMetrics(objSummaryDto, marginCalcReq);

		// add the aggregated values into map
		byProductMap.put(productId, objSummaryDto);

	}

	
	/*
	 * ****************************************************************
	 * Method used to Calculate the metrics information 
	 * Argument 1: summary DTO
	 * Argument 2: mode
	 * ****************************************************************
	 */

	public void CalculateStoreMetrics(SummaryDataDTO summaryDTO, boolean marginReq) throws GeneralException {
		// Find the Total Movement
		//String marginFlag = PropertyManager.getProperty("MARGIN_CALCULATION_REQUIRED",null);
		
		try {
			double regularRevenue = summaryDTO.getRegularRevenue(); 
			double saleRevenue = summaryDTO.getSaleRevenue();
			double totalRevenue = regularRevenue+ saleRevenue;

			double regularMargin = summaryDTO.getSaleMargin(); 
			double saleMargin = summaryDTO.getRegularMargin();
			double totalMargin = regularMargin + saleMargin;
			
			double totalMovement = summaryDTO.getSaleMovement()+ summaryDTO.getRegularMovement();
			double totIgVolumeRevenue = summaryDTO.getigRegVolumeRev()+ summaryDTO.getigSaleVolumeRev();
			double totMovementVolume = summaryDTO.getregMovementVolume()+ summaryDTO.getsaleMovementVolume();

			double totalMarginPercent = 0;
			double regularMarginPercent = 0;
			double salemarginpercent  =0;

			double avgOrderSize = 0;
			
			if( totalRevenue > 0 && totalMargin > 0)
				 totalMarginPercent  = totalMargin /  totalRevenue * 100;
			 
			if( regularRevenue > 0 && regularMargin > 0)
				 regularMarginPercent =  regularMargin / regularRevenue * 100;

			if( saleRevenue > 0 && saleMargin > 0)
				 salemarginpercent = saleMargin / saleRevenue * 100;

			
			if (totalRevenue > 0 && summaryDTO.getTotalVisitCount() > 0 )
				avgOrderSize = totalRevenue /  summaryDTO.getTotalVisitCount();
			
			summaryDTO.setTotalRevenue(totalRevenue); 
			summaryDTO.setTotalMovement(totalMovement);
			summaryDTO.settotMovementVolume(totMovementVolume);
			summaryDTO.setigtotVolumeRev(totIgVolumeRevenue);

			summaryDTO.setAverageOrderSize(avgOrderSize);

			if(marginReq){
				summaryDTO.setTotalMargin(totalMargin);
				summaryDTO.setRegularMargin(regularMargin);
				summaryDTO.setSaleMargin(saleMargin);
			  
				summaryDTO.setTotalMarginPer(setMaxMarginPercentage(totalMarginPercent));
				summaryDTO.setRegularMarginPer(setMaxMarginPercentage(regularMarginPercent));
				summaryDTO.setSaleMarginPer(setMaxMarginPercentage(salemarginpercent)); 
			}

			// Calculation for Private label
			double plregularRevenue = summaryDTO.getPLRegularRevenue(); 
			double plsaleRevenue = summaryDTO.getPLSaleRevenue();
			double pltotalRevenue = plregularRevenue+ plsaleRevenue;

			double plregularMargin = summaryDTO.getPLRegularMargin();
			double plsaleMargin = summaryDTO.getPLSaleMargin();
			double pltotalMargin = plregularMargin + plsaleMargin;
			
			double pltotalMovement = summaryDTO.getPLSaleMovement()+ summaryDTO.getPLRegularMovement();
			double pltotMovementVolume = summaryDTO.getPLregMovementVolume()+ summaryDTO.getPLsaleMovementVolume();

			double pltotalMarginPercent = 0;
			double plregularMarginPercent = 0;
			double plsalemarginpercent  =0;

			double plavgOrderSize = 0;
			
			if( pltotalRevenue > 0 && pltotalMargin > 0)
				pltotalMarginPercent  = pltotalMargin /  pltotalRevenue * 100;
			 
			if( plregularRevenue > 0 && plregularMargin > 0)
				plregularMarginPercent =  plregularMargin / plregularRevenue * 100;

			if( plsaleRevenue > 0 && plsaleMargin > 0)
				plsalemarginpercent = plsaleMargin / plsaleRevenue * 100;

			if (pltotalRevenue > 0 && summaryDTO.getPLTotalVisitCount() > 0 )
				plavgOrderSize = pltotalRevenue /  summaryDTO.getPLTotalVisitCount();
			
			summaryDTO.setPLTotalRevenue(pltotalRevenue); 
			summaryDTO.setPLTotalMovement(pltotalMovement);
			summaryDTO.setPLtotMovementVolume(pltotMovementVolume);
			summaryDTO.setPLAverageOrderSize(plavgOrderSize);

			if(marginReq){
				summaryDTO.setPLTotalMargin(pltotalMargin);
				summaryDTO.setPLRegularMargin(plregularMargin);
				summaryDTO.setPLSaleMargin(plsaleMargin);
			  
				summaryDTO.setPLTotalMarginPer(setMaxMarginPercentage(pltotalMarginPercent));
				summaryDTO.setPLRegularMarginPer(setMaxMarginPercentage(plregularMarginPercent));
				summaryDTO.setPLSaleMarginPer(setMaxMarginPercentage(plsalemarginpercent)); 
			}
			
		} catch (Exception exe) {
			logger.error("com.pristine.business.entity.SalesaggregationbusinessV2.CalculateStoreMetrics - Error while Calculating Metrics" , exe);
			throw new GeneralException(" Error while CalculateMetrixInformation" , exe);
		}

	}

	
	/*
	 * ****************************************************************
	 * Method used to Calculate the metrix information Argument 1: summarydto
	 * Argument 2: mode
	 * ****************************************************************
	 */

	public void CalculateMetrixInformation(SummaryDataDTO summaryDTO, boolean noMarginCalculation) throws GeneralException {
		// Find the Total Movement
		String marginFlag = PropertyManager.getProperty("MARGIN_CALCULATION_REQUIRED",null);
		
		try {
			// Total Revenue
			double totalRevenue = 0;
			double pltotalRevenue = 0;

			double totalMovement = summaryDTO.getSaleMovement()+ summaryDTO.getRegularMovement();
			double totIgVolumeRevenue = summaryDTO.getigRegVolumeRev()+ summaryDTO.getigSaleVolumeRev();
			double totMovementVolume = summaryDTO.getregMovementVolume()+ summaryDTO.getsaleMovementVolume();
			summaryDTO.setTotalMovement(totalMovement);
			summaryDTO.settotMovementVolume(totMovementVolume);

			double pltotalMovement = summaryDTO.getPLSaleMovement()+ summaryDTO.getPLRegularMovement();
			double pltotMovementVolume = summaryDTO.getPLregMovementVolume()+ summaryDTO.getPLsaleMovementVolume();
			summaryDTO.setPLTotalMovement(pltotalMovement);
			summaryDTO.setPLtotMovementVolume(pltotMovementVolume);
			
			summaryDTO.setigtotVolumeRev(totIgVolumeRevenue);
			
			double totalMargin = 0; double regularMargin = 0; double  saleMargin = 0;
			double itotalMargin = 0 ; double iregularMargin = 0; double isaleMargin = 0;

			double pltotalMargin = 0; double plregularMargin = 0; double  plsaleMargin = 0;
			double plitotalMargin = 0 ; double pliregularMargin = 0; double plisaleMargin = 0;
						
			if(noMarginCalculation){
			  // For Summary Rollup Process // Deal  Cost 
				totalRevenue = summaryDTO.getTotalRevenue();
				regularMargin = summaryDTO.getRegularMargin(); 
				saleMargin    = summaryDTO.getSaleMargin();
				iregularMargin = summaryDTO.getiregularMargin();
				isaleMargin = summaryDTO.getisaleMargin();
				itotalMargin = iregularMargin + isaleMargin;
				totalMargin   = regularMargin + saleMargin;
				
				pltotalRevenue = summaryDTO.getPLTotalRevenue();
				plregularMargin = summaryDTO.getPLRegularMargin(); 
				plsaleMargin    = summaryDTO.getPLSaleMargin();
				pliregularMargin = summaryDTO.getPLiregularMargin();
				plisaleMargin = summaryDTO.getPLisaleMargin();
				plitotalMargin = pliregularMargin + plisaleMargin;
				pltotalMargin   = plregularMargin + plsaleMargin;
			 } 
			else{ 
				totalRevenue = summaryDTO.getSaleRevenue()+ summaryDTO.getRegularRevenue();
				
				// For Summary Daily Process
				double saleDealCost = summaryDTO.getSaleDealCost();
				double regularDealCost = summaryDTO.getRegularDealCost();
				totalMargin     = totalRevenue- (saleDealCost + regularDealCost);
				regularMargin   = summaryDTO.getRegularRevenue()- regularDealCost;
				saleMargin      = summaryDTO.getSaleRevenue() - saleDealCost;

				pltotalRevenue = summaryDTO.getPLSaleRevenue()+ summaryDTO.getPLRegularRevenue();
				
				// For Summary Daily Process
				double plsaleDealCost = summaryDTO.getPLSaleDealCost();
				double plregularDealCost = summaryDTO.getPLRegularDealCost();
				pltotalMargin     = pltotalRevenue- (plsaleDealCost + plregularDealCost);
				plregularMargin   = summaryDTO.getPLRegularRevenue()- plregularDealCost;
				plsaleMargin      = summaryDTO.getPLSaleRevenue() - plsaleDealCost;			
			}
			
			summaryDTO.setTotalRevenue(totalRevenue);
			summaryDTO.setPLTotalRevenue(pltotalRevenue);
			
			 double totalMarginPercent = 0;
			 double regularMarginPercent = 0;
			 double salemarginpercent  =0;
			 
			 double itotalMarginPercent = 0;
			 double iregularMarginPercent = 0;
			 double isalemarginpercent  =0;
			 
			 double pltotalMarginPercent = 0;
			 double plregularMarginPercent = 0;
			 double plsalemarginpercent  =0;
			 
			 double plitotalMarginPercent = 0;
			 double pliregularMarginPercent = 0;
			 double plisalemarginpercent  =0;
			 
			 
			 if( totalRevenue !=0)
				 totalMarginPercent  = totalMargin /  totalRevenue * 100;
			 if( summaryDTO.getRegularRevenue() != 0)
				 regularMarginPercent =  regularMargin / summaryDTO.getRegularRevenue() * 100;
			 if( summaryDTO.getSaleRevenue() != 0)
				 salemarginpercent = saleMargin / summaryDTO.getSaleRevenue() * 100;
			 
			 if( pltotalRevenue !=0)
				 pltotalMarginPercent  = pltotalMargin /  pltotalRevenue * 100;
			 if( summaryDTO.getPLRegularRevenue() != 0)
				 plregularMarginPercent =  plregularMargin / summaryDTO.getPLRegularRevenue() * 100;
			 if( summaryDTO.getPLSaleRevenue() != 0)
				 plsalemarginpercent = plsaleMargin / summaryDTO.getPLSaleRevenue() * 100;

			 if(noMarginCalculation){
				 if( summaryDTO.getitotalRevenue() != 0)
					 itotalMarginPercent  = itotalMargin /  summaryDTO.getitotalRevenue() * 100;
				 if( summaryDTO.getiregularRevenue() != 0)
					 iregularMarginPercent =  iregularMargin / summaryDTO.getiregularRevenue() * 100;
				 if( summaryDTO.getisaleRevenue() != 0)
					 isalemarginpercent = isaleMargin / summaryDTO.getisaleRevenue() * 100;

				 if( summaryDTO.getPLitotalRevenue() != 0)
					 plitotalMarginPercent  = plitotalMargin /  summaryDTO.getPLitotalRevenue() * 100;
				 if( summaryDTO.getPLiregularRevenue() != 0)
					 pliregularMarginPercent =  pliregularMargin / summaryDTO.getPLiregularRevenue() * 100;
				 if( summaryDTO.getPLisaleRevenue() != 0)
					 plisalemarginpercent = plisaleMargin / summaryDTO.getPLisaleRevenue() * 100;
			 }
			  
			 if( marginFlag.equalsIgnoreCase("YES")){
				summaryDTO.setTotalMargin(totalMargin);
				summaryDTO.setRegularMargin(regularMargin);
				summaryDTO.setSaleMargin(saleMargin);

				summaryDTO.setTotalMarginPer(setMaxMarginPercentage(totalMarginPercent));
				summaryDTO.setRegularMarginPer(setMaxMarginPercentage(regularMarginPercent));
				summaryDTO.setSaleMarginPer(setMaxMarginPercentage(salemarginpercent)); 
				  
				summaryDTO.setitotalMarginPer(setMaxMarginPercentage(itotalMarginPercent));
				summaryDTO.setiregularMarginPer(setMaxMarginPercentage(iregularMarginPercent));
				summaryDTO.setisaleMarginPer(setMaxMarginPercentage(isalemarginpercent));

				summaryDTO.setPLTotalMargin(pltotalMargin);
				summaryDTO.setPLRegularMargin(plregularMargin);
				summaryDTO.setPLSaleMargin(plsaleMargin);
				
				summaryDTO.setPLTotalMarginPer(setMaxMarginPercentage(pltotalMarginPercent));
				summaryDTO.setPLRegularMarginPer(setMaxMarginPercentage(plregularMarginPercent));
				summaryDTO.setPLSaleMarginPer(setMaxMarginPercentage(plsalemarginpercent)); 
				  
				summaryDTO.setPLitotalMarginPer(setMaxMarginPercentage(plitotalMarginPercent));
				summaryDTO.setPLiregularMarginPer(setMaxMarginPercentage(pliregularMarginPercent));
				summaryDTO.setPLisaleMarginPer(setMaxMarginPercentage(plisalemarginpercent));
			 }
			 
			  // if condition remove for adding the visit count in all level
			  	 
			  double avgOrderSize = totalRevenue /  summaryDTO.getTotalVisitCount();;
			  summaryDTO.setAverageOrderSize(avgOrderSize);
			 			  
			  if (  noMarginCalculation  && summaryDTO.getitotalVisitCount() > 0) {
				  avgOrderSize = summaryDTO.getitotalRevenue() / summaryDTO.getitotalVisitCount();
				  summaryDTO.setiaverageOrderSize(avgOrderSize);
			  }
			  
			  double plavgOrderSize = pltotalRevenue /  summaryDTO.getPLTotalVisitCount();;
			  summaryDTO.setPLAverageOrderSize(plavgOrderSize);
			  
			  if (  noMarginCalculation  && summaryDTO.getPLitotalVisitCount() > 0) {
				  plavgOrderSize = summaryDTO.getPLitotalRevenue() / summaryDTO.getPLitotalVisitCount();
				  summaryDTO.setPLiaverageOrderSize(plavgOrderSize);
			  }
			  
		} catch (Exception exe) {
			logger.error(" Error while Calculating Metrics" , exe);
			throw new GeneralException(" Error while CalculateMetrixInformation" , exe);
		}

	}

	/*
	 * ****************************************************************
	 * Method used aggregate the grouping list 
	 * Argument 1: MovementDailyAggregateDTO List 
	 * output : Daily Summary Records added into hashlist
	 * 
	 * @throws Exception
	 * ****************************************************************
	 */

	public void calculateGroupMetrics(
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			ProductGroupDTO prodGroupobj,
			HashMap<String, Integer> productVisitCount) throws GeneralException {

		_groupRegularQuantity = 0;
		_groupRegularRevenue = 0;
		_groupSaleQuantity = 0;
		_groupSaleRevenue = 0;
		_groupVisitCount = 0;

		// process added for movement by volume
		_groupRegVolumeMovement = 0;
		_groupSaleVolumeMovement = 0;
		_groupIgRegVolumeRevenue = 0;
		_groupIgSaleVolumeRevenue = 0;

		// added for margin calculation
		_groupRegularMargin = 0;
		_groupSaleMargin = 0;
		_groupPLRegularRevenue = 0;
		_groupPLSaleRevenue = 0;
		_groupPLRegularQuantity = 0;
		_groupPLSaleQuantity = 0;
		_groupPLRegVolumeMovement = 0;
		_groupPLSaleVolumeMovement = 0;
		_groupPLRegularMargin = 0;
		_groupPLSaleMargin = 0;		
		_groupPLVisitCount = 0;
		
		ArrayList<ProductDTO> productChildList = prodGroupobj.getProductData();
		HashMap<String, SummaryDataDTO> childMap = productMap.get(prodGroupobj
				.getChildLevelId());
		int productLevelId = prodGroupobj.getProductLevelId();
		int tempProductId = 0;
		HashMap<String, SummaryDataDTO> groupingHashMap = new HashMap<String, SummaryDataDTO>();

		try {

			for (int ii = 0; ii < productChildList.size(); ii++) {
				ProductDTO productDTO = productChildList.get(ii);
				

				// Assign the productId to tempProductId
				if (tempProductId == 0) {
					tempProductId = productDTO.getProductId();
				}
				if (tempProductId == productDTO.getProductId()) {
					// add with previous group data
					if (childMap.containsKey(String.valueOf(productDTO
							.getChildProductId()))){
						SumupGroupList(childMap.get(String.valueOf(productDTO
								.getChildProductId())), productVisitCount,
								productDTO.getProductId());
					}
				} else {
					// aggregate the product
					productLevelAggregation(_groupRegularQuantity,
							_groupSaleQuantity, _groupRegularRevenue,
							_groupSaleRevenue, tempProductId, productLevelId,
							groupingHashMap, productMap,
							_groupRegVolumeMovement, _groupSaleVolumeMovement,
							_groupIgRegVolumeRevenue,
							_groupIgSaleVolumeRevenue, _groupVisitCount,
							_groupRegularMargin, _groupSaleMargin,
							_groupPLRegularQuantity, _groupPLSaleQuantity, 
							_groupPLRegularRevenue, _groupSaleRevenue,
							_groupPLRegVolumeMovement, _groupPLSaleVolumeMovement,
							_groupPLVisitCount,
							_groupPLRegularMargin, _groupPLSaleMargin);

					tempProductId = productDTO.getProductId();

					_groupRegularQuantity = 0;
					_groupRegularRevenue = 0;
					_groupSaleQuantity = 0;
					_groupSaleRevenue = 0;
					_groupVisitCount = 0;
					_groupRegVolumeMovement = 0;
					_groupSaleVolumeMovement = 0;
					_groupIgRegVolumeRevenue = 0;
					_groupIgSaleVolumeRevenue = 0;
					_groupRegularMargin = 0;
					_groupSaleMargin = 0;

					_groupPLRegularRevenue = 0;
					_groupPLSaleRevenue = 0;
					_groupPLRegularMargin = 0;
					_groupPLSaleMargin = 0;
					_groupPLRegularQuantity = 0;
					_groupPLSaleQuantity = 0;
					_groupPLRegVolumeMovement = 0;
					_groupPLSaleVolumeMovement = 0;
					_groupPLVisitCount = 0;
					
					if (childMap.containsKey(String.valueOf(productDTO
							.getChildProductId()))){
						logger.debug("Child product exist");
						SumupGroupList(childMap.get(String.valueOf(productDTO
								.getChildProductId())), productVisitCount,
								productDTO.getProductId());
					}
				}
			}

			productLevelAggregation(_groupRegularQuantity, _groupSaleQuantity,
					_groupRegularRevenue, _groupSaleRevenue, tempProductId,
					productLevelId, groupingHashMap, productMap,
					_groupRegVolumeMovement, _groupSaleVolumeMovement,
					_groupIgRegVolumeRevenue, _groupIgSaleVolumeRevenue,
					_groupVisitCount, _groupRegularMargin, _groupSaleMargin,
					_groupPLRegularQuantity, _groupPLSaleQuantity,
					_groupPLRegularRevenue, _groupPLSaleRevenue,
					_groupPLRegVolumeMovement, _groupPLSaleVolumeMovement,
					_groupPLVisitCount, _groupPLRegularMargin, _groupPLSaleMargin);

		} catch (Exception exe) {
			logger.error(" Error while Aggregate the higher level products...",
					exe);
			throw new GeneralException(
					" Error while Aggregate the higher level products...", exe);
		}

	}
	


	public void calculateGroupMetricsV2(
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			ProductGroupDTO prodGroupobj,
			HashMap<String, Integer> productVisitCount, 
			HashMap<String, Integer> plProductVisitCount, 
			boolean visitSum) throws GeneralException {

		_groupRegularQuantity = 0;
		_groupRegularRevenue = 0;
		_groupSaleQuantity = 0;
		_groupSaleRevenue = 0;
		_groupVisitCount = 0;

		// process added for movement by volume
		_groupRegVolumeMovement = 0;
		_groupSaleVolumeMovement = 0;
		_groupIgRegVolumeRevenue = 0;
		_groupIgSaleVolumeRevenue = 0;

		// added for margin calculation
		_groupRegularMargin = 0;
		_groupSaleMargin = 0;

		
		//For Private label metrics
		_groupPLRegularRevenue = 0;
		_groupPLSaleRevenue = 0;
		_groupRegularMargin = 0;
		_groupSaleMargin = 0;		
		_groupPLRegularQuantity = 0;
		_groupPLSaleQuantity = 0;
		_groupPLRegVolumeMovement = 0;
		_groupPLSaleVolumeMovement = 0;
		_groupPLVisitCount = 0;

		ArrayList<ProductDTO> productChildList = prodGroupobj.getProductData();
		HashMap<String, SummaryDataDTO> childMap = productMap.get(prodGroupobj
				.getChildLevelId());
		int productLevelId = prodGroupobj.getProductLevelId();
		int tempProductId = 0;
		HashMap<String, SummaryDataDTO> groupingHashMap = new HashMap<String, SummaryDataDTO>();

		logger.debug("Processing for Product Level:" + prodGroupobj.getProductLevelId());
		
		try {

			for (int ii = 0; ii < productChildList.size(); ii++) {
				ProductDTO productDTO = productChildList.get(ii);

				// Assign the productId to tempProductId
				if (tempProductId == 0) {
					tempProductId = productDTO.getProductId();
				}
				if (tempProductId == productDTO.getProductId()) {
					if (childMap.containsKey(String.valueOf(productDTO
							.getChildProductId()))){

						SumupGroupListV2(childMap.get(String.valueOf(productDTO
								.getChildProductId())), productVisitCount, 
								plProductVisitCount, productDTO.getProductId());
					}
				} else {
					// aggregate the product
					// code added for movement by volume....
					// 6 new addition operation added
					//logger.debug("Product change...update existing data" + productDTO.getProductId());
					productLevelAggregation(_groupRegularQuantity,
							_groupSaleQuantity, _groupRegularRevenue,
							_groupSaleRevenue, tempProductId, productLevelId,
							groupingHashMap, productMap,
							_groupRegVolumeMovement, _groupSaleVolumeMovement,
							_groupIgRegVolumeRevenue,
							_groupIgSaleVolumeRevenue, _groupVisitCount,
							_groupRegularMargin, _groupSaleMargin,
							_groupPLRegularQuantity, _groupPLSaleQuantity, 
							_groupPLRegularRevenue, _groupPLSaleRevenue,
							_groupPLRegVolumeMovement, _groupPLSaleVolumeMovement,
							_groupPLVisitCount,
							_groupPLRegularMargin, _groupPLSaleMargin);

					tempProductId = productDTO.getProductId();

					_groupRegularQuantity = 0;
					_groupRegularRevenue = 0;
					_groupSaleQuantity = 0;
					_groupSaleRevenue = 0;
					_groupVisitCount = 0;
					_groupRegVolumeMovement = 0;
					_groupSaleVolumeMovement = 0;
					_groupIgRegVolumeRevenue = 0;
					_groupIgSaleVolumeRevenue = 0;
					_groupRegularMargin = 0;
					_groupSaleMargin = 0;

					_groupPLRegularRevenue = 0;
					_groupPLSaleRevenue = 0;
					_groupPLRegularMargin = 0;
					_groupPLSaleMargin = 0;
					_groupPLRegularQuantity = 0;
					_groupPLSaleQuantity = 0;
					_groupPLRegVolumeMovement = 0;
					_groupPLSaleVolumeMovement = 0;
					_groupPLVisitCount = 0;
					
					if (childMap.containsKey(String.valueOf(productDTO
							.getChildProductId()))){
						SumupGroupListV2(childMap.get(String.valueOf(productDTO
								.getChildProductId())), productVisitCount,
								plProductVisitCount, productDTO.getProductId());
					
					}
				}
			}

			productLevelAggregation(_groupRegularQuantity, _groupSaleQuantity,
					_groupRegularRevenue, _groupSaleRevenue, tempProductId,
					productLevelId, groupingHashMap, productMap,
					_groupRegVolumeMovement, _groupSaleVolumeMovement,
					_groupIgRegVolumeRevenue, _groupIgSaleVolumeRevenue,
					_groupVisitCount, _groupRegularMargin, _groupSaleMargin,
					_groupPLRegularQuantity, _groupPLSaleQuantity,
					_groupPLRegularRevenue, _groupPLSaleRevenue,
					_groupPLRegVolumeMovement, _groupPLSaleVolumeMovement,
					_groupPLVisitCount, _groupPLRegularMargin, _groupPLSaleMargin);

		} catch (Exception exe) {
			logger.error(" Error while Aggregate the higher level products...",
					exe);
			throw new GeneralException(
					" Error while Aggregate the higher level products...", exe);
		}

	}

	
	
	private void productLevelAggregation(double groupRegularQuantity,
			double groupSaleQuantity, double groupRegularRevenue,
			double groupSaleRevenue, int tempProductId, int productLevelId,
			HashMap<String, SummaryDataDTO> groupingHashMap,
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			double RegVolumeMovement, double SaleVolumeMovement,
			double IgRegVolumeRevenue, double IgSaleVolumeRevenue,
			double groupVisitCount , double groupRegularMargin ,
			double groupSaleMargin,
			
			double groupPLRegularQuantity, double groupPLSaleQuantity, 
			double groupPLRegularRevenue, double groupPLSaleRevenue,
			double RegPLVolumeMovement, double SalePLVolumeMovement,
			double groupPLVisitCount , 
			double groupPLRegularMargin, double groupPLSaleMargin) 
										throws GeneralException {

		// Asgin the summary data into dto

		//logger.debug("In productLevelAggregation......");
		
		SummaryDataDTO summaryDTO = new SummaryDataDTO();
		summaryDTO.setRegularMovement(groupRegularQuantity);
		summaryDTO.setRegularRevenue(groupRegularRevenue);
		summaryDTO.setSaleMovement(groupSaleQuantity);
		summaryDTO.setSaleRevenue(groupSaleRevenue);
		summaryDTO.setProductId(String.valueOf(tempProductId));
		summaryDTO.setProductLevelId(productLevelId);
		summaryDTO.setTotalVisitCount(groupVisitCount);

		// code added for margin
		summaryDTO.setRegularMargin(groupRegularMargin);
		summaryDTO.setSaleMargin(groupSaleMargin);

		// code added for movement by volume
		summaryDTO.setregMovementVolume(RegVolumeMovement);
		summaryDTO.setsaleMovementVolume(SaleVolumeMovement);
		summaryDTO.setigRegVolumeRev(IgRegVolumeRevenue);
		summaryDTO.setigSaleVolumeRev(IgSaleVolumeRevenue);
		
		summaryDTO.setTotalRevenue(groupRegularRevenue + groupSaleRevenue);

		summaryDTO.setPLRegularMovement(groupPLRegularQuantity);
		summaryDTO.setPLRegularRevenue(groupPLRegularRevenue);
		summaryDTO.setPLSaleMovement(groupPLSaleQuantity);
		summaryDTO.setPLSaleRevenue(groupPLSaleRevenue);
		summaryDTO.setPLTotalVisitCount(groupPLVisitCount);
		summaryDTO.setPLRegularMargin(groupPLRegularMargin);
		summaryDTO.setPLSaleMargin(groupPLSaleMargin);
		summaryDTO.setPLregMovementVolume(RegPLVolumeMovement);
		summaryDTO.setPLsaleMovementVolume(SalePLVolumeMovement);
		summaryDTO.setPLTotalRevenue(groupPLRegularRevenue + groupPLSaleRevenue);
		
		// Calculate the other metrics info
		CalculateMetrixInformation(summaryDTO, true);
		
		//logger.debug("Total Revenue...." + summaryDTO.getTotalRevenue());
		if (summaryDTO.getTotalRevenue() != 0) {
			/*if( summaryDTO.getTotalRevenue() == 100 && productLevelId == 6){
				summaryDTO.setTotalRevenue(0);
				summaryDTO.setRegularRevenue(0);
			}*/
				
			//Add the DTO into hashMap
			groupingHashMap.put(String.valueOf(tempProductId), summaryDTO);
			//logger.debug("Add into main map for....." + productLevelId);
			productMap.put(productLevelId, groupingHashMap);
		}
		
		 
	}


	/*
	 * ****************************************************************
	 * Method used to sum the Group level records
	 * Argument 1 : RegularQuantity
	 * Argument 2 : RegularRevenue
	 * Argument 3 : SaleQuantity 
	 * Argument 4 :SaleRevenue
	 * ****************************************************************
	 */

	private void SumupGroupList(SummaryDataDTO summaryDataDTO,
			HashMap<String, Integer> productVisitCount, int childProductId) {

		_groupRegularQuantity += summaryDataDTO.getRegularMovement();
		_groupRegularRevenue += summaryDataDTO.getRegularRevenue();
		_groupSaleQuantity += summaryDataDTO.getSaleMovement();
		_groupSaleRevenue += summaryDataDTO.getSaleRevenue();

		// code added for movement by volume
		_groupRegVolumeMovement += summaryDataDTO.getregMovementVolume();
		_groupSaleVolumeMovement += summaryDataDTO.getsaleMovementVolume();
		_groupIgRegVolumeRevenue += summaryDataDTO.getigRegVolumeRev();
		_groupIgSaleVolumeRevenue += summaryDataDTO.getigSaleVolumeRev();
		
		// code added for margin calculation
		_groupRegularMargin += summaryDataDTO.getRegularMargin();
		_groupSaleMargin += summaryDataDTO.getSaleMargin();
		
		// logger.debug("Regular Revenue" + _groupRegularRevenue);
		// logger.debug("Sale Revenue" + _groupSaleRevenue);
		

		if (productVisitCount != null
				&& productVisitCount
						.containsKey(String.valueOf(childProductId))) {
			_groupVisitCount = productVisitCount.get(String
					.valueOf(childProductId));
			
			  //logger.debug("Product Id:" + summaryDataDTO.getProductId()
			  //		  			+ ", Visit Count:" + _groupVisitCount);
			 
		} else {
			_groupVisitCount += summaryDataDTO.getTotalVisitCount();
		}

	}


	private void SumupGroupListV2(SummaryDataDTO summaryDataDTO,
			HashMap<String, Integer> productVisitCount, 
			HashMap<String, Integer> productPLVisitCount, 
											int childProductId) {

		_groupRegularQuantity += summaryDataDTO.getRegularMovement();
		_groupRegularRevenue += summaryDataDTO.getRegularRevenue();
		_groupSaleQuantity += summaryDataDTO.getSaleMovement();
		_groupSaleRevenue += summaryDataDTO.getSaleRevenue();

		// code added for movement by volume
		_groupRegVolumeMovement += summaryDataDTO.getregMovementVolume();
		_groupSaleVolumeMovement += summaryDataDTO.getsaleMovementVolume();
		_groupIgRegVolumeRevenue += summaryDataDTO.getigRegVolumeRev();
		_groupIgSaleVolumeRevenue += summaryDataDTO.getigSaleVolumeRev();
		
		// code added for margin calculation
		_groupRegularMargin += summaryDataDTO.getRegularMargin();
		_groupSaleMargin += summaryDataDTO.getSaleMargin();
		
		//Private label aggregation
		_groupPLRegularRevenue += summaryDataDTO.getPLRegularRevenue();
		_groupPLSaleRevenue += summaryDataDTO.getPLSaleRevenue();
		_groupPLRegularMargin += summaryDataDTO.getPLRegularMargin();
		_groupPLSaleMargin += summaryDataDTO.getPLSaleMargin();
		_groupPLRegularQuantity += summaryDataDTO.getPLRegularMovement();
		_groupPLSaleQuantity += summaryDataDTO.getPLSaleMovement();
		_groupPLRegVolumeMovement += summaryDataDTO.getPLregMovementVolume();
		_groupPLSaleVolumeMovement += summaryDataDTO.getPLsaleMovementVolume();

		if (productVisitCount != null
				&& productVisitCount
						.containsKey(String.valueOf(childProductId))) {
			_groupVisitCount = productVisitCount.get(String
					.valueOf(childProductId));
			 
		} else {
			_groupVisitCount += summaryDataDTO.getTotalVisitCount();
		}


		// Visit at all item level		
		if (productVisitCount != null
				&& productVisitCount
						.containsKey(String.valueOf(childProductId))) {
			_groupVisitCount = productVisitCount.get(String
					.valueOf(childProductId));
			 
		} else {
			_groupVisitCount += summaryDataDTO.getTotalVisitCount();
		}
		
		// Visit at Private label level
		if (productPLVisitCount != null
				&& productPLVisitCount
						.containsKey(String.valueOf(childProductId))) {
			_groupPLVisitCount = productPLVisitCount.get(String
					.valueOf(childProductId));
			 
		} else {
			_groupPLVisitCount += summaryDataDTO.getPLTotalVisitCount();
		}		
	}
	
	
	/*
	 * ****************************************************************
	 * Method used to itreate the daily list and product list(sum of last aggregated records )
	 * and find the total sum 
	 * Argument 1 : Daily List
	 * Argument 2 : Product List
	 * Argument 3 : Calendar Id
	 * Returns productList
	 * @catch Exception
	 * ****************************************************************
	 */
	
	public HashMap<String, SummaryDataDTO> sumupAggregation(
			HashMap<String, SummaryDataDTO> dailyMap,
			HashMap<String, SummaryDataDTO> productMap, int calendarId) throws GeneralException {

		HashMap<String, SummaryDataDTO> mainMap = new HashMap<String, SummaryDataDTO>();

		Object[] productArray = productMap.values().toArray();
		for (int wM = 0; wM < productArray.length; wM++) {
			SummaryDataDTO productDto = (SummaryDataDTO) productArray[wM];

			if (dailyMap.containsKey(productDto.getProductLevelId() + "_"
					+ productDto.getProductId())) {

				weekAggregation(productDto,	dailyMap.get(productDto.getProductLevelId() + "_"
								+ productDto.getProductId()));
				dailyMap.remove(productDto.getProductLevelId() + "_"
						+ productDto.getProductId());
				mainMap.put(
						productDto.getProductLevelId() + "_"
								+ productDto.getProductId(), productDto);

			} else {
				productDto.setsummaryCtdId(0);
				mainMap.put(
						productDto.getProductLevelId() + "_"
								+ productDto.getProductId(), productDto);
			}

		}

		Object[] dailyArray = dailyMap.values().toArray();
		for (int wM = 0; wM < dailyArray.length; wM++) {
			SummaryDataDTO dailyDto = (SummaryDataDTO) dailyArray[wM];
			mainMap.put(
					dailyDto.getProductLevelId() + "_"
							+ dailyDto.getProductId(), dailyDto);
		}

		return mainMap;

	}
		

	private void weekAggregation(SummaryDataDTO productDto,
			SummaryDataDTO dailyDto) throws GeneralException {
		
		
		productDto.setsummaryCtdId(dailyDto.getsummaryCtdId());
		productDto.setTotalMovement(productDto.getTotalMovement()
				+ dailyDto.getTotalMovement());
		productDto.setRegularMovement(productDto.getRegularMovement()
				+ dailyDto.getRegularMovement());
		productDto.setSaleMovement(productDto.getSaleMovement()
				+ dailyDto.getSaleMovement());
		productDto.setTotalRevenue(productDto.getTotalRevenue()
				+ dailyDto.getTotalRevenue());
		productDto.setRegularRevenue(productDto.getRegularRevenue()
				+ dailyDto.getRegularRevenue());
		productDto.setSaleRevenue(productDto.getSaleRevenue()
				+ dailyDto.getSaleRevenue());
		productDto.setTotalMargin(productDto.getTotalMargin()
				+ dailyDto.getTotalMargin());
		productDto.setRegularMargin(productDto.getRegularMargin()
				+ dailyDto.getRegularMargin());
		productDto.setSaleMargin(productDto.getSaleMargin()
				+ dailyDto.getSaleMargin());
		productDto.setTotalVisitCount(productDto.getTotalVisitCount()
				+ dailyDto.getTotalVisitCount());

		productDto.setitotalMovement(productDto.getitotalMovement()
				+ dailyDto.getitotalMovement());
		productDto.setiregularMovement(productDto.getiregularMovement()
				+ dailyDto.getiregularMovement());
		productDto.setisaleMovement(productDto.getisaleMovement()
				+ dailyDto.getisaleMovement());
		productDto.setitotalRevenue(productDto.getitotalRevenue()
				+ dailyDto.getitotalRevenue());
		productDto.setiregularRevenue(productDto.getiregularRevenue()
				+ dailyDto.getiregularRevenue());
		productDto.setisaleRevenue(productDto.getisaleRevenue()
				+ dailyDto.getisaleRevenue());
		productDto.setitotalMargin(productDto.getitotalMargin()
				+ dailyDto.getitotalMargin());
		productDto.setiregularMargin(productDto.getiregularMargin()
				+ dailyDto.getiregularMargin());
		productDto.setisaleMargin(productDto.getisaleMargin()
				+ dailyDto.getisaleMargin());
		productDto.setitotalVisitCount(productDto.getitotalVisitCount()
				+ dailyDto.getitotalVisitCount());

		// add the code for movement by volume
		productDto.setregMovementVolume(productDto.getregMovementVolume()
				+ dailyDto.getregMovementVolume());
		productDto.setsaleMovementVolume(productDto.getsaleMovementVolume()
				+ dailyDto.getsaleMovementVolume());
		productDto.setigRegVolumeRev(productDto.getigRegVolumeRev()
				+ dailyDto.getigRegVolumeRev());
		productDto.setigSaleVolumeRev(productDto.getigSaleVolumeRev()
				+ dailyDto.getigSaleVolumeRev());
		productDto.settotMovementVolume(productDto.gettotMovementVolume()
				+ dailyDto.gettotMovementVolume());
		productDto.setigtotVolumeRev(productDto.getigtotVolumeRev()
				+ dailyDto.getigtotVolumeRev());
		productDto.setidRegMovementVolume(productDto.getidRegMovementVolume()
				+ dailyDto.getidRegMovementVolume());
		productDto.setidSaleMovementVolume(productDto.getidSaleMovementVolume()
				+ dailyDto.getidSaleMovementVolume());
		productDto.setidIgRegVolumeRev(productDto.getidIgRegVolumeRev()
				+ dailyDto.getidIgRegVolumeRev());
		productDto.setidIgSaleVolumeRev(productDto.getidIgSaleVolumeRev()
				+ dailyDto.getidIgSaleVolumeRev());
		productDto.setidTotMovementVolume(productDto.getidTotMovementVolume()
				+ dailyDto.getidTotMovementVolume());
		productDto.setIdIgtotVolumeRev(productDto.getIdIgtotVolumeRev()
				+ dailyDto.getIdIgtotVolumeRev());

		CalculateMetrixInformation(productDto,  true);

	}
	
	
	/*
	 *  Method used to do the movement by volume and 
	 *  calculte the igonore movement by volume revenue
	 * Argument 1 :  uomMap
	 * Argument 2 : regularQuantity
	 * Argument 3 :  saleQuantity
	 * Argument 4 : uomId
	 * @return
	 * 
	 */
	public void processMovementByVolume(HashMap<String, String> uomMapDb,
			HashMap<String, String> uomMapPro,
			MovementDailyAggregateDTO movementDataDto) throws GeneralException {

		String uomId = movementDataDto.getuomId();

		try {

			if (uomMapDb.containsKey(uomId)) {

				String uomName = uomMapDb.get(uomId);
				if (uomMapPro.containsKey(uomName)) {

					// logger.debug(" Processing Movement By Volume for.... " +
					// uomName + "_" + movementDataDto.getupc());

					CalculateMovementByVolume(uomName, movementDataDto);
				} else {
					// logger.debug("  Not Processing Movement By Volume for.... "+
					// uomName + "_" + movementDataDto.getupc());

					movementDataDto.setigRegVolumeRev(movementDataDto
							.getRevenueRegular());
					movementDataDto.setigSaleVolumeRev(movementDataDto
							.getRevenueSale());
					movementDataDto.setregMovementVolume(0);
					movementDataDto.setsaleMovementVolume(0);

				}
			} else {

				// logger.debug("  Uom Id Null (Not Processing Movement By volume).... "+
				// movementDataDto.getupc());

				movementDataDto.setigRegVolumeRev(movementDataDto
						.getRevenueRegular());
				movementDataDto.setigSaleVolumeRev(movementDataDto
						.getRevenueSale());

				// code added for
				// movement by volume not reqired means set the deafult values
				movementDataDto.setregMovementVolume(0);
				movementDataDto.setsaleMovementVolume(0);

			}
		} catch (Exception exe) {
			logger.error("Error while calculating MovementByVolume", exe);
			throw new GeneralException("processMovementByVolume", exe);
		}

	}

	private void CalculateMovementByVolume(String uomName,
			MovementDailyAggregateDTO movementDataDto) throws GeneralException {

		double regmovementByVolume = 0;
		double salemovementByVolume = 0;
		double regularQuantity = movementDataDto.getregMovementVolume();
		double saleQuantity = movementDataDto.getsaleMovementVolume();

		try {

			// pound LB = 16* oz
			if (uomName.equalsIgnoreCase("LB")) {
				regmovementByVolume = regularQuantity * 16;
				salemovementByVolume = saleQuantity * 16;
			}
			// Ounce 16 OZ = 1LB
			else if (uomName.equalsIgnoreCase("OZ")) {
				regmovementByVolume = regularQuantity * 1;
				salemovementByVolume = saleQuantity * 1;
			}
			// mILLI liter 1 ml = 0.0338140225589 OZ
			else if (uomName.equalsIgnoreCase("ML")) {
				regmovementByVolume = regularQuantity * 0.0338140225589;
				salemovementByVolume = saleQuantity * 0.0338140225589;
			}
			// Quart Q = 32OZ
			else if (uomName.equalsIgnoreCase("QT")) {
				regmovementByVolume = regularQuantity * 32;
				salemovementByVolume = saleQuantity * 32;
			}
			// Gallon G = 128 OZ
			else if (uomName.equalsIgnoreCase("GA")) {
				regmovementByVolume = regularQuantity * 128;
				salemovementByVolume = saleQuantity * 128;
			}
			// pint = 16 oz
			else if (uomName.equalsIgnoreCase("PT")) {
				regmovementByVolume = regularQuantity * 16;
				salemovementByVolume = saleQuantity * 16;
			}
			// Gram 1 Gram = 0.0352739619 OZ
			else if (uomName.equalsIgnoreCase("GR")) {
				regmovementByVolume = regularQuantity * 0.0352739619;
				salemovementByVolume = saleQuantity * 0.0352739619;
			}
			// Litre = 33.8140227
			else if (uomName.equalsIgnoreCase("LT")) {
				regmovementByVolume = regularQuantity * 33.8140227;
				salemovementByVolume = saleQuantity * 33.8140227;
			}
			// fluid ounces FZ = OZ
			else if (uomName.equalsIgnoreCase("FZ")) {
				regmovementByVolume = regularQuantity * 1;
				salemovementByVolume = saleQuantity * 1;
			}

			// logger.debug(" RegMovementVolume... " + regmovementByVolume +
			// "----- "+ salemovementByVolume);

			movementDataDto.setregMovementVolume(regmovementByVolume);
			movementDataDto.setsaleMovementVolume(salemovementByVolume);
		} catch (Exception exe) {
			logger.error("Error while converting to Ounce:", exe);
			throw new GeneralException("CalculateMovementByVolume",	exe);
		}
	}
	
	
	/*
	 * Method used to calculate the state based store revenue
	 * Argument 1 : MovementDailyAggregateDTO
	 * Argument 2 : HashMap<String, Double> getstoreBasedGasValue
	 * Argument 3 : storeState
	 */

	private void calculateStoreBasedGasRevenue(
			MovementDailyAggregateDTO objMoveDto,
			HashMap<String, Double> storeBasedGasValueMap, String storeState) {
		
		// code added for getting gas revenue based on gross price
		
		if (prestoSubsriber == 50) //For TOPS
		{
			objMoveDto.setRevenueRegular(objMoveDto.getregGrossRevenue());
			objMoveDto.setRevenueSale(objMoveDto.getsaleGrossRevenue());			
		}
		else //For other than TOPS chain
		{
			objMoveDto.setRevenueRegular(objMoveDto.getRevenueRegular());
			objMoveDto.setRevenueSale(objMoveDto.getRevenueSale());
		}		
		
		// check property file map null or not
		if (storeBasedGasValueMap != null) {

			// check the map contains key as given store state
			if (storeBasedGasValueMap.containsKey(storeState)) {

				double actualGasvalue = storeBasedGasValueMap.get(storeState);
				
				// check revenue 0 or not
				if (objMoveDto.getRevenueRegular() != 0) {

					objMoveDto.setRevenueRegular(objMoveDto
							.getRevenueRegular() * actualGasvalue);
				} else {

					objMoveDto.setRevenueSale(objMoveDto.getRevenueSale()
							* actualGasvalue);
				}

			}
		}
	}

	
	
	
	/*
	 * ****************************************************************
	 * Function to get deal cost for item based on Quantity or Weight
	 * Argument 1: Cost Hashmap
	 * Argument 2: Daily Movement Data DTO
	 * ****************************************************************
	 */
	public double getCost(HashMap<String, Double> dealCostMap, 
									MovementDailyAggregateDTO objMoveDto) {
	
		double unitDealCost = 0;
		double calcDealCost = 0;
		
		if (dealCostMap.containsKey(String.valueOf(objMoveDto.getItemCode()))) {
			
			unitDealCost = dealCostMap.get(String.valueOf(objMoveDto.getItemCode()));
			
			//logger.debug("Cost for Item ~ " + objMoveDto.getItemCode() + "|" + unitDealCost);
			
			if (objMoveDto.getActualQuantity() != 0){
				calcDealCost = unitDealCost * objMoveDto.getActualQuantity();
			}
			else if (objMoveDto.getActualWeight() != 0){
				calcDealCost = unitDealCost * objMoveDto.getActualWeight();
			}
		}
		else
		{
			//logger.debug("No Cost found for:" + objMoveDto.getItemCode());
		}
		return calcDealCost;
	}

	
	public void adjustCouponInfo(HashMap<Integer, 
		HashMap<String, SummaryDataDTO>> productMap, 
		HashMap<String, Double> couponAggregationMap){
		
		if (productMap.containsKey(Constants.POSDEPARTMENT)){
			
			HashMap<String, SummaryDataDTO> posDepartmentMap = productMap.get(Constants.POSDEPARTMENT); 

			
			for (String couponKey : couponAggregationMap.keySet()) {
			    //logger.debug("Map key.............." + couponKey);

			    double couponRev = couponAggregationMap.get(couponKey);
				//totalcouponRev += couponRev;
			    
			    //logger.debug("Coupon Revenue......." + couponRev);
			    
			    if (posDepartmentMap.containsKey(couponKey)) {
			    	SummaryDataDTO objPosDto = posDepartmentMap.get(couponKey);
					//logger.debug("Revenue before......." + objPosDto.getRegularRevenue());					
					objPosDto.setRegularRevenue(objPosDto.getRegularRevenue() - couponRev);
					//logger.debug("Revenue after........" + objPosDto.getRegularRevenue());
			    }
			    //else
				//	unadjustedCouponRev += couponRev;
			}		
		
			if (posDepartmentMap.size() >0){
				productMap.remove(Constants.POSDEPARTMENT);
				productMap.put(Constants.POSDEPARTMENT, posDepartmentMap);
			}
		}
	}
	


	public void calculateStoreMetrics(
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			double storeVisitCount, int childProductLevelId, int locationId,
			double totalCoupon, double unadjustedCoupon) throws GeneralException {

		double storeRegularQuantity = 0;
		double storeRegularRevenue	 = 0;
		double storeSaleQuantity    = 0;
		double storeSaleRevenue     = 0;
		//double storeTotalRevenue    = 0;
		double storeRegVolumeMovement    = 0;
		double storeSaleVolumeMovement   = 0;
		double storeIgRegVolumeRevenue   = 0;
		double storeIgSaleVolumeRevenue  = 0;
		double storeRegularMargin = 0;
		double storeSaleMargin = 0;
		//double storeVisitCount = 0;

		HashMap<String, SummaryDataDTO> childProductMap = productMap.get(childProductLevelId);
		Object[] productArray = childProductMap.values().toArray();
		
		for (int wM = 0; wM < productArray.length; wM++) {
			SummaryDataDTO productDto = (SummaryDataDTO) productArray[wM];
			storeRegularQuantity += productDto.getRegularMovement();
			storeRegularRevenue	 += productDto.getRegularRevenue();
			storeSaleQuantity    += productDto.getSaleMovement();
			storeSaleRevenue     += productDto.getSaleRevenue();
			//storeTotalRevenue    += productDto.getTotalRevenue();
			storeRegVolumeMovement    += productDto.getregMovementVolume();
			storeSaleVolumeMovement   += productDto.getsaleMovementVolume();
			storeIgRegVolumeRevenue   += productDto.getigRegVolumeRev();
			storeIgSaleVolumeRevenue  += productDto.getigSaleVolumeRev();
			storeRegularMargin += productDto.getRegularMargin();
			storeSaleMargin += productDto.getSaleMargin();
		}

/*		logger.debug("storeRegularQuantity"+ storeRegularQuantity);
		logger.debug("storeRegularRevenue"+  storeRegularRevenue);
		logger.debug("storeSaleQuantity"+  storeSaleQuantity);
		logger.debug("storeSaleRevenue"+  storeSaleRevenue);
		logger.debug("storeTotalRevenue"+  storeTotalRevenue);
		logger.debug("storeRegVolumeMovement"+  storeRegVolumeMovement);
		logger.debug("storeSaleVolumeMovement"+  storeSaleVolumeMovement);
		logger.debug("storeIgRegVolumeRevenue"+  storeIgRegVolumeRevenue);
		logger.debug("storeIgSaleVolumeRevenue"+  storeIgSaleVolumeRevenue);
		logger.debug("storeRegularMargin"+  storeRegularMargin);
		logger.debug("storeSaleMargin"+  storeSaleMargin);
		logger.debug("storeVisitCount"+  storeVisitCount);*/
		
		SummaryDataDTO summaryDTO = new SummaryDataDTO();
		summaryDTO.setRegularMovement(storeRegularQuantity);
		summaryDTO.setRegularRevenue(storeRegularRevenue);
		summaryDTO.setSaleMovement(storeSaleQuantity);
		summaryDTO.setSaleRevenue(storeSaleRevenue);
		summaryDTO.setProductId("0");
		summaryDTO.setProductLevelId(Constants.LOCATIONLEVELID);
		summaryDTO.setTotalVisitCount(storeVisitCount);
		summaryDTO.setRegularMargin(storeRegularMargin);
		summaryDTO.setSaleMargin(storeSaleMargin);
		summaryDTO.setregMovementVolume(storeRegVolumeMovement);
		summaryDTO.setsaleMovementVolume(storeSaleVolumeMovement);
		summaryDTO.setigRegVolumeRev(storeIgRegVolumeRevenue);
		summaryDTO.setigSaleVolumeRev(storeIgSaleVolumeRevenue);
		
		if (childProductLevelId == Constants.POSDEPARTMENT)
			summaryDTO.setTotalRevenue(storeRegularRevenue + storeSaleRevenue - unadjustedCoupon);
		else
			summaryDTO.setTotalRevenue(storeRegularRevenue + storeSaleRevenue - totalCoupon);
		
		CalculateMetrixInformation(summaryDTO, true);
		
		if (summaryDTO.getTotalRevenue() != 0) {
			HashMap<String, SummaryDataDTO> storeProductMap = new HashMap<String, SummaryDataDTO>();
			storeProductMap.put(String.valueOf(locationId), summaryDTO);
			//logger.debug("Add into main map for....." + productLevelId);
			productMap.put(Constants.LOCATIONLEVELID, storeProductMap);
		}		
	}
	
	/*
	 * ****************************************************************
	 * Function to get deal cost for item based on Quantity or Weight
	 * Argument 1: Cost Hashmap
	 * Argument 2: Daily Movement Data DTO
	 * ****************************************************************
	 */
	public void getNoCostItemData(HashMap<String, Double> dealCostMap, 
									MovementDailyAggregateDTO objMoveDto, HashMap<String, Double> noCostItemMap) {
	
		String itemCode = String.valueOf(objMoveDto.getItemCode());
		
		if (!dealCostMap.containsKey(itemCode)) {
			//logger.debug("Cost info not found for " + itemCode);
			
			double revenue=0.0;
			
			if (noCostItemMap.containsKey(itemCode)) {
				revenue = noCostItemMap.get(itemCode) + 
					objMoveDto.getRevenueRegular() + 
					objMoveDto.getRevenueSale();

				noCostItemMap.remove(itemCode);
				noCostItemMap.put(itemCode, revenue);
			}
			else {
				revenue = objMoveDto.getRevenueRegular() + 
						objMoveDto.getRevenueSale();
				
				noCostItemMap.put(itemCode, revenue);
			}
		}
	}
	
	
	/*
	 * ****************************************************************
	 * Function to fix maximum / minimum margin %
	 * Argument 1: Cost Margin Percentage
	 * ****************************************************************
	 */
	private double setMaxMarginPercentage(double marginPercentage) {
		double maxValue = marginPercentage;
	
		if (maxValue > maxMarginPercentage)
			maxValue = maxMarginPercentage;
		else if (maxValue < minMarginPercentage)
			maxValue = minMarginPercentage;
		
		return maxValue;
	}


}	
