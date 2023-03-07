/*
 * Title: Household summary service
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	14/03/2017	John Britto		Initial Version 
 **********************************************************************************
 */
package com.pristine.service.customer;

import org.apache.log4j.Logger;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.customer.HouseholdSalesSummaryDAO;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.customer.HouseholdSummaryDTO;
import com.pristine.dto.customer.HouseholdSummaryDailyInDTO;
import com.pristine.exception.GeneralException;

public class HouseholdSummaryService {
	private static Logger logger = Logger.getLogger("HouseholdSummaryService");
	private int _cardTransCount = 0;
	private int _cardlessTransCount =0;

	final double  maxMarginPercentage = 999.99;
	final double  minMarginPercentage = -999.99;
	
	/*
	 * ****************************************************************
	 * Steps 
	 * Step 1: Delete existing aggregation
	 * Step 2: Get movement data
	 * Step 3: Get Cost data
	 * Step 4: Get unique transactions @customer level
	 * Step 5: Get unique household data
	 * Step 6: Iterate to aggregate household and item level data
	 * Argument 1: RetailCalendarDTO (Calendar Id and Process Date) 
	 * Argument 2: StoreDTO (Store Number, Store Id and Visit Count) 
	 * Argument 3: SalesAggregationDailyDao 
	 * Argument 4: MovementDAO 
	 * Argument 5 :calendar_id 
	 * Argument 6 :SummaryDataDTO 
	 * Argument 7 :SalesAggregationDao 
	 * Argument 8 : CostDataLoadV2
	 * ****************************************************************
	 */
	public int processDailySummaryData(Connection dbConn, HouseholdSummaryDailyInDTO inDto) 
													throws GeneralException {
		
		HouseholdSalesSummaryDAO objHouseholdDao = new HouseholdSalesSummaryDAO();
		
		// Object for MovementDAO
		MovementDAO objMovement = new MovementDAO();
		
		List<MovementDailyAggregateDTO> movementDataList = 
								new ArrayList<MovementDailyAggregateDTO>();
		
		SalesaggregationbusinessV2 businessObj = new SalesaggregationbusinessV2();

		int processStatus = 0;
		
		try{
			// delete the previous aggregation for the store and calendar id
			logger.debug("Delete previous Aggregation Data...");
			objHouseholdDao.deleteHouseholdSalesSummaryDaily(dbConn, 
					inDto.getStoreId(), inDto.getcalendarId());

			if (inDto.getTargetTable().equalsIgnoreCase("MD")){
//				movementDataList = objMovement.getMovementDailyDataV2(_conn, 
//					objStoreDto.getstoreNumber(), objCalendarDto.getCalendarId(), 
//					_excludeCategory, _leastProductLevel, _gasItems);
			}
			else if (inDto.getTargetTable().equalsIgnoreCase("TL") )
				movementDataList = objMovement.getTransDataForHouseholdDaily(
																dbConn, inDto);
			
			if (movementDataList.size() > 0) {

				HashMap<String, Double> itemCostMap = new HashMap<String, Double>();
				itemCostMap = GetCostData(dbConn, movementDataList, inDto);
				
				//Calculate household level summary
				HashMap<Integer, HouseholdSummaryDTO> householdMap = 
									calculateHouseholdSummary(movementDataList, 
													itemCostMap, businessObj, inDto);
				
				// Calculate household item level summary
				HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>> 
					householdItemMap =  calculateHouseholdItemSummary(
					movementDataList, itemCostMap, businessObj, inDto);
				
				// Calculate item level summary for each household
				//Insert Household data
				if (householdMap != null && householdMap.size() > 0){
					objHouseholdDao.insertHouseholdSalesSummaryDaily(dbConn, 
						householdMap, inDto.getStoreId(), inDto.getcalendarId());
					
					objHouseholdDao.insertItemSalesSummaryDaily(dbConn, 
						householdItemMap, inDto.getStoreId(), inDto.getcalendarId());
					
					logger.info("Total Transaction with card: " + _cardTransCount);
					logger.info("Total Transaction without card: " + _cardlessTransCount);
				}
			}			
		}
		catch (Exception ex) {
			logger.error("Error in processSummaryData", ex);
			throw new GeneralException(" processSummaryData Error", ex);
		}
		
		return processStatus;
	}
	
	/*
	 * ****************************************************************
	 * Function to calculate the household level summary
	 * Argument 1: Movement data
	 * Argument 2: Item cost list
	 * Argument 3: Business service object 
	 * ****************************************************************
	 */
	private HashMap<Integer, HouseholdSummaryDTO> calculateHouseholdSummary(
			List<MovementDailyAggregateDTO> movementList, 
			HashMap<String, Double> itemCostMap,
			SalesaggregationbusinessV2 businessObj,
			HouseholdSummaryDailyInDTO inDto) throws GeneralException{

		HashMap<Integer, Integer> gasItems = inDto.getGasItems();
		HashMap<String, String> uomMapDB = inDto.getUOMLookup();
		HashMap<String, String> uomMapPro = inDto.getUOMConvReq();

		
		HashMap<Integer, HouseholdSummaryDTO> householdMap = new HashMap<Integer, HouseholdSummaryDTO>();
		
		HashMap<String, Integer> cardTransCount = new HashMap<String, Integer>();
		HashMap<String, Integer> nonCardTransCount = new HashMap<String, Integer>();
		
		int lasthouseholdNo =0;
		HouseholdSummaryDTO objsummaryDto = new HouseholdSummaryDTO();
		MovementDailyAggregateDTO objMoveDto = new MovementDailyAggregateDTO();
		
		double regularRevenue = 0;
		double saleRevenue = 0;
		double regularMargin = 0;
		double saleMargin = 0;
		double regularUnit = 0;
		double saleUnit = 0;
		double regularVolume = 0;
		double saleVolume = 0;
		double regularCost = 0;
		double dealCost = 0;
		int visitCount =0;
		double plRegularRevenue = 0;
		double plSaleRevenue = 0;
		double plRegularMargin = 0;
		double plSaleMargin = 0;
		double plRegularMovement = 0;
		double plSaleMovement = 0;
		double plRegularVolume = 0;
		double plSaleVolume = 0;		
		double plRegularCost = 0;
		double plDealCost = 0;
		int plVisitCount =0;
		String lastTransNo = "";
		String lastPLTransNo = "";
 		
		for (int ii = 0; ii < movementList.size(); ii++) {
			objMoveDto = new MovementDailyAggregateDTO();
			objMoveDto = movementList.get(ii);

			//Check transaction done using household card
			if (objMoveDto.getHouseholdNumber() >0){
				if (!cardTransCount.containsKey(objMoveDto.gettransactionNo() ))
					cardTransCount.put(objMoveDto.gettransactionNo(), 0);
			}
			else{
				if (!nonCardTransCount.containsKey(objMoveDto.gettransactionNo()))
					nonCardTransCount.put(objMoveDto.gettransactionNo(), 0);
				
				continue;
			}
			
			// logger.debug("Processing Item Code" + objMoveDto.getItemCode());
			if (gasItems.size() >0 && 
					gasItems.containsKey(objMoveDto.getItemCode())){
				logger.debug("GAS Item" + objMoveDto.getItemCode());
				continue;
			}			
			
			// If transaction is for new customer then add the current customer into the collection	
			if (lasthouseholdNo > 0 && objMoveDto.getHouseholdNumber() != lasthouseholdNo){

				objsummaryDto = new HouseholdSummaryDTO();
				objsummaryDto.setCustomerId(lasthouseholdNo);
				objsummaryDto.setTotalVisit(visitCount);
				objsummaryDto.setRegularRevenue(regularRevenue);
				objsummaryDto.setSaleRevenue(saleRevenue);
				objsummaryDto.setRegularMargin(regularMargin);
				objsummaryDto.setSaleMargin(saleMargin);
				objsummaryDto.setRegularUnit(regularUnit);
				objsummaryDto.setSaleUnit(saleUnit);
				objsummaryDto.setRegularVolume(regularVolume);
				objsummaryDto.setSaleVolume(saleVolume);
				objsummaryDto.setDealCost(dealCost);
				objsummaryDto.setRegularCost(regularCost);
				objsummaryDto.setPLTotalVisit(plVisitCount);				
				objsummaryDto.setPLRegularRevenue(plRegularRevenue);
				objsummaryDto.setPLSaleRevenue(plSaleRevenue);
				objsummaryDto.setPLRegularUnit(plRegularMovement);
				objsummaryDto.setPLSaleUnit(plSaleMovement);
				objsummaryDto.setPLRegularVolume(plRegularVolume);
				objsummaryDto.setPLSaleVolume(plSaleVolume);
				objsummaryDto.setPLRegularMargin(plRegularMargin);
				objsummaryDto.setPLSaleMargin(plSaleMargin);
				objsummaryDto.setPLRegularCost(plRegularCost);
				objsummaryDto.setPLDealCost(plDealCost);
				CalculateFinalMetrics(objsummaryDto);
				householdMap.put(lasthouseholdNo, objsummaryDto);
				
				//Reset variables
				regularRevenue = 0;
				saleRevenue = 0;
				regularMargin = 0;
				saleMargin = 0;
				regularUnit = 0;
				saleUnit = 0;
				regularVolume = 0;
				saleVolume = 0;
				regularCost = 0;
				dealCost = 0;
				visitCount =0;
				plRegularRevenue = 0;
				plSaleRevenue = 0;
				plRegularMargin = 0;
				plSaleMargin = 0;
				plRegularMovement = 0;
				plSaleMovement = 0;
				plRegularVolume = 0;
				plSaleVolume = 0;		
				plRegularCost = 0;
				plDealCost = 0;
				plVisitCount =0;
				lastTransNo = "";
				lastPLTransNo = "";
			}
			
			if (!lastTransNo.equalsIgnoreCase(objMoveDto.gettransactionNo()))
				visitCount++;

			regularUnit += objMoveDto.getRegularQuantity();
			saleUnit += objMoveDto.getSaleQuantity();
			regularRevenue += objMoveDto.getRevenueRegular();
			saleRevenue += objMoveDto.getRevenueSale();
			
			if (objMoveDto.getPrivateLabel() ==1){
				if (!lastPLTransNo.equalsIgnoreCase(objMoveDto.gettransactionNo()))
					plVisitCount++;
				
				plRegularMovement += objMoveDto.getRegularQuantity();
				plSaleMovement += objMoveDto.getSaleQuantity();
				plRegularRevenue += objMoveDto.getRevenueRegular();
				plSaleRevenue += objMoveDto.getRevenueSale();
			}			
			
			// Get deal cost 
			double itemCost = getCost(itemCostMap, objMoveDto);

			if (itemCost != 0){
				if (objMoveDto.getFlag().equalsIgnoreCase("Y")){
					saleMargin += (objMoveDto.getRevenueSale() - itemCost);
					dealCost += itemCost;
					
					if (objMoveDto.getPrivateLabel() ==1){
						plSaleMargin += (objMoveDto.getRevenueSale() - itemCost);
						plDealCost += itemCost;
					}
				}
				else{
					regularMargin += (objMoveDto.getRevenueRegular() - itemCost);
					regularCost += itemCost;
					
					if (objMoveDto.getPrivateLabel() ==1){
						plRegularMargin += (objMoveDto.getRevenueRegular() - itemCost);
						plRegularCost += itemCost;
					}
				}
			}
			
			// Calculate movement by volume
			businessObj.processMovementByVolume(uomMapDB, uomMapPro, objMoveDto);
			
			// Calculate Movement By Volume
			regularVolume += objMoveDto.getregMovementVolume();
			saleVolume += objMoveDto.getsaleMovementVolume();
			
			if (objMoveDto.getPrivateLabel() ==1){
				plRegularVolume += objMoveDto.getregMovementVolume();
				plSaleVolume += objMoveDto.getsaleMovementVolume();
			}
			
			lasthouseholdNo = objMoveDto.getHouseholdNumber(); 
			lastTransNo = objMoveDto.gettransactionNo();
			lastPLTransNo = objMoveDto.gettransactionNo();
		}
		
		if (lasthouseholdNo > 0 && (regularRevenue > 0 || saleRevenue >0)){
			objsummaryDto = new HouseholdSummaryDTO();
			objsummaryDto.setCustomerId(lasthouseholdNo);
			objsummaryDto.setTotalVisit(visitCount);
			objsummaryDto.setRegularRevenue(regularRevenue);
			objsummaryDto.setSaleRevenue(saleRevenue);
			objsummaryDto.setRegularMargin(regularMargin);
			objsummaryDto.setSaleMargin(saleMargin);
			objsummaryDto.setRegularUnit(regularUnit);
			objsummaryDto.setSaleUnit(saleUnit);
			objsummaryDto.setRegularVolume(regularVolume);
			objsummaryDto.setSaleVolume(saleVolume);
			objsummaryDto.setDealCost(dealCost);
			objsummaryDto.setRegularCost(regularCost);
			objsummaryDto.setPLTotalVisit(plVisitCount);				
			objsummaryDto.setPLRegularRevenue(plRegularRevenue);
			objsummaryDto.setPLSaleRevenue(plSaleRevenue);
			objsummaryDto.setPLRegularUnit(plRegularMovement);
			objsummaryDto.setPLSaleUnit(plSaleMovement);
			objsummaryDto.setPLRegularVolume(plRegularVolume);
			objsummaryDto.setPLSaleVolume(plSaleVolume);
			objsummaryDto.setPLRegularMargin(plRegularMargin);
			objsummaryDto.setPLSaleMargin(plSaleMargin);
			objsummaryDto.setPLRegularCost(plRegularCost);
			objsummaryDto.setPLDealCost(plDealCost);
			CalculateFinalMetrics(objsummaryDto);
			householdMap.put(lasthouseholdNo, objsummaryDto);		
		}

		if (cardTransCount.size() >0)
			_cardTransCount = cardTransCount.size(); 

		if (nonCardTransCount.size() >0)
			_cardlessTransCount = nonCardTransCount.size();		
		
		return householdMap;
	}	
	
	
	/*
	 * ****************************************************************
	 * Function to calculate the household level summary
	 * Argument 1: Movement data
	 * Argument 2: Item cost list
	 * Argument 3: Business service object 
	 * ****************************************************************
	 */
	private HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>> calculateHouseholdItemSummary(
			List<MovementDailyAggregateDTO> movementList, 
			HashMap<String, Double> itemCostMap,
			SalesaggregationbusinessV2 businessObj, 
			HouseholdSummaryDailyInDTO inDto) throws GeneralException{

		HashMap<Integer, Integer> gasItems = inDto.getGasItems();
			
		HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>> householdmMap = new HashMap<Integer, HashMap<Integer, HouseholdSummaryDTO>>();
		
		int lasthouseholdNo =0;
		HouseholdSummaryDTO objsummaryDto = new HouseholdSummaryDTO();
		MovementDailyAggregateDTO objMoveDto = new MovementDailyAggregateDTO();
		
		//Variable to hold metrics
		double regularRevenue = 0;
		double saleRevenue = 0;
		double regularMargin = 0;
		double saleMargin = 0;
		double regularUnits = 0;
		double saleUnits = 0;
		double regularVolume = 0;
		double saleVolume = 0;
		double regularCost = 0;
		double saleCost = 0;
		double visitCount =0;
		String lastTransNo = "";
 		int lastItemCode =0;
 		int curItemCode =0;

 		//Collection to hold item level metrics
 		HashMap<Integer, HouseholdSummaryDTO> itemMap = new HashMap<Integer, HouseholdSummaryDTO>();
		
		logger.debug("household | Trans No | Item Code | VisitCount | Reg Rev | Sale Rev | Reg Cost | Deal Cost | Reg Units | Sale Units | Reg Vol | Sale Vol | Reg Weight | Sale Weight");
 		
 		for (int ii = 0; ii < movementList.size(); ii++) {
			objMoveDto = new MovementDailyAggregateDTO();
			objMoveDto = movementList.get(ii);


			// Skip transaction which don't have loyalty card 
			if (objMoveDto.getHouseholdNumber() >0)
			{}
			else{
				continue;
			}			
			
			if (gasItems.size() >0 && 
					gasItems.containsKey(objMoveDto.getItemCode())){
				logger.debug("GAS Item" + objMoveDto.getItemCode());
				continue;
			}			
			
			curItemCode = objMoveDto.getItemCode();
			
			// If transaction is for new customer then add the current customer into the collection	
			if (lasthouseholdNo > 0 && objMoveDto.getHouseholdNumber() != lasthouseholdNo){
				
				// Add current values into collection
				objsummaryDto = new HouseholdSummaryDTO();
				objsummaryDto.setCustomerId(lasthouseholdNo);
				objsummaryDto.setTotalVisit(visitCount);
				objsummaryDto.setRegularRevenue(regularRevenue);
				objsummaryDto.setSaleRevenue(saleRevenue);
				objsummaryDto.setRegularMargin(regularMargin);
				objsummaryDto.setSaleMargin(saleMargin);
				objsummaryDto.setRegularUnit(regularUnits);
				objsummaryDto.setSaleUnit(saleUnits);
				objsummaryDto.setRegularVolume(regularVolume);
				objsummaryDto.setSaleVolume(saleVolume);
				objsummaryDto.setDealCost(saleCost);
				objsummaryDto.setRegularCost(regularCost);
				CalculateFinalMetrics(objsummaryDto);

				if (itemMap.containsKey(lastItemCode))
					itemMap.remove(lastItemCode);
				
				//Current item into collection
				itemMap.put(lastItemCode, objsummaryDto);
				
				//Add current household into the collection
				householdmMap.put(lasthouseholdNo, itemMap);
				
				// Reset objects - Begins
				// Reset item object
				objsummaryDto = new HouseholdSummaryDTO();
				
				//Reset item collection for next household
				itemMap = new HashMap<Integer, HouseholdSummaryDTO>();
				
				//Reset metrics variables
				regularRevenue = 0;
				saleRevenue = 0;
				regularMargin = 0;
				saleMargin = 0;
				regularUnits = 0;
				saleUnits = 0;
				regularVolume = 0;
				saleVolume = 0;
				regularCost = 0;
				saleCost = 0;
				visitCount =0;
				lastTransNo = "";
				lastItemCode = 0;
				// Reset objects - Ends
			}

			if (lastItemCode > 0 && lastItemCode != curItemCode){
				
				// Add current values into collection
				objsummaryDto = new HouseholdSummaryDTO();
				objsummaryDto.setCustomerId(lasthouseholdNo);
				objsummaryDto.setTotalVisit(visitCount);
				objsummaryDto.setRegularRevenue(regularRevenue);
				objsummaryDto.setSaleRevenue(saleRevenue);
				objsummaryDto.setRegularMargin(regularMargin);
				objsummaryDto.setSaleMargin(saleMargin);
				objsummaryDto.setRegularUnit(regularUnits);
				objsummaryDto.setSaleUnit(saleUnits);
				objsummaryDto.setRegularVolume(regularVolume);
				objsummaryDto.setSaleVolume(saleVolume);
				objsummaryDto.setDealCost(saleCost);
				objsummaryDto.setRegularCost(regularCost);
				CalculateFinalMetrics(objsummaryDto);

				// Check existence of item and remove
				if (itemMap.containsKey(lastItemCode))
					itemMap.remove(lastItemCode);
				
				//Current item into collection
				itemMap.put(lastItemCode, objsummaryDto);
								
				// Check if new item exist in the collection
				if (itemMap.containsKey(curItemCode)){
					objsummaryDto = new HouseholdSummaryDTO();
					objsummaryDto = itemMap.get(curItemCode);
					visitCount = objsummaryDto.getTotalVisit();
					regularRevenue = objsummaryDto.getRegularRevenue();
					saleRevenue = objsummaryDto.getSaleRevenue();
					regularMargin = objsummaryDto.getRegularMargin();
					saleMargin = objsummaryDto.getSaleMargin();
					regularUnits = objsummaryDto.getRegularUnit();
					saleUnits = objsummaryDto.getSaleUnit();
					regularVolume = objsummaryDto.getRegularVolume();
					saleVolume = objsummaryDto.getSaleVolume();
					saleCost = objsummaryDto.getDealCost();
					regularCost = objsummaryDto.getRegularCost();
				}
				else
				{
					visitCount =0;
					regularRevenue = 0;
					saleRevenue = 0;
					regularMargin = 0;
					saleMargin = 0;
					regularUnits = 0;
					saleUnits = 0;
					regularVolume = 0;
					saleVolume = 0;
					regularCost = 0;
					saleCost = 0;
				}
			} 
			
			if (!lastTransNo.equalsIgnoreCase(objMoveDto.gettransactionNo()) || lastItemCode != curItemCode)
				visitCount++;

			regularRevenue += objMoveDto.getRevenueRegular();
			saleRevenue += objMoveDto.getRevenueSale();

			regularUnits += objMoveDto.getRegularQuantity();
			saleUnits += objMoveDto.getSaleQuantity();
			
			// Get deal cost 
			double itemCost = getCost(itemCostMap, objMoveDto);

			if (itemCost != 0){
				if (objMoveDto.getFlag().equalsIgnoreCase("Y")){
					saleMargin += (objMoveDto.getRevenueSale() - itemCost);
					saleCost += itemCost;
				}
				else{
					regularMargin += (objMoveDto.getRevenueRegular() - itemCost);
					regularCost += itemCost;
				}
			}
			
			// Calculate movement by volume - Already calculated, no need to call
			// businessObj.processMovementByVolume(uomMapDB, uomMapPro, objMoveDto);
			
			// Calculate Movement By Volume
			regularVolume += objMoveDto.getregMovementVolume();
			saleVolume += objMoveDto.getsaleMovementVolume();
			
			
			lasthouseholdNo = objMoveDto.getHouseholdNumber(); 
			lastTransNo = objMoveDto.gettransactionNo();
			lastItemCode = curItemCode;
			
			logger.debug(lasthouseholdNo + " | " + lastTransNo + " | " + lastItemCode   + " | " +  
			visitCount  + " | " + regularRevenue  + " | " + saleRevenue  + " | " +	regularCost   + " | " +	saleCost  + " | " +	
			regularUnits  + " | " + saleUnits  + " | " + regularVolume  + " | " + saleVolume + " | " + objMoveDto.getregMovementVolume() + " | " + objMoveDto.getsaleMovementVolume());  
		}
		
		if (lasthouseholdNo > 0 && (regularRevenue != 0 || saleRevenue != 0)){

			objsummaryDto = new HouseholdSummaryDTO();
			objsummaryDto.setCustomerId(lasthouseholdNo);
			objsummaryDto.setTotalVisit(visitCount);
			objsummaryDto.setRegularRevenue(regularRevenue);
			objsummaryDto.setSaleRevenue(saleRevenue);
			objsummaryDto.setRegularMargin(regularMargin);
			objsummaryDto.setSaleMargin(saleMargin);
			objsummaryDto.setRegularUnit(regularUnits);
			objsummaryDto.setSaleUnit(saleUnits);
			objsummaryDto.setRegularVolume(regularVolume);
			objsummaryDto.setSaleVolume(saleVolume);
			objsummaryDto.setDealCost(saleCost);
			objsummaryDto.setRegularCost(regularCost);
			CalculateFinalMetrics(objsummaryDto);

			// Check existence of item and remove
			if (itemMap.containsKey(lastItemCode))
				itemMap.remove(lastItemCode);
			
			//Current item into collection
			itemMap.put(lastItemCode, objsummaryDto);
			
			//Add current household into the collection
			householdmMap.put(lasthouseholdNo, itemMap);
		}

		return householdmMap;
	} 
	
	
	/*
	 * ****************************************************************
	 * Method used to Calculate the metrics information 
	 * Argument 1: summary DTO
	 * Argument 2: mode
	 * ****************************************************************
	 */

	public void CalculateFinalMetrics(HouseholdSummaryDTO summaryDTO) throws GeneralException {
		// Find the Total Movement
		//String marginFlag = PropertyManager.getProperty("MARGIN_CALCULATION_REQUIRED",null);
		
		try {
			double regularRevenue = summaryDTO.getRegularRevenue(); 
			double saleRevenue = summaryDTO.getSaleRevenue();
			double totalRevenue = regularRevenue+ saleRevenue;

			double regularMargin = summaryDTO.getSaleMargin(); 
			double saleMargin = summaryDTO.getRegularMargin();
			double totalMargin = regularMargin + saleMargin;
			
			double totalMovement = summaryDTO.getSaleUnit()+ summaryDTO.getRegularUnit();
			double totMovementVolume = summaryDTO.getRegularVolume()+ summaryDTO.getSaleVolume();

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

			
			if (totalRevenue > 0 && summaryDTO.getTotalVisit() > 0 )
				avgOrderSize = totalRevenue /  summaryDTO.getTotalVisit();
			
			summaryDTO.setTotalRevenue(totalRevenue); 
			summaryDTO.setTotalUnit(totalMovement);
			summaryDTO.setTotalVolume(totMovementVolume);
			summaryDTO.setAverageOrderSize(avgOrderSize);

			summaryDTO.setTotalMargin(totalMargin);
			summaryDTO.setRegularMargin(regularMargin);
			summaryDTO.setSaleMargin(saleMargin);
		  
			summaryDTO.setTotalMarginPct(setMaxMarginPercentage(totalMarginPercent));
			summaryDTO.setRegularMarginPct(setMaxMarginPercentage(regularMarginPercent));
			summaryDTO.setSaleMarginPct(setMaxMarginPercentage(salemarginpercent)); 

			// Calculation for Private label
			double plregularRevenue = summaryDTO.getPLRegularRevenue(); 
			double plsaleRevenue = summaryDTO.getPLSaleRevenue();
			double pltotalRevenue = plregularRevenue+ plsaleRevenue;

			double plregularMargin = summaryDTO.getPLRegularMargin();
			double plsaleMargin = summaryDTO.getPLSaleMargin();
			double pltotalMargin = plregularMargin + plsaleMargin;
			
			double pltotalMovement = summaryDTO.getPLSaleUnit()+ summaryDTO.getPLRegularUnit();
			double pltotMovementVolume = summaryDTO.getPLRegularVolume()+ summaryDTO.getPLSaleVolume();

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

			if (pltotalRevenue > 0 && summaryDTO.getPLTotalVisit() > 0 )
				plavgOrderSize = pltotalRevenue /  summaryDTO.getPLTotalVisit();
			
			summaryDTO.setPLTotalRevenue(pltotalRevenue); 
			summaryDTO.setPLTotalUnit(pltotalMovement);
			summaryDTO.setPLTotalVolume(pltotMovementVolume);
			summaryDTO.setPLAverageOrderSize(plavgOrderSize);

				summaryDTO.setPLTotalMargin(pltotalMargin);
				summaryDTO.setPLRegularMargin(plregularMargin);
				summaryDTO.setPLSaleMargin(plsaleMargin);
			  
				summaryDTO.setPLTotalMarginPct(setMaxMarginPercentage(pltotalMarginPercent));
				summaryDTO.setPLRegularMarginPct(setMaxMarginPercentage(plregularMarginPercent));
				summaryDTO.setPLSaleMarginPct(setMaxMarginPercentage(plsalemarginpercent)); 
			
		} catch (Exception exe) {
			logger.error("com.pristine.service.customer.HouseholdSummaryService.CalculateFinalMetrics - Error while Calculating Metrics" , exe);
			throw new GeneralException(" Error while CalculateMetrixInformation" , exe);
		}
	}
	
	/*
	 * ****************************************************************
	 * Function to get deal cost for item based on Quantity or Weight
	 * Argument 1: Cost Hash map
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
	
	private HashMap<String, Double> GetCostData(Connection dbConn, 
						List<MovementDailyAggregateDTO> movementList, 
		HouseholdSummaryDailyInDTO inDto) throws GeneralException{
		
		HashMap<String, Double> itemCostMap = new HashMap<String, Double>();
		// Object for CostDataLoadV2
		CostDataLoad objCostLoad = new CostDataLoad();		
		
		// get the distinct item code list from transaction data for
		// getting deal cost
		
		logger.debug("Get distinct item code list from movement data");
		List<String> itemCodeList = new ArrayList<>();
		itemCodeList = GetDistinctItemList(movementList);
		
		// get the deal cost info from cost loader for margin calculation
		logger.debug("Get the cost info");
		itemCostMap = objCostLoad.getRetailCost(dbConn, 
			inDto.getStoreNo(), itemCodeList, inDto.getcalendarId());
			
		if (itemCostMap.size() <1)
			logger.info("Cost data not found");

		return itemCostMap;
	}
	
	
	/*
	 * ****************************************************************
	 * Get distinct item code from the movement data list 
	 * Argument 1: Movement data
	 * Return: Item List 
	 * ****************************************************************
	 */
	private List<String> GetDistinctItemList(List<MovementDailyAggregateDTO> movementList)
	{

		HashMap<Integer, Integer> itemMap = new HashMap<Integer, Integer>();
		List<String> itemList = new ArrayList<String>();

		for (int ii = 0; ii < movementList.size(); ii++) {
			int itemCode = movementList.get(ii).getItemCode();
			
			if (!itemMap.containsKey(itemCode))
				itemMap.put(itemCode, 0);
		}
		
		if (itemMap.size() >0){
			for (Integer itemdKey : itemMap.keySet())
				itemList.add(itemdKey.toString());
		}
		
		logger.debug("Total item count: " + itemList.size());
		
		return itemList;
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