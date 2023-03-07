package com.pristine.business.entity.riteaid;

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

public class Salesaggregationbusiness {

	static Logger logger = Logger.getLogger("Salesaggregationbusiness");

	final double  maxMarginPercentage = 999.99;
	final double  minMarginPercentage = -999.99;
	
	double _groupRegularQuantity = 0;
	double _groupRegularRevenue	 = 0;
	double _groupSaleQuantity    = 0;
	double _groupSaleRevenue     = 0;
	double _groupTotalRevenue    = 0;
	
	// Variable added for Movement by volume aggregation
	double _groupRegVolumeMovement    = 0;
	double _groupSaleVolumeMovement   = 0;
	double _groupIgRegVolumeRevenue   = 0;
	double _groupIgSaleVolumeRevenue  = 0;
	
	// add the visit count for product level
	double _groupVisitCount = 0;
	
	
	// variable added for product level margin
	double _groupRegularMargin = 0;
	double _groupSaleMargin = 0;
	
	/*
	 * ****************************************************************
	 * Method used to Itreate the movement Daily list 
	 * Argument 1: List<MovementDailyAggregateDTO>
	 * Argument 2: HashMap<Integer, HashMap<String, SummaryDailyDTO>>
	 * Argument 3: locationId
	 * Argument 4: HashMap<String, String> uomMapDb
	 * Argument 5: HashMap<String, String> uomMapPro
	 * Argument 6:subCateVisitMap
	 * Argument 7:cateVisitMap
	 * Argument 8:departmentVisitMap
	 * Argument 9:posDepartmentVisitMap
	 * Argument 10: storeLevelVisitCount
	 * Argument 11 : SpecialCriteriaDTO
	 * Return productHashMap
	 * @throws Exception
	 * ****************************************************************
	 */

	public void calculateProductSummary(
			List<MovementDailyAggregateDTO> movementList,
			HashMap<Integer, HashMap<String, SummaryDataDTO>> productMap,
			SummaryDataDTO objStoreDto, HashMap<String, String> uomMapDb,
			HashMap<String, String> uomMapPro,
			HashMap<String, Integer> subCateVisitMap,
			HashMap<String, Integer> cateVisitMap,
			HashMap<String, Integer> departmentVisitMap,
			HashMap<String, Integer> storeVisitCount,
			HashMap<String, Double> couponAggregationMap,
			SpecialCriteriaDTO objSpecialDto,
			HashMap<String, Double> dealCostMap) throws GeneralException {

		// logger.info(" Itreate the daily summary records starts ");
		HashMap<String, SummaryDataDTO> subCategoryMap = new HashMap<String, SummaryDataDTO>();
		HashMap<String, SummaryDataDTO> categoryMap = new HashMap<String, SummaryDataDTO>();
		HashMap<String, SummaryDataDTO> departmentMap = new HashMap<String, SummaryDataDTO>();
		HashMap<String, SummaryDataDTO> storeMap = new HashMap<String, SummaryDataDTO>();

		// get the level id from constant class
		int subCategoryLevelId = Constants.SUBCATEGORYLEVELID;
		int categoryLevelId = Constants.CATEGORYLEVELID;
		int deptlevelId = Constants.DEPARTMENTLEVELID;
		
		double totRegularMovement = 0;
		double totSaleMovement = 0;
		double totRegularRevenue = 0;
		double totSaleRevenue = 0;

		// variable added for deal cost
		double storeSaleDealCost = 0;
		double storeRegularDealCost =0;
		// Varible added for movement by volume
		double totRegVolumeMovement = 0;
		double totSaleVolumeMovement = 0;
		double totIgRegVolumeRevenue = 0;
		double totIgSaleVolumeRevenue = 0;

		SummaryDataDTO objSummarryDto = null;

		for (int ii = 0; ii < movementList.size(); ii++) {

			// Get the movement DTO
			MovementDailyAggregateDTO objMoveDto = movementList.get(ii);

			String subCatId = objMoveDto.get_subcategoryid();
			String catId = objMoveDto.get_categoryid();
			String deptId = objMoveDto.get_departmentid();
					
			// Get deal cost 
			double processDealCost = getDealCost(dealCostMap, objMoveDto);
			

			// call the movement by volume business....
			processMovementByVolume(uomMapDb, uomMapPro, objMoveDto);

			totRegularMovement += objMoveDto.get_regularQuantity();
			totSaleMovement += objMoveDto.get_saleQuantity();
			totRegularRevenue += objMoveDto.getRevenueRegular();
			totSaleRevenue += objMoveDto.getRevenueSale();

			/*If the transaction is Sale the add deal to Sale
			If the transaction is Regular the add deal to Sale*/
			if (objMoveDto.getFlag().equalsIgnoreCase("Y")){
				storeSaleDealCost += processDealCost;
			}
			else {
				storeRegularDealCost += processDealCost;
			}

			// Calculate Movement By Volume
			totRegVolumeMovement += objMoveDto.getregMovementVolume();
			totSaleVolumeMovement += objMoveDto.getsaleMovementVolume();
			totIgRegVolumeRevenue += objMoveDto.getigRegVolumeRev();
			totIgSaleVolumeRevenue += objMoveDto.getigSaleVolumeRev();

			// old process remove on 17-08-2012
			// new process added for sub-category
			// Aggregate Subcategory Data	
			if (subCatId != null) {
				if (subCategoryMap.containsKey(subCatId)) {
					// Update Subcategory aggregation data into Map
					aggregateDatadto(objMoveDto, subCategoryMap,
							"ADDPROCESS", subCatId, subCategoryLevelId,
							subCateVisitMap, processDealCost);
				} else {
					// Add Subcategory aggregation data into Map
					aggregateDatadto(objMoveDto, subCategoryMap,
							"NEWPROCESS", subCatId, subCategoryLevelId,
							subCateVisitMap, processDealCost);
				}
			}

			if (catId != null) {
				// Aggregate Category Data
				if (categoryMap.containsKey(catId)) {
					// Update Category aggregation data into Map
					aggregateDatadto(objMoveDto, categoryMap, "ADDPROCESS",
							catId, categoryLevelId, cateVisitMap,
							processDealCost);
				} else {
					// Add sub category aggregation data into Map
					aggregateDatadto(objMoveDto, categoryMap, "NEWPROCESS",
							catId, categoryLevelId, cateVisitMap,
							processDealCost);
				}
			}

			if (deptId != null) {
				// Aggregate Department Data
				if (departmentMap.containsKey(deptId)) {
					// Update Department aggregation data into Map
					aggregateDatadto(objMoveDto, departmentMap,
							"ADDPROCESS", deptId, deptlevelId,
							departmentVisitMap, processDealCost);
				} else {
					// Add Department aggregation data into Map
					aggregateDatadto(objMoveDto, departmentMap,
							"NEWPROCESS", deptId, deptlevelId,
							departmentVisitMap, processDealCost);
				}
			}
			
		}

		
		objSummarryDto = new SummaryDataDTO();
		objSummarryDto.setRegularMovement(totRegularMovement);
		objSummarryDto.setSaleMovement(totSaleMovement);
		objSummarryDto.setSaleRevenue(totSaleRevenue);
		objSummarryDto.setRegularRevenue(totRegularRevenue);
		objSummarryDto.setProductId("");
		objSummarryDto.setProductLevelId(0);
		objSummarryDto.setSaleDealCost(storeSaleDealCost);
		objSummarryDto.setRegularDealCost(storeRegularDealCost);

		// code added for Movement By Volume
		objSummarryDto.setregMovementVolume(totRegVolumeMovement);
		objSummarryDto.setsaleMovementVolume(totSaleVolumeMovement);
		objSummarryDto.setigRegVolumeRev(totIgRegVolumeRevenue);
		objSummarryDto.setigSaleVolumeRev(totIgSaleVolumeRevenue);
		// get the store level Visit Count
		if (storeVisitCount.containsKey(objStoreDto.getstoreNumber())) {
			objSummarryDto.setTotalVisitCount(storeVisitCount.get(objStoreDto
					.getstoreNumber()));
		}

		// Calculate the other metrix info
		CalculateMetrixInformation(objSummarryDto, false);
		storeMap.put(String.valueOf(objStoreDto.getLocationId()),
				objSummarryDto);

		if (subCategoryMap.size() > 0)
			productMap.put(Constants.SUBCATEGORYLEVELID, subCategoryMap);

		if (categoryMap.size() > 0)
			productMap.put(Constants.CATEGORYLEVELID, categoryMap);

		if (departmentMap.size() > 0)
			productMap.put(Constants.DEPARTMENTLEVELID, departmentMap);

		if (storeMap.size() > 0)
			productMap.put(Constants.LOCATIONLEVELID, storeMap);

		logger.debug(" totRegularQuantity " + totRegularMovement);
		logger.debug(" totSaleQuantity " + totSaleMovement);
		logger.debug(" totRegularRevenue " + totRegularRevenue);
		logger.debug(" totSaleRevenue " + totSaleRevenue);

		/* return gasdto; */
	}
	
	/*
	 *  Sum the all product level aggregation and stored into map
	 *  Argument 1 : MovementDailyAggregateDTO
	 *  Argument 2 : HashMap<String, SummaryDataDTO>
	 *  Argument 3 : modeOfProcess
	 *  Argument 4 : productId
	 *  Argument 5 : productLevelId
	 *  Argument 6 : HashMap<String, Integer>
	 *  Arguemnt 7 : couponRevenue
	 * 
	 * 
	 */
	private void aggregateDatadto(MovementDailyAggregateDTO movementDataDto,
			HashMap<String, SummaryDataDTO> byProductMap, String modeOfProcess,
			String productId, int productLevelId,
			HashMap<String, Integer> visitMap,  double dealCost) throws GeneralException {

		SummaryDataDTO objSummaryDto = new SummaryDataDTO();

		// check the new for map or exist
		if (modeOfProcess.equalsIgnoreCase("NEWPROCESS")) {

			// Aggregation process for TotalRevenue
			objSummaryDto.setRegularMovement(movementDataDto
					.get_regularQuantity());
			objSummaryDto.setSaleMovement(movementDataDto.get_saleQuantity());
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
			
			/*If the transaction is Sale the add deal to Sale
			If the transaction is Regular the add deal to Sale*/
			if (movementDataDto.getFlag().equalsIgnoreCase("Y")){
				objSummaryDto.setSaleDealCost(dealCost);
			}
			else {
				objSummaryDto.setRegularDealCost(dealCost);
			}
				
		} else if (modeOfProcess.equalsIgnoreCase("ADDPROCESS")) {

			SummaryDataDTO objExistingSummaryDto = byProductMap.get(productId);

			// Aggregation process for TotalRevenue
			objSummaryDto.setRegularMovement(movementDataDto
					.get_regularQuantity()
					+ objExistingSummaryDto.getRegularMovement());
			objSummaryDto.setSaleMovement(movementDataDto.get_saleQuantity()
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
			
			/*If the transaction is Sale the add deal to Sale
			If the transaction is Regular the add deal to Sale*/
			if (movementDataDto.getFlag().equalsIgnoreCase("Y")){
				objSummaryDto.setSaleDealCost(dealCost + objExistingSummaryDto.getSaleDealCost());
				objSummaryDto.setRegularDealCost(objExistingSummaryDto.getRegularDealCost());

			}
			else {
				objSummaryDto.setRegularDealCost(dealCost + objExistingSummaryDto.getRegularDealCost());
				objSummaryDto.setSaleDealCost(objExistingSummaryDto.getSaleDealCost());
			}
		}

		// add product-id and product-level-id
		objSummaryDto.setProductId(productId);
		objSummaryDto.setProductLevelId(productLevelId);

		if (productLevelId != Constants.POSDEPARTMENT) {
			// find the visit count from the map
			if (visitMap.containsKey(productId)) {
				objSummaryDto.setTotalVisitCount(visitMap.get(productId));
			} else {
				objSummaryDto.setTotalVisitCount(0);
			}
		}

		// Calculate the other Metrics info
		CalculateMetrixInformation(objSummaryDto,  false);

		// add the aggregated values into map
		byProductMap.put(productId, objSummaryDto);

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
			double totalMovement = summaryDTO.getSaleMovement()+ summaryDTO.getRegularMovement();
			double totIgVolumeRevenue = summaryDTO.getigRegVolumeRev()+ summaryDTO.getigSaleVolumeRev();
			double totMovementVolume = summaryDTO.getregMovementVolume()+ summaryDTO.getsaleMovementVolume();
			summaryDTO.setTotalMovement(totalMovement);
			summaryDTO.settotMovementVolume(totMovementVolume);
			summaryDTO.setigtotVolumeRev(totIgVolumeRevenue);
			
			double totalMargin = 0; double regularMargin = 0; double  saleMargin = 0;
			double itotalMargin = 0 ; double iregularMargin = 0; double isaleMargin = 0; 
						
			if(noMarginCalculation){
			  // For Summary Rollup Process // Deal  Cost 
				totalRevenue = summaryDTO.getTotalRevenue();
				regularMargin = summaryDTO.getRegularMargin(); 
				saleMargin    = summaryDTO.getSaleMargin();
				iregularMargin = summaryDTO.getiregularMargin();
				isaleMargin = summaryDTO.getisaleMargin();
				itotalMargin = iregularMargin + isaleMargin;
				totalMargin   = regularMargin + saleMargin;
			 } 
			
			else{ 
				  
				totalRevenue = summaryDTO.getSaleRevenue()+ summaryDTO.getRegularRevenue();
				
				// For Summary Daily Process
				double saleDealCost = summaryDTO.getSaleDealCost();
				double regularDealCost = summaryDTO.getRegularDealCost();
				totalMargin     = totalRevenue- (saleDealCost + regularDealCost);
				regularMargin   = summaryDTO.getRegularRevenue()- regularDealCost;
				saleMargin      = summaryDTO.getSaleRevenue() - saleDealCost;
			   }
			
			summaryDTO.setTotalRevenue(totalRevenue);
			
			 double totalMarginPercent = 0;
			 double regularMarginPercent = 0;
			 double salemarginpercent  =0;
			 
			 double itotalMarginPercent = 0;
			 double iregularMarginPercent = 0;
			 double isalemarginpercent  =0;
			 
			
			 if( totalRevenue !=0)
			 totalMarginPercent  = totalMargin /  totalRevenue * 100;
			 if( summaryDTO.getRegularRevenue() != 0)
			 regularMarginPercent =  regularMargin / summaryDTO.getRegularRevenue() * 100;
			 if( summaryDTO.getSaleRevenue() != 0)
			 salemarginpercent = saleMargin / summaryDTO.getSaleRevenue() * 100;
			 
			 if(noMarginCalculation){
			 if( summaryDTO.getitotalRevenue() != 0)
			 itotalMarginPercent  = itotalMargin /  summaryDTO.getitotalRevenue() * 100;
			 if( summaryDTO.getiregularRevenue() != 0)
			 iregularMarginPercent =  iregularMargin / summaryDTO.getiregularRevenue() * 100;
			 if( summaryDTO.getisaleRevenue() != 0)
			 isalemarginpercent = isaleMargin / summaryDTO.getisaleRevenue() * 100;
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

			 }
			  // if condition remove for adding the visit count in all level
			  	 
			  double avgOrderSize = totalRevenue /  summaryDTO.getTotalVisitCount();;
			  summaryDTO.setAverageOrderSize(avgOrderSize);
			 			  
			  if (  noMarginCalculation  && summaryDTO.getitotalVisitCount() > 0) {
			  avgOrderSize = summaryDTO.getitotalRevenue() / summaryDTO.getitotalVisitCount();
			  summaryDTO.setiaverageOrderSize(avgOrderSize);
			  }
			  
			 
		} catch (Exception exe) {
			logger.error(" Error while CalculateMetrixInformation" , exe);
			throw new GeneralException(" Error while CalculateMetrixInformation" , exe);
		}

	}

	/*
	 * ****************************************************************
	 * Method used aggregate the grouping list Argument 1:
	 * MovementDailyAggregateDTO List output : Daily Summary Records Itreated
	 * and added into hashlist
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
							.getChildProductId())))
						SumupGroupList(childMap.get(String.valueOf(productDTO
								.getChildProductId())), productVisitCount,
								productDTO.getProductId());
				} else {
					// aggregate the product
					// code added for movement by volume....
					// 6 new addion operaion added
					productLevelAggregation(_groupRegularQuantity,
							_groupSaleQuantity, _groupRegularRevenue,
							_groupSaleRevenue, tempProductId, productLevelId,
							groupingHashMap, productMap,
							_groupRegVolumeMovement, _groupSaleVolumeMovement,
							_groupIgRegVolumeRevenue,
							_groupIgSaleVolumeRevenue, _groupVisitCount,
							_groupRegularMargin, _groupSaleMargin);

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

					if (childMap.containsKey(String.valueOf(productDTO
							.getChildProductId())))
						SumupGroupList(childMap.get(String.valueOf(productDTO
								.getChildProductId())), productVisitCount,
								productDTO.getProductId());
				}
			}

			productLevelAggregation(_groupRegularQuantity, _groupSaleQuantity,
					_groupRegularRevenue, _groupSaleRevenue, tempProductId,
					productLevelId, groupingHashMap, productMap,
					_groupRegVolumeMovement, _groupSaleVolumeMovement,
					_groupIgRegVolumeRevenue, _groupIgSaleVolumeRevenue,
					_groupVisitCount, _groupRegularMargin, _groupSaleMargin);

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
			double groupSaleMargin) throws GeneralException {

		// Asgin the summary data into dto

		SummaryDataDTO summaryDTO = new SummaryDataDTO();
		summaryDTO.setRegularMovement(groupRegularQuantity);
		summaryDTO.setRegularRevenue(groupRegularRevenue);
		summaryDTO.setSaleMovement(groupSaleQuantity);
		summaryDTO.setSaleRevenue(groupSaleRevenue);
		summaryDTO.setProductId(String.valueOf(tempProductId));
		summaryDTO.setProductLevelId(productLevelId);
		summaryDTO.setTotalVisitCount(groupVisitCount);

		// code added for nmargin
		summaryDTO.setRegularMargin(groupRegularMargin);
		summaryDTO.setSaleMargin(groupSaleMargin);

		// code added for movement by volume
		summaryDTO.setregMovementVolume(RegVolumeMovement);
		summaryDTO.setsaleMovementVolume(SaleVolumeMovement);
		summaryDTO.setigRegVolumeRev(IgRegVolumeRevenue);
		summaryDTO.setigSaleVolumeRev(IgSaleVolumeRevenue);
		
		summaryDTO.setTotalRevenue(groupRegularRevenue + groupSaleRevenue);

		// Calculate the other metrix info
		CalculateMetrixInformation(summaryDTO, true);
		
		
		if (summaryDTO.getTotalRevenue() != 0) {
			if( summaryDTO.getTotalRevenue() == 100 && productLevelId == 6){
				summaryDTO.setTotalRevenue(0);
				summaryDTO.setRegularRevenue(0);
			}
				
			// add the dto into hashMap
			groupingHashMap.put(String.valueOf(tempProductId), summaryDTO);
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
		
		

		if (productVisitCount != null
				&& productVisitCount
						.containsKey(String.valueOf(childProductId))) {
			_groupVisitCount = productVisitCount.get(String
					.valueOf(childProductId));
			/*
			 * logger.info(" Product Id..." + summaryDataDTO.getProductId()
			 * +"---Visit Count...." + _groupVisitCount);
			 */
		} else {
			_groupVisitCount += summaryDataDTO.getTotalVisitCount();
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
	private void processMovementByVolume(HashMap<String, String> uomMapDb,
			HashMap<String, String> uomMapPro,
			MovementDailyAggregateDTO movementDataDto) throws GeneralException {

		String uomId = movementDataDto.getuomId();

		try {

			if (uomMapDb.containsKey(uomId)) {

				String uomName = uomMapDb.get(uomId);
				if (uomMapPro.containsKey(uomName)) {

					// logger.info(" Processing Movement By Volume for.... " +
					// uomName + "_" + movementDataDto.getupc());

					CalculateMovementByVolume(uomName, movementDataDto);
				} else {
					// logger.info("  Not Processing Movement By Volume for.... "+
					// uomName + "_" + movementDataDto.getupc());

					movementDataDto.setigRegVolumeRev(movementDataDto
							.getRevenueRegular());
					movementDataDto.setigSaleVolumeRev(movementDataDto
							.getRevenueSale());
					movementDataDto.setregMovementVolume(0);
					movementDataDto.setsaleMovementVolume(0);

				}
			} else {

				// logger.info("  Uom Id Null (Not Processing Movement By volume).... "+
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
			logger.error(" processMovementByVolume method Error.....  ", exe);
			throw new GeneralException(
					"processMovementByVolume method Error.....  ", exe);
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
			logger.error(" CalculateMovementByVolume error... ", exe);
			throw new GeneralException("CalculateMovementByVolume error... ",
					exe);
		}

	}
	

	/*
	 * ****************************************************************
	 * Function to get deal cost for item based on Quantity or Weight
	 * Argument 1: Cost Hashmap
	 * Argument 2: Daily Movement Data DTO
	 * ****************************************************************
	 */
	private double getDealCost(HashMap<String, Double> dealCostMap, MovementDailyAggregateDTO objMoveDto) {
	
		double unitDealCost = 0;
		double calcDealCost = 0;
		
		if (dealCostMap.containsKey(String.valueOf(objMoveDto.getItemCode()))) {
			
			unitDealCost = dealCostMap.get(String.valueOf(objMoveDto.getItemCode()));
			
			if (objMoveDto.getActualQuantity() != 0){
				calcDealCost = unitDealCost * objMoveDto.getActualQuantity();
			}
			else if (objMoveDto.getActualWeight() != 0){
				calcDealCost = unitDealCost * objMoveDto.getActualWeight();
			}
		}
		else
		{
			logger.debug("No Cost found for " + objMoveDto.getItemCode());
		}
		return calcDealCost;
	}

	/*
	 * ****************************************************************
	 * Function to fix max / min margin %
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
