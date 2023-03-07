package com.pristine.service.offermgmt.perishable;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import com.pristine.business.util.BusinessHelper;
import com.pristine.dao.offermgmt.perishable.PerishableDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.offermgmt.RetailPriceCostKey;
import com.pristine.dto.offermgmt.perishable.PerishableCostFeedDTO;
import com.pristine.dto.offermgmt.perishable.PerishableOrderDTO;
import com.pristine.dto.offermgmt.perishable.PerishableRelationshipDTO;
import com.pristine.dto.offermgmt.perishable.PerishableSellDTO;
import com.pristine.dto.offermgmt.perishable.TargetLocationDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.util.Constants;
import com.pristine.util.NumberUtil;
import com.pristine.util.PropertyManager;

import org.apache.log4j.Logger;


public class PerishableCostService {
	
	private static Logger logger = Logger.getLogger("StoreFileExport");

	  public HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> 
	  	getLatestOrderItemCostList(RetailCostServiceOptimized costService, 
		ArrayList<PerishableRelationshipDTO> relationshipsList, 
		LinkedHashMap<Integer, RetailCalendarDTO> calendarMap, 
		int chainId) throws GeneralException{
	  
		  logger.debug("List all order items...");
		  Set<Integer> itemList = getOrderItemList(relationshipsList);
		  HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap = 
				  new HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>>();
		  
		  
		  for (int calendarId: calendarMap.keySet()) {
			  logger.debug("Call cost API to get cost for calendar id: " + calendarId);
			  
			  HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataTempMap = costService.getRetailCost(calendarId, chainId, true, true, itemList);

			  if (costDataTempMap != null && costDataTempMap.size() > 0){
				  Set<Integer> costNotFoundItems = new HashSet<>();
				  
				  logger.debug("Cost not found for " + costDataTempMap.size() + " items");
				  
				  
				  //Check for the cost existence for each item
				  itemList.forEach(item->{
					  if(costDataTempMap.containsKey(item)){
						  logger.debug("Cost exist for item:" + item);
						  costDataMap.put(item, costDataTempMap.get(item));
					  }else{
						  costNotFoundItems.add(item);
					  }
				  });
				  
				  if (costNotFoundItems != null && costNotFoundItems.size() >0){
					  itemList = new HashSet<Integer>();
					  itemList = costNotFoundItems;
				  }
				  else{
					  break;
				  }
			  }
			  else
			  {
				  logger.debug("Cost not found for any of the items");
			  }
		  }
		  
		  logger.debug("Cost found for " + costDataMap.size());

		  return costDataMap;
	  }	
	
	  private Set<Integer> getOrderItemList(ArrayList<PerishableRelationshipDTO> relationshipList){
		  
		  logger.debug("Organize order items....");
		  Set<Integer> itemCodeSetT = new HashSet<Integer>();
		  HashMap<Integer, Integer> itemList = new HashMap<Integer, Integer>();
		  
		  for (int i=0; i < relationshipList.size(); i++){
			  List<PerishableOrderDTO> orderList = relationshipList.get(i).getOrderData(); 
			  
			  if (orderList != null && orderList.size() >0){
				  for (int j=0; j < orderList.size(); j++){
					  if (!itemList.containsKey(orderList.get(j).getItemCode())){
						  itemList.put(orderList.get(j).getItemCode(), 0);
						  itemCodeSetT.add(orderList.get(j).getItemCode());
					  }
				  }
			  }
		  }
		  logger.debug("Total order items: " + itemCodeSetT.size());
		  
		  return itemCodeSetT;
	  }	
	
	  public HashMap<Integer, HashMap<Integer, TargetLocationDTO>> getTargetLocations(
			  ArrayList<PerishableRelationshipDTO> relationshipsList, 
			  PerishableCostService pcsObj,  PerishableDAO perishableDaoObj, 
			  int productLeveId,	int productId) throws GeneralException{
			  
			  logger.debug("List all sell items...");
			  List<Integer> itemCodeList = getSellItemList(relationshipsList);
			  
			  HashMap<Integer, HashMap<Integer, TargetLocationDTO>> targetLocationMap = 
				  new HashMap<Integer, HashMap<Integer, TargetLocationDTO>>();
			  
			  targetLocationMap = pcsObj.getTargetLocation(productLeveId, productId, perishableDaoObj, itemCodeList);
			  
			  return targetLocationMap;
		  }

	  private List<Integer> getSellItemList(ArrayList<PerishableRelationshipDTO> relationshipList){
		  
		  logger.debug("Organize order items....");
		  List<Integer> itemCodeList = new ArrayList<Integer>();
		  HashMap<Integer, Integer> itemList = new HashMap<Integer, Integer>();
		  
		  for (int i=0; i < relationshipList.size(); i++){
			  List<PerishableSellDTO> sellList = relationshipList.get(i).getSellData();
			  
			  if (sellList != null && sellList.size() >0){
				  for (int j=0; j < sellList.size(); j++){
					  if (!itemList.containsKey(sellList.get(j).getItemCode())){
						  itemList.put(sellList.get(j).getItemCode(), 0);
						  itemCodeList.add(sellList.get(j).getItemCode());
					  }
				  }
			  }
		  }
		  logger.debug("Total sellable items: " + itemCodeList.size());
		  
		  return itemCodeList;
	  }	  	  
	
	  private double calculateSellableItemCost(RetailCostServiceOptimized costService, 
		  PerishableSellDTO sellItemDto, 
		  HashMap<RetailPriceCostKey, RetailCostDTO> itemCostMap, 
		  PerishableOrderDTO orderItemDto, TargetLocationDTO locationDto, 
		  int chainId, int locationType){
		  
		  double orderCost = getOrderItemCost(costService, itemCostMap, locationDto, chainId, locationType);
		  
		  double sellCost = orderCost * sellItemDto.getTypicalPriceRatio() / orderItemDto.getPriceRatioFactor();
		  
		  logger.debug("Order item cost is: " + orderCost + "& sellable item cost is: " + sellCost);
		  
		  return sellCost;
		  
	  }
	  
	  // Function to generate cost feed for single order - multiple customer item
	public HashMap<String, PerishableCostFeedDTO> generateCostFeedforSOMS(
			  						RetailCostServiceOptimized costService,
					ArrayList<PerishableRelationshipDTO> relationshipsList, 
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap,
		HashMap<Integer, HashMap<Integer, TargetLocationDTO>> targetLocation, 
		int chainId, boolean storeCostRequired, boolean zoneCostRequired, String costEffDate){
		  
		// Output Hash map
		HashMap<String, PerishableCostFeedDTO> costFeedMap = new HashMap<String, PerishableCostFeedDTO>(); 
			
		TargetLocationDTO locationDto = new TargetLocationDTO();
			
		logger.debug("Total items need Cost feed: " + targetLocation.size());
		
		//Iterate for each item in target location
		for (int itemCode : targetLocation.keySet()) {
			logger.debug("Process for item: " + itemCode);
				
			HashMap<Integer, TargetLocationDTO> storeMap = targetLocation.get(itemCode);
				
			// Iterate for each store 
			for (int storeId: storeMap.keySet()) {
				
				List<Double> costList = new ArrayList<Double>();
				
				// Cost generation at store level - Begins
				// If cost is @ store level
				if (storeCostRequired){

					String storeKey = itemCode + "_" + Constants.STORE_LEVEL_ID + "_" + storeId;
						
					// If cost not exist item & store combination, then proceed
					if (!costFeedMap.containsKey(storeKey)){
					
						locationDto = new TargetLocationDTO();
						locationDto = storeMap.get(storeId);
						
						String retailerItemCode ="";
							
						// Same sellable item can be there in more than one relation
						// In such cases, calculate cost based on different 
						// relationship and pick the maximum cost
						for (int relLoop=0; relLoop < relationshipsList.size(); relLoop++){
							for (int sellLoop=0; sellLoop < relationshipsList.get(relLoop).getSellData().size(); sellLoop++){
								if (itemCode == relationshipsList.get(relLoop).getSellData().get(sellLoop).getItemCode()){
									PerishableOrderDTO orderItemDto = relationshipsList.get(relLoop).getOrderData().get(0);
									PerishableSellDTO sellItemDto = relationshipsList.get(relLoop).getSellData().get(sellLoop);
									retailerItemCode = sellItemDto.getSellCode();
										
									int orderItemCode = orderItemDto.getItemCode();
										
									if (costDataMap.containsKey(orderItemCode)){
										HashMap<RetailPriceCostKey, RetailCostDTO> itemCostMap = costDataMap.get(orderItemCode);
											
										double itemCost = calculateSellableItemCost(costService, 
											sellItemDto, itemCostMap, orderItemDto, locationDto, 
											   				chainId, Constants.STORE_LEVEL_ID);
											
										if (itemCost > 0)
											costList.add(itemCost);
									}
									
								// TO DO: Skip from this loop
								}								
							}
						}
							
						// Get maximum cost for the item 
						double itemCost = getMaximumCost(costList);
							
						// Add cost to collection
						if (itemCost > 0){
							addSellableItemCost(costFeedMap, retailerItemCode, itemCost, 
							locationDto, storeKey, Constants.STORE_LEVEL_ID, costEffDate);
						}
					}
				}
				// Cost generation at store level - Ends
				
				// Cost generation at zone level - Begins
				// If cost is @ zone level
				if (zoneCostRequired){

					String zoneKey = itemCode + "_" + Constants.ZONE_LEVEL_ID + "_" + locationDto.getZoneId();
						
					// If cost not exist item & store combination, then proceed
					if (!costFeedMap.containsKey(zoneKey)){
					
						locationDto = new TargetLocationDTO();
						locationDto = storeMap.get(storeId);
						
						String retailerItemCode ="";
							
						// Same sellable item can be there in more than one relation
						// In such cases, calculate cost based on different 
						// relationship and pick the maximum cost
						for (int relLoop=0; relLoop < relationshipsList.size(); relLoop++){
							for (int sellLoop=0; sellLoop < relationshipsList.get(relLoop).getSellData().size(); sellLoop++){
								if (itemCode == relationshipsList.get(relLoop).getSellData().get(sellLoop).getItemCode()){
									PerishableOrderDTO orderItemDto = relationshipsList.get(relLoop).getOrderData().get(0);
									PerishableSellDTO sellItemDto = relationshipsList.get(relLoop).getSellData().get(sellLoop);
									retailerItemCode = sellItemDto.getSellCode();
										
									int orderItemCode = orderItemDto.getItemCode();
										
									if (costDataMap.containsKey(orderItemCode)){
										HashMap<RetailPriceCostKey, RetailCostDTO> itemCostMap = costDataMap.get(orderItemCode);
											
										double itemCost = calculateSellableItemCost(costService, 
											sellItemDto, itemCostMap, orderItemDto, locationDto, 
											   				chainId, Constants.ZONE_LEVEL_ID);
											
										if (itemCost > 0)
											costList.add(itemCost);
									}
									
								// TO DO: Skip from this loop
								}								
							}
						}
							
						// Get maximum cost for the item 
						double itemCost = getMaximumCost(costList);
							
						// Add cost to collection
						if (itemCost > 0){
							addSellableItemCost(costFeedMap, retailerItemCode, itemCost, 
									locationDto, zoneKey, Constants.ZONE_LEVEL_ID, costEffDate);
						}
					}
				}
				// Cost generation at zone level - Ends
				
			}
		}
			
		return costFeedMap;
	}
	  
	public HashMap<String, PerishableCostFeedDTO> generateCostFeedforMOSS(
		RetailCostServiceOptimized costService,
		ArrayList<PerishableRelationshipDTO> relationshipsList, 
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap,
		HashMap<Integer, HashMap<Integer, TargetLocationDTO>> targetLocation, 
		int chainId, boolean storeCostRequired, boolean zoneCostRequired, String costEffDate){

		//Output Hash map
		HashMap<String, PerishableCostFeedDTO> costFeedMap = new HashMap<String, PerishableCostFeedDTO>(); 
		
		//Object for output feed DTO
		TargetLocationDTO locationDto = new TargetLocationDTO();
		
		logger.debug("Total items need Cost feed: " + targetLocation.size());
		
		//Iterate for each item in target location
		for (int itemCode : targetLocation.keySet()) {
			logger.debug("Process for item: " + itemCode);
			
			HashMap<Integer, TargetLocationDTO> storeMap = targetLocation.get(itemCode);
			
			//Loop and locate the sellable item code
			for (int i=0; i < relationshipsList.size(); i++){
				if (itemCode == relationshipsList.get(i).getSellData().get(0).getItemCode()){
				
					//PerishableSellDTO sellDataObj = relationshipsList.get(i).getSellData().get(1);
					String retailerItemCode = relationshipsList.get(i).getSellData().get(0).getSellCode();
					
					// Generate cost for each authorized store
					for (int storeId: storeMap.keySet()) {
						locationDto = new TargetLocationDTO();
						locationDto = storeMap.get(storeId);
						
						if (storeCostRequired){
						
							String storeKey = itemCode + "_" + Constants.STORE_LEVEL_ID + "_" + storeId;
							
							// Check existence of cost for item and store combination
							// If not exist then generate cost
							if (!costFeedMap.containsKey(storeKey)){
	
								// Calculate the cost for the recipe
								double itemCost = calculateRecipeCost(costService,
									relationshipsList.get(i), costDataMap,
									locationDto, chainId, Constants.STORE_LEVEL_ID);
								
								// Add cost data into feed map
								if (itemCost > 0){
									addSellableItemCost(costFeedMap, retailerItemCode, 
										itemCost, locationDto, storeKey, Constants.STORE_LEVEL_ID, costEffDate);
								}
							}
						}

						if (zoneCostRequired){
							String zoneKey = itemCode + "_" + Constants.ZONE_LEVEL_ID + "_" + locationDto.getZoneId();
							
							// Check existence of cost for item and zone combination
							// If not exist then generate cost
							if (!costFeedMap.containsKey(zoneKey)){
	
								// Calculate the cost for the recipe
								double itemCost = calculateRecipeCost(costService,
									relationshipsList.get(i), costDataMap,
									locationDto, chainId, Constants.ZONE_LEVEL_ID);
								
								// Add cost data into feed map
								if (itemCost > 0){
									addSellableItemCost(costFeedMap, retailerItemCode, 
										itemCost, locationDto, zoneKey, Constants.ZONE_LEVEL_ID, costEffDate);
								}
							}
						}
					}				
			   
				// Exit from this for loop and proceed with next item
				}
		   }
		}
		  
		  return costFeedMap;
	}
	
	private double getOrderItemCost(RetailCostServiceOptimized costService, 
		HashMap<RetailPriceCostKey, RetailCostDTO> costMap, 
		TargetLocationDTO locationDto, int chainId, int locationType){
		
		RetailPriceCostKey storeKey = new RetailPriceCostKey(Constants.STORE_LEVEL_TYPE_ID, locationDto.getStoreId());
		RetailPriceCostKey zoneKey = new RetailPriceCostKey(Constants.ZONE_LEVEL_TYPE_ID, locationDto.getZoneId());
		RetailPriceCostKey chainKey = new RetailPriceCostKey(Constants.CHAIN_LEVEL_TYPE_ID, chainId);

		double itemCost=0;
		
		if (locationType == Constants.ZONE_LEVEL_ID){
			RetailCostDTO costDTO = costService.findCostForZone(costMap, zoneKey, chainKey);
			itemCost = costDTO.getListCost();
		}
		else if (locationType == Constants.STORE_LEVEL_ID){
			RetailCostDTO costDTO = costService.findCostForStore(costMap, storeKey, zoneKey, chainKey);
			itemCost = costDTO.getListCost();
		}		
		
		return itemCost;
	}  
	  
	public double calculateRecipeCost(RetailCostServiceOptimized costService, 
								PerishableRelationshipDTO relationshipDataObj,
		HashMap<Integer, HashMap<RetailPriceCostKey, RetailCostDTO>> costDataMap,								
				TargetLocationDTO locationDto, int chainId, int locationType){

		HashMap<RetailPriceCostKey, RetailCostDTO> costMap = new HashMap<RetailPriceCostKey, RetailCostDTO>();
		double recipeCost = 0;
		
		
		List<PerishableOrderDTO> orderDataList = relationshipDataObj.getOrderData();
		
		for (int i=0; i < orderDataList.size(); i++){
			double itemCost = 0;
			costMap = new HashMap<RetailPriceCostKey, RetailCostDTO>();
			int itemCode = orderDataList.get(i).getItemCode();

			if (costDataMap.containsKey(itemCode)){
				costMap = costDataMap.get(itemCode);
				itemCost = getOrderItemCost(costService,costMap,	locationDto, chainId, locationType);
			}
			else 
				logger.warn("Cost not found for item: " + itemCode);
			
			if (itemCost > 0){
				double ingCost = calculateIngredientCost(orderDataList.get(i), itemCost);
				recipeCost += ingCost;
				logger.warn("Order item " + orderDataList.get(i).getOrderCode() + " List cost: " + itemCost + " & Ingredient cost: " + ingCost);
			}
			else
				logger.warn("No cost found for item:" + orderDataList.get(i).getItemCode());
		}
		
		if (relationshipDataObj.getAdditionalCost() > 0)
			recipeCost += relationshipDataObj.getAdditionalCost();

		if (relationshipDataObj.getLaborCost() > 0)
			recipeCost += relationshipDataObj.getLaborCost();		
		
		logger.debug("Recipe cost is:" + recipeCost);
		
		return recipeCost;
	}
	
	public double calculateIngredientCost(PerishableOrderDTO orderDataObj, double itemCost){
		BusinessHelper bizHelperObj = new BusinessHelper();
		double ingCost =0;
		
        // Order UOM & Ingredient Recipe are same
        if (orderDataObj.getItemUOM() == orderDataObj.getIngUOMMajor())
        {
            ingCost = (itemCost / orderDataObj.getItemSize()) * orderDataObj.getIngSize();

            //This block yet to test
            if (orderDataObj.getIngUOMMinor() > 0)
            {
                double orderIngUomMinorConv = bizHelperObj.ConvertUOMMinorToOZ(orderDataObj.getIngUOMMajor(), orderDataObj.getIngUOMMinor());
                ingCost = ingCost + (itemCost * orderIngUomMinorConv);
            }
        }
        // Order UOM & Ingredient Recipe are NOT same
        else
        {
            // Order UOM is Dozen & Ingredient UOM Each or Count
            if (orderDataObj.getItemUOM() == Constants.UOM_DZ &&
                (orderDataObj.getIngUOMMajor() == Constants.UOM_EA ||
        		orderDataObj.getIngUOMMajor() == Constants.UOM_CT))
            {
                ingCost = (itemCost / (12 * orderDataObj.getItemSize())) * orderDataObj.getIngSize();
            }
            // Order UOM Each or Count & Ingredient UOM Dozen
            else if ((orderDataObj.getItemUOM() == Constants.UOM_EA ||
            		orderDataObj.getItemUOM() == Constants.UOM_CT) &&
            		orderDataObj.getIngUOMMajor() == Constants.UOM_DZ)
            {
                ingCost = (itemCost / orderDataObj.getItemSize()) * (orderDataObj.getIngSize() * 12);
            }
            // Order UOM Each & Ingredient UOM Count or Order UOM Count & Ing UOM Each
            else if ((orderDataObj.getItemUOM() == Constants.UOM_EA &&
            		orderDataObj.getIngUOMMajor() == Constants.UOM_CT) ||
                (orderDataObj.getItemUOM() == Constants.UOM_CT &&
                		orderDataObj.getIngUOMMajor() == Constants.UOM_EA))
            {
                ingCost = (itemCost / orderDataObj.getItemSize()) * orderDataObj.getIngUOMMajor();
            }
            // If Order && Ingredient are convertable quantity
            else if ((orderDataObj.getItemUOM() == Constants.UOM_LB ||
        		orderDataObj.getItemUOM() == Constants.UOM_OZ ||
				orderDataObj.getItemUOM() == Constants.UOM_ML ||
				orderDataObj.getItemUOM() == Constants.UOM_QT ||
				orderDataObj.getItemUOM() == Constants.UOM_GA ||
				orderDataObj.getItemUOM() == Constants.UOM_PT ||
				orderDataObj.getItemUOM() == Constants.UOM_GR ||
				orderDataObj.getItemUOM() == Constants.UOM_LT ||
				orderDataObj.getItemUOM() == Constants.UOM_FZ ||
				orderDataObj.getItemUOM() == Constants.UOM_CUP ||
				orderDataObj.getItemUOM() == Constants.UOM_TBLSP ||
				orderDataObj.getItemUOM() == Constants.UOM_TSP) &&
                (orderDataObj.getIngUOMMajor() == Constants.UOM_LB ||
        		orderDataObj.getIngUOMMajor() == Constants.UOM_OZ ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_ML ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_QT ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_GA ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_PT ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_GR ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_LT ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_FZ ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_CUP ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_TBLSP ||
				orderDataObj.getIngUOMMajor() == Constants.UOM_TSP))
            {
                double orderItemUomConv = bizHelperObj.ConvertUOMToOZ(orderDataObj.getItemUOM());
                double orderIngUomConv = bizHelperObj.ConvertUOMToOZ(orderDataObj.getIngUOMMajor());

                ingCost = (itemCost / (orderItemUomConv * orderDataObj.getItemSize())) * (orderIngUomConv * orderDataObj.getIngSize());

                if (orderDataObj.getIngUOMMinor() > 0)
                {
                    double orderIngUomMinorConv = bizHelperObj.ConvertUOMMinorToOZ(orderDataObj.getIngUOMMajor(), orderDataObj.getIngUOMMinor());
                    ingCost = ingCost + (itemCost / (orderItemUomConv * orderDataObj.getItemSize())) * orderIngUomMinorConv;
                }
            }
            else{
                ingCost = -1;
            }
        }
        //logger.debug("Ingredient cost: " + ingCost);
        
		return ingCost;
	}

	public void setPriceRatio(ArrayList<PerishableRelationshipDTO> relationshipDataList){
		for (int loop=0; loop < relationshipDataList.size(); loop++)
			setPriceRatioForOrder(relationshipDataList.get(loop));
	}
	
	public void setPriceRatioForOrder(PerishableRelationshipDTO relationshipDataObj){

		List<PerishableSellDTO> sellDataList = relationshipDataObj.getSellData();
		PerishableOrderDTO orderDataObj = relationshipDataObj.getOrderData().get(0);
        double minPrice = 0;
        double priceRatioFactor = 0;

		// Step 1: find minimum typical price - Begins
        for (int i = 0; i < sellDataList.size(); i++)
        {
        	double typicalPrice = sellDataList.get(i).getTypicalPrice();
        	
            if (typicalPrice > 0 && (minPrice == 0 || minPrice > typicalPrice))
                minPrice = sellDataList.get(i).getTypicalPrice();
        }
        
     // Step 2: Calculate price ratio - Start
        if (minPrice > 0){
	        for (int i = 0; i < sellDataList.size(); i++)
	        {
	            double availableSellQty = 0;
	            double priceRatio = 0;
	            double typicalPrice = sellDataList.get(i).getTypicalPrice();
	            
	            // Calculate price ratio
	            priceRatio = typicalPrice / minPrice;
	            sellDataList.get(i).setTypicalPriceRatio(priceRatio);
	
	                // Calculate available quantity
	                availableSellQty = orderDataObj.getItemSize() * sellDataList.get(i).getItemYield() / 100;
	
	
	                // Calculate price ratio factor
	                priceRatioFactor += availableSellQty * priceRatio;
	        }
	        
	        orderDataObj.setPriceRatioFactor(priceRatioFactor);
        }
        // Step 2: Calculate price ratio - End
	}
	
	public double calculateItemCost(PerishableSellDTO sellDataObj, double orderCost, double priceRatioFactor){
		double sellCost = 0;
		
		sellCost = (orderCost * sellDataObj.getTypicalPriceRatio()) / priceRatioFactor;
		
		return sellCost;
	}
	
	public HashMap<Integer, HashMap<Integer, TargetLocationDTO>> getTargetLocation(
		int productLeveId,	int productId, PerishableDAO perishableDaoObj, 
					List<Integer> itemCodeList) throws GeneralException {

		HashMap<Integer, HashMap<Integer, TargetLocationDTO>> locationMap = 
				new HashMap<Integer, HashMap<Integer, TargetLocationDTO>>();
			
		perishableDaoObj.getTargetLocation(locationMap,productLeveId, 
												productId, itemCodeList);

		
		if (locationMap != null && locationMap.size() > 0)
			logger.debug("Total items with location mapping: " + locationMap.size());
		else
			logger.debug("No location mapping data found!");

		return locationMap;
	}
	
	public void writeCostData(HashMap<String, PerishableCostFeedDTO> costFeedDataMap,
			String rootPath, String rootPathBackup, String relativeOutputPath) 
													throws GeneralException {
		PrintWriter pw = null;
		FileWriter fw = null;
		try {
			String FileName = PropertyManager.getProperty("COST_OUT_FILE_NAME", "");
			
			String fileName = FileName + ".txt";
			String outputPath = rootPath + "/" + relativeOutputPath + "/" + fileName;
			logger.debug("Output file path: " + outputPath);
			File file = new File(outputPath);
			
			// If file not exist, create new. If exist, append the same
			if (!file.exists())
				fw = new FileWriter(outputPath);
			else
				fw = new FileWriter(outputPath, true);
			
			pw = new PrintWriter(fw, true);
			try {

				for (String dataKey : costFeedDataMap.keySet()) {
					PerishableCostFeedDTO costFileExportDTO = costFeedDataMap.get(dataKey);
					pw.print(costFileExportDTO.getRetailerItemCode()); //RETAILER ITEM CODE
					pw.print("|");
					pw.print(costFileExportDTO.getLocationType()); //Location Type #
					pw.print("|");
					pw.print(costFileExportDTO.getLocationCode()); //Location code
					pw.print("|");
					pw.print(costFileExportDTO.getCost());// Cost
					pw.print("|");
					pw.print(costFileExportDTO.getCostEffectiveDate());	//PRICE EFFECTIVE DATE
					pw.print("|");
					pw.println("");
				} 
				pw.close();
				
				// PrestoUtil.zipFile(fileName, rootPath + "/" + relativeOutputPath, FileName + ".zip");
				// PrestoUtil.moveFile (outputPath, rootPathBackup + "/" + relativeOutputPath);
			}catch (Exception ex) {
			        logger.error("Outer Exception - JavaException", ex);
			        ex.printStackTrace();
			        throw new GeneralException("Outer Exception - JavaException - " + ex.toString());
			     }
			} catch (Exception ex) {
			    logger.error("JavaException", ex);
			    throw new GeneralException("JavaException - " + ex.toString());
	    }
	}
	
	private void addSellableItemCost(HashMap<String, PerishableCostFeedDTO> costFeedMap, 
		String retailerItemCode, double itemCost, TargetLocationDTO locationDto, 
					String locationKey, int locationType, String costEffDate){
		
		PerishableCostFeedDTO sellCostDto = new PerishableCostFeedDTO();
		sellCostDto.setRetailerItemCode(retailerItemCode);
		sellCostDto.setLocationType(locationType);
		if (locationType == Constants.STORE_LEVEL_ID)
			sellCostDto.setLocationCode(locationDto.getStoreNo());
		else if (locationType == Constants.ZONE_LEVEL_ID)
			sellCostDto.setLocationCode(locationDto.getZoneNum());
		
		sellCostDto.setCost(NumberUtil.round(itemCost, 3));
		sellCostDto.setCostEffectiveDate(costEffDate);
		costFeedMap.put(locationKey, sellCostDto);
		
		if (locationType == Constants.STORE_LEVEL_ID)
		logger.debug("Cost $" + itemCost + " for store: " + locationDto.getStoreNo() + " and item: " + retailerItemCode + " added to collection");
		else if (locationType == Constants.ZONE_LEVEL_ID)
		logger.debug("Cost $" + itemCost + " for Zone: " + locationDto.getZoneNum() + " and item: " + retailerItemCode + " added to collection");
	}
	
	
	
	private double getMaximumCost(List<Double> costList){
		double itemCost = 0;
		if (costList != null && costList.size() > 0){
			
			if (costList.size() == 1)
				itemCost = costList.get(0);
			else{
				for (int itemLoop =0; itemLoop < costList.size(); itemLoop++){
					if (itemCost < costList.get(itemLoop))
						itemCost = costList.get(itemLoop);
				}
			}
		}
		
		return itemCost;
	}
}
