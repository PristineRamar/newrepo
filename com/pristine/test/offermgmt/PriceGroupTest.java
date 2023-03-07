package com.pristine.test.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.offermgmt.PriceGroupRelationDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.offermgmt.BrandClassLookup;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.MostOccurrenceData;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.service.offermgmt.constraint.LIGConstraint;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class PriceGroupTest {
	private Connection conn = null;
	private static Logger logger = Logger.getLogger("Testing");
	/**
	 * @param args
	 * @throws OfferManagementException 
	 */
	public static void main(String[] args) throws OfferManagementException {
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("analysis.properties");
		PriceGroupTest priceGroupTest = new PriceGroupTest();
		priceGroupTest.intialSetup();
		priceGroupTest.getPriceGroupData();		
		//priceGroupTest.ligPriceTest();

	}

	public void getPriceGroupData() throws OfferManagementException
	{
		List<ExecutionTimeLog> executionTimeLogs = new ArrayList<ExecutionTimeLog>();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		PriceGroupRelationDAO pgDAO = new PriceGroupRelationDAO(conn);
		PRStrategyDTO inputDTO = new PRStrategyDTO();
		RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		ItemService itemService = new ItemService(executionTimeLogs);
		List<PRItemDTO> allStoreItems = new ArrayList<PRItemDTO>();
		int runId = -1;
		try {
			RetailCalendarDTO runForWeek = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(-1), Constants.CALENDAR_WEEK);
			List<Integer> priceZoneStores = new ArrayList<Integer>();
			
			inputDTO.setStartCalendarId(runForWeek.getCalendarId());
			inputDTO.setEndCalendarId(runForWeek.getCalendarId());
			inputDTO.setStartDate(runForWeek.getStartDate());
			inputDTO.setEndDate(runForWeek.getEndDate());

			inputDTO.setLocationLevelId(6);
			inputDTO.setLocationId(200); // Zone - 693

			inputDTO.setProductLevelId(4); // Category
			inputDTO.setProductId(2487); // TOPS->CHEESE-REFRIG(1303), Trash bag(149), peanut butter(264), shrimp(569)

			priceZoneStores = itemService.getPriceZoneStores(conn, inputDTO.getProductLevelId(), inputDTO.getProductId(),
					inputDTO.getLocationLevelId(), inputDTO.getLocationId());
			
			allStoreItems = itemService.getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores);					
			itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, runId, inputDTO, allStoreItems);
			
			HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupRelation = pgDAO.getPriceGroupDetails(inputDTO, itemDataMap);

			//********* Set default brand precedence if not defined *************/
			
			 setDefaultBranPrecedence(itemDataMap);
			
			
			
			// Copies price group information for items in itemList. If price
			// group relation is defined for LIR, it is copied to all items in
			// the group
			
			
			//Fix: within a size group, there are different UOM, due to this
			// size relation gives unrealistic size range, which causes other issues
			// Maintain same uom within a size relation, by converting to most
			// common UOM
			
			//Loop each price group
			for (Map.Entry<String, HashMap<ItemKey, PRPriceGroupDTO>> outEntry : priceGroupRelation.entrySet()) {
				logger.debug("Price Group Name:" + outEntry.getKey());

				// Find most common UOM (consider only if there are 2 UOM's LB & OZ)
				String mostCommonUOM = "";
				for (Map.Entry<ItemKey, PRPriceGroupDTO> inEntry : outEntry.getValue().entrySet()) {
					PRPriceGroupDTO priceGroupDTO = inEntry.getValue();
					List<PRItemDTO> items = new ArrayList<PRItemDTO>();
					String uom = priceGroupDTO.getUomName().toUpperCase();

					logger.debug("ItemKey:" + inEntry.getKey().toString());
					logger.debug("Item Size:" + priceGroupDTO.getItemSize() + ",UOM:" + priceGroupDTO.getUomName());

					if (uom.equals("LB") || uom.equals("OZ")) {
						PRItemDTO itemDTO = new PRItemDTO();
						itemDTO.setUOMName(uom);
						items.add(itemDTO);
					} else {
						mostCommonUOM = "";
						break;
					}

					MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
					mostCommonUOM = (String) mostOccurrenceData.getMaxOccurance(items, "UOM");
				}

				// Adjust the size (currently LB to OZ & OZ to LB only is handled)
				if (mostCommonUOM != null && mostCommonUOM.toUpperCase().equals("LB") || mostCommonUOM.toUpperCase().equals("OZ")) {
					mostCommonUOM = mostCommonUOM.toUpperCase();

					for (PRPriceGroupDTO priceGroupDTO : outEntry.getValue().values()) {
						String uom = priceGroupDTO.getUomName().toUpperCase();
						double size = priceGroupDTO.getItemSize();

						// OZ to LB
						if (mostCommonUOM.equals("LB") && uom.equals("OZ")) {

							logger.debug("Existing Size and UOM:" + size + "," + uom);

							priceGroupDTO.setItemSize(size / 16.0);
							priceGroupDTO.setUomName("LB");

							logger.debug("New Size and UOM:" + priceGroupDTO.getItemSize() + "," + priceGroupDTO.getUomName());

						} else if (mostCommonUOM.equals("OZ") && uom.equals("LB")) {
							// LB to OZ
							logger.debug("Existing Size and UOM:" + size + "," + uom);

							priceGroupDTO.setItemSize(size * 16);
							priceGroupDTO.setUomName("OZ");

							logger.debug("New Size and UOM:" + priceGroupDTO.getItemSize() + "," + priceGroupDTO.getUomName());
						}
					}
				}

			}
			 
			
			pricingEngineDAO.populatePriceGroupDetails(itemDataMap, priceGroupRelation);
			
//			for (PRItemDTO item : itemDataMap.values()) {
//				if (item.getItemCode() == 55057) {
//					ItemKey relatedItemKey = PRCommonUtil.getItemKey(item.getItemCode(), false);
//					itemDataMap.get(relatedItemKey);
//					checkIfAnyNationalOrSizeBrandAvailable(item);
//				}
//			}
			
			logger.debug("test");
		} catch (GeneralException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}
	
	private void setDefaultBranPrecedence(HashMap<ItemKey, PRItemDTO> itemDataMap) {
		// There was an item with 3 brand relation and all 3 items has relationship as 10 - 20% below,
		// but at the end, the recommended price matched 2 of the national brand. This is because, they
		// haven't assigned any brand precedence. Also they wanted it to be lower than the lowest price of all three.
		// so it is decided to change the precedence based on the relation operator. If below/above is present
		// then item with lowest/highest price is given higher precedence, so that it recommends below/above the
		// lowest/highest
		// look for jira issue :: PROM-1287

		PricingEngineService pricingEngineService = new PricingEngineService();

		// Loop each item 
		for (PRItemDTO itemDTO : itemDataMap.values()) {
			PRPriceGroupDTO priceGroupDTO = itemDTO.getPgData();

			if (!itemDTO.isLir() && priceGroupDTO != null) {
				// Loop inside each relationship
				for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationMap : priceGroupDTO.getRelationList().entrySet()) {
					ArrayList<PRPriceGroupRelatedItemDTO> relatedItemList = relationMap.getValue();
					boolean isBrandPrecedenceDefined = false;
					boolean isAboveRelationPresent = false;
					boolean isBelowRelationPresent = false;

					for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relatedItemList) {
						// If at least one precedence defined, then don't change anything
						if (priceGroupRelatedItemDTO.getBrandPrecedence() > 0) {
							isBrandPrecedenceDefined = true;
							break;
						}
					}

					// Check if the item has more than one brand relation and brand precedence is not defined
					if (relationMap.getKey() == PRConstants.BRAND_RELATION && relatedItemList.size() > 1 && !isBrandPrecedenceDefined) {
						// Check if below or above relation used
						for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relatedItemList) {
							if (priceGroupRelatedItemDTO.getPriceRelation() != null
									&& priceGroupRelatedItemDTO.getPriceRelation().getOperatorText() != null) {
								String operatorText = priceGroupRelatedItemDTO.getPriceRelation().getOperatorText();

								if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)
										|| PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
										|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
									isAboveRelationPresent = true;
									break;
								} else {
									isBelowRelationPresent = true;
									break;
								}
							}
						}

						// Keep item detail in a temp list with related item unite price
						List<PRItemDTO> tempRelatedItemList = new ArrayList<PRItemDTO>();
						// find related item cur reg price for lig / non-lig
						// If the related item is not authorized or there is no cur price, still add in the list
						// with 0 as cur price
						for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relatedItemList) {
							PRItemDTO tempItemDTO = new PRItemDTO();
							if (priceGroupRelatedItemDTO.getIsLig()) {
								List<PRItemDTO> ligMembers = pricingEngineService.getLigMembers(priceGroupRelatedItemDTO.getRelatedItemCode(),
										(List<PRItemDTO>) itemDataMap.values());

								tempItemDTO.setItemCode(priceGroupRelatedItemDTO.getRelatedItemCode());
								tempItemDTO.setLir(true);

								if (ligMembers.size() > 0) {
									MostOccurrenceData mostOccurrenceData = new MostOccurrenceData();
									Object object = mostOccurrenceData.getMaxOccurance(ligMembers, "CurRegPriceInMultiples");
									if (object != null) {
										MultiplePrice curRegPrice = (MultiplePrice) object;
										tempItemDTO.setRegMPack(curRegPrice.multiple);
										if (curRegPrice.multiple > 1) {
											tempItemDTO.setRegMPrice(curRegPrice.price);
										} else {
											tempItemDTO.setRegPrice(curRegPrice.price);
										}
									}
								}
							} else {
								tempItemDTO.setItemCode(priceGroupRelatedItemDTO.getRelatedItemCode());
								tempItemDTO.setLir(false);
								ItemKey itemKey = PRCommonUtil.getItemKey(tempItemDTO);
								if (itemDataMap.get(itemKey) != null) {
									tempItemDTO.setRegMPack(itemDataMap.get(itemKey).getRegMPack());
									tempItemDTO.setRegPrice(itemDataMap.get(itemKey).getRegPrice());
									tempItemDTO.setRegMPrice(itemDataMap.get(itemKey).getRegMPrice());
								}
							}

							tempRelatedItemList.add(tempItemDTO);
						}

						// Sort based on operator text
						if (isAboveRelationPresent) {
							// Set the precedence sorted by current unit price in descending order
							Collections.sort(tempRelatedItemList, new Comparator<PRItemDTO>() {
								public int compare(PRItemDTO a, PRItemDTO b) {
									Double unitPrice1 = PRCommonUtil.getUnitPrice(a.getRegMPack(), a.getRegPrice(), a.getRegMPrice(), true);
									Double unitPrice2 = PRCommonUtil.getUnitPrice(b.getRegMPack(), b.getRegPrice(), b.getRegMPrice(), true);
									return unitPrice2.compareTo(unitPrice1);
								}
							});
						} else if (isBelowRelationPresent) {
							// Set the precedence sorted by current unit price in ascending order
							Collections.sort(tempRelatedItemList, new Comparator<PRItemDTO>() {
								public int compare(PRItemDTO a, PRItemDTO b) {
									Double unitPrice1 = PRCommonUtil.getUnitPrice(a.getRegMPack(), a.getRegPrice(), a.getRegMPrice(), true);
									Double unitPrice2 = PRCommonUtil.getUnitPrice(b.getRegMPack(), b.getRegPrice(), b.getRegMPrice(), true);
									return unitPrice1.compareTo(unitPrice2);
								}
							});
						}

						// Assign precedence
						for (PRPriceGroupRelatedItemDTO priceGroupRelatedItemDTO : relatedItemList) {
							int precendence = 1;
							for (PRItemDTO tempItemDTO : tempRelatedItemList) {
								ItemKey relatedItemKey = new ItemKey(priceGroupRelatedItemDTO.getRelatedItemCode(),
										(priceGroupRelatedItemDTO.getIsLig() ? PRConstants.LIG_ITEM_INDICATOR : PRConstants.NON_LIG_ITEM_INDICATOR));

								ItemKey sortedItemKey = PRCommonUtil.getItemKey(tempItemDTO);

								// look for the item and assign precedence
								if (sortedItemKey.equals(relatedItemKey)) {
									priceGroupRelatedItemDTO.setBrandPrecedence(precendence);
									break;
								}
							}
							precendence = precendence + 1;
						}
					}
				}
			}
		}
	}
	
	private boolean checkIfAnyNationalOrSizeBrandAvailable(PRItemDTO itemInfo){
		boolean isNationalBrandOrSizeRelationAvailable = false;
		boolean isStoreBrandRelationAvailable = false;
		boolean isNationalBrandRelationAvailable = false;
		boolean isSizeRelationAvailable = false;
		
		if (itemInfo.getPgData() != null) {
			NavigableMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> navigableMap = null;
			navigableMap = itemInfo.getPgData().getRelationList();
			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
				if (entry.getKey() == PRConstants.BRAND_RELATION) {
					for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
						if (itemInfo.getPgData().getBrandClassId() == BrandClassLookup.NATIONAL.getBrandClassId()) {
							isNationalBrandRelationAvailable = true;
						}
						else{
							isStoreBrandRelationAvailable = true;
						}							
					}
				}
			}
			
			for (Map.Entry<Character, ArrayList<PRPriceGroupRelatedItemDTO>> entry : navigableMap.entrySet()) {
				if (entry.getKey() == PRConstants.SIZE_RELATION) {
					for (PRPriceGroupRelatedItemDTO relatedItem : entry.getValue()) {
						//Ignore size lead items as size relation will not be applied
						if(relatedItem.getRelatedItemCode() > 0 )
							isSizeRelationAvailable = true;
					}
				}
			}
		}
		
		if(isStoreBrandRelationAvailable){
			isNationalBrandOrSizeRelationAvailable = false;
		}
		else if(isNationalBrandRelationAvailable || isSizeRelationAvailable){
			isNationalBrandOrSizeRelationAvailable = true;
		}

		return isNationalBrandOrSizeRelationAvailable;
	}
	
	private void ligPriceTest(){
		LIGConstraint ligConstraint = new LIGConstraint();
		List<PRItemDTO> ligMembers = new ArrayList<PRItemDTO>();
		PRItemDTO prItemDTO;
		Double[] priceRange;
		MultiplePrice ligPrice = null;
		
		//Single common price point
		prItemDTO = new PRItemDTO();
		prItemDTO.setPriceRange(null);
		ligMembers.add(prItemDTO);
		
		prItemDTO = new PRItemDTO();
		priceRange = new Double[] {2.69, 2.79};
		prItemDTO.setPriceRange(priceRange);
		ligMembers.add(prItemDTO);
		
		prItemDTO = new PRItemDTO();
		priceRange = new Double[] {2.89, 2.69};
		prItemDTO.setPriceRange(priceRange);
		ligMembers.add(prItemDTO);
		
		prItemDTO = new PRItemDTO();
		priceRange = new Double[] {2.69, 2.99};
		prItemDTO.setPriceRange(priceRange);
		ligMembers.add(prItemDTO);
		ligPrice = ligConstraint.getLigLevelPrice(ligMembers, true);
		
		// Multiple common price point and common rec price
		ligMembers.clear();
		prItemDTO = new PRItemDTO();
		priceRange = new Double[] {2.69, 2.79};
		prItemDTO.setPriceRange(priceRange);
//		prItemDTO.setRecommendedRegPrice(2.79);
		prItemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.79));
		ligMembers.add(prItemDTO);
		
//		prItemDTO = new PRItemDTO();
//		priceRange = new Double[] {2.69, 2.79, 2.89};
//		prItemDTO.setPriceRange(priceRange);
//		prItemDTO.setRecommendedRegPrice(2.69);
//		ligMembers.add(prItemDTO);
//		
//		prItemDTO = new PRItemDTO();
//		priceRange = new Double[] {2.69, 2.79, 2.89};
//		prItemDTO.setPriceRange(priceRange);
//		prItemDTO.setRecommendedRegPrice(2.69);
//		ligMembers.add(prItemDTO);
		ligPrice = ligConstraint.getLigLevelPrice(ligMembers, true);
		
		// No common price point
		ligMembers.clear();
		prItemDTO = new PRItemDTO();
		priceRange = new Double[] {2.69, 2.79};
		prItemDTO.setPriceRange(priceRange);
		ligMembers.add(prItemDTO);
		
		prItemDTO = new PRItemDTO();
		priceRange = new Double[] {2.59, 2.89};
		prItemDTO.setPriceRange(priceRange);
		ligMembers.add(prItemDTO);
		
		prItemDTO = new PRItemDTO();
		priceRange = new Double[] {2.69, 2.79, 2.89};
		prItemDTO.setPriceRange(priceRange);
		ligMembers.add(prItemDTO);
		ligPrice = ligConstraint.getLigLevelPrice(ligMembers, true);
		
		// Multiple common price point and no common rec price
		ligMembers.clear();
		prItemDTO = new PRItemDTO();
		priceRange = new Double[] { 2.69, 2.79 };
		prItemDTO.setPriceRange(priceRange);
//		prItemDTO.setRecommendedRegPrice(2.69);
		prItemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.69));
		ligMembers.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		priceRange = new Double[] { 2.69, 2.79 };
		prItemDTO.setPriceRange(priceRange);
//		prItemDTO.setRecommendedRegPrice(2.79);
		prItemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.79));
		ligMembers.add(prItemDTO);

		prItemDTO = new PRItemDTO();
		priceRange = new Double[] { 2.69, 2.79, 2.89 };
		prItemDTO.setPriceRange(priceRange);
//		prItemDTO.setRecommendedRegPrice(2.69);
		prItemDTO.setRecommendedRegPrice(new MultiplePrice(1, 2.69));
		ligMembers.add(prItemDTO);
		ligPrice = ligConstraint.getLigLevelPrice(ligMembers, true);
		
	}
	
	public void intialSetup() {
		initialize();
	}
	
	protected void setConnection() {
		if (conn == null) {
			try {
				conn = DBManager.getConnection();
			} catch (GeneralException exe) {
				logger.error("Error while connecting to DB:" + exe);
				System.exit(1);
			}
		}
	}

	protected void setConnection(Connection conn) {
		this.conn = conn;
	}

	/**
	 * Initializes object
	 */
	protected void initialize() {
		setConnection();
	}
}
