package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.PriceGroupAdjustmentService;
import com.pristine.service.offermgmt.mwr.itemattributes.ItemAttributeService;
import com.pristine.test.offermgmt.TestHelper;
import com.pristine.test.offermgmt.mwr.PriceGroupDataHelper;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class PriceGroupMissingLeadJUnitTest {

	public static final Integer ITEM_CODE_TEST_1234 = 1234;
	public static final Integer ITEM_CODE_TEST_LEAD = 4125;
	public static final Integer ITEM_CODE_TEST_DEPENDENT_1 = 4566;
	public static final Integer ITEM_CODE_TEST_DEPENDENT_2 = 4545;
	public static final Integer ITEM_CODE_TEST_DEPENDENT_3 = 4699;
	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COST_INCREASE = 1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Long LAST_X_WEEKS_MOV_1 = 4587l;
	public static final Integer RET_LIR_ID = 110244;
	public static final Integer RET_LIR_ITEM_CODE = 950735;

	private int itemPriceQty = 2;
	private double itemPrice = 4.69, listCost = 2.1, dealCost = 2.1, compPrice = 3.49;
	private String priceGroupName = "PG1";

	@Before
	public void init() {
		PropertyManager.initialize("recommendation.properties");
	}

	/**
	 * Size relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 * Case: ITEM_CODE_TEST_DEPENDENT_1 is unauthorized. Now, ITEM_CODE_TEST_DEPENDENT_2 should depend on ITEM_CODE_TEST_LEAD
	 * 
	 */
	@Test
	public void case1SizeRelationWithUnAuthorizedLead() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Dependent item 2
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO);
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		// Unauthorized size lead
		dependent1.setAuthorized(false);
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_1) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
		});
	}
	
	
	/**
	 * Size relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 * Case: ITEM_CODE_TEST_DEPENDENT_1 is inactive/discontinued. Now, ITEM_CODE_TEST_DEPENDENT_2 should depend on ITEM_CODE_TEST_LEAD
	 * 
	 */
	@Test
	public void case2SizeRelationWithInActiveLead() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Dependent item 2
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO);
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		// Unauthorized size lead
		dependent1.setActive(false);
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_1) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
		});
	}
	
	
	/**
	 * Size relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 * Case: ITEM_CODE_TEST_DEPENDENT_1 is non-moving item. Now, ITEM_CODE_TEST_DEPENDENT_2 should depend on ITEM_CODE_TEST_LEAD
	 * 
	 */
	@Test
	public void case3SizeRelationWithNonMovingLead() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Dependent item 2
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO);
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		// Unauthorized size lead
		dependent1.setNonMovingItem(true);
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_1) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
		});
	}
	
	
	/**
	 * Brand relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 * Case: ITEM_CODE_TEST_DEPENDENT_1 is non-moving item. Now, ITEM_CODE_TEST_DEPENDENT_2 should depend on ITEM_CODE_TEST_LEAD
	 * Adjustments should maintain the $ gap or % gap
	 */
	@Test
	public void case4BrandRelationWithNonMovingLead() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Dependent item 2
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForBrandRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO, 2,
				Constants.DEFAULT_NA, PRConstants.VALUE_TYPE_$);
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		// Unauthorized size lead
		dependent1.setNonMovingItem(true);
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForBrandRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO, 2,
				Constants.DEFAULT_NA, PRConstants.VALUE_TYPE_$);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_1) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.BRAND_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				int expectedDollarGap = 4;
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.BRAND_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				int minValue = (int) relatedItemDTO.getPriceRelation().getMinValue();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
				assertEquals("Incorrect $ Gap!!!", expectedDollarGap, minValue);
			}
		});
	}
	
	/**
	 * Brand relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 * Case: ITEM_CODE_TEST_DEPENDENT_1 is non-moving item. Now, ITEM_CODE_TEST_DEPENDENT_2 should depend on ITEM_CODE_TEST_LEAD
	 * Adjustments should maintain the $ gap or % gap
	 */
	@Test
	public void case5BrandRelationWithNonMovingLead() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Dependent item 2
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForBrandRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO, 1,
				3, PRConstants.VALUE_TYPE_$);
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		// Unauthorized size lead
		dependent1.setNonMovingItem(true);
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForBrandRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO, 2,
				4, PRConstants.VALUE_TYPE_$);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_1) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.BRAND_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				int expectedMinDollarGap = 3;
				int expectedMaxDollarGap = 7;
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.BRAND_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				int minValue = (int) relatedItemDTO.getPriceRelation().getMinValue();
				int maxValue = (int) relatedItemDTO.getPriceRelation().getMaxValue();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
				assertEquals("Incorrect $ Gap!!!", expectedMinDollarGap, minValue);
				assertEquals("Incorrect $ Gap!!!", expectedMaxDollarGap, maxValue);
			}
		});
	}
	
	/**
	 * Brand relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 * Case: ITEM_CODE_TEST_LEAD is non-moving item. No Change in dependency
	 * 
	 */
	@Test
	public void case6BrandRelationWithNonMovingLead() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Dependent item 2
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForBrandRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO, 1,
				3, PRConstants.VALUE_TYPE_$);
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForBrandRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO, 2,
				4, PRConstants.VALUE_TYPE_$);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		// Unauthorized size lead
		leadItem.setNonMovingItem(true);
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_DEPENDENT_1;
				int expectedMinDollarGap = 1;
				int expectedMaxDollarGap = 3;
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.BRAND_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				int minValue = (int) relatedItemDTO.getPriceRelation().getMinValue();
				int maxValue = (int) relatedItemDTO.getPriceRelation().getMaxValue();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
				assertEquals("Incorrect $ Gap!!!", expectedMinDollarGap, minValue);
				assertEquals("Incorrect $ Gap!!!", expectedMaxDollarGap, maxValue);
			}
		});
	}
	
	
	/**
	 * Size relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 * Case: ITEM_CODE_TEST_DEPENDENT_1 is unauthorized. Now, ITEM_CODE_TEST_DEPENDENT_2 should depend on ITEM_CODE_TEST_LEAD
	 * 
	 */
	@Test
	public void case7SizeRelationWithMultipleUnAuthorizedLead() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		
		// Dependent item 1
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		// Unauthorized size lead
		dependent2.setAuthorized(false);
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO);
		
		// Dependent item 2
		PRItemDTO dependent3 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_3, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent3);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_3, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_DEPENDENT_2, priceGroupDTO);
		
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		// Unauthorized size lead
		dependent1.setAuthorized(false);
		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_1) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_3) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
		});
	}
	
	
	/**
	 * Size relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 */
	@Test
	public void case8SizeRelationNormalCase() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Dependent item 2
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO);
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForSizeRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_1) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_DEPENDENT_1;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.SIZE_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
		});
	}
	
	
	/**
	 * Brand relation. ITEM_CODE_TEST_DEPENDENT_2 is dependent on ITEM_CODE_TEST_DEPENDENT_1 
	 * and ITEM_CODE_TEST_DEPENDENT_1 is depedent on ITEM_CODE_TEST_LEAD
	 * 
	 * Case: Normal case
	 */
	@Test
	public void case9BrandRelationWithNonMovingLead() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Dependent item 2
		PRItemDTO dependent2 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_2, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent2);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_2, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForBrandRelation(ITEM_CODE_TEST_DEPENDENT_1, priceGroupDTO, 2,
				Constants.DEFAULT_NA, PRConstants.VALUE_TYPE_$);
		
		
		// Dependent item 1
		PRItemDTO dependent1 = TestHelper.getTestItem(ITEM_CODE_TEST_DEPENDENT_1, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		
		itemDataMap = TestHelper.setItemDataMap(itemDataMap, dependent1);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_DEPENDENT_1, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupDataHelper.populateRelationListForBrandRelation(ITEM_CODE_TEST_LEAD, priceGroupDTO, 2,
				Constants.DEFAULT_NA, PRConstants.VALUE_TYPE_$);
		

		// Lead item
		PRItemDTO leadItem = TestHelper.getTestItem(ITEM_CODE_TEST_LEAD, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, leadItem);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_LEAD, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);
		
		PriceGroupAdjustmentService adjustmentService = new PriceGroupAdjustmentService();
		
		adjustmentService.adjustPriceGroupsByDiscontinuedItems(itemDataMap, null);
		
		itemDataMap.forEach((itemKey, itemDTO) -> {
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_LEAD) {
				assertEquals("Price Group Not Set!!!", priceGroupId, itemDTO.getPgData().getPriceGroupId());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_1) {
				int expectedLeadItem = ITEM_CODE_TEST_LEAD;
				
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.BRAND_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
			}
			
			if(itemDTO.getItemCode() == ITEM_CODE_TEST_DEPENDENT_2) {
				int expectedLeadItem = ITEM_CODE_TEST_DEPENDENT_1;
				int expectedDollarGap = 2;
				ArrayList<PRPriceGroupRelatedItemDTO> relList = itemDTO.getPgData().getRelationList().get(PRConstants.BRAND_RELATION);
				
				PRPriceGroupRelatedItemDTO relatedItemDTO = relList.iterator().next();
				int minValue = (int) relatedItemDTO.getPriceRelation().getMinValue();
				assertEquals("Incorrect lead!!!", expectedLeadItem, relatedItemDTO.getRelatedItemCode());
				assertEquals("Incorrect $ Gap!!!", expectedDollarGap, minValue);
			}
		});
	}
}
