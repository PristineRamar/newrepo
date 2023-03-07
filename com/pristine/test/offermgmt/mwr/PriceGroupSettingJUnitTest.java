package com.pristine.test.offermgmt.mwr;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupDTO;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.mwr.itemattributes.ItemAttributeService;
import com.pristine.test.offermgmt.TestHelper;
import com.pristine.util.PropertyManager;

public class PriceGroupSettingJUnitTest {

	public static final Integer ITEM_CODE_TEST_1234 = 1234;
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
	 * Price group is at given item level
	 */
	@Test
	public void case1ItemLevelPG() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Item
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_1234, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);

		assertEquals("Price Group Not Set!!!", priceGroupId, item.getPgData().getPriceGroupId());
	}

	/**
	 * Price group is at LIG level of given item
	 */
	@Test
	public void case2LIGLevelPG() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Item
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setRetLirId(RET_LIR_ID);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(RET_LIR_ID, priceGroupId, true);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, true, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);

		assertEquals("Price Group Not Set!!!", priceGroupId, item.getPgData().getPriceGroupId());
	}

	/**
	 * Price group is at both and LIG and item level
	 */
	@Test
	public void case3BothItemAndLIGLevelPG() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Item
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setRetLirId(RET_LIR_ID);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(RET_LIR_ID, priceGroupId, true);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, true, priceGroupName);

		priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_1234, priceGroupId + 1, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);

		assertEquals("Price Group Not Set!!!", priceGroupId, item.getPgData().getPriceGroupId());
	}

	/**
	 * Price group is at LIG member level
	 */
	@Test
	public void case4LIGMemberLevelPG() {
		HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
		HashMap<String, HashMap<ItemKey, PRPriceGroupDTO>> priceGroupMap = new HashMap<>();
		ItemAttributeService itemAttributeService = new ItemAttributeService();

		// Inputs
		int priceGroupId = 1;

		// Item
		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, itemPriceQty, null, itemPrice, listCost, dealCost,
				COST_NO_CHANGE, COMP_STR_ID_TEST_967, compPrice, null, LAST_X_WEEKS_MOV_1);

		item.setRetLirId(RET_LIR_ID);

		itemDataMap = TestHelper.setItemDataMap(itemDataMap, item);

		PRPriceGroupDTO priceGroupDTO = PriceGroupDataHelper.getPriceGroupDTO(ITEM_CODE_TEST_1234, priceGroupId, false);

		priceGroupMap = PriceGroupDataHelper.setPriceGroupMap(priceGroupMap, priceGroupDTO, false, priceGroupName);

		itemAttributeService.setPriceGroupDetails(itemDataMap, priceGroupMap);

		assertEquals("Price Group Not Set!!!", priceGroupId, item.getPgData().getPriceGroupId());
	}
}
