package com.pristine.test.offermgmt;


import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.util.offermgmt.PRConstants;
public class CostGapDirectionJunitTest {

	PRPriceGroupRelnDTO priceGroupRelnDTO= new PRPriceGroupRelnDTO();
	
	/*
	 * @Test public void test12() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 13.94; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(21.93); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 26.49);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch="34.48";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch,String.valueOf(range.getStartVal()));
	 * 
	 * 
	 * 
	 * }
	 * 
	 * 
	 * 
	 * 
	 * 
	 * @Test public void test1() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 24.8; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(25.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch="54.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch,String.valueOf(range.getStartVal()));
	 * 
	 * 
	 * 
	 * }
	 * 
	 * @Test public void test2() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 24.8; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(30.8); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch="55.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch,String.valueOf(range.getStartVal()));
	 * 
	 * 
	 * 
	 * }
	 * 
	 * 
	 * @Test public void test3() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 24.8; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(20.41); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch="54.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch,String.valueOf(range.getStartVal()));
	 * 
	 * }
	 * 
	 * 
	 * @Test public void test4() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 24.8; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(25.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(10); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch="54.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch,String.valueOf(range.getStartVal()));
	 * 
	 * 
	 * 
	 * }
	 * 
	 * @Test public void test5() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 24.8; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(30.8); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(10); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch="55.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch,String.valueOf(range.getStartVal()));
	 * 
	 * 
	 * 
	 * }
	 * 
	 * @Test public void test6() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 24.8; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(20.41); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(10); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch="54.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch,String.valueOf(range.getStartVal()));
	 * 
	 * 
	 * 
	 * }
	 * 
	 * 
	 */
  
  }
 