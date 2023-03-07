package com.pristine.test.offermgmt;

import static org.junit.Assert.*;

import org.junit.Test;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.util.offermgmt.PRConstants;

public class costGapDirectionPer {

	PRPriceGroupRelnDTO priceGroupRelnDTO = new PRPriceGroupRelnDTO();
	/*
	 * @Test public void test1() { // if costgap>0 and costgap>startrange char char
	 * relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0; Double
	 * relatedItemSize = 1.00; double RelatedItemListCost = 25.99; PRItemDTO itemDTO
	 * = new PRItemDTO(); itemDTO.setItemCode(21706); itemDTO.setListCost(31.99);
	 * itemDTO.setItemSize(1.00); MultiplePrice relatedItemPrice1 = new
	 * MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(10);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String startVal = "55.99"; String endval = "59.99"; range =
	 * priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1, relatedItemSize,
	 * itemDTO.getItemSize(), relationType, sizeShelfPCT, itemDTO.getListCost(),
	 * RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(startVal, String.valueOf(range.getStartVal()));
	 * assertEquals(endval, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test2() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 25.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(27.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(10);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String startVal = "54.99"; String endval = "59.99"; range =
	 * priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1, relatedItemSize,
	 * itemDTO.getItemSize(), relationType, sizeShelfPCT, itemDTO.getListCost(),
	 * RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(startVal, String.valueOf(range.getStartVal()));
	 * assertEquals(endval, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test3() { // if cost gap<0
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 25.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(20.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(10);
	 * priceGroupRelnDTO.setOperatorText(">"); priceGroupRelnDTO.setRetailType('U');
	 * String startVal = "54.99"; String endval = "59.99"; range =
	 * priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1, relatedItemSize,
	 * itemDTO.getItemSize(), relationType, sizeShelfPCT, itemDTO.getListCost(),
	 * RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(startVal, String.valueOf(range.getStartVal()));
	 * assertEquals(endval, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test4() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 20.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(26.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(10);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String startVal = "44.99"; String endval = "47.49"; range =
	 * priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1, relatedItemSize,
	 * itemDTO.getItemSize(), relationType, sizeShelfPCT, itemDTO.getListCost(),
	 * RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(startVal, String.valueOf(range.getStartVal()));
	 * assertEquals(endval, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test5() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 15.40; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(6.00); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 41.49);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(30); priceGroupRelnDTO.setMaxValue(35);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String startVal = "32.09"; String endval = "32.09"; range =
	 * priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1, relatedItemSize,
	 * itemDTO.getItemSize(), relationType, sizeShelfPCT, itemDTO.getListCost(),
	 * RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(startVal, String.valueOf(range.getStartVal()));
	 * assertEquals(endval, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test6() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 20.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(16.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(10);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String startVal = "45.99"; String endval = "47.49"; range =
	 * priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1, relatedItemSize,
	 * itemDTO.getItemSize(), relationType, sizeShelfPCT, itemDTO.getListCost(),
	 * RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(startVal, String.valueOf(range.getStartVal()));
	 * assertEquals(endval, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test7() {
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 20.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(14.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 49.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(10);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String startVal = "44.99"; String endval = "47.49"; range =
	 * priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1, relatedItemSize,
	 * itemDTO.getItemSize(), relationType, sizeShelfPCT, itemDTO.getListCost(),
	 * RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(startVal, String.valueOf(range.getStartVal()));
	 * assertEquals(endval, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 */
}
