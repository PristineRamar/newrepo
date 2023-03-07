package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.util.offermgmt.PRConstants;

public class costGapforLessthanReln {

	PRPriceGroupRelnDTO priceGroupRelnDTO = new PRPriceGroupRelnDTO();

	/*
	 * @Test public void test1() { // When costGap>0 and cost gap< brand gap
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 20.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(24.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 44.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch = "39.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test2() { // When costGap>0 and greater than brandgap
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 20.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(30.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 44.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch = "39.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test3() { // When costGap<0 and lesser than brand gap char
	 * relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0; Double
	 * relatedItemSize = 1.00; double RelatedItemListCost = 20.99; PRItemDTO itemDTO
	 * = new PRItemDTO(); itemDTO.setItemCode(21706); itemDTO.setListCost(16.99);
	 * itemDTO.setItemSize(1.00); MultiplePrice relatedItemPrice1 = new
	 * MultiplePrice(1, 44.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch = "40.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test4() { // costgap < 0 and abs(costgap)>brandgap
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 23.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(16.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 44.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_$);
	 * priceGroupRelnDTO.setMinValue(5); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch = "39.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * // Percentage relationship
	 * 
	 * @Test public void test5() { // if cost Gap>0 and costGap<Brand Gap char
	 * relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0; Double
	 * relatedItemSize = 1.00; double RelatedItemListCost = 20.99; PRItemDTO itemDTO
	 * = new PRItemDTO(); itemDTO.setItemCode(21706); itemDTO.setListCost(24.99);
	 * itemDTO.setItemSize(1.00); MultiplePrice relatedItemPrice1 = new
	 * MultiplePrice(1, 44.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(10); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch = "40.49";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test6() { // if costGap>0 and cost gap> brand gap char
	 * relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0; Double
	 * relatedItemSize = 1.00; double RelatedItemListCost = 20.99; PRItemDTO itemDTO
	 * = new PRItemDTO(); itemDTO.setItemCode(21706); itemDTO.setListCost(32.99);
	 * itemDTO.setItemSize(1.00); MultiplePrice relatedItemPrice1 = new
	 * MultiplePrice(1, 44.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(10); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch = "40.49";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test7() { // if cost gap<0 and abs(CostGap)> brand gap
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 30.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(19.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 44.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(10); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch = "40.49";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 * 
	 * @Test public void test8() { // if cost gap<0 and abs(CostGap)< brand gap
	 * 
	 * char relationType = PRConstants.BRAND_RELATION; double sizeShelfPCT = 0;
	 * Double relatedItemSize = 1.00; double RelatedItemListCost = 30.99; PRItemDTO
	 * itemDTO = new PRItemDTO(); itemDTO.setItemCode(21706);
	 * itemDTO.setListCost(28.99); itemDTO.setItemSize(1.00); MultiplePrice
	 * relatedItemPrice1 = new MultiplePrice(1, 44.99);
	 * 
	 * PRRange range = new PRRange();
	 * priceGroupRelnDTO.setValueType(PRConstants.VALUE_TYPE_PCT);
	 * priceGroupRelnDTO.setMinValue(10); priceGroupRelnDTO.setMaxValue(-9999);
	 * priceGroupRelnDTO.setOperatorText("<"); priceGroupRelnDTO.setRetailType('U');
	 * String rangeMatch = "42.99";
	 * 
	 * range = priceGroupRelnDTO.getPriceRangeUnitCost(relatedItemPrice1,
	 * relatedItemSize, itemDTO.getItemSize(), relationType, sizeShelfPCT,
	 * itemDTO.getListCost(), RelatedItemListCost, itemDTO);
	 * 
	 * assertEquals(rangeMatch, String.valueOf(range.getEndVal()));
	 * 
	 * }
	 */

}
