package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRPriceGroupRelnDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class SizeRelationJUnitTest {
	PRPriceGroupRelnDTO priceGroupRelnDTO = null;
	
	@Before
	public void init() {
		PropertyManager.initialize("com/pristine/test/offermgmt/AllClients.properties");
		priceGroupRelnDTO = new PRPriceGroupRelnDTO();
		priceGroupRelnDTO.setOperatorText(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM);
		priceGroupRelnDTO.setRetailType(PRConstants.RETAIL_TYPE_UNIT);
	}
	
	/***
	 * Both item in size relation has same size
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCase1() throws Exception {
		
		PRRange range = priceGroupRelnDTO.getSizePriceRangeWithNoValueType(new MultiplePrice(1, 7.99), 32, 32, 0);
		
		PRRange expRange = new PRRange();
		
		expRange.setStartVal(7.99);
		expRange.setEndVal(7.99);
		
		assertEquals("Mismatch", expRange.toString(), range.toString());
	}
	
	/***
	 * Size related items sizes are narrow and there must be minimum 1 cent difference
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCase2() throws Exception {
		
		PRRange range = priceGroupRelnDTO.getSizePriceRangeWithNoValueType(new MultiplePrice(1, 2.99), 40, 29, 0);
		
		PRRange expRange = new PRRange();
		
		expRange.setStartVal(2.19);
		expRange.setEndVal(2.98);
		
		assertEquals("Mismatch", expRange.toString(), range.toString());
	}
	
	/***
	 *  
	 * 
	 * @throws Exception
	 */
	@Test
	public void testCase3() throws Exception {
		
		PRRange range = priceGroupRelnDTO.getSizePriceRangeWithNoValueType(new MultiplePrice(1, 1.99), 29, 40, 0);
		
		PRRange expRange = new PRRange();
		
		expRange.setStartVal(2);
		expRange.setEndVal(2.72);
		
		assertEquals("Mismatch", expRange.toString(), range.toString());
	}
	
	@Test
	public void testCase4() throws Exception {
		
		PRRange range = priceGroupRelnDTO.getSizePriceRangeWithNoValueType(new MultiplePrice(1, 8.59), 80, 38, 0);
		
		PRRange expRange = new PRRange();
		
		expRange.setStartVal(4.12);
		expRange.setEndVal(8.58);
		
		assertEquals("Mismatch", expRange.toString(), range.toString());
	}
	
	@Test
	public void testCase5() throws Exception {
		
		PRRange range = priceGroupRelnDTO.getSizePriceRangeWithNoValueType(new MultiplePrice(1, 9.59), 25.3, 16.9, 0);
		
		PRRange expRange = new PRRange();
		
		
		if(range.getStartVal() != Constants.DEFAULT_NA)
			range.setStartVal(PRFormatHelper.roundToTwoDecimalDigitAsDouble(range.getStartVal()));
		if(range.getEndVal() != Constants.DEFAULT_NA)
			range.setEndVal(PRFormatHelper.roundToTwoDecimalDigitAsDouble(range.getEndVal()));
		
		PRRange nwRange = range.getRange(range);
		
		expRange.setStartVal(6.47);
		expRange.setEndVal(9.58);
		
		assertEquals("Mismatch", expRange.toString(), nwRange.toString());
	}
}
