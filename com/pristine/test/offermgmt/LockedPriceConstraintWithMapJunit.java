package com.pristine.test.offermgmt;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.constraint.LockedPriceConstraint;

public class LockedPriceConstraintWithMapJunit {

	// private static Logger logger =
	// Logger.getLogger("LockedPriceConstraintWithMapJunit");

	public static final Integer COST_NO_CHANGE = 0;
	public static final Integer COST_INCREASE = 1;
	public static final Integer COMP_STR_ID_TEST_967 = 967;
	public static final Integer ITEM_CODE_TEST_1234 = 1234;

	/**
	 * When item is on lockprice and there is a cost change locked price is below
	 * MAP then Map retail should be recommended with rounding applied
	 * 
	 * @throws Exception
	 */
	@Test
	public void TestLockedPricewithMapandCostChangeforItem() throws Exception {
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/20/2022", "10/04/2022", false, -1, -1,
				-1);

		TestHelper.setLocPriceConstraint(strategy, 1, 55.99);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 694.99, null, 226.76, 219.53, COST_INCREASE,
				COMP_STR_ID_TEST_967, 3.49, strategy, 92);
		item.setLockedRetail(700);
		item.getStrategyDTO().getGuidelines().setMarginGuideline(null);
		item.getStrategyDTO().getConstriants().setCostConstraint(null);

		item.setMapRetail(700);
		item.setObjectiveTypeId(15);

		LockedPriceConstraint locPriceConstraint = new LockedPriceConstraint(item, item.getExplainLog(), "TOPS");
		locPriceConstraint.applyLockedPriceConstraint("TRUE");

		assertEquals("Mismatch in recommendedPrice", (Double) 700.29,
				(Double) item.getRecommendedRegPrice().getUnitPrice());

	}

	@Test
	public void TestLockedPriceGreaterThanMapAndCostChangeforItem() throws Exception {
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/20/2022", "10/04/2022", false, -1, -1,
				-1);

		TestHelper.setLocPriceConstraint(strategy, 1, 709.00);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 729.99, null, 226.76, 219.53, COST_INCREASE,
				COMP_STR_ID_TEST_967, 3.49, strategy, 92);
		item.getStrategyDTO().getGuidelines().setMarginGuideline(null);
		item.getStrategyDTO().getConstriants().setCostConstraint(null);

		item.setMapRetail(709.00);
		item.setObjectiveTypeId(15);

		LockedPriceConstraint locPriceConstraint = new LockedPriceConstraint(item, item.getExplainLog(), "TOPS");
		locPriceConstraint.applyLockedPriceConstraint("TRUE");

		assertEquals("Mismatch in recommendedPrice", (Double) 709.0,
				(Double) item.getRecommendedRegPrice().getUnitPrice());

	}

	@Test
	public void TestLockedPriceGreaterThanMapAndNoCostChangeforItem() throws Exception {
		PRStrategyDTO strategy = TestHelper.getStrategy(1, 6, 66, 4, 264, "01/20/2022", "10/04/2022", false, -1, -1,
				-1);

		TestHelper.setLocPriceConstraint(strategy, 1, 55.00);
		TestHelper.setRoundingConstraint(strategy, TestHelper.getRoundingTableTable1());

		PRItemDTO item = TestHelper.getTestItem(ITEM_CODE_TEST_1234, 1, 729.99, null, 226.76, 219.53, 0,
				COMP_STR_ID_TEST_967, 3.49, strategy, 92);

		item.setMapRetail(709.19);
		item.setObjectiveTypeId(15);

		LockedPriceConstraint locPriceConstraint = new LockedPriceConstraint(item, item.getExplainLog(), "TOPS");
		locPriceConstraint.applyLockedPriceConstraint("TRUE");

		assertEquals("Mismatch in recommendedPrice", (Double) 709.29,
				(Double) item.getRecommendedRegPrice().getUnitPrice());

	}

}
