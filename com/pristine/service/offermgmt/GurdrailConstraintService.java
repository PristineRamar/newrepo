package com.pristine.service.offermgmt;

import com.pristine.dto.offermgmt.PRRange;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class GurdrailConstraintService {

	public static void getGuardrailPctRange(String operatorText, double value, double compPrice, PRRange range) {
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			if (value != Constants.DEFAULT_NA) {

				if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
					range.setEndVal(compPrice + (compPrice * (value / 100)));
				} else
					range.setEndVal(compPrice + (compPrice * (value / 100)) + 0.01);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
			if (value != Constants.DEFAULT_NA) {
				
				if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText))
					range.setEndVal(compPrice - (compPrice * (value / 100)));
				else
					range.setEndVal(compPrice - (compPrice * (value / 100)) - 0.01);
				 
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			if (value != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice + (compPrice * (value / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			if (value != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice - (compPrice * (value / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_WITHIN.equals(operatorText)) {
			if (value != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (value / 100)));
				range.setEndVal(compPrice + (compPrice * (value / 100)));
			}
		}
	}

	public static void getGuardrailRange$(String operatorText, double value, double compPrice, PRRange range) {
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {

			if (value != Constants.DEFAULT_NA) {

				if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
					range.setEndVal(compPrice + value);
				} else
					range.setEndVal(compPrice + value + 0.01);

			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
			if (value != Constants.DEFAULT_NA) {

				if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText))
					range.setEndVal(compPrice - value);
				else
					range.setEndVal((compPrice - value) - 0.01);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			if (value != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice + value);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			if (value != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice - value);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_WITHIN.equals(operatorText)) {

			if (value != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - value);
				range.setEndVal(compPrice + value);
			}
		}
	}

	public static void getGuardrailRangeWithNoValueType(String operatorText, double compPrice, PRRange range) {
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)) {
			range.setEndVal((compPrice + 0.01));
		} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			range.setEndVal(compPrice);
		} else if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_EQUAL.equals(operatorText)) {
			range.setStartVal(compPrice);
			range.setEndVal(compPrice);
		} else if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			range.setEndVal((compPrice + 0.01));
			// range.setEndVal((compPrice + 0.01));
		} else if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)) {
			if (compPrice - 0.01 <= 0) {
				double pct = Double.parseDouble(PropertyManager.getProperty("PR_LESSER_PCT_DIFF", "1"));

				range.setEndVal(compPrice - (compPrice * pct / 100));

				// range.setEndVal(compPrice - (compPrice * pct / 100));
			} else {

				range.setEndVal((compPrice - 0.01));
				// range.setEndVal((compPrice - 0.01));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			range.setEndVal(compPrice);
		} else {
			range.setEndVal(compPrice);
		}
	}

}
