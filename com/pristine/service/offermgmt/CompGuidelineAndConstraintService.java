package com.pristine.service.offermgmt;

import com.pristine.dto.offermgmt.PRRange;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class CompGuidelineAndConstraintService {

	public static void getCompPricePctRange(String operatorText, double minValue, double maxValue, double compPrice,
			PRRange range) {
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + (compPrice * (minValue / 100)));
				range.setEndVal(compPrice + (compPrice * (maxValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + (compPrice * (minValue / 100)));
				if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText))
					range.setEndVal(compPrice + (compPrice * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice);
				if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText))
					range.setEndVal(compPrice + (compPrice * (maxValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (maxValue / 100)));
				range.setEndVal(compPrice - (compPrice * (minValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice - (compPrice * (minValue / 100)));
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
					range.setStartVal(compPrice - (compPrice * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice);
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
					range.setStartVal(compPrice - (compPrice * (maxValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + (compPrice * (minValue / 100)));
				range.setEndVal(compPrice + (compPrice * (maxValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice + (compPrice * (minValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (maxValue / 100)));
				range.setEndVal(compPrice - (compPrice * (minValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (minValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_WITHIN.equals(operatorText)) {
			// Only within X% is supported
			if ((minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) && (minValue == maxValue)) {
				range.setStartVal(compPrice - (compPrice * (maxValue / 100)));
				range.setEndVal(compPrice + (compPrice * (minValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (minValue / 100)));
				range.setEndVal(compPrice + (compPrice * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (maxValue / 100)));
				range.setEndVal(compPrice + (compPrice * (maxValue / 100)));
			}
		}
	}

	public static void getCompPriceRange$(String operatorText, double minValue, double maxValue, double compPrice,
			PRRange range) {
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + minValue);
				range.setEndVal(compPrice + maxValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + minValue);
				if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText))
					range.setEndVal(compPrice + minValue);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - maxValue);
				range.setEndVal(compPrice - minValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice - minValue);
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
					range.setStartVal(compPrice - minValue);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + minValue);
				range.setEndVal(compPrice + maxValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice + minValue);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - maxValue);
				range.setEndVal(compPrice - minValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - minValue);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_WITHIN.equals(operatorText)) {
			// Only within $5 is supported
			if ((minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) && (minValue == maxValue)) {
				range.setStartVal(compPrice - maxValue);
				range.setEndVal(compPrice + minValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - minValue);
				range.setEndVal(compPrice + minValue);
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - maxValue);
				range.setEndVal(compPrice + maxValue);
			}
		}
	}
	
	public static void getCompPriceRangeWithNoValueType(String operatorText, double compPrice, PRRange range) {
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)) {
			range.setStartVal((compPrice + 0.01));
		} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			range.setStartVal(compPrice);
		} else if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_EQUAL.equals(operatorText)) {
			range.setStartVal(compPrice);
			range.setEndVal(compPrice);
		} else if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			range.setStartVal((compPrice + 0.01));
			range.setEndVal((compPrice + 0.01));
		} else if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)) {
			if (compPrice - 0.01 <= 0) {
				double pct = Double.parseDouble(PropertyManager.getProperty("PR_LESSER_PCT_DIFF", "1"));
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
					range.setStartVal(compPrice - (compPrice * pct / 100));
				range.setEndVal(compPrice - (compPrice * pct / 100));
			} else {
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
					range.setStartVal((compPrice - 0.01));
				range.setEndVal((compPrice - 0.01));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			range.setEndVal(compPrice);
		}
	}
}
