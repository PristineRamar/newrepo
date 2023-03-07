package com.pristine.util.offermgmt;

import java.text.DecimalFormat;

import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRRangeForLog;
import com.pristine.util.Constants;

public class PRFormatHelper {

	static DecimalFormat roundToTwoDecimalDigit = new DecimalFormat("#############.##");
	static DecimalFormat roundToOneDecimalDigit = new DecimalFormat("#############.#"); 
	static DecimalFormat twoDecimalDigit = new DecimalFormat("0.00");
	static DecimalFormat roundTofourDecimalDigit = new DecimalFormat("0.####");
	static DecimalFormat priceRangeFormatForLog = new DecimalFormat("#############.##"); 
	
	public static String doubleToTwoDigitString(double inputVal){
		String output = twoDecimalDigit.format(inputVal);
		return output;
	}
	
	public static String roundToTwoDecimalDigit(double inputVal){
		String output = roundToTwoDecimalDigit.format(inputVal);
		return output;
	}
	
	public static String roundToFourDecimalDigit(double inputVal){
		String output = roundTofourDecimalDigit.format(inputVal);
		return output;
	}
	
	public static double roundToTwoDecimalDigitAsDouble(double inputVal){
		double output = Double.valueOf(roundToTwoDecimalDigit.format(inputVal));
		return output;
	}
	
	public static double roundToOneDecimalDigitAsDouble(double inputVal){
		return Double.valueOf(roundToOneDecimalDigit.format(inputVal));
	}
	
	public static void formatRangeForLog(PRRange actualRange, PRRangeForLog rangeForLog){
		if(actualRange.getStartVal() == Constants.DEFAULT_NA)
			rangeForLog.setStartVal("-");
		else
			rangeForLog.setStartVal(priceRangeFormatForLog.format(actualRange.getStartVal()));
		
		if(actualRange.getEndVal() == Constants.DEFAULT_NA)
			rangeForLog.setEndVal("-");
		else
			rangeForLog.setEndVal(priceRangeFormatForLog.format(actualRange.getEndVal()));		 
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}
}
