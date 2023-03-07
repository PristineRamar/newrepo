package com.pristine.util.offermgmt;

import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRPriceGroupRelatedItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.ItemKey;
//import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;


public class PRCommonUtil {
	private static Logger logger = Logger.getLogger("PRCommonUtil");
	
	public static ItemKey getItemKey(PRItemDTO itemDTO) {
		ItemKey itemKey;
		if (itemDTO.isLir())
			itemKey = new ItemKey(itemDTO.getItemCode(), PRConstants.LIG_ITEM_INDICATOR);
		else
			itemKey = new ItemKey(itemDTO.getItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);

		return itemKey;
	}

	public static ItemKey getRelatedItemKey(PRPriceGroupRelatedItemDTO relatedItem) {
		ItemKey itemKey;
		if (relatedItem.getIsLig())
			itemKey = new ItemKey(relatedItem.getRelatedItemCode(), PRConstants.LIG_ITEM_INDICATOR);
		else
			itemKey = new ItemKey(relatedItem.getRelatedItemCode(), PRConstants.NON_LIG_ITEM_INDICATOR);

		return itemKey;
	}

	public static ItemKey getItemKey(int retLirIdOritemCode, boolean isLig) {
		ItemKey itemKey;
		if (isLig)
			itemKey = new ItemKey(retLirIdOritemCode, PRConstants.LIG_ITEM_INDICATOR);
		else
			itemKey = new ItemKey(retLirIdOritemCode, PRConstants.NON_LIG_ITEM_INDICATOR);

		return itemKey;
	}
	
	public static String getCommaSeperatedStringFromLongArray(List<Long> input) {
		String output = "";

		if (input != null && input.size() > 0) {
			for (Long d : input) {
				output = output + "," + d;
			}
			output = output.substring(1);
		}

		return output;
	}
	
	public static String getCommaSeperatedStringFromIntArray(List<Integer> input){
		String output = "";
		
		if(input != null && input.size() > 0) {
			for(Integer d : input){
				output = output + "," + d;
			}
			output = output.substring(1);
		}
		
		return output;
	}
	
	public static String getCommaSeperatedStringFromStrArray(List<String> input){
		String output = "";
		
		if(input != null && input.size() > 0) {
			for(String d : input){
				output = output + "," + d;
			}
			output = output.substring(1);
		}
		
		return output;
	}
	
	public static String getCommaSeperatedStringFromDoubleArray(List<Double> input) {
		String output = "";

		if (input != null && input.size() > 0) {
			for (Double d : input) {
				output = output + "," + d;
			}
			output = output.substring(1);
		}

		return output;
	}
	
	public static String getCommaSeperatedStringFromStrSet(Set<String> input){
		String output = "";
		
		if(input != null && input.size() > 0) {
			for(String d : input){
				output = output + "," + d;
			}
			output = output.substring(1);
		}
		
		return output;
	}
	
	public static String getCommaSeperatedStringFromIntSet(Set<Integer> input){
		String output = "";
		
		if(input != null && input.size() > 0) {
			for(Integer d : input){
				output = output + "," + d;
			}
			output = output.substring(1);
		}
		
		return output;
	}
	
	@SuppressWarnings("unchecked")
	public static String getCommaSeperatedStringFromString(Object input, boolean isAddDoubleCodes) {
		String output = "";
		if (input != null) {
			if (input instanceof HashSet<?>) {
				HashSet<String> convInput = (HashSet<String>) input;
				for (String d : convInput) {
					if(isAddDoubleCodes) {
						output = output + "," + '"' + d + '"';
					} else {
						output = output + "," + d;
					}
				}
			} else if (input instanceof List<?>) {
				List<String> convInput = (List<String>) input;
				for (String d : convInput) {
					if(isAddDoubleCodes) {
						output = output + "," + '"' + d + '"';
					} else {
						output = output + "," + d;
					}
				}
			} else if (input instanceof String[]) {
				String[] convInput = (String[]) input;
				for (String d : convInput) {
					if(isAddDoubleCodes) {
						output = output + "," + '"' + d + '"';
					} else {
						output = output + "," + d;
					}
				}
			}

			if (output != "") {
				output = output.substring(1);
			}
		}
		return output;
	}
	
	@SuppressWarnings("unchecked")
	public static String getCommaSeperatedStringFromInt(Object input){
		String output = "";
		if (input != null) {
			if (input instanceof HashSet<?>) {
				HashSet<Integer> convInput = (HashSet<Integer>) input;
				for (Integer d : convInput) {
					output = output + "," + d;
				}
			} else if (input instanceof List<?>) {
				List<Integer> convInput = (List<Integer>) input;
				for (Integer d : convInput) {
					output = output + "," + d;
				}
			} else if (input instanceof int[]) {
				int[] convInput = (int[]) input;
				for (Integer d : convInput) {
					output = output + "," + d;
				}
			} else if (input instanceof Integer[]) {
				int[] convInput = (int[]) input;
				for (Integer d : convInput) {
					output = output + "," + d;
				}
			}

			if (output != "") {
				output = output.substring(1);
			}
		}
		return output;
	}
	
	@SuppressWarnings("unchecked")
	public static String getCommaSeperatedStringFromDouble(Object input){
		String output = "";
		if (input != null) {
			if (input instanceof HashSet<?>) {
				HashSet<Double> convInput = (HashSet<Double>) input;
				for (Double d : convInput) {
					output = output + "," + d;
				}
			} else if (input instanceof List<?>) {
				List<Double> convInput = (List<Double>) input;
				for (Double d : convInput) {
					output = output + "," + d;
				}
			} else if (input instanceof double[]) {
				double[] convInput = (double[]) input;
				for (Double d : convInput) {
					output = output + "," + d;
				}
			} else if (input instanceof Double[]) {
				Double[] convInput = (Double[]) input;
				for (Double d : convInput) {
					output = output + "," + d;
				}
			}

			if (output != "") {
				output = output.substring(1);
			}
		}
		return output;
	}
	
	public static double getUnitPrice(Integer multiple, Double regPrice, Double regMPrice, boolean roundUnitPrice){
		double unitPrice = 0;
		Double tempPrice = null;
		
		//if multiple is null or 0, make as 1, otherwise the actual value
		multiple = ((multiple == null ? 1 : (multiple != null && multiple == 0) ? 1 : multiple));
		
		if(multiple > 1 && regMPrice != null && regMPrice > 0){
			if(roundUnitPrice)
				tempPrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(regMPrice / multiple));
			else
				tempPrice = Double.valueOf(regMPrice / multiple);
			unitPrice = (tempPrice != null ? tempPrice : 0);
		}else if (multiple > 1 && (regMPrice == null || regMPrice == 0)){
			//If multiple > 1, but there is no m price, then consider reg price as m price
			if(roundUnitPrice)
				tempPrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(regPrice / multiple));
			else
				tempPrice = Double.valueOf(regPrice / multiple);
			unitPrice = (tempPrice != null ? tempPrice : 0);
		}else {
			multiple = 1;
			unitPrice = (regPrice != null ? (roundUnitPrice? Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(regPrice)) : regPrice) : 0);
		}
		
		return unitPrice;
	}
	
	public static double getUnitPrice(MultiplePrice multiplePrice, boolean roundUnitPrice) {
		// if multiple is null or 0, make as 1, otherwise the actual value
		double unitPrice = 0;

		if (multiplePrice != null) {
			Integer multiple = (multiplePrice.multiple == null ? 1 : (multiplePrice.multiple == 0 ? 1
					: multiplePrice.multiple));
			Double price = (multiplePrice.price != null ? multiplePrice.price : 0);
			if(roundUnitPrice)
				unitPrice = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(price / multiple));
			else
				unitPrice = Double.valueOf(price / multiple);
		}
		return unitPrice;
	}
	
	public static MultiplePrice getMultiplePrice(Integer multiple, Double regPrice, Double regMPrice) {
		MultiplePrice multiplePrice = null;
		// Check if at least one of the price is present
		if ((regMPrice != null && regMPrice > 0) || (regPrice != null && regPrice > 0)) {
			Integer mul = (multiple == null ? 1 : (multiple == 0 ? 1 : multiple));
			Double price;
			if (mul > 1 && regMPrice != null && regMPrice > 0) {
				price = regMPrice;
			} else if (mul > 1 && (regMPrice == null || regMPrice == 0)){
				//If multiple > 1, but there is no m price, then consider reg price as m price
				price = regPrice;
			}
			else {
				mul = 1;
				price = (regPrice != null ? regPrice : 0);
			}
			multiplePrice = new MultiplePrice(mul, price);
		}
		return multiplePrice;
	}
	
	public static Double getMarginDollar(MultiplePrice multiplePrice, Double cost, Double predictedMov) {
		Double marginDollar = null;
		if (cost != null && cost >= 0 && predictedMov != null && predictedMov > 0) {
			double unitPrice = getUnitPrice(multiplePrice, true);
			marginDollar = (unitPrice * predictedMov) - (cost * predictedMov);
		}
		return marginDollar;
	}
	
	public static Double getSalesDollar(MultiplePrice multiplePrice, Double predictedMov) {
		Double salesDollar = null;
		if (multiplePrice != null && predictedMov != null && predictedMov > 0) {
			double unitPrice = getUnitPrice(multiplePrice, true);
			salesDollar = (unitPrice * predictedMov);
		}
		return salesDollar;
	}
	
	public static double getMarginPCT(MultiplePrice multiplePrice, Double cost, Double predictedMov) {
		double marginPCT = 0;
		if (cost != null && cost >= 0 && predictedMov != null && predictedMov > 0) {
			double unitPrice = getUnitPrice(multiplePrice, true);
			marginPCT = ((((unitPrice * predictedMov) - (cost * predictedMov)) / (unitPrice * predictedMov))) * 100;
		}
		return marginPCT;
	}

	public static double getMarginPCT(Double totalRevenue, Double totalCost) {
		double marginPCT = 0;
		if (totalRevenue > 0) {
			marginPCT = ((totalRevenue - totalCost) / (totalRevenue)) * 100;
		}
		return marginPCT;
	}
	
	public static double getDirectMarginPCT(Double totalRevenue, Double totalMargin) {
		double marginPCT = 0;
		if (totalRevenue > 0) {
			marginPCT = (totalMargin / (totalRevenue)) * 100;
		}
		return marginPCT;
	}
	
	public String integerAsString(String input) {
		String output;
		try {
			output = Integer.toString(Integer.parseInt(input));
		} catch (Exception e) {
			output = input.trim();

		}
		return output;
	}
	
	//Consider items only when it has both cur retail and cur cost when aggregating units or sales or margin at lig level
	public static boolean canConsiderItemForCalculation(MultiplePrice curRegMultiplePrice, Double cost, Integer predictionStatus) {
		boolean canConsider = false;
		double unitPrice = PRCommonUtil.getUnitPrice(curRegMultiplePrice, true);
	//commenting condition to check the prediction status for audit calulcation on 05/30/22
		if (unitPrice > 0
		/*
		 * && (predictionStatus != null && (predictionStatus ==
		 * PredictionStatus.SUCCESS.getStatusCode() || predictionStatus ==
		 * PredictionStatus.ERROR_NO_MOV_DATA_ANY_UPC.getStatusCode() ||
		 * predictionStatus ==
		 * PredictionStatus.ERROR_NO_MOV_DATA_SPECIFIC_UPC.getStatusCode() ||
		 * predictionStatus == PredictionStatus.ERROR_NO_SUBST_IMPACT.getStatusCode() ||
		 * predictionStatus == PredictionStatus.SHIPPER_ITEM.getStatusCode()))
		 */)
			canConsider = true;
		return canConsider;
	}
	
	public static String getWeeklyAdLocationQuery(int chainId, int locationLevelId, int locationId) {
		StringBuilder sb = new StringBuilder();
		// Check weekly ad is defined at chain level
		sb.append(" ((WAD.LOCATION_ID = " + chainId + "AND WAD.LOCATION_LEVEL_ID = " + Constants.CHAIN_LEVEL_ID + ") ");

		if (locationLevelId == Constants.STORE_LEVEL_ID) {
			// Check weekly ad is defined at zone list level
			sb.append(" OR (WAD.LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE  CHILD_LOCATION_LEVEL_ID = "
					+ Constants.ZONE_LEVEL_ID + " AND ");
			sb.append(" CHILD_LOCATION_ID = (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = " + locationId + "))");
			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + Constants.ZONE_LIST_LEVEL_ID + ") ");

			// Check weekly ad is defined at store list level
			sb.append(" OR (WAD.LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_GROUP_RELATION ");
			sb.append(" WHERE CHILD_LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ") ");
			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");

			// Check weekly ad is defined at Zone level
			sb.append(" OR (WAD.LOCATION_ID IN (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = " + locationId + ") ");
			sb.append(" AND WAD.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + ") ");

			// Check weekly ad is defined at store level
			sb.append(" OR (WAD.LOCATION_ID = " + locationId + " AND WAD.LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")");
		} else if (locationLevelId == Constants.STORE_LIST_LEVEL_ID) {
			// Check weekly ad is defined at store list level
			sb.append(" OR (WAD.LOCATION_LEVEL_ID = " + locationLevelId + " AND WAD.LOCATION_ID = " + locationId + ")");

			// Check weekly ad is defined at store level
			sb.append(" OR (WAD.LOCATION_LEVEL_ID  = " + Constants.STORE_LEVEL_ID + " AND WAD.LOCATION_ID IN ");
			sb.append(" (SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + "))");

			// Check weekly ad is defined at zone level
			sb.append(" OR (WAD.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + " AND WAD.LOCATION_ID IN ");
			sb.append(" (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID IN (SELECT CHILD_LOCATION_ID ");
			sb.append(" FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND LOCATION_ID = " + locationId);
			sb.append(" AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")))");
		}
		sb.append(" ) ");
		
		return sb.toString();
	}
	
	public static String getPromotionLocationQuery(int chainId, int locationLevelId, int locationId) {
		StringBuilder sb = new StringBuilder();
		sb.append(" ((PL.LOCATION_ID = " + chainId + "AND PL.LOCATION_LEVEL_ID = " + Constants.CHAIN_LEVEL_ID + ") ");

		if (locationLevelId == Constants.STORE_LEVEL_ID) {
			// Check if defined at store level
			sb.append(" OR (PL.LOCATION_ID = " + locationId + " AND PL.LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")");

			// Check if defined at store list level
			sb.append(" OR (PL.LOCATION_ID IN (SELECT LOCATION_ID FROM LOCATION_GROUP_RELATION ");
			sb.append(" WHERE CHILD_LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ") ");
			sb.append(" AND PL.LOCATION_LEVEL_ID = " + Constants.STORE_LIST_LEVEL_ID + ") ");

			// Check if defined at Zone level
			sb.append(" OR (PL.LOCATION_ID IN (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID = " + locationId + ") ");
			sb.append(" AND PL.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + ") ");
			
		} else if (locationLevelId == Constants.STORE_LIST_LEVEL_ID) {
			// Check if defined at store list level
			sb.append(" OR (PL.LOCATION_LEVEL_ID = " + locationLevelId + " AND PL.LOCATION_ID = " + locationId + ")");

			// Check is defined at store level
			sb.append(" OR (PL.LOCATION_LEVEL_ID  = " + Constants.STORE_LEVEL_ID + " AND PL.LOCATION_ID IN ");
			sb.append(" (SELECT CHILD_LOCATION_ID FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND LOCATION_ID = " + locationId + " AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + "))");

			// Check if defined at zone level
			sb.append(" OR (PL.LOCATION_LEVEL_ID = " + Constants.ZONE_LEVEL_ID + " AND PL.LOCATION_ID IN ");
			sb.append(" (SELECT PRICE_ZONE_ID FROM COMPETITOR_STORE WHERE COMP_STR_ID IN (SELECT CHILD_LOCATION_ID ");
			sb.append(" FROM LOCATION_GROUP_RELATION WHERE LOCATION_LEVEL_ID = " + locationLevelId);
			sb.append(" AND LOCATION_ID = " + locationId);
			sb.append(" AND CHILD_LOCATION_LEVEL_ID = " + Constants.STORE_LEVEL_ID + ")))");
		}
		sb.append(" ) ");
		return sb.toString();
	}
	
	//Write lig level details
	public static void logLigDetails(List<PRItemDTO> recommendedItems) {
		logger.debug("********* LIG Details ****************");
		for (PRItemDTO itemDTO : recommendedItems) {
			if (itemDTO.isLir()) {
				logger.debug(itemDTO.customToString1());
			}
		}
		logger.debug("********* LIG Details ****************");
	}
	
	public static String getPriceForLog(MultiplePrice multiplePrice) {
		String log = "";
		log = multiplePrice != null ? multiplePrice.toString() : "/";
		return log;
	}
	
	public static MultiplePrice getCurRegPrice(PRItemDTO itemDTO) {
		MultiplePrice curRegPrice = PRCommonUtil.getMultiplePrice(itemDTO.getRegMPack(), itemDTO.getRegPrice(), itemDTO.getRegMPrice());
		return curRegPrice;
	}
	
	public static void setLog4jProperties(String logFileName) throws GeneralException {
		String logTypes = PropertyManager.getProperty("log4j.rootLogger");
		String appender = PropertyManager.getProperty("log4j.appender.logFile");
		String logPath = PropertyManager.getProperty("log4j.appender.logFile.File");
		String maxFileSize = PropertyManager.getProperty("log4j.appender.logFile.MaxFileSize");
		String patternLayout = PropertyManager.getProperty("log4j.appender.logFile.layout");
		String conversionPattern = PropertyManager.getProperty("log4j.appender.logFile.layout.ConversionPattern");
		
		String appenderConsole = PropertyManager.getProperty("log4j.appender.console");
		String appenderConsoleLayout = PropertyManager.getProperty("log4j.appender.console.layout");
		String appenderConsoleLayoutPattern = PropertyManager.getProperty("log4j.appender.console.layout.ConversionPattern");
		
		logPath = logPath + "/" + logFileName + ".log";

		Properties props = new Properties();
		props.setProperty("log4j.rootLogger", logTypes);
		props.setProperty("log4j.appender.logFile", appender);
		props.setProperty("log4j.appender.logFile.File", logPath);
		props.setProperty("log4j.appender.logFile.MaxFileSize", maxFileSize);
		props.setProperty("log4j.appender.logFile.layout", patternLayout);
		props.setProperty("log4j.appender.logFile.layout.ConversionPattern", conversionPattern);
		
		props.setProperty("log4j.appender.console", appenderConsole);
		props.setProperty("log4j.appender.console.layout", appenderConsoleLayout);
		props.setProperty("log4j.appender.console.layout.ConversionPattern", appenderConsoleLayoutPattern);
		PropertyConfigurator.configure(props);
	}
	
	
	public static List<RetailCalendarDTO> getPreviousCalendars(HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, String weekStartDate,
			int noOfWeeksHistory) throws GeneralException {
		List<RetailCalendarDTO> calendars = new ArrayList<RetailCalendarDTO>();
		Date latestWeekWithPriceStartDate = DateUtil.toDate(weekStartDate);
		for (int i = 0; i < noOfWeeksHistory; i++) {
			String tempWeekStartDate = DateUtil.dateToString(DateUtil.incrementDate(latestWeekWithPriceStartDate, -(7 * (i))),
					Constants.APP_DATE_FORMAT);
			if (allWeekCalendarDetails.get(tempWeekStartDate) != null) {
				RetailCalendarDTO retailCalendarDTO = allWeekCalendarDetails.get(tempWeekStartDate);
				calendars.add(retailCalendarDTO);
			}
		}
		return calendars;
	}
	
	public static double getLiftPCT(double baseNumber, double compareTo) {
		double liftPCT = 0;
		if (baseNumber != 0) {
			liftPCT = ((compareTo - baseNumber) / baseNumber) * 100;
		}

		return liftPCT;
	}
	
	public static long roundDouble(double input) {
		return Math.round(input);
	}
	
	public static double getSaleDiscountPCT(MultiplePrice regPrice, MultiplePrice salePrice, boolean roundToTwoDecimal) {
		double saleDiscountPCT = 0;
		double regUnitPrice = PRCommonUtil.getUnitPrice(regPrice, true);
		double saleUnitPrice = salePrice != null ? PRCommonUtil.getUnitPrice(salePrice, true) : 0;

		if (regUnitPrice > 0 && saleUnitPrice > 0) {
			if (roundToTwoDecimal) {
				saleDiscountPCT = Double.valueOf(PRFormatHelper.roundToTwoDecimalDigit(((regUnitPrice - saleUnitPrice) / regUnitPrice) * 100));
			} else {
				saleDiscountPCT = ((regUnitPrice - saleUnitPrice) / regUnitPrice) * 100;
			}
		}
		
		return saleDiscountPCT;
	}
	
	
	/**
	 * 
	 * @return date formatter
	 */
	public static DateTimeFormatter getDateFormatter(){
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
		return formatter;
	}
	
	public static double round(double value, int places) {
	    if (places < 0) throw new IllegalArgumentException();

	    long factor = (long) Math.pow(10, places);
	    value = value * factor;
	    long tmp = Math.round(value);
	    return (double) tmp / factor;
	}
	


	public static List<RetailCalendarDTO> getFutureCalendars(HashMap<String, RetailCalendarDTO> allWeekCalendarDetails,
			String weekStartDate, int noOfWeeksHistory)throws GeneralException {
		List<RetailCalendarDTO> calendars = new ArrayList<RetailCalendarDTO>();
		Date latestWeekWithPriceStartDate = DateUtil.toDate(weekStartDate);
		for (int i = 1; i <= noOfWeeksHistory; i++) {
			String tempWeekStartDate = DateUtil.dateToString(DateUtil.incrementDate(latestWeekWithPriceStartDate, +(7 * (i))),
					Constants.APP_DATE_FORMAT);
			if (allWeekCalendarDetails.get(tempWeekStartDate) != null) {
				RetailCalendarDTO retailCalendarDTO = allWeekCalendarDetails.get(tempWeekStartDate);
				calendars.add(retailCalendarDTO);
			}
		}
		return calendars;
	}
	
	public static String getCommaSeperatedStringFromLongSet(Set<Long> input){
		String output = "";
		
		if(input != null && input.size() > 0) {
			for(Long d : input){
				output = output + "," + d;
			}
			output = output.substring(1);
		}
		
		return output;
	}
	
	public static DecimalFormat getTwoDForm() {

		return new DecimalFormat("##########.##");
	}
	
}
