package com.pristine.dto.offermgmt;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.service.offermgmt.guideline.BrandGuideline;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
//import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

public class PRConstraintRounding  implements  Cloneable{
	private static Logger logger = Logger.getLogger("PRConstraintRounding");
//	private String roundingDigitLogic = PropertyManager.getProperty("PR_COST_CHANGE_ROUNDING_DIGIT_BEHAVIOR", "");
	
	private long cRoundingId;
	private long cId;
	private int[] roundingDigits;
	private char next;
	private char nearest;
	private char noRounding;
	private int roundingTableId;
	private TreeMap<String, PRRoundingTableDTO> roundingTableContent = null;
	
	public long getcRoundingId() {
		return cRoundingId;
	}
	public void setcRoundingId(long cRoundingId) {
		this.cRoundingId = cRoundingId;
	}
	public long getcId() {
		return cId;
	}
	public void setcId(long cId) {
		this.cId = cId;
	}
	public int[] getRoundingDigits() {
		return roundingDigits;
	}
	public void setRoundingDigits(int[] roundingDigits) {
		this.roundingDigits = roundingDigits;
	}
	public char getNext() {
		return next;
	}
	public void setNext(char next) {
		this.next = next;
	}
	public char getNearest() {
		return nearest;
	}
	public void setNearest(char nearest) {
		this.nearest = nearest;
	}
	public char getNoRounding() {
		return noRounding;
	}
	public void setNoRounding(char noRounding) {
		this.noRounding = noRounding;
	}
	public int getRoundingTableId() {
		return roundingTableId;
	}
	public void setRoundingTableId(int roundingTableId) {
		this.roundingTableId = roundingTableId;
	}
	public TreeMap<String, PRRoundingTableDTO> getRoundingTableContent() {
		return roundingTableContent;
	}
	public void setRoundingTableContent(
			TreeMap<String, PRRoundingTableDTO> roundingTableContent) {
		this.roundingTableContent = roundingTableContent;
	}
	

	/**
	 * 
	 * @param itemInfo
	 * @param inputRange
	 * @param explainLog
	 * @return rounding digits
	 */
	public Double[] getRoundingDigits(PRItemDTO itemInfo, PRRange inputRange, PRExplainLog explainLog) {
		String roundingConfig = PropertyManager.getProperty("ROUNDING_LOGIC", PRConstants.ROUND_CLOSEST);

		return getRoundingDigits(itemInfo, inputRange, explainLog, roundingConfig);
	}
	
	
	/**
	 * 
	 * @param itemInfo
	 * @param inputRange
	 * @param explainLog
	 * @param roundingConfig
	 * @return rounding digits by rounding logic
	 */
	public Double[] getRoundingDigits(PRItemDTO itemInfo, PRRange inputRange, PRExplainLog explainLog,
			String roundingConfig) {
		double[] newRangeArr = null;
		Double[] priceRangeArr = null;
		List<Double> roundingDigits = new ArrayList<Double>();		
		PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.ROUNDING.getConstraintTypeId());
		FilterPriceRange filterPriceRange = new FilterPriceRange();	
//		boolean isPrePrice = filterPriceRange.checkIfConstraintApplied(explainLog, ConstraintTypeLookup.PRE_PRICE.getConstraintTypeId());
//		boolean isLockedPrice = filterPriceRange.checkIfConstraintApplied(explainLog, ConstraintTypeLookup.LOCKED_PRICE.getConstraintTypeId());
				
		String roundingLogic = PRConstants.ROUND_UP;
		if(itemInfo.getRoundingLogic() != "")
			roundingLogic = itemInfo.getRoundingLogic();
		
		if (filterPriceRange.checkIfConstraintApplied(explainLog, ConstraintTypeLookup.MIN_MAX.getConstraintTypeId())) {
			PRRange minMaxRange = filterPriceRange.getConstraintRange(explainLog, ConstraintTypeLookup.MIN_MAX.getConstraintTypeId());
			
			//Find round up/round down 
			itemInfo.setRoundingLogic(filterPriceRange.findRoudingLogic(inputRange, minMaxRange));
			
			newRangeArr = getRange(inputRange.getStartVal(), inputRange.getEndVal(), itemInfo.getRoundingLogic());
			
			//if the min/max range is too short, rounding up/down may result in breaking of min/max e.g. 2.13 - 2.15
			// Check if the rounding digits is within the min/max
			List<Double> newRangeList = filterPriceRange.getRoundingDigitsWithinRange(newRangeArr, minMaxRange);	
			
			// If no rounding digit is within range, then use current retail
			if(newRangeList.size() == 0){
				if(itemInfo.getRegPrice() != null){
					newRangeList.add(itemInfo.getRegPrice());
				}
			}
			// If there is no current retail, then round up
			if(newRangeList.size() == 0){
				newRangeArr = getRange(minMaxRange.getStartVal(), minMaxRange.getEndVal(), PRConstants.ROUND_UP);
			}else{
				double[] newRangeArr1 = new double[newRangeList.size()];
				int counter = 0;
				for(Double price : newRangeList){
					newRangeArr1[counter] = price;
					counter++;
				}
				newRangeArr = newRangeArr1;
			}
		}
		else if(itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND)) {
			BrandGuideline brandGuideline = new BrandGuideline();
			double range[] = brandGuideline.getRangeWithinBrandRange(inputRange, itemInfo.getStoreBrandRelationRange(), 
					itemInfo.getStrategyDTO().getConstriants().getRoundingConstraint());
			if(range.length == 0) 
				newRangeArr = getRange(inputRange.getStartVal(), inputRange.getEndVal(), roundingLogic);
			else
				newRangeArr = range;
		}else {
			double[] roundingDigitsWithinTheRange = getRange(inputRange.getStartVal(), inputRange.getEndVal());
			boolean isBreakingConstraint = filterPriceRange.isRangeBreakingConstraint(explainLog);
			//If Ahold cost changed or final range has at least one rounding digit or the range
			//break any constraints or pre price or locked price
			/*if (itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN) || 
					roundingDigitsWithinTheRange.length > 1 || isPrePrice || isLockedPrice || isBreakingConstraint) {
				newRangeArr = getRange(inputRange.getStartVal(), inputRange.getEndVal(), roundingLogic);
			}*/
			if (itemInfo.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN) || isBreakingConstraint) {
				newRangeArr = getRange(inputRange.getStartVal(), inputRange.getEndVal(), roundingLogic);
			}  else if (roundingDigitsWithinTheRange.length > 1) {
				newRangeArr = getRange(inputRange.getStartVal(), inputRange.getEndVal());
			} else {
				double lowerPricePoint = 0, higherPricePoint = 0, closestPricePoint = 0, otherPricePoint = 0;
				//Added on 23rd June 2015, to handle when final range doesn't have any rounding digit in-between
				/**
				 * 1) First guideline will always decide the price range to start with 
				 * and depending on the priority of other guidelines the price range will get narrow.
				 * 2) If price range is so narrow that there is no rounding digit in between them, 
				 * you have a choice to pick from 2 price points which are closer to the final price range.
				 * 3) You can pick the price point which is closer to the current retail. If this price point happens to 
				 * break the first guideline, pick another one.
				 */
				// Find both price point (round up and round down)
				double[] lowerPricePointArray = getRange(inputRange.getStartVal(), inputRange.getEndVal(), PRConstants.ROUND_DOWN);
				double[] higherPricePointArray = getRange(inputRange.getStartVal(), inputRange.getEndVal(), PRConstants.ROUND_UP);
				lowerPricePoint = lowerPricePointArray.length > 0 ? lowerPricePointArray[0] : lowerPricePoint;
				higherPricePoint = higherPricePointArray.length > 0 ? higherPricePointArray[0] : higherPricePoint;
				
				if(roundingConfig.equals(PRConstants.ROUND_UP)) {
					newRangeArr = new double[1];
					newRangeArr[0] = higherPricePoint;
				} else if (roundingConfig.equals(PRConstants.ROUND_DOWN)) {
					newRangeArr = new double[1];
					newRangeArr[0] = lowerPricePoint;
				} else {
					if(lowerPricePoint == 0 && higherPricePoint > 0) {
						newRangeArr = new double[1];
						newRangeArr[0] = higherPricePoint;
					} if(higherPricePoint == 0 && lowerPricePoint > 0) {
						newRangeArr = new double[1];
						newRangeArr[0] = lowerPricePoint;
					} else {
						ClosestAndOtherPricePoints closestAndOtherPP = getClosestPPToFinalRange(lowerPricePoint, higherPricePoint, inputRange);
						PRRange firstAvailableRange = filterPriceRange.getFirstAvailableGuidelineRange(explainLog);
						if(firstAvailableRange == null){
							newRangeArr = new double[1];
							newRangeArr[0] = closestAndOtherPP.getClosestPP();
							if(closestAndOtherPP.getClosestPP() == 0 && closestAndOtherPP.getOtherPP() > 0) {
								newRangeArr[0] = closestAndOtherPP.getOtherPP();	
							}
						} else {
							// Check if that price point doesn't break the first available guideline
							// If it breaks, then pick another price point
							List<Double> closerPPWithinRange = filterPriceRange.getRoundingDigitsWithinRange(new double[] { closestAndOtherPP.getClosestPP() },
									firstAvailableRange);
							List<Double> otherPPWithinRange = filterPriceRange.getRoundingDigitsWithinRange(new double[] { closestAndOtherPP.getOtherPP() },
									firstAvailableRange);
							// If other price points within the range
							if(closerPPWithinRange.size() == 0 && otherPPWithinRange.size() > 0){
								newRangeArr = new double[1];
								newRangeArr[0] = closestAndOtherPP.getOtherPP();
							} 
							else {
								newRangeArr = new double[1];
								newRangeArr[0] = closestAndOtherPP.getClosestPP();
							}
						}
					}
				}
			}				
		}
		
		Arrays.sort(newRangeArr);
		priceRangeArr = new Double[newRangeArr.length];
		
		/*// Added for handling AutoZone 90% threshold
		priceRangeArr = filterPricePointsXNos(itemInfo, priceRangeArr);*/
		int counter = 0;
		for(double price : newRangeArr){
			//changes done by Bhargavi on 9/24/2021
			//Update to fix the retail price = 0
			if(price == 0.0)
				priceRangeArr[counter] = inputRange.getStartVal();
			else
				priceRangeArr[counter] = price;
			//changes ended
			counter++;
		}			
		
		int noOfPricePointsToBeFiltered = Integer
				.parseInt(PropertyManager.getProperty("PRICE_POINTS_FILTER_RANGE", "0"));
		priceRangeArr = filterPricePointsXNos(itemInfo, priceRangeArr, noOfPricePointsToBeFiltered);
		
		if(counter > priceRangeArr.length) {
			itemInfo.setPricePointsFiltered(noOfPricePointsToBeFiltered);
		}
		
		Arrays.sort(priceRangeArr);
		for(double price : priceRangeArr){
			roundingDigits.add(price);
		}	
		
		guidelineAndConstraintLog.setRoundingDigits(roundingDigits);
		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
//		logger.debug("Rounding Constraint -- " + "Input Range: " + inputRange.toString() +
//				",Rounding Digits: " + guidelineAndConstraintLog.getRoundingDigits());  
				 
		return priceRangeArr;
	}
	
	
	/**
	 * 
	 * @param itemDTO
	 * @param priceRange
	 * @return filtered price points by X numbers. specifc to AutoZone. To handle 90% threshold
	 */
	private Double[] filterPricePointsXNos(PRItemDTO itemDTO, Double[] priceRange, int noOfPricePointsToBeFiltered) {
		Double[] newPriceRange = null;
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(itemDTO);
		if (noOfPricePointsToBeFiltered == 0 || curRegPrice == null) {
			return priceRange;
		} else {
			List<Double> allPricePoints = new ArrayList<>();
			List<Double> filteredPricePoints = new ArrayList<>();
			boolean isCurrentPricePartOfPricePoints = false;
			for (Double pricePoint : priceRange) {
				if(pricePoint != null) {
					allPricePoints.add(pricePoint);
					if (pricePoint == curRegPrice.getUnitPrice()) {
						isCurrentPricePartOfPricePoints = true;
					}	
				}
			}

			List<Double> pricePointsAboveCurrentPrice = allPricePoints.stream()
					.filter(p -> p > curRegPrice.getUnitPrice()).collect(Collectors.toList());
			List<Double> pricePointsBelowCurrentPrice = allPricePoints.stream()
					.filter(p -> p < curRegPrice.getUnitPrice()).collect(Collectors.toList());

			Collections.sort(pricePointsAboveCurrentPrice);
			Collections.reverse(pricePointsBelowCurrentPrice);

			for (int i = 0; i < pricePointsAboveCurrentPrice.size(); i++) {
				Double price = pricePointsAboveCurrentPrice.get(i);
				if (i < noOfPricePointsToBeFiltered) {
					filteredPricePoints.add(price);
				} else {
					break;
				}
			}

			for (int i = 0; i < pricePointsBelowCurrentPrice.size(); i++) {
				Double price = pricePointsBelowCurrentPrice.get(i);
				if (i < noOfPricePointsToBeFiltered) {
					filteredPricePoints.add(price);
				} else {
					break;
				}
			}

			if (isCurrentPricePartOfPricePoints) {
				filteredPricePoints.add(curRegPrice.getUnitPrice());
			}

			newPriceRange = filteredPricePoints.toArray(new Double[filteredPricePoints.size()]);
			

		}
		return newPriceRange;
	}
	
	private ClosestAndOtherPricePoints getClosestPPToFinalRange(double lowerPricePoint, double higherPricePoint, PRRange inputRange) {
		
		double diffInLowerPP = 0, diffInHigherPP = 0, closestPP = 0, otherPP = 0;
		if(inputRange.getStartVal() != Constants.DEFAULT_NA && inputRange.getEndVal() != Constants.DEFAULT_NA) {
			diffInLowerPP = (lowerPricePoint>0)? Math.abs((inputRange.getStartVal() - lowerPricePoint)) : 0;
			diffInHigherPP = (higherPricePoint > 0)? Math.abs((higherPricePoint - inputRange.getEndVal())) : 0;
			
			// LP = 1, HP = 0.5 = HP
			// LP = 0.5, HP = 1 = LP
			// LP = 0.5, HP = 0.5 = HP
			
			if(diffInLowerPP < diffInHigherPP) {
				closestPP = lowerPricePoint;
				otherPP = higherPricePoint;
			}else {
				closestPP =  higherPricePoint;
				otherPP = lowerPricePoint;
			}
		}
		
		return new ClosestAndOtherPricePoints(closestPP, otherPP);
	}
	/**
	 * Returns Range of numbers between startVal and endVal
	 * @param startVal
	 * @param endVal
	 * @return
	 */
	public double[] getRange(double startVal, double endVal, String roundingLogicWhenNoPricePoint){
		
//		long startTime = System.currentTimeMillis();
		List<Double> newRangeList = new ArrayList<Double>();
		
		//If both the ends are zero/not available or end digit alone is not available, always round up
		if((startVal == 0 && endVal == 0) || (startVal == Constants.DEFAULT_NA && endVal == Constants.DEFAULT_NA) ||
				(startVal != Constants.DEFAULT_NA && endVal == Constants.DEFAULT_NA)) {
			if(startVal == Constants.DEFAULT_NA && endVal == Constants.DEFAULT_NA){
				startVal = 0;
				endVal = 0;
			}
				
			double[] priceRange;
			priceRange = roundupPriceToNextRoundingDigit(startVal, endVal);
			if(priceRange.length > 0)
				newRangeList.add(priceRange[0]);
		}else if(roundingDigits != null && roundingDigits.length > 0){
			newRangeList = getRangeFromRoundingDigits(startVal, endVal);
		}else if(roundingTableId > 0 || roundingTableContent.size() > 0){
			newRangeList = getRangeFromRoundingTable(startVal, endVal, roundingLogicWhenNoPricePoint);
//			if(newRangeList == null || newRangeList.size() == 0){
//				double[] priceRange;				
//				if(roundingLogicWhenNoPricePoint.toUpperCase().equals(PRConstants.ROUND_UP)){
//					priceRange = roundupPriceToNextRoundingDigit(startVal, endVal);
//					if(priceRange.length > 0)
//						newRangeList.add(priceRange[0]);
//				}else if(roundingLogicWhenNoPricePoint.toUpperCase().equals(PRConstants.ROUND_DOWN)){
//					priceRange = roundupPriceToPreviousRoundingDigit(startVal, endVal);
//					if(priceRange.length > 0)
//						newRangeList.add(priceRange[0]);
//				}
//				else{
//					priceRange = getClosestPriceRange(startVal, endVal);
//					for(double price : priceRange){
//						newRangeList.add(price);
//					}
//				}				
//			}
		}else
			newRangeList = getFullRange(startVal, endVal);
		
		Collections.sort(newRangeList);
		double[] newRangeArr = new double[newRangeList.size()];
		int counter = 0;
		for(Double price : newRangeList){
			newRangeArr[counter] = price;
			counter++;
		}
//		long endTime = System.currentTimeMillis();
		//logger.debug("Time taken to retrieve range from rounding constraint - " + (endTime - startTime) + "ms");
		return newRangeArr;
	}
	
	//Return price points between the startVal and endVal, if no price points found return empty array 
	public double[] getRange(double startVal, double endVal) {
		double[] newRangeArr;		 
		List<Double> newRangeList = new ArrayList<Double>();

		// If both the ends are zero/not set, always round up
		if((startVal == 0 && endVal == 0) || (startVal == Constants.DEFAULT_NA && endVal == Constants.DEFAULT_NA)) {
			if(startVal == Constants.DEFAULT_NA && endVal == Constants.DEFAULT_NA){
				startVal = 0;
				endVal = 0;
			}
			double[] priceRange;
			priceRange = roundupPriceToNextRoundingDigit(startVal, endVal);
			if (priceRange.length > 0)
				newRangeList.add(priceRange[0]);
			
//			logger.debug("getRange() - rounding to next digit");
		} else if (roundingDigits != null && roundingDigits.length > 0){
//			logger.debug("getRange() - rounding using digits " + roundingDigits.length);
			newRangeList = getRangeFromRoundingDigits(startVal, endVal);
		}
		else if (roundingTableId > 0 || roundingTableContent.size() > 0) {
			//logger.debug("getRange() - rounding using table " + roundingTableId);
			newRangeList = getRangeFromRoundingTable(startVal, endVal, "");
		} else {
//			logger.debug("getRange() - full range " + roundingTableId);
			newRangeList = getFullRange(startVal, endVal);
		}

		if (newRangeList == null || newRangeList.size() == 0) {
			newRangeArr = new double[0];			
		}else {
			Collections.sort(newRangeList);			
		}
		
		newRangeArr = new double[newRangeList.size()];
		int counter = 0;
		for (Double price : newRangeList) {
			newRangeArr[counter] = price;
			counter++;
		}
		return newRangeArr;
	}
	
	/**
	 * Returns Range of numbers between startVal and endVal that ends with the roundingDigits
	 * @param startVal
	 * @param endVal
	 * @return
	 */
	private ArrayList<Double> getRangeFromRoundingDigits(double startVal, double endVal){
//		logger.info("getrangefromroundingdigits " + startVal + "\t" + endVal);
		ArrayList<Double> newRangeList = new ArrayList<Double>();
		DecimalFormat dFormat = new DecimalFormat("#########.00");
		double incrementBy = 0.01;
		for(int roundingDigit : roundingDigits){
			String roundingDigitStr = String.valueOf(roundingDigit);
			
			String start = dFormat.format(startVal);
			double dStart = Double.parseDouble(start);
			String end = dFormat.format(endVal);
			double dEnd = Double.parseDouble(end);
			
			while(dStart <= dEnd){
				if(start.endsWith(roundingDigitStr)){
					String tStartStr = dFormat.format(dStart);
					if(Double.parseDouble(tStartStr) > 0)
						newRangeList.add(Double.parseDouble(tStartStr));
				}
				dStart = dStart+ incrementBy;
				start = dFormat.format(dStart);
			}
		}
		return newRangeList;
	}
	
	/**
	 * Returns Range of numbers between startVal and endVal using rounding table content
	 * @param startVal
	 * @param endVal
	 * @return
	 */
	private ArrayList<Double> getRangeFromRoundingTable_old(double startVal, double endVal, String roundingLogic){
		//logger.debug("Get Range from rounding table - " + startVal + "\t" + endVal);
		if(startVal != Constants.DEFAULT_NA && endVal == Constants.DEFAULT_NA){
			endVal = startVal + 1;
		}else if(startVal == Constants.DEFAULT_NA && endVal != Constants.DEFAULT_NA){
			startVal = endVal - 1;
		}
		
//		long startTime = System.currentTimeMillis();
		ArrayList<Double> newRangeList = new ArrayList<Double>();
		double start = Integer.parseInt(String.valueOf(startVal).substring(0, String.valueOf(startVal).indexOf(".")));
		double end = Integer.parseInt(String.valueOf(endVal).substring(0, String.valueOf(endVal).indexOf(".")));
		String startStr = String.valueOf(start).substring(0, String.valueOf(start).indexOf("."));
		while(start <= end){
			for(Map.Entry<String, PRRoundingTableDTO> entry : roundingTableContent.entrySet()){
				String[] recRange = entry.getKey().split("-");
				double recStart = Double.parseDouble(recRange[0]);
				double recEnd = Double.parseDouble(recRange[1]);
				
				if(start >= recStart && start <= recEnd){
					String[] allowedEndDigits = entry.getValue().getAllowedEndDigits().split(",");
					for(String endDigit : allowedEndDigits){
						String precision = endDigit.substring(endDigit.indexOf(".")+1, endDigit.length());
						precision = startStr + "." + precision;
						double price = Double.parseDouble(precision);
						if(price >= startVal && price <= endVal){
							newRangeList.add(price);
						}
					}
				}
			}
			start++;
			startStr = String.valueOf(start).substring(0, String.valueOf(start).indexOf("."));
		}
//		long endTime = System.currentTimeMillis();
		//logger.info("Time taken to retrieve range from rounding table - " + (endTime - startTime) + "ms");
		return newRangeList;
	}
	
	private ArrayList<Double> getRangeFromRoundingTable(double inputStartPricePoint, double inputEndPricePoint, String roundingLogic) {
		/*
		 * Get range whose start and end range is within input start price point.
		 * Get range whose start and end range is within input end price point.
		 * Get all ranges from rounding table whose start is > first range end and end is < second range start.
		 * Find all possible price points for each range.
		 * Pick price point which is within the input range
		 */
	 

		/*
		 * E.g. Input Range:4.08 - 4.72 Rounding Ranges: 
		 * 3.90-4.14 -> 0.99, 
		 * 4.15-4.39 -> 0.29, 
		 * 4.40-4.65 -> 0.49, 
		 * 4.66-4.89 -> 0.79
		 **/
		
		ArrayList<Double> finalPricePoints = new ArrayList<Double>();
		Set<Double> distinctPricePoints = new HashSet<Double>();
		HashMap<RoundingRangeKey, PRRoundingTableDTO> possibleRanges = new HashMap<RoundingRangeKey, PRRoundingTableDTO>();
		HashMap<RoundingRangeKey, PRRoundingTableDTO> allRanges = convertRoundingTableContent();
		
		//Set start and end if it is not there
		if(inputStartPricePoint != Constants.DEFAULT_NA && inputEndPricePoint == Constants.DEFAULT_NA){
			inputEndPricePoint = PRFormatHelper.roundToTwoDecimalDigitAsDouble(inputStartPricePoint + 1);
		}else if(inputStartPricePoint == Constants.DEFAULT_NA && inputEndPricePoint != Constants.DEFAULT_NA){
			inputStartPricePoint = PRFormatHelper.roundToTwoDecimalDigitAsDouble(inputEndPricePoint - 1);
		}
		
		//NU:: 9th Feb 2018, bug fix: sometime the start range is having too many decimals (2.74-1 -> 1.7400001) and
		// due to that the rounding range are not found 
		inputStartPricePoint = PRFormatHelper.roundToTwoDecimalDigitAsDouble(inputStartPricePoint);
		inputEndPricePoint = PRFormatHelper.roundToTwoDecimalDigitAsDouble(inputEndPricePoint);
		
		
		// Get all possible rounding ranges from rounding table for the input range
		possibleRanges = getPossibleRoundingRanges(inputStartPricePoint, inputEndPricePoint, allRanges);
		
		// Get possible price points within each range
		for (Entry<RoundingRangeKey, PRRoundingTableDTO> entry : possibleRanges.entrySet()) {
			Set<Double> tempPricePoints = getPricePointsWithinTheRange(inputStartPricePoint, inputEndPricePoint, entry.getKey().getStartPrice(),
					entry.getKey().getEndPrice(), entry.getValue().getAllowedEndDigits(), entry.getValue());
			if (tempPricePoints.size() == 0) {
				Set<Double> allPricePoints = getPricePointsWithinTheRange(entry.getKey().getStartPrice(), entry.getKey().getEndPrice(),
						entry.getValue().getAllowedEndDigits(), entry.getValue());
				double closedPricePoint = 0, nextPricePoint = 0, prevPricePoint = 0;
				closedPricePoint = getClosestPriceRange(inputStartPricePoint, allPricePoints);

				if (roundingLogic.toUpperCase().equals("") && closedPricePoint > 0) {
					distinctPricePoints.add(closedPricePoint);
				} else if (roundingLogic.toUpperCase().equals(PRConstants.ROUND_UP)) {

					// if the closest price > than input start range, then don't round up
					if (closedPricePoint >= inputStartPricePoint) {
						distinctPricePoints.add(closedPricePoint);
					} else {

						// Find next rounding digit
						allPricePoints.clear();
						HashMap<RoundingRangeKey, PRRoundingTableDTO> tempPossibleRanges = getPossibleRoundingRanges(inputStartPricePoint,
								PRFormatHelper.roundToTwoDecimalDigitAsDouble(inputEndPricePoint + 2), allRanges);

						for (Entry<RoundingRangeKey, PRRoundingTableDTO> entry1 : tempPossibleRanges.entrySet()) {
							allPricePoints.addAll(getPricePointsWithinTheRange(entry1.getKey().getStartPrice(), entry1.getKey().getEndPrice(),
									entry1.getValue().getAllowedEndDigits(), entry1.getValue()));
						}

						if (allPricePoints.size() > 0) {
							//Changes done by Bhargavi on 9/24/2021
							//update the Rounding prod issue
//							Double maxAllPricePoint = allPricePoints.stream().max(Comparator.comparing(String::valueOf)).get();
//							Double minAllPricePoint = allPricePoints.stream().min(Comparator.comparing(String::valueOf)).get();
							Double maxAllPricePoint = allPricePoints.stream().max(Comparator.comparing(Double::valueOf)).get();
							Double minAllPricePoint = allPricePoints.stream().min(Comparator.comparing(Double::valueOf)).get();
							//Changes ended
							final double tempClosedPricePoint = closedPricePoint;
//							nextPricePoint = allPricePoints.stream().filter(p -> p > tempClosedPricePoint).sorted().findFirst().get();
							if(closedPricePoint >= minAllPricePoint  && closedPricePoint < maxAllPricePoint)
								nextPricePoint = allPricePoints.stream().filter(p -> p > tempClosedPricePoint).sorted().findFirst().get();
							else
								nextPricePoint = closedPricePoint;
							distinctPricePoints.add(nextPricePoint);
						}
					}

				} else if (roundingLogic.toUpperCase().equals(PRConstants.ROUND_DOWN)) {

					Double startVal = inputStartPricePoint - 2;
					startVal = PRFormatHelper.roundToTwoDecimalDigitAsDouble(startVal < 0 ? 0 : startVal);

					// if the closest price > than input start range, then don't round down
					if (closedPricePoint <= inputStartPricePoint) {
						distinctPricePoints.add(closedPricePoint);
					} else {

						// Find next rounding digit
						allPricePoints.clear();
						HashMap<RoundingRangeKey, PRRoundingTableDTO> tempPossibleRanges = getPossibleRoundingRanges(startVal, 
								inputEndPricePoint, allRanges);

						for (Entry<RoundingRangeKey, PRRoundingTableDTO> entry1 : tempPossibleRanges.entrySet()) {
							allPricePoints.addAll(getPricePointsWithinTheRange(entry1.getKey().getStartPrice(), entry1.getKey().getEndPrice(),
									entry1.getValue().getAllowedEndDigits(), entry1.getValue()));
						}

						if (allPricePoints.size() > 0) {
							final double tempClosedPricePoint = closedPricePoint;
							Optional<Double> prevPrPoint = allPricePoints.stream().filter(p -> p.doubleValue() < tempClosedPricePoint)
									.sorted(Comparator.reverseOrder()).findFirst();
							
							if(prevPrPoint.isPresent()) {
								prevPricePoint = prevPrPoint.get();
							}
							
							/*prevPricePoint = allPricePoints.stream().filter(p -> p.doubleValue() < tempClosedPricePoint)
									.sorted(Comparator.reverseOrder()).findFirst().get();*/
							distinctPricePoints.add(prevPricePoint);
						}
					}
				}

			} else {
				//There is only price point and round up and round down is coming
				distinctPricePoints.addAll(tempPricePoints);
			}
			
			//logger.debug("Before excluding pp: " + distinctPricePoints.size());
			//distinctPricePoints = filterPricePointsByExcludeList(distinctPricePoints, entry.getValue());
			//logger.debug("After excluding pp: " + distinctPricePoints.size());
		}
		
		// 29th Jan 2018: When threshold or cost or lower/higher retail constraint is broken
		// pick only rounding digits within the range, if none of the rounding digit fall within
		// the range, then stay with actual output
		
		PRRange inputRange = new PRRange();
		inputRange.setStartVal(inputStartPricePoint);
		inputRange.setEndVal(inputEndPricePoint);
		
		double[] tempFinalPricePoints = new double[distinctPricePoints.size()];
		int counter = 0;
		for(Double price : distinctPricePoints){
			tempFinalPricePoints[counter] = price;
			counter++;
		}
		
		List<Double> filteredPricePoints = new FilterPriceRange().getRoundingDigitsWithinRange(tempFinalPricePoints, inputRange);				 
		if (filteredPricePoints.size() > 0) {
			distinctPricePoints.clear();
			distinctPricePoints.addAll(filteredPricePoints);
		}
		
		finalPricePoints.addAll(distinctPricePoints);
		
		return finalPricePoints;
	}
	
	
	private HashMap<RoundingRangeKey, PRRoundingTableDTO> convertRoundingTableContent() {
		HashMap<RoundingRangeKey, PRRoundingTableDTO> allRanges = new HashMap<RoundingRangeKey, PRRoundingTableDTO>();

		for (Map.Entry<String, PRRoundingTableDTO> entry : roundingTableContent.entrySet()) {
			String[] recRange = entry.getKey().split("-");
			RoundingRangeKey tempRange = new RoundingRangeKey(Double.parseDouble(recRange[0]), Double.parseDouble(recRange[1]));
			allRanges.put(tempRange, entry.getValue());
		}
		return allRanges;
	}
	
	
	private HashMap<RoundingRangeKey, PRRoundingTableDTO> getPossibleRoundingRanges(double inputStartPricePoint, double inputEndPricePoint,
			HashMap<RoundingRangeKey, PRRoundingTableDTO> allRanges) {

		RoundingRangeKey startPriceRange = null;
		RoundingRangeKey endPriceRange = null;
		HashMap<RoundingRangeKey, PRRoundingTableDTO> possibleRanges = new HashMap<RoundingRangeKey, PRRoundingTableDTO>();

		for (Map.Entry<RoundingRangeKey, PRRoundingTableDTO> entry : allRanges.entrySet()) {
			RoundingRangeKey tempRange = entry.getKey();
			double recStart = tempRange.getStartPrice();
			double recEnd = tempRange.getEndPrice();
			

			// Get range whose start and end range is within input start range
			if (inputStartPricePoint >= recStart && inputStartPricePoint <= recEnd) {
				possibleRanges.put(tempRange, entry.getValue());
				startPriceRange = tempRange;
			}

			// Get range whose start and end range is within input end range
			if (inputEndPricePoint >= recStart && inputEndPricePoint <= recEnd) {
				possibleRanges.put(tempRange, entry.getValue());
				endPriceRange = tempRange;
			}
		}

		if (startPriceRange != null && endPriceRange != null) {
			// Get all ranges whose start is > first range end and end is < second range start
			for (Map.Entry<RoundingRangeKey, PRRoundingTableDTO> entry : allRanges.entrySet()) {
				if (entry.getKey().getStartPrice() > startPriceRange.getEndPrice() && entry.getKey().getEndPrice() < endPriceRange.getStartPrice()) {
					possibleRanges.put(entry.getKey(), entry.getValue());
				}
			}
		}

		return possibleRanges;
	}
	
	
	
	/***
	 * Returns all price points by applying the allowed end digits between the start and end range
	 * and which is falling the input ranges
	 * @param inputStartPricePoint
	 * @param inputEndPricePoint
	 * @param roundingTableStartRange
	 * @param roundingTableEndRange
	 * @param allowedEndDigits
	 * @return
	 */
	private Set<Double> getPricePointsWithinTheRange(double inputStartPricePoint, double inputEndPricePoint, Double roundingTableStartRange,
			Double roundingTableEndRange, String allowedEndDigits, PRRoundingTableDTO pRRoundingTableDTO) {
		
		Set<Double> finalRoundingDigits = new HashSet<Double>();
		double roundingTableStartWholeNumber = Integer.parseInt(String.valueOf(roundingTableStartRange).substring(0,
				String.valueOf(roundingTableStartRange).indexOf(".")));
		double roundingTableEndWholeNumber = Integer.parseInt(
				String.valueOf(roundingTableEndRange).substring(0, String.valueOf(roundingTableEndRange).indexOf(".")));
		 
		logger.debug("inputStartPricePoint: " + inputStartPricePoint + ", inputEndPricePoint: " + inputEndPricePoint
				+ ", roundingTableStartRange:" + roundingTableStartRange + "roundingTableEndRange"
				+ roundingTableEndRange);
	
		if (roundingTableStartRange <= inputStartPricePoint && roundingTableEndRange >= inputEndPricePoint) {
			roundingTableStartWholeNumber = (int) inputStartPricePoint;
			roundingTableEndWholeNumber = (int) inputEndPricePoint;
		}

		String startStr = String.valueOf(roundingTableStartWholeNumber).substring(0, String.valueOf(roundingTableStartWholeNumber).indexOf("."));

		int increment = 0;
		while (roundingTableStartWholeNumber <= roundingTableEndWholeNumber) {
			String[] arrAllowedEndDigits = allowedEndDigits.split(",");
			for (String endDigit : arrAllowedEndDigits) {
				String precision = endDigit.substring(endDigit.indexOf(".") + 1, endDigit.length());
				String befDecimal = endDigit.substring(0, endDigit.indexOf("."));

				if(befDecimal.equals(""))
					increment = 0;
				else
					increment = Integer.parseInt(befDecimal);

				if(increment > 0)
				{
					if(startStr.length()>1) {
						String subStartStr = startStr.substring(0, startStr.length()-befDecimal.length());
						precision = subStartStr + befDecimal + "." + precision;
						double price = Double.parseDouble(precision);
						if((price >= inputStartPricePoint && price <= inputEndPricePoint) &&
								(price >= roundingTableStartRange && price <= roundingTableEndRange))
							finalRoundingDigits.add(price);
					}
					else
					{
						precision = befDecimal+ "." + precision;
						double price = Double.parseDouble(precision);
						if((price >= inputStartPricePoint && price <= inputEndPricePoint) &&
								(price >= roundingTableStartRange && price <= roundingTableEndRange))
							finalRoundingDigits.add(price);
					}

				}
				else
				{
					precision = startStr + "." + precision;
					double price = Double.parseDouble(precision);
					if((price >= inputStartPricePoint && price <= inputEndPricePoint) &&
							(price >= roundingTableStartRange && price <= roundingTableEndRange)) {
						finalRoundingDigits.add(price);
					}
				}

			}
			roundingTableStartWholeNumber++;
			startStr = String.valueOf(roundingTableStartWholeNumber).substring(0, String.valueOf(roundingTableStartWholeNumber).indexOf("."));
		}

//		while (roundingTableStartWholeNumber <= roundingTableEndWholeNumber) {
//			String[] arrAllowedEndDigits = allowedEndDigits.split(",");
//			for (String endDigit : arrAllowedEndDigits) {
//				String precision = endDigit.substring(endDigit.indexOf(".") + 1, endDigit.length());
//				precision = startStr + "." + precision;
//				double price = Double.parseDouble(precision);
//				if (price >= inputStartPricePoint && price <= inputEndPricePoint) {
//					finalRoundingDigits.add(price);
//				}
//			}
//			roundingTableStartWholeNumber++;
//			startStr = String.valueOf(roundingTableStartWholeNumber).substring(0, String.valueOf(roundingTableStartWholeNumber).indexOf("."));
//		}
		
		finalRoundingDigits = filterPricePointsByExcludeList(finalRoundingDigits, pRRoundingTableDTO);
		if (pRRoundingTableDTO.getAllowedPrices() != null
				&& !Constants.EMPTY.equals(pRRoundingTableDTO.getAllowedPrices())) {
		finalRoundingDigits = filterPricePointsByAllowedPriceList(finalRoundingDigits, pRRoundingTableDTO,inputStartPricePoint,inputEndPricePoint);
		}
		return finalRoundingDigits;
	}
	
	/***
	 * Returns all price points by applying the allowed end digits between the start and end range
	 * and which is falling within those ranges
	 * @param roundingTableStartRange
	 * @param roundingTableEndRange
	 * @param allowedEndDigits
	 * @return
	 */
	private Set<Double> getPricePointsWithinTheRange(double roundingTableStartRange, double roundingTableEndRange,
			String allowedEndDigits, PRRoundingTableDTO pRRoundingTableDTO) {

		Set<Double> finalRoundingDigits = new HashSet<Double>();
		double roundingTableStartWholeNumber = Integer.parseInt(String.valueOf(roundingTableStartRange).substring(0,
				String.valueOf(roundingTableStartRange).indexOf(".")));
		double roundingTableEndWholeNumber = Integer.parseInt(
				String.valueOf(roundingTableEndRange).substring(0, String.valueOf(roundingTableEndRange).indexOf(".")));

		String startStr = String.valueOf(roundingTableStartWholeNumber).substring(0,
				String.valueOf(roundingTableStartWholeNumber).indexOf("."));

		while (roundingTableStartWholeNumber <= roundingTableEndWholeNumber) {
			String[] arrAllowedEndDigits = allowedEndDigits.split(",");
			for (String endDigit : arrAllowedEndDigits) {
				String precision = endDigit.substring(endDigit.indexOf(".") + 1, endDigit.length());
				String befDecimal = endDigit.substring(0, endDigit.indexOf("."));

				int increment;
				if(befDecimal.equals(""))
					increment = 0;
				else
					increment = Integer.parseInt(befDecimal);

				if(increment > 0)
				{
					if(startStr.length()>1) {
						String subStartStr = startStr.substring(0, startStr.length()-befDecimal.length());
						precision = subStartStr + befDecimal + "." + precision;
						double price = Double.parseDouble(precision);
						if(price >= roundingTableStartRange && price <= roundingTableEndRange)
							finalRoundingDigits.add(price);
					}
					else
					{
						precision = befDecimal+ "." + precision;
						double price = Double.parseDouble(precision);
						if(price >= roundingTableStartRange && price <= roundingTableEndRange)
							finalRoundingDigits.add(price);
					}

				}
				else
				{
					precision = startStr + "." + precision;
					double price = Double.parseDouble(precision);
					if (price >= roundingTableStartRange && price <= roundingTableEndRange) {
						finalRoundingDigits.add(price);
					}
				}

			}
			roundingTableStartWholeNumber++;
			startStr = String.valueOf(roundingTableStartWholeNumber).substring(0, String.valueOf(roundingTableStartWholeNumber).indexOf("."));
		}

//		while (roundingTableStartWholeNumber <= roundingTableEndWholeNumber) {
//			String[] arrAllowedEndDigits = allowedEndDigits.split(",");
//			for (String endDigit : arrAllowedEndDigits) {
//				String precision = endDigit.substring(endDigit.indexOf(".") + 1, endDigit.length());
//				precision = startStr + "." + precision;
//				double price = Double.parseDouble(precision);
//				if (price >= roundingTableStartRange && price <= roundingTableEndRange) {
//					finalRoundingDigits.add(price);
//				}
//			}
//			roundingTableStartWholeNumber++;
//			startStr = String.valueOf(roundingTableStartWholeNumber).substring(0,
//					String.valueOf(roundingTableStartWholeNumber).indexOf("."));
//		}

		finalRoundingDigits = filterPricePointsByExcludeList(finalRoundingDigits, pRRoundingTableDTO);
		
		return finalRoundingDigits;
	}
	
	private double getClosestPriceRange(double inputPricePoint, Set<Double> allPricePoints) {
		boolean isStartValuePresent = false;
		double finalPrice = 0;
		
		if (inputPricePoint != Constants.DEFAULT_NA) {
			isStartValuePresent = true;
		} else {
			return finalPrice;
		}

		boolean first = true;
		
		double finalDiff = 0;
		for (Double price : allPricePoints) {
			if (isStartValuePresent) {
				if (first) {
					finalPrice = price;
					finalDiff = Math.abs(price - inputPricePoint);
					first = false;
				} else {
					double diff = Math.abs(price - inputPricePoint);
					if (diff < finalDiff) {
						finalPrice = price;
						finalDiff = diff;
					}
				}
			}
		}

		return finalPrice;
	}
	
	/**
	 * Returns Range of numbers between startVal and endVal
	 * @param startVal
	 * @param endVal
	 * @return
	 */
	private ArrayList<Double> getFullRange(double startVal, double endVal){
		ArrayList<Double> newRangeList = new ArrayList<Double>();
		DecimalFormat dFormat = new DecimalFormat("#########.00");
		double incrementBy = 0.01;

		String start = dFormat.format(startVal);
		double dStart = Double.parseDouble(start);
		String end = dFormat.format(endVal);
		double dEnd = Double.parseDouble(end);
			
		while(dStart <= dEnd){
			String tStartStr = dFormat.format(dStart);
			if(Double.parseDouble(tStartStr) > 0)
				newRangeList.add(Double.parseDouble(tStartStr));
			dStart = dStart+ incrementBy;
			start = dFormat.format(dStart);
		}
		
		return newRangeList;
	}
	
	/**
	 * Returns Range of numbers between startVal and endVal (where one of the two does not have a value assigned)
	 * @param startVal
	 * @param endVal
	 * @return
	 */
	public double[] getClosestPriceRange(double startVal, double endVal){
		boolean isStartValuePresent = false;
		boolean isEndValuePresent = false;
		if(startVal != Constants.DEFAULT_NA && endVal != Constants.DEFAULT_NA){
			isStartValuePresent = true;
			isEndValuePresent = true;
		}else if(startVal != Constants.DEFAULT_NA){
			isStartValuePresent = true;
			endVal = startVal;
		}else if(endVal != Constants.DEFAULT_NA){
			isEndValuePresent = true;
			startVal = endVal;
		}else{
			return new double[0];
		}
		
		ArrayList<Double> newRangeList = new ArrayList<Double>();
		
		if(roundingDigits != null && roundingDigits.length > 0)
			newRangeList = getRangeFromRoundingDigits(Math.floor(startVal - 2), Math.floor(endVal + 2));
		else if(roundingTableId > 0 || roundingTableContent.size() > 0)
			newRangeList = getRangeFromRoundingTable(Math.floor(startVal - 2), Math.floor(endVal + 2), "");
		else
			newRangeList = getFullRange(Math.floor(startVal - 2), Math.floor(endVal + 2));
		
		boolean first = true;
		double finalPrice = 0;
		double finalDiff = 0;
		for(Double price : newRangeList){
			if(isStartValuePresent){
				if(price > startVal){
					if(first){
						finalPrice = price;
						finalDiff = Math.abs(price - startVal);
						first = false;
					}else{
						double diff = Math.abs(price - startVal);
						if(diff < finalDiff){
							finalPrice = price;
							finalDiff = diff;
						}
					}
				}
			}
			if(isEndValuePresent){
				if(price < endVal){
					if(first){
						finalPrice = price;
						finalDiff = Math.abs(price - endVal);
						first = false;
					}else{
						double diff = Math.abs(price - endVal);
						if(diff < finalDiff){
							finalPrice = price;
							finalDiff = diff;
						}
					}
				}
			}
		}
		
		double[] priceRange = new double[1];
		priceRange[0] = finalPrice;
		//logger.info("Rounding Constraint - " + finalPrice);
		return priceRange;
	}
	
	/**
	 * Sets Next, Previous and Recommended price points as price points for opportunities
	 * @param item	Item Object
	 */
	public void getNextAndPreviousPrice(PRItemDTO item, Boolean addOnlyRecPrice) {
//		double recPrice = item.getRecommendedRegPrice();
		double recPrice = item.getRecommendedRegPrice().price;
		/* If recPrice is 2.19, startVal will be 1 and endVal will be 3. getRange() will return
		 * price points within this range. This is done just to restrict the price range for getting previous and next price points
		 */
		
		/*double startVal = Math.floor(recPrice - 1);
		double endVal = Math.floor(recPrice + 1);*/
		
		double startVal = recPrice - 1;
		if(startVal < 0)
			startVal = 0;
		double endVal = recPrice + 1;
		
		
		double[] priceRange = getRange(startVal, endVal, PRConstants.ROUND_CLOSEST);
		
		item.addOpportunities(1, recPrice);
		if (!addOnlyRecPrice) {
			double prevPrice = priceRange[0];
			for (double price : priceRange) {
				if (price == recPrice) {
					if (prevPrice != price) {
						item.addOpportunities(1, prevPrice);
					}
				}
				if (prevPrice == recPrice) {
					item.addOpportunities(1, price);
				}
				prevPrice = price;
			}
		}
		
		if(item.getOppurtunities() != null && item.getOppurtunities().size() == 1){
			logger.debug("No oppurtunities found for item " + item.getItemCode() + " and recommended price " +  PRCommonUtil.getPriceForLog(item.getRecommendedRegPrice()));
		}
	}

	private double[] roundupPriceToNextRoundingDigit(double startVal, double endVal)
	{
		// If both val is not present
		if(startVal ==  Constants.DEFAULT_NA && endVal == Constants.DEFAULT_NA){
			return new double[0];
		}
		if(startVal !=  Constants.DEFAULT_NA && endVal != Constants.DEFAULT_NA){
			startVal = endVal;
		}
		else if(startVal ==  Constants.DEFAULT_NA){
			startVal = endVal;
		}
		else if (endVal == Constants.DEFAULT_NA){
			endVal = startVal;
		}
		
		//Add $2 to end val to get the next rounding price
		endVal = endVal + 2;
		
		ArrayList<Double> newRangeList = new ArrayList<Double>();

		if (roundingDigits != null && roundingDigits.length > 0)
			newRangeList = getRangeFromRoundingDigits(startVal, endVal);
		else if (roundingTableId > 0 || roundingTableContent.size() > 0)
			newRangeList = getRangeFromRoundingTable(startVal, endVal, PRConstants.ROUND_UP);
		else
			newRangeList = getFullRange(startVal, endVal);
		
		Collections.sort(newRangeList);
		double[] newRangeArr = new double[newRangeList.size()];		
		int counter = 0;
		for(Double price : newRangeList){
			newRangeArr[counter] = price;
			counter++;
		}
		return newRangeArr;		 
	}
	
	private double[] roundupPriceToPreviousRoundingDigit(double startVal, double endVal)
	{
		// If both val is not present
		if (startVal == Constants.DEFAULT_NA && endVal == Constants.DEFAULT_NA) {
			return new double[0];
		}
		
		if(startVal !=  Constants.DEFAULT_NA && endVal != Constants.DEFAULT_NA){
			endVal = startVal;
		}
		else if (startVal == Constants.DEFAULT_NA) {
			startVal = endVal;
		} else if (endVal == Constants.DEFAULT_NA) {
			endVal = startVal;
		}			
	 
		//Subtract $1 to end val to get the previous rounding price
		startVal = startVal - 2;

		if(startVal < 0)
			startVal = 0;		
		
		List<Double> newRangeList = new ArrayList<Double>();

		if (roundingDigits != null && roundingDigits.length > 0)
			newRangeList = getRangeFromRoundingDigits(startVal, endVal);
		else if (roundingTableId > 0 || roundingTableContent.size() > 0)
			newRangeList = getRangeFromRoundingTable(startVal, endVal, PRConstants.ROUND_DOWN);
		else
			newRangeList = getFullRange(startVal, endVal);

		Collections.sort(newRangeList);
		double[] newRangeArr;
		if(newRangeList.size() > 0){
			newRangeArr = new double[1];
			newRangeArr[0] = newRangeList.get(newRangeList.size() - 1);
		}else{
			newRangeArr = new double[0];
		}		
		 
		return newRangeArr; 				
		 	 
	}
		
	public double roundupPriceToNextRoundingDigit(double pricePoint)
	{	
		double nextRoundingDigit = 0d;
		double startPricePoint, endPricePoint;
		double[] priceRange;		
		
		startPricePoint = pricePoint;
		endPricePoint = pricePoint + 2;
		
		priceRange = getRange(startPricePoint, endPricePoint);
		Arrays.sort(priceRange);
		if (priceRange.length > 0)
			nextRoundingDigit = priceRange[0];
		
		return nextRoundingDigit;		 
	}
	
	public double roundupPriceToPreviousRoundingDigit(double pricePoint)
	{
		double previousRoundingDigit = 0d;
		double startPricePoint, endPricePoint;
		double[] priceRange;		
		
		startPricePoint = pricePoint - 2;
		endPricePoint = pricePoint;
		
		if(startPricePoint < 0)
			startPricePoint = 0;		 
		
		priceRange = getRange(startPricePoint, endPricePoint);
		Arrays.sort(priceRange);
		if (priceRange.length > 0)
			previousRoundingDigit = priceRange[priceRange.length - 1];
		
		return previousRoundingDigit;		 	 
	}
	
	@SuppressWarnings("unchecked")
	@Override
    protected Object clone() throws CloneNotSupportedException {
		PRConstraintRounding cloned = (PRConstraintRounding) super.clone();
			
		if(cloned.getRoundingTableContent() != null)
			cloned.setRoundingTableContent((TreeMap<String, PRRoundingTableDTO>) cloned.getRoundingTableContent().clone());
		
		return cloned;
    }
	
	/**
	 * 
	 * @param distinctPricePoints
	 * @param pRRoundingTableDTO
	 * @return filtered price points by exclude list
	 */
	private Set<Double> filterPricePointsByExcludeList(Set<Double> distinctPricePoints, PRRoundingTableDTO pRRoundingTableDTO){
		Set<Double> filteredList = new HashSet<>();
		
		if (pRRoundingTableDTO.getExcludedPrices() != null
				&& !Constants.EMPTY.equals(pRRoundingTableDTO.getExcludedPrices())) {
			//logger.debug("Exc. price points: " + pRRoundingTableDTO.getExcludedPrices());
			String[] excludePriceArr = pRRoundingTableDTO.getExcludedPrices().split(",");
			Set<Double> excludePriceList = new HashSet<>();
			for(String excPr: excludePriceArr) {
				double pp = Double.parseDouble(excPr.trim());
				excludePriceList.add(pp);	
			}
			
			for(Double priceP: distinctPricePoints) {
				if(!excludePriceList.contains(priceP)) {
					filteredList.add(priceP);
				}
			}
		} else {
			filteredList = distinctPricePoints;
		}
		
		return filteredList;
	}
	
	private Set<Double> filterPricePointsByAllowedPriceList(Set<Double> distinctPricePoints,
			PRRoundingTableDTO pRRoundingTableDTO, double inputStartPricePoint, double inputEndPricePoint) {
	
		String[] allowedPrArray = pRRoundingTableDTO.getAllowedPrices().split(",");
		
		for (String allowedPr : allowedPrArray) {
			double pp = Double.parseDouble(allowedPr.trim());
			if (pp >= inputStartPricePoint && pp <= inputEndPricePoint) {
				distinctPricePoints.add(pp);
			}

		}
		return distinctPricePoints;
	}
}
