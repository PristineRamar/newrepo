package com.pristine.service.offermgmt.guideline;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
//import java.text.DecimalFormat;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRCompDetailLog;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRGuidelineComp;
import com.pristine.dto.offermgmt.PRGuidelineCompDetail;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.service.offermgmt.CompGuidelineAndConstraintService;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.service.offermgmt.GuidelineTypeLookup;
import com.pristine.service.offermgmt.PriceIndexCalculation;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class MultiCompGuideline {
	private static Logger logger = Logger.getLogger("MultiCompGuideline");
	PRItemDTO inputItem;
	PRRange inputPriceRange;
	FilterPriceRange filterPriceRange = new FilterPriceRange();
	//HashMap<MultiCompetitorKey, CompetitiveDataDTO> multiCompLatestPriceMap;
	String curWeekStartDate;
	private PRExplainLog explainLog;

	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

	double minBadIndex = Double.parseDouble(PropertyManager.getProperty("MIN_BAD_INDEX", "0"));
	double maxBadIndex = Double.parseDouble(PropertyManager.getProperty("MAX_BAD_INDEX", "0"));
	boolean chkFutureCost = Boolean.parseBoolean(PropertyManager.getProperty("USE_FUTURECOST", "FALSE"));

	public MultiCompGuideline(PRItemDTO inputItem, PRRange inputPriceRange, 
			PRExplainLog explainLog, String curWeekStartDate) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		//this.multiCompLatestPriceMap = multiCompLatestPriceMap;
		this.explainLog = explainLog;
		this.curWeekStartDate = curWeekStartDate;
	}

	public PRGuidelineAndConstraintOutput applyMultipleCompGuideline() throws Exception {
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();
		PRStrategyDTO strategyDTO = inputItem.getStrategyDTO();
		PRRange filteredPriceRange;
		PRRange multiCompRange = new PRRange();
		guidelineAndConstraintLog.setGuidelineTypeId(GuidelineTypeLookup.COMPETITION.getGuidelineTypeId());

		// If no competitor found
		if (strategyDTO.getGuidelines().getCompGuideline() == null) {
			guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
			guidelineAndConstraintOutput.isGuidelineApplied = false;
			//guidelineAndConstraintLog.setIsConflict(false);
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
			guidelineAndConstraintLog.setMessage("No Competitor Found");
			guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			//logger.debug("No Multi Competitor Found - " + inputItem.getItemCode());

		}
		// If no competitor found
		else if (strategyDTO.getGuidelines().getCompGuideline().getCompetitorDetails().size() == 0) {
			guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
			guidelineAndConstraintOutput.isGuidelineApplied = false;
			//guidelineAndConstraintLog.setIsConflict(false);
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
			guidelineAndConstraintLog.setMessage("No Competitor Found");
			guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			//logger.debug("No Multi Competitor Found - " + inputItem.getItemCode());
		} else {
			// No Price found
			boolean isPriceFound = false;
			List<PRGuidelineCompDetail> compDetails = strategyDTO.getGuidelines().getCompGuideline().getCompetitorDetails();
			//MultiCompetitorKey multiCompetitorKey;
			//PRGuidelineComp compGuideline = this.inputItem.getStrategyDTO().getGuidelines().getCompGuideline();	
			for (PRGuidelineCompDetail guidelineCompDetail : compDetails) {
				LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guidelineCompDetail.getCompStrId());
				if(inputItem.getAllCompPrice().get(locationKey) != null){
					MultiplePrice compRegPrice = inputItem.getAllCompPrice().get(locationKey);
					if(compRegPrice.price > 0){
						isPriceFound = true;
						break;
					}
				}
			}

			// No Price found
			if (!isPriceFound) {
				guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
				guidelineAndConstraintOutput.isGuidelineApplied = false;
				//guidelineAndConstraintLog.setIsConflict(false);
				guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
				guidelineAndConstraintLog.setMessage("No Competition Price");
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
				//logger.debug("No Comp Price data found for item code - " + inputItem.getItemCode());
			} else {
				multiCompRange = applyMultiCompGuideline();
				//Adjust if there is negative range
				filterPriceRange.handleNegativeRange(multiCompRange);		
				inputItem.setMultiCompRange(multiCompRange);
				filteredPriceRange = filterPriceRange.filterRange(inputPriceRange, multiCompRange);

				// Ahold, Cost Change - ignore the multi comp guideline 
				if (inputItem.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND)
						|| inputItem.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_MARGIN)) {
					// If this is the first guideline
					if (inputPriceRange.getStartVal() == Constants.DEFAULT_NA && inputPriceRange.getEndVal() == Constants.DEFAULT_NA) {
						guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
					} else {
						if(inputItem.getIsMarginGuidelineApplied() || inputItem.getIsStoreBrandRelationApplied()) {
							guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
						}
						else{
							guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
						}
						// If it is store brand, then put the actual conflict
						//if (inputItem.getCostChangeBehavior().equals(PRConstants.COST_CHANGE_AHOLD_STORE_BRAND))
							//guidelineAndConstraintOutput.outputPriceRange.setConflict(filteredPriceRange.isConflict());
					}
				} else {
					guidelineAndConstraintOutput.outputPriceRange = filteredPriceRange;
					filterPriceRange.updateRoundingLogic(inputItem, inputPriceRange, multiCompRange, filteredPriceRange, "");
				}

				guidelineAndConstraintOutput.isGuidelineApplied = true;
				//guidelineAndConstraintLog.setIsConflict(guidelineAndConstraintOutput.outputPriceRange.isConflict());
				guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(guidelineAndConstraintOutput.isGuidelineApplied);
				guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(multiCompRange);
				//If Cost is not available pass input range as output range
				if ((inputItem.getListCost() == null || inputItem.getListCost() <= 0)
						&& (inputItem.getRegPrice() != null && inputItem.getRegPrice() > 0)) {
					guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
					guidelineAndConstraintLog.setOutputPriceRange(inputPriceRange);
				}
				else
					guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			}
		}

		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		
		/*
		 * logger.debug("Multi Comp Guideline -- " + "Is Applied: " +
		 * guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() +
		 * ",Input Range: " + inputPriceRange.toString() + ",Multi Comp Range: " +
		 * multiCompRange.toString() + ",Output Range: " +
		 * guidelineAndConstraintOutput.outputPriceRange.toString());
		 */
		
		return guidelineAndConstraintOutput;
	}
	
	private PRRange applyMultiCompGuideline() throws Exception{
		PRRange guidelineRange = new PRRange();
		PRGuidelineComp compGuideline = this.inputItem.getStrategyDTO().getGuidelines().getCompGuideline();	
		List<PRGuidelineCompDetail> compDetails = compGuideline.getCompetitorDetails();
		double groupRepPrice = 0d;
		
		//If min or average
		if (compGuideline.getGroupPriceType() == PRConstants.GROUP_PRICE_TYPE_AVG) {
			groupRepPrice = findAvgPrice(this.inputItem.getStrategyDTO(), compGuideline, compDetails);
			guidelineRange = getPriceRangeWithNoValueType(groupRepPrice);
		} else if (compGuideline.getGroupPriceType() == PRConstants.GROUP_PRICE_TYPE_MIN) {
			groupRepPrice = findMinPrice(this.inputItem.getStrategyDTO(), compGuideline, compDetails);
			guidelineRange = getPriceRangeWithNoValueType(groupRepPrice);
		} else if (compGuideline.getGroupPriceType() == PRConstants.GROUP_PRICE_TYPE_MAX) {
			groupRepPrice = findMaxPrice(this.inputItem.getStrategyDTO(), compGuideline, compDetails);
			guidelineRange = getPriceRangeWithNoValueType(groupRepPrice);
		} else {
			guidelineRange = applyCompRule(this.inputItem.getStrategyDTO(), compGuideline, compDetails);
		}
		
		return guidelineRange;
	}
	
	private double findMinPrice(PRStrategyDTO strategyDTO, PRGuidelineComp compGuideline, List<PRGuidelineCompDetail> compDetails) throws Exception{
		double minPrice = 0d;
		//MultiCompetitorKey multiCompetitorKey;
		for (PRGuidelineCompDetail guidelineCompDetail : compDetails) {
			//multiCompetitorKey = new MultiCompetitorKey(guidelineCompDetail.getCompStrId(), inputItem.getItemCode());

			LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guidelineCompDetail.getCompStrId());
			
			if(inputItem.getAllCompPrice().get(locationKey) != null){
				
				MultiplePrice compRegPrice = inputItem.getAllCompPrice().get(locationKey);
				
				String checkDate = inputItem.getAllCompPriceCheckDate().get(locationKey);
				
				double compUnitPrice = getValidCompPrice(compRegPrice, guidelineCompDetail.getCompStrId(),
						compGuideline.getLatestPriceObservationDays(), checkDate);
				
				boolean isCompRetailBelowCost = false;
				if (compGuideline.isIgnoreCompBelowCost() && compUnitPrice > 0) {
					double listCost = setCost();
					isCompRetailBelowCost = compUnitPrice < listCost;
				}
				
				PRRange range = new PRRange();
				double adjustedPrice = compUnitPrice;
				if (compUnitPrice > 0) {
					if (guidelineCompDetail.getValueType() == PRConstants.VALUE_TYPE_$) {
						adjustedPrice = getAdjustedPriceFor$Type(guidelineCompDetail, compUnitPrice, range);
					} else if (guidelineCompDetail.getValueType() == PRConstants.VALUE_TYPE_PCT) {
						adjustedPrice = getAdjustedPriceForPCTType(guidelineCompDetail, compUnitPrice, range);
					}
				}
				
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), checkDate, compRegPrice.multiple, compUnitPrice,
						compRegPrice.multiple, guidelineCompDetail.getGuidelineText(), range, range,
						isCompRetailBelowCost);
				
				//Out of check date
				if (adjustedPrice > 0 && !isCompRetailBelowCost) {
					if (minPrice == 0) {
						minPrice = adjustedPrice;
					} else {
						if (adjustedPrice < minPrice) {
							minPrice = adjustedPrice;
						}
					}
				}
				
			}else{
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), "", 0, 0, 0, "", new PRRange(), new PRRange(), false);
			}
			
			/*//Get item price from map
			if (this.multiCompLatestPriceMap.get(multiCompetitorKey) != null) {
				CompetitiveDataDTO competitiveDataDTO = multiCompLatestPriceMap.get(multiCompetitorKey);
				double tempPrice = getPriceInLastObsDay(strategyDTO, competitiveDataDTO, compGuideline.getLatestPriceObservationDays());
				writeCompDetailLog(competitiveDataDTO.compStrId, competitiveDataDTO.checkDate, competitiveDataDTO.regMPack, 
						tempPrice, competitiveDataDTO.regMPrice, "", new PRRange(), new PRRange());
				//Out of check date
				if (tempPrice > 0) {
					if (minPrice == 0) {
						minPrice = tempPrice;
					} else {
						if (tempPrice < minPrice) {
							minPrice = tempPrice;
						}
					}
				}
			}else{
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), "", 0, 0, 0, "", new PRRange(), new PRRange());
			}*/
		}
		return minPrice;
	}
	
	private double findAvgPrice(PRStrategyDTO strategyDTO, PRGuidelineComp compGuideline, List<PRGuidelineCompDetail> compDetails) throws Exception{
		double avgPrice = 0d;
		double avgCount = 0d;
		double totalPrice = 0d;
		//MultiCompetitorKey multiCompetitorKey;
		for (PRGuidelineCompDetail guidelineCompDetail : compDetails) {
			
			LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guidelineCompDetail.getCompStrId());
			
			if(inputItem.getAllCompPrice().get(locationKey) != null){
				MultiplePrice compRegPrice = inputItem.getAllCompPrice().get(locationKey);
				
				String checkDate = inputItem.getAllCompPriceCheckDate().get(locationKey);
				
				
				double compUnitPrice = getValidCompPrice(compRegPrice, guidelineCompDetail.getCompStrId(),
						compGuideline.getLatestPriceObservationDays(), checkDate);
				
				boolean isCompRetailBelowCost = false;
				if (compGuideline.isIgnoreCompBelowCost() && compUnitPrice > 0) {
					// Compare against future cost if present
					double listCost = setCost();
					isCompRetailBelowCost = compUnitPrice < listCost;
				}
				
				PRRange range = new PRRange();
				double adjustedPrice = compUnitPrice;
				if (compUnitPrice > 0) {
					if (guidelineCompDetail.getValueType() == PRConstants.VALUE_TYPE_$) {
						adjustedPrice = getAdjustedPriceFor$Type(guidelineCompDetail, compUnitPrice, range);
					} else if (guidelineCompDetail.getValueType() == PRConstants.VALUE_TYPE_PCT) {
						adjustedPrice = getAdjustedPriceForPCTType(guidelineCompDetail, compUnitPrice, range);
					}
				}

				writeCompDetailLog(guidelineCompDetail.getCompStrId(), checkDate, compRegPrice.multiple, compUnitPrice,
						compRegPrice.multiple, guidelineCompDetail.getGuidelineText(), range, range,
						isCompRetailBelowCost);
				
				//Out of check date
				if(adjustedPrice > 0 && !isCompRetailBelowCost){
					totalPrice = totalPrice + adjustedPrice;
					avgCount = avgCount + 1;
				}
				
			}else{
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), "", 0, 0, 0, "", new PRRange(), new PRRange(), false);
			}
			
			
			/*multiCompetitorKey = new MultiCompetitorKey(guidelineCompDetail.getCompStrId(), inputItem.getItemCode());
			//Get item price from map
			if (this.multiCompLatestPriceMap.get(multiCompetitorKey) != null) {
				CompetitiveDataDTO competitiveDataDTO = multiCompLatestPriceMap.get(multiCompetitorKey);
				double tempPrice = getPriceInLastObsDay(strategyDTO, competitiveDataDTO, compGuideline.getLatestPriceObservationDays());
				writeCompDetailLog(competitiveDataDTO.compStrId, competitiveDataDTO.checkDate, competitiveDataDTO.regMPack,
						tempPrice, competitiveDataDTO.regMPrice, "", new PRRange(), new PRRange());
				//Out of check date
				if(tempPrice > 0){
					totalPrice = totalPrice + tempPrice;
					avgCount = avgCount + 1;
				}
			}else{
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), "", 0, 0, 0, "", new PRRange(), new PRRange());
			}*/
		}
		avgPrice = ((avgCount > 1) ? (totalPrice/avgCount) : totalPrice);
		return avgPrice;
	}
	
	private PRRange getPriceRangeWithNoValueType(double itemPrice) {
		PRRange range = new PRRange();
		double price = itemPrice;
		PRGuidelineComp compGuideline = this.inputItem.getStrategyDTO().getGuidelines().getCompGuideline();
		
		if (itemPrice > 0) {
			if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(compGuideline.getRelationalOperatorText())) {
				range.setEndVal(price);
			} else if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM
					.equals(compGuideline.getRelationalOperatorText())) {
				range.setStartVal(price);
				range.setEndVal(price);
			} else {
				if (price - 0.01 <= 0) {
					//logger.debug("Reduce by pct");
					double pct = Double.parseDouble(PropertyManager.getProperty("PR_LESSER_PCT_DIFF", "1"));
					range.setEndVal(price - (price * pct / 100));
				} else
					range.setEndVal((price - 0.01));
			}
		}
		return range;		 
	}
	
	
	/**
	 * 
	 * @param compRegPrice
	 * @param compStrId
	 * @param obsDay
	 * @param checkDate
	 * @return valid comp price
	 */
	private double getValidCompPrice(MultiplePrice compRegPrice, int compStrId, int obsDay, String checkDate) {

		PriceIndexCalculation pic = new PriceIndexCalculation();
		MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(inputItem);
		MultiplePrice compPrice = compRegPrice;
		double compUnitPrice = 0d;
		if (this.curWeekStartDate != null && checkDate != null) {
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);

			LocalDate itemCheckDate = LocalDate.parse(checkDate, formatter);

			LocalDate curWeekDate = LocalDate.parse(this.curWeekStartDate, formatter);

			LocalDate lastObsDate = curWeekDate.minus(obsDay, ChronoUnit.DAYS);

			if (lastObsDate.isEqual(itemCheckDate) || lastObsDate.isBefore(itemCheckDate)) {
				// 9th March 2017, handle bad comp check, ignore bad comp checks
				if (!pic.isBadCompCheck(curRegPrice, compPrice, minBadIndex, maxBadIndex)) {
					
					compUnitPrice = compRegPrice.price;
					if (compRegPrice.multiple > 1) {
						compUnitPrice = compRegPrice.price / compRegPrice.multiple;
					}

				} else {
					logger.debug("Comp price: " + (compPrice != null ? compPrice.toString() : "") + " of item:"
							+ inputItem.getItemCode() + " of comp store: " + compStrId
							+ " is ignored as it is a bad comp check");
				}
			}else{
				logger.debug("Start Date is not in range: last Obs date: " + lastObsDate.toString() + " check date: "
						+ itemCheckDate.toString());
			}
		} else {
			logger.debug("Start Date or Check Date is null, Item Code: " + this.inputItem.getItemCode());
		}

		return compUnitPrice;
	}
	
	/*private double getPriceInLastObsDay(PRStrategyDTO strategyDTO, CompetitiveDataDTO competitiveDataDTO, 
			int obsDay) throws Exception{
		Date currentWeekStartDate, itemCheckDate;
		double regUnitPrice = 0d;
		PriceIndexCalculation pic = new PriceIndexCalculation();
		
		try {
			if (this.curWeekStartDate != null && competitiveDataDTO.checkDate != null) {
				//strategyStartDate = new SimpleDateFormat("MM/dd/yyyy").parse(strategyDTO.getStartDate());
				//Date itemCheckDate = new SimpleDateFormat("MM/dd/yyyy").parse(competitiveDataDTO.checkDate);
				
				currentWeekStartDate = DateUtil.toDate(this.curWeekStartDate);
				itemCheckDate = DateUtil.toDate(competitiveDataDTO.checkDate);
				Calendar cal = Calendar.getInstance();
				cal.setTime(currentWeekStartDate);
				cal.add(Calendar.DATE, -(obsDay));
				Date lastObsDay = cal.getTime();

				// If check date is before last observation day or it is equal
				// (include both days, strategy first day and last obs day)
				if (itemCheckDate.after(lastObsDay) || itemCheckDate.equals(lastObsDay)) {
//					regUnitPrice = (double) (competitiveDataDTO.regMPack > 1 ? (competitiveDataDTO.regMPrice / competitiveDataDTO.regMPack)
//							: competitiveDataDTO.regPrice);			
					
					MultiplePrice curRegPrice = PRCommonUtil.getCurRegPrice(inputItem);
					MultiplePrice compPrice = PRCommonUtil.getMultiplePrice(competitiveDataDTO.regMPack, (double) competitiveDataDTO.regPrice,
							(double) competitiveDataDTO.regMPrice);
					
					//9th March 2017, handle bad comp check, ignore bad comp checks
					if (!pic.isBadCompCheck(curRegPrice, compPrice, minBadIndex, maxBadIndex)) {
						regUnitPrice = (double) (competitiveDataDTO.regMPack > 1 ? (competitiveDataDTO.regMPrice / competitiveDataDTO.regMPack)
								: competitiveDataDTO.regPrice);			
						
					} else {
						logger.info("Comp price: " + (compPrice != null ? compPrice.toString() : "") + " of item:" + inputItem.getItemCode()
								+ " of comp store: " + competitiveDataDTO.compStrId + " is ignored as it is a bad comp check");
					}
				}	
				
			}else{
				logger.debug("Start Date or Check Date is null : Strategy Id: " + strategyDTO.getStrategyId() + "Item Code: " + this.inputItem.getItemCode());
			}
			
		} catch (GeneralException e) {
			logger.error("Exception in getPriceInLastObsDay() -- " + e.toString(), e);
			throw new Exception();
		}
		return regUnitPrice;
	}*/
	
	private void writeCompDetailLog(int compStrId, String checkDate, int regMPack, double regUnitPrice, double regMPrice,
			String guidelineText, PRRange guidelineRange, PRRange finalRange, boolean isCompRetailBelowCost){
		//Set logs
//		DecimalFormat format = new DecimalFormat("######.##"); 
		PRCompDetailLog prCompDetailLog = new PRCompDetailLog();
		prCompDetailLog.setCompStrId(compStrId);	
		prCompDetailLog.setIsPricePresent(false);
		if(regUnitPrice > 0){
			prCompDetailLog.setCheckDate(checkDate);
			MultiplePrice multiplePrice = PRCommonUtil.getMultiplePrice(regMPack, regUnitPrice, regMPrice);
			//prCompDetailLog.setRegPrice(format.format(regUnitPrice));
			prCompDetailLog.setMultiple(String.valueOf(multiplePrice.multiple));
			prCompDetailLog.setRegPrice(String.valueOf(multiplePrice.price));
			prCompDetailLog.setIsPricePresent(true);
		}
		prCompDetailLog.setMaxRetailText(guidelineText);
		prCompDetailLog.setGuidelineRange(guidelineRange);
		prCompDetailLog.setFinalRange(finalRange);
		prCompDetailLog.setCompRetailBelowCost(
				isCompRetailBelowCost ? String.valueOf(Constants.YES) : String.valueOf(Constants.NO));
		guidelineAndConstraintLog.getCompDetails().add(prCompDetailLog);
	}
	
	private PRRange applyCompRule(PRStrategyDTO strategyDTO, PRGuidelineComp compGuideline, 
			List<PRGuidelineCompDetail> compDetails) throws Exception {
		PRRange priceRange = null;
		PRRange filteredPriceRange = new PRRange();
		//MultiCompetitorKey multiCompetitorKey;
		
		// Loop each competitor guideline
		for (PRGuidelineCompDetail guidelineCompDetail : compDetails) {
			//multiCompetitorKey = new MultiCompetitorKey(guidelineCompDetail.getCompStrId(), inputItem.getItemCode());
			
			LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guidelineCompDetail.getCompStrId());
			
			if(inputItem.getAllCompPrice().get(locationKey) != null){
				
				MultiplePrice compRegPrice = inputItem.getAllCompPrice().get(locationKey);
				
				String checkDate = inputItem.getAllCompPriceCheckDate().get(locationKey);
				
				double compUnitPrice = getValidCompPrice(compRegPrice, guidelineCompDetail.getCompStrId(),
						compGuideline.getLatestPriceObservationDays(), checkDate);
				
				boolean isCompRetailBelowCost = false;
				if(compGuideline.isIgnoreCompBelowCost() && compUnitPrice > 0) {
					double listCost=setCost();
					isCompRetailBelowCost = compUnitPrice < listCost;	
				}
				
				if(!isCompRetailBelowCost) {
					// Get Price Range from each competitor's price
					priceRange = getCompPriceRange(guidelineCompDetail, compUnitPrice);	
				} else {
					priceRange = new PRRange();
				}

				/*
				 * logger.debug("***Comp price: " + compUnitPrice);
				 * 
				 * logger.debug("Price Range for store: " + guidelineCompDetail.toString() +
				 * " ,Price Range:" + priceRange.toString());
				 */ 
				 
				filteredPriceRange = filterPriceRange.filterRange(filteredPriceRange, priceRange);
				  
				//logger.debug("Filtered Range:" + filteredPriceRange.toString());
				 
				
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), checkDate, compRegPrice.multiple, compUnitPrice,
						compRegPrice.price, guidelineCompDetail.getGuidelineText(), priceRange, filteredPriceRange,
						isCompRetailBelowCost);
			}else{
				
				/*
				 * logger.debug("***No Comp price found: " + guidelineCompDetail.getCompStrId()
				 * + "Item code: " + inputItem.getItemCode());
				 */
				
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), "", 0, 0, 0, guidelineCompDetail.getGuidelineText(), 
						new PRRange(), filteredPriceRange, false);
			}
			
			
			/*// Get item price from map
			if (this.multiCompLatestPriceMap.get(multiCompetitorKey) != null) {
				CompetitiveDataDTO competitiveDataDTO = multiCompLatestPriceMap.get(multiCompetitorKey);
				double compPrice = getPriceInLastObsDay(strategyDTO, competitiveDataDTO, compGuideline.getLatestPriceObservationDays());
				// Get Price Range from each competitor's price
				priceRange = getCompPriceRange(guidelineCompDetail, compPrice);
				logger.debug("Price Range for store: " + guidelineCompDetail.toString() + 
						" ,Price Range:" + priceRange.toString());
				filteredPriceRange = filterPriceRange.filterRange(filteredPriceRange, priceRange);
				logger.debug("Filtered Range:" + filteredPriceRange.toString());
				writeCompDetailLog(competitiveDataDTO.compStrId, competitiveDataDTO.checkDate, competitiveDataDTO.regMPack,
						compPrice, competitiveDataDTO.regMPrice, guidelineCompDetail.getGuidelineText(), priceRange, filteredPriceRange);
			} else {
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), "", 0, 0, 0, guidelineCompDetail.getGuidelineText(), 
						new PRRange(), filteredPriceRange);
			}*/
		}

		return filteredPriceRange;
	}
	
	private PRRange getCompPriceRange(PRGuidelineCompDetail guidelineCompDetail, double compPrice) {
		PRRange range = new PRRange();
		if(compPrice > 0){
			if (PRConstants.VALUE_TYPE_PCT == guidelineCompDetail.getValueType()) {
				range = getCompPriceRangePCT(guidelineCompDetail, compPrice);
			} else if (PRConstants.VALUE_TYPE_$ == guidelineCompDetail.getValueType()) {
				range = getCompPriceRange$(guidelineCompDetail, compPrice);			
			} else{
				range = getCompPriceRangeNoValueType(guidelineCompDetail, compPrice);
			}
		}
		return range;
	}
	
	private PRRange getCompPriceRangePCT(PRGuidelineCompDetail guidelineCompDetail, double compPrice) {
		PRRange range = new PRRange();
		double minValue = guidelineCompDetail.getMinValue();
		double maxValue = guidelineCompDetail.getMaxValue();
		String operatorText = guidelineCompDetail.getRelationalOperatorText();
		// >, >=, above, <, <=, below, lower, not >, within
		CompGuidelineAndConstraintService.getCompPricePctRange(operatorText, minValue, maxValue, compPrice, range);
		
		// Below logic is commentd. it is moved to common class as it is required in multiple places
		// Changes done by Pradeep on 12/04/2019 for implementing Guardrail constraint
		/*if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + (compPrice * (minValue / 100)));
				range.setEndVal(compPrice + (compPrice * (maxValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + (compPrice * (minValue / 100)));
				if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText))
					range.setEndVal(compPrice + (compPrice * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA){
				range.setStartVal(compPrice);
				if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText))
					range.setEndVal(compPrice + (compPrice * (maxValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText) || PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (maxValue / 100)));
				range.setEndVal(compPrice - (compPrice * (minValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice - (compPrice * (minValue / 100)));
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
					range.setStartVal(compPrice - (compPrice * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA){
				range.setEndVal(compPrice);
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
				range.setStartVal(compPrice - (compPrice * (maxValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText) || 
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + (compPrice * (minValue / 100)));
				range.setEndVal(compPrice + (compPrice * (maxValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice + (compPrice * (minValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText) || 
				PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (maxValue / 100)));
				range.setEndVal(compPrice - (compPrice * (minValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - (compPrice * (minValue / 100)));
			}
		}else if (PRConstants.PRICE_GROUP_EXPR_WITHIN.equals(operatorText)) {
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
		}*/
		return range;
	}
		
	private PRRange getCompPriceRange$(PRGuidelineCompDetail guidelineCompDetail, double compPrice) {
		PRRange range = new PRRange();
		double minValue = guidelineCompDetail.getMinValue();
		double maxValue = guidelineCompDetail.getMaxValue();
		String operatorText = guidelineCompDetail.getRelationalOperatorText();
		// >, >=, above, <, <=, below, lower, not >, within
		CompGuidelineAndConstraintService.getCompPriceRange$(operatorText, minValue, maxValue, compPrice, range);
		
		// Below logic is commentd. it is moved to common class as it is required in multiple places
		// Changes done by Pradeep on 12/04/2019 for implementing Guardrail constraint
		/*if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
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
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText) || PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - maxValue);
				range.setEndVal(compPrice - minValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice - minValue);
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
					range.setStartVal(compPrice - minValue);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText) || 
				PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice + minValue);
				range.setEndVal(compPrice + maxValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(compPrice + minValue);
			}
		}else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText) || 
				PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - maxValue);
				range.setEndVal(compPrice - minValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(compPrice - minValue);
			}
		}
		else if (PRConstants.PRICE_GROUP_EXPR_WITHIN.equals(operatorText)) {
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
		}*/

		return range;
	}

	private PRRange getCompPriceRangeNoValueType(PRGuidelineCompDetail guidelineCompDetail, double compPrice) {
		PRRange range = new PRRange();
		String operatorText = guidelineCompDetail.getRelationalOperatorText();
		// >, >=, =, equal, above, below, <, lower, <=, not >

		CompGuidelineAndConstraintService.getCompPriceRangeWithNoValueType(operatorText, compPrice, range);
		
		// Below logic is commentd. it is moved to common class as it is required in multiple places
		// Changes done by Pradeep on 12/04/2019 for implementing Guardrail constraint
		/*if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)) {
			range.setStartVal((compPrice + 0.01));
		} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
			range.setStartVal(compPrice);
		} else if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(operatorText) || PRConstants.PRICE_GROUP_EXPR_EQUAL.equals(operatorText)) {
			range.setStartVal(compPrice);
			range.setEndVal(compPrice);
		} else if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			range.setStartVal((compPrice + 0.01));
			range.setEndVal((compPrice + 0.01));
		} else if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText) || PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText) || PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText) ) {
			if (compPrice - 0.01 <= 0) {
				double pct = Double.parseDouble(PropertyManager.getProperty("PR_LESSER_PCT_DIFF", "1"));
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))
					range.setStartVal(compPrice - (compPrice * pct/100));
				range.setEndVal(compPrice - (compPrice * pct / 100));
			} else{
				if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText))				
					range.setStartVal((compPrice - 0.01));	
				range.setEndVal((compPrice - 0.01));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)				
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			range.setEndVal(compPrice);
		} */

		return range;
	}
	
	/**
	 * 
	 * @param compGuideline
	 * @param guidelineCompDetail
	 * @param compUnitPrice
	 * @return price adjusted by comp guideline
	 */
	private double getAdjustedPriceFor$Type(PRGuidelineCompDetail guidelineCompDetail, double compUnitPrice, PRRange range) {
		double adjustedRetail = compUnitPrice;
		String operatorText = guidelineCompDetail.getRelationalOperatorText();
		double minValue = guidelineCompDetail.getMinValue();
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA) {
				adjustedRetail = adjustedRetail + minValue;
				range.setStartVal(adjustedRetail);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA) {
				adjustedRetail = adjustedRetail - minValue;
				range.setEndVal(adjustedRetail);
			}
		}else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA) {
				adjustedRetail = adjustedRetail + (compUnitPrice * (minValue / 100));
				range.setEndVal(adjustedRetail);
				
			}
		}else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)){
			if (minValue != Constants.DEFAULT_NA) {
				adjustedRetail = adjustedRetail - (compUnitPrice * (minValue / 100));
				range.setStartVal(adjustedRetail);
			}
		}else if (PRConstants.PRICE_GROUP_EXPR_WITHIN.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(adjustedRetail - (compUnitPrice * (minValue / 100)));
				range.setEndVal(adjustedRetail + (compUnitPrice * (minValue / 100)));
			}
		}
		return adjustedRetail;
	}
	
	/**
	 * 
	 * @param compGuideline
	 * @param guidelineCompDetail
	 * @param compUnitPrice
	 * @return price adjusted by comp guideline
	 */
	private double getAdjustedPriceForPCTType(PRGuidelineCompDetail guidelineCompDetail, double compUnitPrice, PRRange range) {
		double adjustedRetail = compUnitPrice;
		String operatorText = guidelineCompDetail.getRelationalOperatorText();
		double minValue = guidelineCompDetail.getMinValue();
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA) {
				adjustedRetail = adjustedRetail + (compUnitPrice * (minValue / 100));
				range.setStartVal(adjustedRetail);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA) {
				adjustedRetail = adjustedRetail - (compUnitPrice * (minValue / 100));
				range.setEndVal(adjustedRetail);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA) {
				
				adjustedRetail = adjustedRetail + (compUnitPrice * (minValue / 100));
				range.setEndVal(adjustedRetail);
				
			}
		}else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)){
			if (minValue != Constants.DEFAULT_NA) {
				adjustedRetail = adjustedRetail - (compUnitPrice * (minValue / 100));
				range.setStartVal(adjustedRetail);
			}
		}else if (PRConstants.PRICE_GROUP_EXPR_WITHIN.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(adjustedRetail - (compUnitPrice * (minValue / 100)));
				range.setEndVal(adjustedRetail + (compUnitPrice * (minValue / 100)));
			}
		}
		return adjustedRetail;
	}
	 
	/**
	 * Added by Karishma on 06/14/2021 to check  compPrice against future cost if present and only if there is a cost change
	 * @return
	 */
	public double setCost() {
		double finalCost = 0;
		//Added existing property check to the function because future cost functionality/use is applicable for RA only and not FF
		//For FF future cost is for display purpose
		if (chkFutureCost && inputItem.getFutureListCost()!=null && inputItem.getFutureListCost() > 0  && inputItem.getCostChgIndicator()!=0)
			finalCost = inputItem.getFutureListCost();
		// condition added to use this cost for AZ on 11/17 as list cost has freight charge added
		// in it
		else if (inputItem.getListCostWtotFrChg() > 0) {
			finalCost = inputItem.getListCostWtotFrChg();
		} else

			finalCost = inputItem.getListCost();
		return finalCost;
	}
	
	/**
	 * Added for AZ enhancement on 07/07/2021
	 * @param strategyDTO
	 * @param compGuideline
	 * @param compDetails
	 * @return
	 * @throws Exception
	 */
	private double findMaxPrice(PRStrategyDTO strategyDTO, PRGuidelineComp compGuideline, List<PRGuidelineCompDetail> compDetails) throws Exception{
		double maxPrice = 0d;
		//MultiCompetitorKey multiCompetitorKey;
		for (PRGuidelineCompDetail guidelineCompDetail : compDetails) {
			//multiCompetitorKey = new MultiCompetitorKey(guidelineCompDetail.getCompStrId(), inputItem.getItemCode());

			LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guidelineCompDetail.getCompStrId());
			
			if(inputItem.getAllCompPrice().get(locationKey) != null){
				
				MultiplePrice compRegPrice = inputItem.getAllCompPrice().get(locationKey);
				
				String checkDate = inputItem.getAllCompPriceCheckDate().get(locationKey);
				
				double compUnitPrice = getValidCompPrice(compRegPrice, guidelineCompDetail.getCompStrId(),
						compGuideline.getLatestPriceObservationDays(), checkDate);
				
				boolean isCompRetailBelowCost = false;
				if (compGuideline.isIgnoreCompBelowCost() && compUnitPrice > 0) {
					double listCost = setCost();
					isCompRetailBelowCost = compUnitPrice < listCost;
				}
				
				PRRange range = new PRRange();
				double adjustedPrice = compUnitPrice;
				if (compUnitPrice > 0) {
					if (guidelineCompDetail.getValueType() == PRConstants.VALUE_TYPE_$) {
						adjustedPrice = getAdjustedPriceFor$Type(guidelineCompDetail, compUnitPrice, range);
					} else if (guidelineCompDetail.getValueType() == PRConstants.VALUE_TYPE_PCT) {
						adjustedPrice = getAdjustedPriceForPCTType(guidelineCompDetail, compUnitPrice, range);
					}
				}
				
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), checkDate, compRegPrice.multiple, compUnitPrice,
						compRegPrice.multiple, guidelineCompDetail.getGuidelineText(), range, range,
						isCompRetailBelowCost);
				
				//Out of check date
				if (adjustedPrice > 0 && !isCompRetailBelowCost) {
					if (maxPrice == 0) {
						maxPrice = adjustedPrice;
					} else {
						if (adjustedPrice > maxPrice) {
							maxPrice = adjustedPrice;
						}
					}
				}
				
			}else{
				writeCompDetailLog(guidelineCompDetail.getCompStrId(), "", 0, 0, 0, "", new PRRange(), new PRRange(), false);
			}
			
		}
		return maxPrice;
	}

	
}
