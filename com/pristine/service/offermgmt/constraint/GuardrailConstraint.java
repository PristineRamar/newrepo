package com.pristine.service.offermgmt.constraint;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRConstraintGuardRailDetail;
import com.pristine.dto.offermgmt.PRConstraintGuardrail;
import com.pristine.dto.offermgmt.PRExplainLog;
import com.pristine.dto.offermgmt.PRGuardRailLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintLog;
import com.pristine.dto.offermgmt.PRGuidelineAndConstraintOutput;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.service.offermgmt.ConstraintTypeLookup;
import com.pristine.service.offermgmt.FilterPriceRange;
import com.pristine.service.offermgmt.GurdrailConstraintService;
import com.pristine.service.offermgmt.PriceIndexCalculation;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class GuardrailConstraint {

	private static Logger logger = Logger.getLogger("GuardRailConstraint");
	private PRItemDTO inputItem;
	private PRRange inputPriceRange;
	private PRExplainLog explainLog;
	String curWeekStartDate;
	@SuppressWarnings("unused")
	private PRRange compRange;
	FilterPriceRange filterPriceRange = new FilterPriceRange();
	PRGuidelineAndConstraintLog guidelineAndConstraintLog = new PRGuidelineAndConstraintLog();

	double minBadIndex = Double.parseDouble(PropertyManager.getProperty("MIN_BAD_INDEX", "0"));
	double maxBadIndex = Double.parseDouble(PropertyManager.getProperty("MAX_BAD_INDEX", "0"));
	boolean chkFutureCost = Boolean.parseBoolean(PropertyManager.getProperty("USE_FUTURECOST", "FALSE"));

	public GuardrailConstraint(PRItemDTO inputItem, PRRange inputPriceRange, PRExplainLog explainLog,
			String curWeekStartDate, PRRange compRange) {
		this.inputItem = inputItem;
		this.inputPriceRange = inputPriceRange;
		this.explainLog = explainLog;
		this.curWeekStartDate = curWeekStartDate;
		this.compRange = compRange;
		guidelineAndConstraintLog.setConstraintTypeId(ConstraintTypeLookup.GUARD_RAIL.getConstraintTypeId());
	}

	public PRGuidelineAndConstraintOutput applyGuardrailConstraint() throws Exception {
		return applyGuardrailConstraint(false);
	}

	private PRGuidelineAndConstraintOutput applyGuardrailConstraint(boolean passInputRangeAsOutputRange)
			throws Exception {
		PRGuidelineAndConstraintOutput guidelineAndConstraintOutput = new PRGuidelineAndConstraintOutput();
		guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
		PRRange guardrailRange = null;

		// If no competitor found
		if (inputItem.getStrategyDTO().getConstriants().getGuardrailConstraint().getCompetitorDetails().size() == 0) {
			guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;
			// guidelineAndConstraintLog.setIsConflict(false);
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(false);
			if (inputItem.getStrategyDTO().getConstriants().getGuardrailConstraint().isZonePresent())
				guidelineAndConstraintLog.setMessage("No Competitor Found");
			guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
			//logger.debug("No Multi Competitor Found - " + inputItem.getItemCode());
		} else {
			// No Price found
			boolean isPriceFound = false;
			boolean isRegpriceFound = false;
			boolean isZonePresent = inputItem.getStrategyDTO().getConstriants().getGuardrailConstraint()
					.isZonePresent();
			List<PRConstraintGuardRailDetail> compDetails = inputItem.getStrategyDTO().getConstriants()
					.getGuardrailConstraint().getCompetitorDetails();

			for (PRConstraintGuardRailDetail guidelineCompDetail : compDetails) {
				if (isZonePresent) {
					int zoneID = guidelineCompDetail.getPriceZoneID();

					if (inputItem.getZonePriceMap() != null) {
						/*
						 * logger.debug( "applyGuardrailConstraint()- zoneid " + zoneID + " for:" +
						 * inputItem.getItemCode());
						 */
						HashMap<Integer, Double> zonePriceMap = inputItem.getZonePriceMap();
						if (zonePriceMap.containsKey(zoneID)) {
							//logger.info("applyGuardrailConstraint()-  regpriceFound :" + zonePriceMap.get(zoneID));
							isRegpriceFound = true;
						}
					}

				}
				LocationKey locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guidelineCompDetail.getCompStrId());
				if (inputItem.getAllCompPrice().get(locationKey) != null) {
					MultiplePrice compRegPrice = inputItem.getAllCompPrice().get(locationKey);
					if (compRegPrice.price > 0) {
						isPriceFound = true;
						break;
					}
				}
			}

			// No Price found
			if (!isPriceFound && !isZonePresent) {
				guidelineAndConstraintOutput.outputPriceRange = inputPriceRange;

				guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(false);
				guidelineAndConstraintLog.setMessage("No Competition Price");
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
				//logger.debug("No Comp Price data found for item code - " + inputItem.getItemCode());
			}
			if (isZonePresent && !isRegpriceFound) {
				guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(false);
				guidelineAndConstraintLog.setMessage("No Recommended Price");
				guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
				//logger.debug("No ReccPrice data found for item code - " + inputItem.getItemCode());

			}
			if (isPriceFound || (isZonePresent && isRegpriceFound)) {
				guardrailRange = applyGuardRailConstraint(inputItem,
						inputItem.getStrategyDTO().getConstriants().getGuardrailConstraint());
			}
		}

		if (guardrailRange != null) {
			// logger.info("compRange:" + compRange + "guardrailRange: "+ guardrailRange);
			/*
			 * if (compRange != null) guidelineAndConstraintOutput.outputPriceRange =
			 * filterPriceRange.filterRange(compRange, guardrailRange); else
			 */
			guidelineAndConstraintOutput.outputPriceRange = filterPriceRange.filterRange(guardrailRange,
					inputPriceRange);

			// logger.info("outputRange:" + guidelineAndConstraintOutput.outputPriceRange);
			guidelineAndConstraintLog.setIsGuidelineOrConstraintApplied(true);
			guidelineAndConstraintLog.setGuidelineOrConstraintPriceRange1(guardrailRange);
			guidelineAndConstraintLog.setOutputPriceRange(guidelineAndConstraintOutput.outputPriceRange);
		}

		explainLog.getGuidelineAndConstraintLogs().add(guidelineAndConstraintLog);
		logger.debug("Guardrail Constraint -- " + "Is Applied: "
				+ guidelineAndConstraintLog.getIsGuidelineOrConstraintApplied() + ",Input Range: "
				+ inputPriceRange.toString() + ",MultiGuardrail Range: "
				+ (guardrailRange != null ? guardrailRange.getStartVal() + ","+ guardrailRange.getEndVal() : "- -")
				+ ",Output Range: " + guidelineAndConstraintOutput.outputPriceRange.toString());
		return guidelineAndConstraintOutput;
	}

	private PRRange applyGuardRailConstraint(PRItemDTO inputItem, PRConstraintGuardrail guardrailConstraint)
			throws Exception {
		PRRange range = new PRRange();

		if (inputItem.getAllCompPrice() != null || inputItem.getZonePriceMap().size() > 0) {

			List<PRConstraintGuardRailDetail> guradrailDetails = guardrailConstraint.getCompetitorDetails();
			double groupRepPrice = 0d;
			// If min or average
			if (guardrailConstraint.getGroupPriceType() == PRConstants.GROUP_PRICE_TYPE_AVG) {
				groupRepPrice = findAvgPrice(guardrailConstraint, guradrailDetails);
				range = getPriceRangeWithNoValueType(groupRepPrice);
			} else if (guardrailConstraint.getGroupPriceType() == PRConstants.GROUP_PRICE_TYPE_MIN) {
				groupRepPrice = findMinPrice(guardrailConstraint, guradrailDetails);
				range = getPriceRangeWithNoValueType(groupRepPrice);
			} else {
				range = applyCompRule(guardrailConstraint, guradrailDetails);
			}

		}

		return range;
	}

	private PRRange applyCompRule(PRConstraintGuardrail guardrailConstraint,
			List<PRConstraintGuardRailDetail> guradrailDetails) throws Exception {
		PRRange priceRange = null;
		PRRange filteredPriceRange = new PRRange();
		// MultiCompetitorKey multiCompetitorKey;

		// Loop each guardRail constraint
		for (PRConstraintGuardRailDetail guardRailDetail : guradrailDetails) {

			LocationKey locationKey = null;
			int priceZoneId = 0;
			double compUnitPrice = 0;
			MultiplePrice compRegPrice = null;
			String checkDate = "";
			boolean isZonePresent = false;
			if (guardRailDetail.getCompStrId() != 0) {
				locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guardRailDetail.getCompStrId());
			}

			priceZoneId = guardRailDetail.getPriceZoneID();

			HashMap<Integer, Double> getZonePriceMap = inputItem.getZonePriceMap();
			if (getZonePriceMap != null) {
				if (inputItem.getZonePriceMap().get(priceZoneId) != null) {
					isZonePresent = true;
				}
			}

			if (inputItem.getAllCompPrice().get(locationKey) != null || isZonePresent) {

				if (locationKey != null) {

					compRegPrice = inputItem.getAllCompPrice().get(locationKey);

					checkDate = inputItem.getAllCompPriceCheckDate().get(locationKey);

					compUnitPrice = getValidCompPrice(compRegPrice, guardRailDetail.getCompStrId(),
							guardrailConstraint.getLatestPriceObservationDays(), checkDate);
				} else if (priceZoneId > 0) {
					compUnitPrice = inputItem.getZonePriceMap().get(priceZoneId);

				}

				boolean isCompRetailBelowCost = false;
				if (guardrailConstraint.isIgnoreCompBelowCost() && compUnitPrice > 0) {
					double listCost = setCost();
					isCompRetailBelowCost = compUnitPrice < listCost;
				}

				if (!isCompRetailBelowCost) {
					// Get Price Range from each competitor's price
					priceRange = getGuardrailRange(guardRailDetail, compUnitPrice);
				} else {
					priceRange = new PRRange();
				}

				filteredPriceRange = filterPriceRange.filterRange(filteredPriceRange, priceRange);
				//logger.debug("Filtered Range:" + filteredPriceRange.toString());

				if (priceZoneId > 0) {
					writeguardrailDetailLog(priceZoneId, "", 1, compUnitPrice, compUnitPrice,
							guardRailDetail.getGuidelineText(), priceRange, filteredPriceRange, isCompRetailBelowCost);

				} else {
					writeguardrailDetailLog(guardRailDetail.getCompStrId(), checkDate, compRegPrice.multiple,
							compUnitPrice, compRegPrice.price, guardRailDetail.getGuidelineText(), priceRange,
							filteredPriceRange, isCompRetailBelowCost);
				}

			} else {

				if (priceZoneId > 0) {

					writeguardrailDetailLog(priceZoneId, "", 0, 0, 0, "", new PRRange(), new PRRange(), false);
				} else {

					/*
					 * logger.debug("***No Comp price found: " + guardRailDetail.getCompStrId() +
					 * "Item code: " + inputItem.getItemCode());
					 */

					writeguardrailDetailLog(guardRailDetail.getCompStrId(), "", 0, 0, 0,
							guardRailDetail.getGuidelineText(), new PRRange(), filteredPriceRange, false);
				}
			}
		}

		return filteredPriceRange;
	}

	private PRRange getGuardrailRange(PRConstraintGuardRailDetail constraintGuardRailComp, double compPrice) {
		PRRange range = new PRRange();
		if (compPrice > 0) {
			if (PRConstants.VALUE_TYPE_PCT == constraintGuardRailComp.getValueType()) {
				range = getGuardrailRangePCT(constraintGuardRailComp, compPrice);
			} else if (PRConstants.VALUE_TYPE_$ == constraintGuardRailComp.getValueType()) {
				range = getGuardrailRange$(constraintGuardRailComp, compPrice);
			} else {
				range = getGuardrailRangeNoValueType(constraintGuardRailComp, compPrice);
			}
		}
		return range;
	}

	private PRRange getGuardrailRangeNoValueType(PRConstraintGuardRailDetail constraintGuardRail, double compPrice) {
		PRRange range = new PRRange();
		String operatorText = constraintGuardRail.getRelationalOperatorText();
		// >, >=, =, equal, above, below, <, lower, <=, not >
		GurdrailConstraintService.getGuardrailRangeWithNoValueType(operatorText, compPrice, range);
		return range;
	}

	private PRRange getGuardrailRange$(PRConstraintGuardRailDetail constraintGuardRail, double compPrice) {
		PRRange range = new PRRange();
		double value = constraintGuardRail.getMinValue();
		/* double maxValue = constraintGuardRail.getMaxValue(); */
		String operatorText = constraintGuardRail.getRelationalOperatorText();
		// >, >=, above, <, <=, below, lower, not >, within
		GurdrailConstraintService.getGuardrailRange$(operatorText, value, compPrice, range);
		return range;
	}

	private double findAvgPrice(PRConstraintGuardrail guardrailConstraint,
			List<PRConstraintGuardRailDetail> guradrailDetails) {

		double avgPrice = 0d;
		double avgCount = 0d;
		double totalPrice = 0d;
		PRRange priceRange = null;

		// MultiCompetitorKey multiCompetitorKey;
		for (PRConstraintGuardRailDetail guardrailDetail : guradrailDetails) {
			PRRange filteredPriceRange = new PRRange();

			LocationKey locationKey = null;
			int priceZoneId = 0;
			double compUnitPrice = 0;
			MultiplePrice compRegPrice = null;
			String checkDate = "";
			boolean isZonePresent = false;
			if (guardrailDetail.getCompStrId() != 0) {
				locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guardrailDetail.getCompStrId());
			}

			priceZoneId = guardrailDetail.getPriceZoneID();

			HashMap<Integer, Double> getZonePriceMap = inputItem.getZonePriceMap();
			if (getZonePriceMap != null) {
				if (inputItem.getZonePriceMap().get(priceZoneId) != null) {
					isZonePresent = true;
				}
			}

			if (inputItem.getAllCompPrice().get(locationKey) != null || isZonePresent) {

				if (locationKey != null) {

					compRegPrice = inputItem.getAllCompPrice().get(locationKey);

					checkDate = inputItem.getAllCompPriceCheckDate().get(locationKey);

					compUnitPrice = getValidCompPrice(compRegPrice, guardrailDetail.getCompStrId(),
							guardrailConstraint.getLatestPriceObservationDays(), checkDate);
				} else if (priceZoneId > 0) {
					compUnitPrice = inputItem.getZonePriceMap().get(priceZoneId);
				}

				boolean isCompRetailBelowCost = false;
				if (guardrailConstraint.isIgnoreCompBelowCost() && compUnitPrice > 0) {
					double listCost = setCost();
					isCompRetailBelowCost = compUnitPrice < listCost;
				}

				if (!isCompRetailBelowCost) {
					// Get Price Range from each competitor's price
					priceRange = getGuardrailRange(guardrailDetail, compUnitPrice);
				} else {
					priceRange = new PRRange();
				}

				filteredPriceRange = filterPriceRange.filterRange(filteredPriceRange, priceRange);
				//logger.debug("Filtered Range:" + filteredPriceRange.toString());

				if (priceZoneId > 0) {
					writeguardrailDetailLog(priceZoneId, "", 1, compUnitPrice, compUnitPrice,
							guardrailDetail.getGuidelineText(), priceRange, filteredPriceRange, isCompRetailBelowCost);

				} else {
					writeguardrailDetailLog(guardrailDetail.getCompStrId(), checkDate, compRegPrice.multiple,
							compUnitPrice, compRegPrice.price, guardrailDetail.getGuidelineText(), priceRange,
							filteredPriceRange, isCompRetailBelowCost);

				}

				if (filteredPriceRange.getEndVal() > 0 && filteredPriceRange.getEndVal() != Constants.DEFAULT_NA
						&& !isCompRetailBelowCost) {
					totalPrice = totalPrice + filteredPriceRange.getEndVal();
					avgCount = avgCount + 1;
				}

			} else {
				if (priceZoneId > 0) {

					writeguardrailDetailLog(priceZoneId, "", 0, 0, 0, "", new PRRange(), new PRRange(), false);
				} else {
					writeguardrailDetailLog(guardrailDetail.getCompStrId(), "", 0, 0, 0, "", new PRRange(),
							new PRRange(), false);
				}

			}

		}
		avgPrice = ((avgCount > 1) ? (totalPrice / avgCount) : totalPrice);
		return avgPrice;
	}

	private void writeguardrailDetailLog(int compStrId, String checkDate, int regMPack, double regUnitPrice,
			double regMPrice, String guidelineText, PRRange guidelineRange, PRRange finalRange,
			boolean isCompRetailBelowCost) {
		// Set logs
//		DecimalFormat format = new DecimalFormat("######.##"); 
		PRGuardRailLog prGuardRailLog = new PRGuardRailLog();
		prGuardRailLog.setCompStrId(compStrId);
		prGuardRailLog.setPricePresent(false);
		if (regUnitPrice > 0) {
			prGuardRailLog.setCheckDate(checkDate);
			MultiplePrice multiplePrice = PRCommonUtil.getMultiplePrice(regMPack, regUnitPrice, regMPrice);
			// prCompDetailLog.setRegPrice(format.format(regUnitPrice));
			prGuardRailLog.setMultiple(String.valueOf(multiplePrice.multiple));
			prGuardRailLog.setRegPrice(String.valueOf(multiplePrice.price));
			prGuardRailLog.setPricePresent(true);
		}
		prGuardRailLog.setMaxRetailText(guidelineText);
		prGuardRailLog.setGuidelineRange(guidelineRange);
		prGuardRailLog.setFinalRange(finalRange);
		prGuardRailLog.setCompRetailBelowCost(
				isCompRetailBelowCost ? String.valueOf(Constants.YES) : String.valueOf(Constants.NO));
		guidelineAndConstraintLog.getGuardRailList().add(prGuardRailLog);
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
			} else {
				logger.debug("Start Date is not in range: last Obs date: " + lastObsDate.toString() + " check date: "
						+ itemCheckDate.toString());
			}
		} else {
			logger.debug("Start Date or Check Date is null, Item Code: " + this.inputItem.getItemCode());
		}

		return compUnitPrice;
	}

	/**
	 * 
	 * @param itemPrice
	 * @return
	 */

	private PRRange getPriceRangeWithNoValueType(double itemPrice) {
		PRRange range = new PRRange();
		double price = itemPrice;
		PRConstraintGuardrail guardrailConstraint = this.inputItem.getStrategyDTO().getConstriants()
				.getGuardrailConstraint();

		if (itemPrice > 0) {
			if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM
					.equals(guardrailConstraint.getRelationalOperatorText())) {
				range.setEndVal(price);
				// range.setStartVal(price);
			} else if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(guardrailConstraint.getRelationalOperatorText())) {
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

	private double findMinPrice(PRConstraintGuardrail guardrailConstraint,
			List<PRConstraintGuardRailDetail> guradrailDetails) throws Exception {
		double minPrice = 0d;
		PRRange priceRange = null;

		for (PRConstraintGuardRailDetail guardRailContraintDetail : guradrailDetails) {
			PRRange filteredPriceRange = new PRRange();
			LocationKey locationKey = null;
			int priceZoneId = 0;
			double compUnitPrice = 0;
			MultiplePrice compRegPrice = null;
			String checkDate = "";
			boolean isZonePresent = false;

			if (guardRailContraintDetail.getCompStrId() != 0) {
				locationKey = new LocationKey(Constants.STORE_LEVEL_ID, guardRailContraintDetail.getCompStrId());
			}

			priceZoneId = guardRailContraintDetail.getPriceZoneID();

			HashMap<Integer, Double> getZonePriceMap = inputItem.getZonePriceMap();
			if (getZonePriceMap != null) {
				if (inputItem.getZonePriceMap().get(priceZoneId) != null) {
					isZonePresent = true;
				}
			}

			if (inputItem.getAllCompPrice().get(locationKey) != null || isZonePresent) {

				if (locationKey != null) {

					compRegPrice = inputItem.getAllCompPrice().get(locationKey);

					checkDate = inputItem.getAllCompPriceCheckDate().get(locationKey);

					compUnitPrice = getValidCompPrice(compRegPrice, guardRailContraintDetail.getCompStrId(),
							guardrailConstraint.getLatestPriceObservationDays(), checkDate);
				} else if (priceZoneId > 0) {
					compUnitPrice = inputItem.getZonePriceMap().get(priceZoneId);

				}

				boolean isCompRetailBelowCost = false;
				if (guardrailConstraint.isIgnoreCompBelowCost() && compUnitPrice > 0) {
					double listCost = setCost();
					isCompRetailBelowCost = compUnitPrice < listCost;
				}

				if (!isCompRetailBelowCost) {
					// Get Price Range from each competitor's price
					priceRange = getGuardrailRange(guardRailContraintDetail, compUnitPrice);
				} else {
					priceRange = new PRRange();
				}

				filteredPriceRange = filterPriceRange.filterRange(filteredPriceRange, priceRange);

				//logger.debug("Filtered Range:" + filteredPriceRange.toString());
				if (priceZoneId > 0) {
					writeguardrailDetailLog(priceZoneId, "", 1, compUnitPrice, compUnitPrice,
							guardRailContraintDetail.getGuidelineText(), priceRange, filteredPriceRange,
							isCompRetailBelowCost);

				} else {
					writeguardrailDetailLog(guardRailContraintDetail.getCompStrId(), checkDate, compRegPrice.multiple,
							compUnitPrice, compRegPrice.price, guardRailContraintDetail.getGuidelineText(), priceRange,
							filteredPriceRange, isCompRetailBelowCost);

				}

				if (filteredPriceRange.getEndVal() > 0 && filteredPriceRange.getEndVal() != Constants.DEFAULT_NA
						&& !isCompRetailBelowCost) {
					if (minPrice == 0) {
						minPrice = filteredPriceRange.getEndVal();
					} else {
						if (filteredPriceRange.getEndVal() < minPrice) {
							minPrice = filteredPriceRange.getEndVal();
						}
					}
				}

			} else {
				if (priceZoneId > 0) {

					writeguardrailDetailLog(priceZoneId, "", 0, 0, 0, "", new PRRange(), new PRRange(), false);
				} else {
					writeguardrailDetailLog(guardRailContraintDetail.getCompStrId(), "", 0, 0, 0, "", new PRRange(),
							new PRRange(), false);
				}

			}

		}
		return minPrice;
	}

	private PRRange getGuardrailRangePCT(PRConstraintGuardRailDetail constraintGuardRail, double compPrice) {
		PRRange range = new PRRange();
		double value = constraintGuardRail.getMinValue();
		/* double maxValue = constraintGuardRail.getMaxValue(); */
		String operatorText = constraintGuardRail.getRelationalOperatorText();
		// >, >=, above, <, <=, below, lower, not >, within
		GurdrailConstraintService.getGuardrailPctRange(operatorText, value, compPrice, range);
		return range;
	}
	
	/**
	 * Added by Karishma on 06/14/2021 to check  compPrice against future cost if present and only if there is a cost change
	 * @return
	 */
	public double setCost() {
		double finalCost = 0;
		//ad//Added existing property check to the function because future cost functionality/use is applicable for RA only and not FF
		//For FF future cost is for display purpose 
		if (chkFutureCost && inputItem.getFutureListCost() != null && inputItem.getFutureListCost() > 0
				&& inputItem.getCostChgIndicator() != 0)
			finalCost = inputItem.getFutureListCost();
		// condition added to use this cost for AZ as list cost has freight charge added in it
		else if (inputItem.getListCostWtotFrChg() > 0) {
			finalCost = inputItem.getListCostWtotFrChg();
		} else

			finalCost = inputItem.getListCost();
		return finalCost;
	}

}
