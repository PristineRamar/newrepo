package com.pristine.service.offermgmt.mwr.itemattributes;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

//import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.CriteriaDTO;
import com.pristine.dto.offermgmt.CriteriaTypeLookup;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.dto.offermgmt.StrategyKey;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;

public class ItemCriteriaService {
	//private static Logger logger = Logger.getLogger("ItemCriteriaService");
	/**
	 * Sets criteria id for items
	 * 
	 * @param itemDataMap
	 * @param criteriaDetails
	 * @param hashMap
	 * @param hashMap
	 * @param hashMap
	 */
	public void setCriteriaIdForItems(HashMap<ItemKey, PRItemDTO> itemDataMap,
			HashMap<Integer, List<CriteriaDTO>> criteriaDetails,
			HashMap<StrategyKey, List<PRStrategyDTO>> strategyMap) {
		// Identify possible cirteria for each item
		// If all criteria are matching for an item, then store the criteria
		// If multiple criteria are matching for an item, set one criteria which has
		// more matches
		boolean CheckStrategyMap = Boolean
				.parseBoolean(PropertyManager.getProperty("CHECK_STRATEGY_FOR_CRITERIA", "FALSE"));

		itemDataMap.forEach((itemKey, itemDTO) -> {
			if (!itemDTO.isLir()) {
				HashMap<Integer, Integer> criteriaCount = new HashMap<Integer, Integer>();
				criteriaDetails.forEach((criteriaId, criteriaList) -> {
					int criteriaMatchingCount = 0;
					for (CriteriaDTO criteriaDTO : criteriaList) {
						if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.FAMILY_UNIT_SALES
								.getCriteriaTypeId()) {
							double familyUnits = Double.parseDouble(criteriaDTO.getValue());
							if (isCriteriaMatching(criteriaDTO.getOprertorText(), familyUnits,
									itemDTO.getFamilyXWeeksMov())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.ITEM_UNIT_SALES
								.getCriteriaTypeId()) {
							double familyUnits = Double.parseDouble(criteriaDTO.getValue());
							if (isCriteriaMatching(criteriaDTO.getOprertorText(), familyUnits,
									itemDTO.getxWeeksMovForAddlCriteria())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.COST.getCriteriaTypeId()) {
							double inputCost = Double.parseDouble(criteriaDTO.getValue());
							if (itemDTO.getCost() != null) {
								if (isCriteriaMatching(criteriaDTO.getOprertorText(), inputCost, itemDTO.getCost())) {
									criteriaMatchingCount++;
								}
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.ITEM_SETUP
								.getCriteriaTypeId()) {
							if (isItemWithinItemSetupDate(criteriaDTO.getOprertorText(), criteriaDTO.getValue(),
									itemDTO.getItemSetupDate(), criteriaDTO.getValueType())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.PLC.getCriteriaTypeId()) {
							if (isMatchingPLC(criteriaDTO.getOprertorText(), criteriaDTO.getValue(),
									itemDTO.getUserAttr10())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.STOCKING_STATUS
								.getCriteriaTypeId()) {
							if (isAttributeMatching(criteriaDTO.getValue(), itemDTO.getUserAttr9())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.COVERAGE_OR_CHOICE
								.getCriteriaTypeId()) {
							if (isAttributeMatching(criteriaDTO.getValue(), itemDTO.getUserAttr12())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.DFM.getCriteriaTypeId()) {
							if (isAttributeMatching(criteriaDTO.getValue(), itemDTO.getUserAttr11())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.STORE_COUNT
								.getCriteriaTypeId()) {
							double storeCountInput = Double.parseDouble(criteriaDTO.getValue());
							if (isCriteriaMatching(criteriaDTO.getOprertorText(), storeCountInput,
									itemDTO.getNoOfStoresItemAuthorized())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.TIER.getCriteriaTypeId()) {

							if (isMatchingTier(criteriaDTO.getOprertorText(), criteriaDTO.getValue(),
									itemDTO.getUserAttr4())) {
								criteriaMatchingCount++;
							}

						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.ITEM_STATUS
								.getCriteriaTypeId()) {
							if (isAttributeMatching(criteriaDTO.getValue(), itemDTO.getUserAttr4())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.LIG_UNIT_SALES
								.getCriteriaTypeId()) {
							double ligUnits = Double.parseDouble(criteriaDTO.getValue());
							if (isCriteriaMatching(criteriaDTO.getOprertorText(), ligUnits,
									itemDTO.getxWeeksMovForAddlCriteriaAtLIGLevel())) {
								criteriaMatchingCount++;
							}
						} else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.PRICE.getCriteriaTypeId()) {
							double inputPrice = Double.parseDouble(criteriaDTO.getValue());
							MultiplePrice currentPrice = PRCommonUtil.getCurRegPrice(itemDTO);
							if (currentPrice != null) {
								if (isCriteriaMatching(criteriaDTO.getOprertorText(), inputPrice,
										PRCommonUtil.getUnitPrice(currentPrice, true))) {
									criteriaMatchingCount++;
								}
							} 
						}
						
						else if (criteriaDTO.getCriteriaTypeId() == CriteriaTypeLookup.NIPO_COST.getCriteriaTypeId()) {
							double inputBaseCost = Double.parseDouble(criteriaDTO.getValue());
							if (itemDTO.getNipoBaseCost() > 0) {
								if (isCriteriaMatching(criteriaDTO.getOprertorText(), inputBaseCost,
										itemDTO.getNipoBaseCost())) {
									criteriaMatchingCount++;
								}
							}
						}

					}
					if (criteriaMatchingCount == criteriaList.size()) {
						criteriaCount.put(criteriaId, criteriaMatchingCount);
					}
				});
				// Changes done by Karishma for AZ on 06/29
				// Check the strategy map to find the correct criteria Id for items falling in
				// multiple criteria based on itemList
				// first prefrence should be given to itemList.
				int maxCriteriaCount = 0;
				int criteriaId = 0;
				List<Integer> itemListCriteria = new ArrayList<Integer>();
				List<Integer> allItemsCriteria = new ArrayList<Integer>();

				for (Map.Entry<Integer, Integer> criteriaEntry : criteriaCount.entrySet()) {
					if (CheckStrategyMap) {
						for (Map.Entry<StrategyKey, List<PRStrategyDTO>> strategy : strategyMap.entrySet()) {
							for (PRStrategyDTO strat : strategy.getValue()) {
								if (strat.getCriteriaId() == criteriaEntry.getKey()) {
									if (strat.getPriceCheckListId() == -1 && itemDTO.getPriceCheckListId() == null) {
										criteriaId = criteriaEntry.getKey();
										allItemsCriteria.add(criteriaId);
									} else {
										if (itemDTO.getPriceCheckListId() != null) {
											if (strat.getPriceCheckListId() == itemDTO.getPriceCheckListId()) {
												criteriaId = criteriaEntry.getKey();
												itemListCriteria.add(criteriaId);
											}
										}
									}
								}
							}

						}

					}

					else if (criteriaEntry.getValue() > maxCriteriaCount) {
						criteriaId = criteriaEntry.getKey();
						maxCriteriaCount = criteriaEntry.getValue();
					}
				}

				if (CheckStrategyMap) {
					if (itemListCriteria.size() > 0) {
						for (Integer ctId : itemListCriteria) {
							if (criteriaCount.containsKey(ctId)) {

								if (criteriaCount.get(ctId) > maxCriteriaCount) {
									criteriaId = ctId;
									maxCriteriaCount = criteriaCount.get(ctId);
								}
							}

						}
					} else if (allItemsCriteria.size() > 0) {
						for (Integer ctId : allItemsCriteria) {
							if (criteriaCount.containsKey(ctId)) {

								if (criteriaCount.get(ctId) > maxCriteriaCount) {
									criteriaId = ctId;
									maxCriteriaCount = criteriaCount.get(ctId);
								}
							}

						}
					}

					// If the item is part of any item list but there is no strategy defined
					// for itemlist then check for the item list applicable to all items with the
					// max criteria's matched
					if (criteriaId == 0) {

						List<Integer> criteriasFromStrategy = new ArrayList<>();

						if (itemDTO.getPriceCheckListId() != null) {
							for (Map.Entry<StrategyKey, List<PRStrategyDTO>> strategy : strategyMap.entrySet()) {
								for (PRStrategyDTO strat : strategy.getValue()) {
									// get all the criteris's defined for all items level
									if (strat.getPriceCheckListId() == -1 && strat.getCriteriaId() != -1) {
										criteriasFromStrategy.add(strat.getCriteriaId());
									}
								}
							}

						}

						for (Map.Entry<Integer, Integer> criteriaEntry : criteriaCount.entrySet()) {

							if (criteriaEntry.getValue() > maxCriteriaCount
									&& criteriasFromStrategy.contains(criteriaEntry.getKey())) {
								criteriaId = criteriaEntry.getKey();
								maxCriteriaCount = criteriaEntry.getValue();
							}

						}

					}
				}
				//logger.info ("Item Code: "+ itemDTO.getItemCode()  + "Criteria Id : "+ criteriaId +" price CheckList: "+ itemDTO.getPriceCheckListId());
				itemDTO.setCriteriaId(criteriaId);
			}
		});
	}

	/**
	 * Separted conditions for <= and >= for additional Criteria Change by Karishma
	 * on 04/03/2020
	 * 
	 * @param operatorText
	 * @param inputVal
	 * @param actValue
	 * @return true if given criteria is matching
	 */

	private boolean isCriteriaMatching(String operatorText, double inputVal, double actValue) {
		boolean isCriteriaMatching = false;

		// Additional Criteria changes as part of PROM 2129
		// Changes done by Bhargavi on 02/23/2021
		// update the actValue to populate the data for all the values (less than,
		// greater than and equal to 0)
		// if (inputVal > 0 && actValue > 0) {
		if (inputVal > 0) {
			if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
				if (actValue > inputVal) {
					isCriteriaMatching = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
				if (actValue >= inputVal) {
					isCriteriaMatching = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
				if (actValue < inputVal) {
					isCriteriaMatching = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
				if (actValue <= inputVal) {
					isCriteriaMatching = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_NOT_GREATER_THAN_EQUAL.equals(operatorText)) {
				if (!(actValue >= inputVal)) {
					isCriteriaMatching = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_NOT_LESSER_THAN_EQUAL.equals(operatorText)) {
				if (!(actValue <= inputVal)) {
					isCriteriaMatching = true;
				}
			}
		}
		return isCriteriaMatching;
	}

	/**
	 * 
	 * @param operatorText
	 * @param inputItemSetup
	 * @param itemSetupDateStr
	 * @param valueType
	 * @return true if item's date is matching with given criteria
	 */
	private boolean isItemWithinItemSetupDate(String operatorText, String inputItemSetup, String itemSetupDateStr,
			String valueType) {
		boolean isItemWithinItemSetupDate = false;
		LocalDate inputDate = parseItemSetup(inputItemSetup, valueType);
		if (itemSetupDateStr != null && !Constants.EMPTY.equals(itemSetupDateStr)) {
			LocalDate itemSetupDate = LocalDate.parse(itemSetupDateStr, PRCommonUtil.getDateFormatter());
			if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
				if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)
						&& (itemSetupDate.isEqual(inputDate) || itemSetupDate.isBefore(inputDate))) {
					isItemWithinItemSetupDate = true;
				}

				if (itemSetupDate.isBefore(inputDate)) {
					isItemWithinItemSetupDate = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_LOWER.equals(operatorText)) {
				if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)
						&& (itemSetupDate.isEqual(inputDate) || itemSetupDate.isAfter(inputDate))) {
					isItemWithinItemSetupDate = true;
				}

				if (itemSetupDate.isAfter(inputDate)) {
					isItemWithinItemSetupDate = true;
				}
			}
		}
		return isItemWithinItemSetupDate;
	}

	/**
	 * 
	 * @param inputItemSetup
	 * @param valueType
	 * @return date in last X days/weeks/periods/quarters/years
	 */
	private LocalDate parseItemSetup(String inputItemSetup, String valueType) {
		LocalDate itemSetupDate = LocalDate.now();
		if (valueType.equals(Constants.CALENDAR_DAY)) {
			int noOfDays = Integer.parseInt(inputItemSetup.trim());
			itemSetupDate = itemSetupDate.minus(noOfDays, ChronoUnit.DAYS);
		} else if (valueType.equals(Constants.CALENDAR_WEEK)) {
			int noOfWeeks = Integer.parseInt(inputItemSetup.trim());
			itemSetupDate = itemSetupDate.minus(noOfWeeks, ChronoUnit.WEEKS);
		} else if (valueType.equals(Constants.CALENDAR_PERIOD)) {
			int noOfPeriods = Integer.parseInt(inputItemSetup.trim());
			itemSetupDate = itemSetupDate.minus((noOfPeriods * 4), ChronoUnit.WEEKS);
		} else if (valueType.equals(Constants.CALENDAR_QUARTER)) {
			int noOfQuarters = Integer.parseInt(inputItemSetup.trim());
			itemSetupDate = itemSetupDate.minus((noOfQuarters * 13), ChronoUnit.WEEKS);
		} else if (valueType.equals(Constants.CALENDAR_YEAR)) {
			int noOfYears = Integer.parseInt(inputItemSetup.trim());
			itemSetupDate = itemSetupDate.minus((noOfYears * 52), ChronoUnit.WEEKS);
		}
		return itemSetupDate;
	}

	/**
	 * 
	 * @param operatorText
	 * @param value
	 * @param itemPLC
	 * @return true if given PLC ranage/single PLC/multiple PLC covers current
	 *         item's PLC
	 */
	private boolean isMatchingPLC(String operatorText, String value, String itemPLC) {
		boolean isMatchingPLC = false;
		if (!"NA".equals(itemPLC)) {
			if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(operatorText) || Constants.EMPTY.equals(operatorText)
					|| operatorText == null) {
				if (value.contains("-")) {
					String plcArr[] = value.split("-");
					int minVal = Integer.parseInt(plcArr[0]);
					int maxVal = Integer.parseInt(plcArr[1]);
					int actPlc = Integer.parseInt(itemPLC);
					if (actPlc >= minVal && actPlc <= maxVal) {
						isMatchingPLC = true;
					}
				} else if (value.contains(",")) {
					String plcArr[] = value.split(",");
					for (String plc : plcArr) {
						if (itemPLC.equals(plc)) {
							isMatchingPLC = true;
							break;
						}
					}
				} else if (value.equals(itemPLC)) {
					isMatchingPLC = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
				int plc = Integer.parseInt(itemPLC);
				int inputPlc = Integer.parseInt(value);

				if (plc > inputPlc && PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)) {
					isMatchingPLC = true;
				} else if (plc >= inputPlc
						&& PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
					isMatchingPLC = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
				int plc = Integer.parseInt(itemPLC);
				int inputPlc = Integer.parseInt(value);

				if (plc < inputPlc && PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)) {
					isMatchingPLC = true;
				} else if (plc <= inputPlc && PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
					isMatchingPLC = true;
				}
			}
		}
		return isMatchingPLC;
	}

	/**
	 * 
	 * @param inputAttr
	 * @param itemAttribute
	 * @return true if item attribute is matching with input or available in input
	 */
	private boolean isAttributeMatching(String inputAttr, String itemAttribute) {
		boolean isAttributeMatching = false;
		String[] valArr = inputAttr.split(",");
		for (String val : valArr) {
			if (val.equalsIgnoreCase(itemAttribute)) {
				isAttributeMatching = true;
				break;
			}
		}
		return isAttributeMatching;
	}

	/**
	 * 
	 * @param operatorText
	 * @param value
	 * @param string
	 * @return true if given tier range/single Tier/multiple Tier covers current
	 *         item's Tier
	 */
	private boolean isMatchingTier(String operatorText, String value, String itemTier) {
		boolean isMatchingTier = false;

		if (itemTier != null && !Constants.EMPTY.equals(itemTier)) {
			if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(operatorText) || Constants.EMPTY.equals(operatorText)
					|| operatorText == null) {
				if (value.contains("-")) {
					String tierArr[] = value.split("-");
					int minVal = Integer.parseInt(tierArr[0]);
					int maxVal = Integer.parseInt(tierArr[1]);
					int actTier = Integer.parseInt(itemTier);
					if (actTier >= minVal && actTier <= maxVal) {
						isMatchingTier = true;
					}
				} else if (value.contains(",")) {
					String tierArr[] = value.split(",");
					for (String tier : tierArr) {
						if (itemTier.equals(tier)) {
							isMatchingTier = true;
							break;
						}
					}
				} else if (value.equals(itemTier)) {
					isMatchingTier = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
				int tier = Integer.parseInt(itemTier);
				int inputTier = Integer.parseInt(value);

				if (tier > inputTier && PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)) {
					isMatchingTier = true;
				} else if (tier >= inputTier
						&& PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
					isMatchingTier = true;
				}
			} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
					|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
				int tier = Integer.parseInt(itemTier);
				int inputTier = Integer.parseInt(value);

				if (tier < inputTier && PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)) {
					isMatchingTier = true;
				} else if (tier <= inputTier
						&& PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
					isMatchingTier = true;
				}
			}
		}
		return isMatchingTier;
	}

}
