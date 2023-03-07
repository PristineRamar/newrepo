package com.pristine.dto.offermgmt;

//import java.io.Serializable;

import org.apache.log4j.Logger;

import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRConstants;
import com.pristine.util.offermgmt.PRFormatHelper;

//public class PRPriceGroupRelnDTO implements Serializable{
	public class PRPriceGroupRelnDTO implements Cloneable {
	private char valueType;
	private double minValue;
	private double maxValue;
	private String operatorText;
	private char retailType;

	private static Logger logger = Logger.getLogger("PRPriceGroupRelnDTO");
	
	public char getValueType() {
		return valueType;
	}
	public void setValueType(char valueType) {
		this.valueType = valueType;
	}
	public double getMinValue() {
		return minValue;
	}
	public void setMinValue(double minValue) {
		this.minValue = minValue;
	}
	public double getMaxValue() {
		return maxValue;
	}
	public void setMaxValue(double maxValue) {
		this.maxValue = maxValue;
	}
	public String getOperatorText() {
		return operatorText;
	}
	public void setOperatorText(String operatorText) {
		this.operatorText = operatorText;
	}
	public char getRetailType() {
		return retailType;
	}
	public void setRetailType(char retailType) {
		this.retailType = retailType;
	}
	
	/**
	 * Returns price range for this price group relation
	 * @param pgData
	 * @return
	 */
	public PRRange getPriceRange(MultiplePrice relatedItemPrice, double relatedItemSize, double itemSize, char relationType, 
			double sizeShelfPCT) {
		PRRange range = null;
		
		//29th Dec 2016, when related item is in multiple, its not taking unit price
		//while applying brand/size relation. Change data type of realtedItemPrice
		//from double to MultiplePrice
	
		if(relationType == PRConstants.BRAND_RELATION){	
			if(PRConstants.VALUE_TYPE_PCT == valueType){
				range = getBrandPriceRangeWithPct(relatedItemPrice, relatedItemSize, itemSize);
			}else if(PRConstants.VALUE_TYPE_$ == valueType){
				range = getBrandPriceRangeWith$(relatedItemPrice, relatedItemSize, itemSize);
			}else{
				range = getBrandPriceRangeWithNoValueType(relatedItemPrice, relatedItemSize, itemSize);
			}
		}else{
			//9th Dec, when related item size is 0, don't apply size relation
			if(relatedItemSize > 0){
//				if(PRConstants.VALUE_TYPE_PCT == valueType){
//					range = getSizePriceRangeWithPct(relatedItemPrice, relatedItemSize, itemSize);
//				}else if(PRConstants.VALUE_TYPE_$ == valueType){
//					range = getSizePriceRangeWith$(relatedItemPrice, relatedItemSize, itemSize);
//				}else{
//					range = getSizePriceRangeWithNoValueType(relatedItemPrice, relatedItemSize, itemSize);
//				}
				range = getSizePriceRangeWithNoValueType(relatedItemPrice, relatedItemSize, itemSize, sizeShelfPCT);
			}else{
				logger.debug("Size Guideline is ignored as Related Item Size is :" + relatedItemSize);
				range = new PRRange();
			}
		}
		
		return range;
	}
	
	/**
	 * Returns price range when brand guideline values are specified in Percentage
	 * @param relatedItemPrice
	 * @param relatedItemSize
	 * @param itemSize
	 * @return
	 */
	public PRRange getBrandPriceRangeWithPct(MultiplePrice relatedItemPrice, double relatedItemSize, double itemSize){
		//logger.debug("Related Item Price - " + relatedItemPrice + "\tRelated Item Size - " + relatedItemSize + "\tItem Size - " + itemSize + 
				//"\tMin Value" + minValue + "\tMax Value" + maxValue);

		PRRange range = new PRRange();
//		double price = relatedItemPrice;
//		if(PRConstants.RETAIL_TYPE_UNIT == retailType){
//			price = relatedItemPrice/relatedItemSize;
//		}

		double relatedItemUnitPrice = PRCommonUtil.getUnitPrice(relatedItemPrice, true);
		double price = relatedItemUnitPrice;
		if (PRConstants.RETAIL_TYPE_UNIT == retailType) {
			price = relatedItemUnitPrice / relatedItemSize;
		}

		if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price + (price * (minValue / 100)));
				range.setEndVal(price + (price * (maxValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(price + (price * (minValue / 100)));
				range.setEndVal(price + (price * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price);
				range.setEndVal(price + (price * (maxValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price + (price * (minValue / 100)));
				range.setEndVal(price + (price * (maxValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(price + (price * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price);
				range.setEndVal(price + (price * (maxValue / 100)));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price - (price * (maxValue / 100)));
				range.setEndVal(price - (price * (minValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(price - (price * (minValue / 100)));
				range.setStartVal(price - (price * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setEndVal(price - (price * (maxValue / 100)));
				range.setStartVal(price);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price - (price * (maxValue / 100)));
				range.setEndVal(price - (price * (minValue / 100)));
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(price - (price * (minValue / 100)));
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setEndVal(price - (price * (maxValue / 100)));
				range.setStartVal(price);
			}
		}

		if (PRConstants.RETAIL_TYPE_UNIT == retailType) {
			if (range.getStartVal() != Constants.DEFAULT_NA)
				range.setStartVal(range.getStartValWithNoRounding() * itemSize);
			if (range.getEndVal() != Constants.DEFAULT_NA)
				range.setEndVal(range.getEndValWithNoRounding() * itemSize);
		}

		// logger.debug("Price Range " + range.getStartVal() + "\t" +
		// range.getEndVal());
		return range;
	}

	/**
	 * Returns price range when size guideline values are specified in Percentage
	 * 
	 * @param relatedItemPrice
	 * @param relatedItemSize
	 * @param itemSize
	 * @return
	 */
//	public PRRange getSizePriceRangeWithPct(double relatedItemPrice, double relatedItemSize, double itemSize){
//		logger.debug("Related Item Price - " + relatedItemPrice + "\tRelated Item Size - " + relatedItemSize + "\tItem Size - " + itemSize + 
//				"\tMin Value" + minValue + "\tMax Value" + maxValue);
//
//		PRRange range = new PRRange();
//		double price = relatedItemPrice;
//		if(PRConstants.RETAIL_TYPE_UNIT == retailType){
//			price = relatedItemPrice/relatedItemSize;
//		}
//		
//		if(minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA){
//			range.setStartVal(price - (price * (minValue/100)));
//			range.setEndVal(price + (price * (maxValue/100)));
//		}else if(minValue != Constants.DEFAULT_NA){
//			if(itemSize > relatedItemSize)
//				range.setEndVal(price - (price * (minValue/100)));
//			else
//				range.setStartVal(price + (price * (minValue/100)));
//		}else if(maxValue != Constants.DEFAULT_NA){
//			if(itemSize > relatedItemSize)
//				range.setStartVal(price + (price * (maxValue/100)));
//			else
//				range.setEndVal(price - (price * (maxValue/100)));
//		}
//		
//		if(PRConstants.RETAIL_TYPE_UNIT == retailType){
//			if(range.getStartVal() != Constants.DEFAULT_NA)
//				range.setStartVal(range.getStartValWithNoRounding() * itemSize);
//			if(range.getEndVal() != Constants.DEFAULT_NA)
//				range.setEndVal(range.getEndValWithNoRounding() * itemSize);
//		}
//		
//		logger.debug("Price Range " + range.getStartVal() + "\t" + range.getEndVal());
//		return range;
//	}

	/**
	 * Returns price range when brand guideline values are specified in Dollar
	 * @param relatedItemPrice
	 * @param relatedItemSize
	 * @param itemSize
	 * @return
	 */
	public PRRange getBrandPriceRangeWith$(MultiplePrice relatedItemPrice, double relatedItemSize, double itemSize) {
		PRRange range = new PRRange();
//		double price = relatedItemPrice;
//		if(PRConstants.RETAIL_TYPE_UNIT == retailType){
//			price = relatedItemPrice/relatedItemSize;
//		}

		double relatedItemUnitPrice = PRCommonUtil.getUnitPrice(relatedItemPrice, true);
		double price = relatedItemUnitPrice;
		if (PRConstants.RETAIL_TYPE_UNIT == retailType) {
			price = relatedItemUnitPrice / relatedItemSize;
		}
		if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price + minValue);
				range.setEndVal(price + maxValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(price + minValue);
				range.setEndVal(price + minValue);
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price);
				range.setEndVal(price + maxValue);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price + minValue);
				range.setEndVal(price + maxValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setStartVal(price + minValue);
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price);
				range.setEndVal(price + maxValue);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price - maxValue);
				range.setEndVal(price - minValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(price - minValue);
				range.setStartVal(price - minValue);
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price);
				range.setEndVal(price - maxValue);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
			if (minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price - maxValue);
				range.setEndVal(price - minValue);
			} else if (minValue != Constants.DEFAULT_NA) {
				range.setEndVal(price - minValue);
			} else if (maxValue != Constants.DEFAULT_NA) {
				range.setStartVal(price);
				range.setEndVal(price - maxValue);
			}
		}

		if (PRConstants.RETAIL_TYPE_UNIT == retailType) {
			if (range.getStartVal() != Constants.DEFAULT_NA)
				range.setStartVal(range.getStartValWithNoRounding() * itemSize);
			if (range.getEndVal() != Constants.DEFAULT_NA)
				range.setEndVal(range.getEndValWithNoRounding() * itemSize);
		}
		return range;
	}

	/**
	 * Returns price range when size guideline values are specified in Dollar
	 * 
	 * @param relatedItemPrice
	 * @param relatedItemSize
	 * @param itemSize
	 * @return
	 */
//	public PRRange getSizePriceRangeWith$(double relatedItemPrice, double relatedItemSize, double itemSize){
//		PRRange range = new PRRange();
//		double price = relatedItemPrice;
//		if(PRConstants.RETAIL_TYPE_UNIT == retailType){
//			price = relatedItemPrice/relatedItemSize;
//		}
//		
//		if(minValue != Constants.DEFAULT_NA && maxValue != Constants.DEFAULT_NA){
//			range.setStartVal(price - minValue);
//			range.setEndVal(price + maxValue);
//		}else if(minValue != Constants.DEFAULT_NA){
//			if(itemSize > relatedItemSize)
//				range.setEndVal(price - minValue);
//			else
//				range.setStartVal(price + minValue);
//		}else if(maxValue != Constants.DEFAULT_NA){
//			if(itemSize > relatedItemSize)
//				range.setStartVal(price + maxValue);
//			else
//				range.setEndVal(price - maxValue);
//		}
//		if(PRConstants.RETAIL_TYPE_UNIT == retailType){
//			if(range.getStartVal() != Constants.DEFAULT_NA)
//				range.setStartVal(range.getStartValWithNoRounding() * itemSize);
//			if(range.getEndVal() != Constants.DEFAULT_NA)
//				range.setEndVal(range.getEndValWithNoRounding() * itemSize);
//		}
//		return range;
//	}

	/**
	 * Returns brand price range when only Expression and No value type is present
	 * 
	 * @param relatedItemPrice
	 * @param relatedItemSize
	 * @param itemSize
	 * @return
	 */
	public PRRange getBrandPriceRangeWithNoValueType(MultiplePrice relatedItemPrice, double relatedItemSize,
			double itemSize) {
//		logger.debug("Related Item Price - " + relatedItemPrice + "\tRelated Item Size - " + relatedItemSize
//				+ "\tItem Size - " + itemSize + "\tMin Value" + minValue + "\tMax Value" + maxValue);

		PRRange range = new PRRange();
		double relatedItemUnitPrice = PRCommonUtil.getUnitPrice(relatedItemPrice, true);
//		double price = relatedItemPrice;
		double price = relatedItemUnitPrice;
		if (PRConstants.RETAIL_TYPE_UNIT == retailType) {
//			price = relatedItemPrice/relatedItemSize;
			price = relatedItemUnitPrice / relatedItemSize;
		}

		if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
			range.setStartVal((price + 0.01));
			range.setEndVal((price + 0.01));
		} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)) {
			range.setStartVal((price + 0.01));
		} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
			range.setStartVal(price);
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
			range.setEndVal(price);
		} else if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(operatorText)) {
			range.setStartVal(price);
			range.setEndVal(price);
		} else if (PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)) {
			if (price - 0.01 <= 0) {
//				logger.debug("Reduce by pct");
				double pct = Double.parseDouble(PropertyManager.getProperty("PR_SIZE_RELATION_PCT_DIFF", "1"));
				range.setEndVal(price - (price * pct / 100));
				range.setStartVal(price - (price * pct / 100));
			} else {
				range.setEndVal((price - 0.01));
				range.setStartVal(price - 0.01);
			}
		} else {
			if (price - 0.01 <= 0) {
//				logger.debug("Reduce by pct");
				double pct = Double.parseDouble(PropertyManager.getProperty("PR_SIZE_RELATION_PCT_DIFF", "1"));
				range.setEndVal(price - (price * pct / 100));
			} else
				range.setEndVal((price - 0.01));
		}

		if (PRConstants.RETAIL_TYPE_UNIT == retailType) {
			if (range.getStartVal() != Constants.DEFAULT_NA)
				range.setStartVal(range.getStartValWithNoRounding() * itemSize);
			if (range.getEndVal() != Constants.DEFAULT_NA)
				range.setEndVal(range.getEndValWithNoRounding() * itemSize);
		}

		return range;
	}

	/**
	 * Returns size price range when only Expression and No value type is present
	 * 
	 * @param relatedItemPrice
	 * @param relatedItemSize
	 * @param itemSize
	 * @return
	 */
	public PRRange getSizePriceRangeWithNoValueType(MultiplePrice relatedItemPrice, double relatedItemSize,
			double itemSize, double sizeShelfPCT) {
		double pct = Double.parseDouble(PropertyManager.getProperty("PR_SIZE_RELATION_PCT_DIFF", "1"));
		// double shelfPct =
		// Double.parseDouble(PropertyManager.getProperty("PR_SIZE_RELATION_SHELF_PCT_DIFF",
		// "1"));
		double shelfPct = 0;
		PRRange range = new PRRange();
		double relatedItemUnitPrice = PRCommonUtil.getUnitPrice(relatedItemPrice, true);
//		double price = relatedItemPrice;
		double price = relatedItemUnitPrice;
		if (PRConstants.RETAIL_TYPE_UNIT == retailType) {
//			price = relatedItemPrice/relatedItemSize;
			price = relatedItemUnitPrice / relatedItemSize;
		}
		// 6th Jan 2015, take shelf pct from strategy
		if (sizeShelfPCT != Constants.DEFAULT_NA) {
			shelfPct = sizeShelfPCT;
		}

		// 19th Dec 2014, put shelf price in lower/higher side

		if (PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)) {
			if (relatedItemSize > itemSize) {
				range.setStartVal(price + (price * pct / 100));
			} else {
				range.setEndVal(price - (price * pct / 100));
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM.equals(operatorText)) {
			if (relatedItemSize > itemSize) {
				range.setStartVal(price);
			} else {
				range.setEndVal(price);
			}
		} else if (PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM.equals(operatorText)) {
			range.setStartVal(price);
			range.setEndVal(price);
		} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM.equals(operatorText)) {
			if (itemSize > relatedItemSize) {
				range.setEndVal(price);
			} else {
				range.setStartVal(price);
			}
		} else {
			if (itemSize > relatedItemSize) {
				range.setEndVal(price - (price * pct / 100));
				// logger.debug("End Val: " + (price - (price * pct/100)));
			} else if (itemSize == relatedItemSize) {
				range.setStartVal(price);
				range.setEndVal(price);
			} else {
				range.setStartVal(price + (price * pct / 100));
				// logger.debug("Start Val: " + (price - (price * pct/100)));
			}
		}
		if (PRConstants.RETAIL_TYPE_UNIT == retailType) {
			if (range.getStartVal() != Constants.DEFAULT_NA) {
				range.setStartVal(range.getStartValWithNoRounding() * itemSize);
			}
			if (range.getEndVal() != Constants.DEFAULT_NA) {
				range.setEndVal(range.getEndValWithNoRounding() * itemSize);
			}
		}

		// Update start and end price points with shelf price
		if (range.getStartVal() == Constants.DEFAULT_NA) {
//			double newPrice = relatedItemPrice + (relatedItemPrice * shelfPct/100); 
			double newPrice = relatedItemUnitPrice + (relatedItemUnitPrice * shelfPct / 100);
			// If the difference in change is < 0.01, add 0.01, the min increase must be
			// 0.01
//			if((newPrice - relatedItemPrice) < 0.01)
//				range.setStartVal(relatedItemPrice + 0.01);
//			else
//				range.setStartVal(newPrice);
			if ((newPrice - relatedItemUnitPrice) < 0.01)
				range.setStartVal(relatedItemUnitPrice + 0.01);
			else
				range.setStartVal(newPrice);
		}

		if (range.getEndVal() == Constants.DEFAULT_NA) {
//			double newPrice = relatedItemPrice - (relatedItemPrice * shelfPct/100);
//			if((newPrice - relatedItemPrice) < 0.01)
//				range.setEndVal(relatedItemPrice + 0.01);
//			else
//				range.setEndVal(newPrice);
			double newPrice = relatedItemUnitPrice - (relatedItemUnitPrice * shelfPct / 100);
			if ((newPrice - relatedItemUnitPrice) < 0.01)
				range.setEndVal(relatedItemUnitPrice - 0.01);
			else
				range.setEndVal(newPrice);
		}

		return range;
	}

	public void copy(PRGuidelineSize sizeGuideline) {
		this.valueType = sizeGuideline.getValueType();
		this.minValue = sizeGuideline.getMinValue();
		this.maxValue = sizeGuideline.getMaxValue();
		this.operatorText = sizeGuideline.getOperatorText();
		this.retailType = PRConstants.RETAIL_TYPE_UNIT;
	}

	public void copy(PRGuidelineBrand brandGuideline) {
		this.valueType = brandGuideline.getValueType();
		this.minValue = brandGuideline.getMinValue();
		this.maxValue = brandGuideline.getMaxValue();
		this.operatorText = brandGuideline.getOperatorText();
		this.retailType = brandGuideline.getRetailType();
	}

	public String formRelationText(char relationType) {
		String relationText = "";
		String minText = "";
		String maxText = "";
		String minMaxSeperator = "-";
		String valueTypeText = "";
		String operatorText = this.operatorText;
		boolean isPutOperatorFirst = false;

		if (relationType == PRConstants.BRAND_RELATION) {
			if (PRConstants.VALUE_TYPE_PCT == valueType) {
				minText = ((minValue != Constants.DEFAULT_NA) ? PRFormatHelper.roundToTwoDecimalDigit(minValue) : "");
				maxText = ((maxValue != Constants.DEFAULT_NA) ? PRFormatHelper.roundToTwoDecimalDigit(maxValue) : "");
			} else {
				minText = ((minValue != Constants.DEFAULT_NA) ? PRFormatHelper.doubleToTwoDigitString(minValue) : "");
				maxText = ((maxValue != Constants.DEFAULT_NA) ? PRFormatHelper.doubleToTwoDigitString(maxValue) : "");
			}
			valueTypeText = ((PRConstants.VALUE_TYPE_PCT == valueType) ? "%" : "");

			// If operator <,>,<=,>=,=, then put operator text first
			if (operatorText.equals(PRConstants.PRICE_GROUP_EXPR_GREATER_SYM)
					|| operatorText.equals(PRConstants.PRICE_GROUP_EXPR_GREATER_THAN_EQUAL_SYM)
					|| operatorText.equals(PRConstants.PRICE_GROUP_EXPR_LESSER_SYM)
					|| operatorText.equals(PRConstants.PRICE_GROUP_EXPR_LESSER_THAN_EQUAL_SYM)
					|| operatorText.equals(PRConstants.PRICE_GROUP_EXPR_EQUAL_SYM)) {
				isPutOperatorFirst = true;
			}

			if (isPutOperatorFirst) {
				relationText = relationText + operatorText;
				relationText = relationText + " ";
			}

			relationText = relationText + minText;
			if (minText != "" && maxText != "") {
				relationText = relationText + minMaxSeperator;
			}
			relationText = relationText + maxText;
			relationText = relationText + valueTypeText;
			if (!isPutOperatorFirst) {
				relationText = relationText + " ";
				relationText = relationText + operatorText;
			}
		}
		relationText = relationText.trim();
		return relationText;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		PRPriceGroupRelnDTO cloned = (PRPriceGroupRelnDTO) super.clone();
		return cloned;
	}

	public PRRange getPriceRangeUnitCost(MultiplePrice relatedItemPrice, double relatedItemSize, double itemSize,
			char relationType, double sizeShelfPCT, Double itemListCost, Double relatedItemListCost,
			PRItemDTO itemDTO, PRRange newRange) {
		PRRange range = null;
//		logger.info("getPriceRangeUnitCost()- Itemcode :" + itemDTO.getItemCode() + "relatedItemPrice: " + relatedItemPrice.getUnitPrice()
//		+ "related Item cost :" + relatedItemListCost + " Itemcost:"+ itemListCost  );

		// 29th Dec 2016, when related item is in multiple, its not taking unit price
		// while applying brand/size relation. Change data type of realtedItemPrice
		// from double to MultiplePrice
		

		if (relationType == PRConstants.BRAND_RELATION) {
			if (PRConstants.VALUE_TYPE_PCT == valueType) {
				range = getBrandPriceRangeWithPct(relatedItemPrice, relatedItemSize, itemSize);
			} else if (PRConstants.VALUE_TYPE_$ == valueType) {
				range = getBrandPriceRangeWith$(relatedItemPrice, relatedItemSize, itemSize);
			} else {
				range = getBrandPriceRangeWithNoValueType(relatedItemPrice, relatedItemSize, itemSize);
			}

			if (itemListCost != null && relatedItemListCost != null) {
				double absCostGap = Math.abs(itemListCost - relatedItemListCost);
				double costGap = (itemListCost - relatedItemListCost);
				double relatedItemUnitPrice = PRCommonUtil.getUnitPrice(relatedItemPrice, true);
				if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
						|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {

					double newPrice = 0;
					if (costGap > 0) {
						newPrice = relatedItemUnitPrice + costGap;
					}

					if (range.getEndVal() != Constants.DEFAULT_NA) {
						if (range.getEndVal() < newPrice) {
							range.setEndVal(newPrice);
							itemDTO.setAucOverride(true);
						}
					}

					if (range.getStartVal() != Constants.DEFAULT_NA) {
						if (range.getStartVal() < newPrice) {
							range.setStartVal(newPrice);
							itemDTO.setAucOverride(true);
						}
					}
					
				
				} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
						|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)) {

					double newPrice = 0;
					if (costGap < 0) {
						newPrice = relatedItemUnitPrice - absCostGap;
					}
				
					if (range.getEndVal() != Constants.DEFAULT_NA) {
						if (range.getEndVal() < newPrice) {
							range.setEndVal(newPrice);
							itemDTO.setAucOverride(true);
						}
					}
					if (range.getStartVal() != Constants.DEFAULT_NA) {
						if (range.getStartVal() < newPrice) {
							range.setStartVal(newPrice);
							itemDTO.setAucOverride(true);
						}
					}
				}
				
			}
		} else {
			// 9th Dec, when related item size is 0, don't apply size relation
			if (relatedItemSize > 0) {
//				if(PRConstants.VALUE_TYPE_PCT == valueType){
//					range = getSizePriceRangeWithPct(relatedItemPrice, relatedItemSize, itemSize);
//				}else if(PRConstants.VALUE_TYPE_$ == valueType){
//					range = getSizePriceRangeWith$(relatedItemPrice, relatedItemSize, itemSize);
//				}else{
//					range = getSizePriceRangeWithNoValueType(relatedItemPrice, relatedItemSize, itemSize);
//				}
				range = getSizePriceRangeWithNoValueType(relatedItemPrice, relatedItemSize, itemSize, sizeShelfPCT);
			} else {
				logger.debug("Size Guideline is ignored as Related Item Size is :" + relatedItemSize);
				range = new PRRange();
			}
		}

		return range;
	}

	@Override
	public String toString() {
		return "PRPriceGroupRelnDTO [valueType=" + valueType + ", minValue=" + minValue + ", maxValue=" + maxValue
				+ ", operatorText=" + operatorText + ", retailType=" + retailType + "]";
	}

	public PRRange getPriceRangeWithCompOverride(MultiplePrice relatedItemPrice, double relatedItemSize,
			double itemSize, Double sizeShelfPCT, char relationType, PRRange compRange, Double itemListCost,
			Double relatedItemListCost, PRItemDTO itemDTO, int compOverrideId, boolean isAUCEnabled, PRRange newRange) {
	
		PRRange range = null;

		if (relationType == PRConstants.BRAND_RELATION) {
			if (PRConstants.VALUE_TYPE_PCT == valueType) {
				range = getBrandPriceRangeWithPct(relatedItemPrice, relatedItemSize, itemSize);
			} else if (PRConstants.VALUE_TYPE_$ == valueType) {
				range = getBrandPriceRangeWith$(relatedItemPrice, relatedItemSize, itemSize);
			} else {
				range = getBrandPriceRangeWithNoValueType(relatedItemPrice, relatedItemSize, itemSize);
			}
			double relatedItemUnitPrice = PRCommonUtil.getUnitPrice(relatedItemPrice, true);	
			//set the operator text to further decide the outout range after compOverride.
			itemDTO.setOperatorText(operatorText);
			
			
			if (compRange != null && !isAUCEnabled) {
			
				double compPriceStartValue = compRange.getStartVal();
				double compPriceEndValue = compRange.getStartVal();
				ApplyCompOverride(compPriceStartValue, compPriceEndValue, range, itemDTO, compOverrideId,relatedItemUnitPrice,newRange);

			} else if (isAUCEnabled || compRange != null) {
				
				double absCostGap = Math.abs(itemListCost - relatedItemListCost);
				double costGap = (itemListCost - relatedItemListCost);
			
				double compPriceStartValue = 0;
				double compPriceEndValue =0;
				if (compRange != null) {
					 compPriceStartValue = compRange.getStartVal();
					 compPriceEndValue = compRange.getStartVal();
				}
				

				if (itemListCost != null && relatedItemListCost != null) {

					if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
							|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {

						double newPrice = 0;
						if (costGap > 0) {
							newPrice = relatedItemUnitPrice + costGap;
						}
						
						if (range.getEndVal() != Constants.DEFAULT_NA) {
							if (range.getEndVal() < newPrice) {
								range.setEndVal(newPrice);
								itemDTO.setAucOverride(true);
							}
						}

						if (range.getStartVal() != Constants.DEFAULT_NA) {
							if (range.getStartVal() < newPrice) {
								range.setStartVal(newPrice);
								itemDTO.setAucOverride(true);
							}
						}
						if (compRange != null) {
							ApplyCompOverride(compPriceStartValue, compPriceEndValue, range, itemDTO, compOverrideId,
									relatedItemUnitPrice, newRange);
						}

						if (itemDTO.isAucOverride()) {

							if (itemDTO.isCompOverride()) {
								itemDTO.setCompOverCost(true);
								itemDTO.setAucOverride(false);
							}
						}
						
						
					} else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
							|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)) {

//						logger.info("absCostGap:"+ absCostGap);
						double newPrice = 0;
						if (costGap < 0) {
							newPrice = relatedItemUnitPrice - absCostGap;
						}
					
//						logger.info("newPrice:"+ newPrice);
//						logger.info("bramdrange :"+ 	range.getEndVal() +";" +	range.getStartVal());
						if (range.getEndVal() != Constants.DEFAULT_NA) {
							if (range.getEndVal() < newPrice) {
								range.setEndVal(newPrice);
								itemDTO.setAucOverride(true);
							}
						}
						if (range.getStartVal() != Constants.DEFAULT_NA) {
							if (range.getStartVal() < newPrice) {
								range.setStartVal(newPrice);
								itemDTO.setAucOverride(true);
							}
						}
						
					
//						logger.info("before comp overrride range  :"+ 	range.getEndVal() +";" +	range.getStartVal());
						if (compRange != null) {
							ApplyCompOverride(compPriceStartValue, compPriceEndValue, range, itemDTO, compOverrideId,
									relatedItemUnitPrice, newRange);
						}

						if (itemDTO.isAucOverride()) {
							if (itemDTO.isCompOverride()) {
								itemDTO.setCompOverCost(true);
								itemDTO.setAucOverride(false);
							}
						}
						
					
					}
				}
			}
		} else {
			// 9th Dec, when related item size is 0, don't apply size relation
			if (relatedItemSize > 0) {
//				if(PRConstants.VALUE_TYPE_PCT == valueType){
//					range = getSizePriceRangeWithPct(relatedItemPrice, relatedItemSize, itemSize);
//				}else if(PRConstants.VALUE_TYPE_$ == valueType){
//					range = getSizePriceRangeWith$(relatedItemPrice, relatedItemSize, itemSize);
//				}else{
//					range = getePriceRangeWithNoValueType(relatedItemPriSizce, relatedItemSize, itemSize);
//				}
				range = getSizePriceRangeWithNoValueType(relatedItemPrice, relatedItemSize, itemSize, sizeShelfPCT);
			} else {
				logger.debug("Size Guideline is ignored as Related Item Size is :" + relatedItemSize);
				range = new PRRange();
			}
		}
		
	
		

		return range;
	}

	public void ApplyCompOverride(double compPriceStartValue, double compPriceEndValue, PRRange range,
			PRItemDTO itemDTO, int compOverrideId, double relatedItemUnitPrice, PRRange newRange) {
		
		if (PRConstants.PRICE_GROUP_EXPR_GREATER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_ABOVE.equals(operatorText)) {
		

			if (compOverrideId == PRConstants.MAX_RET_HT_MIN_RET_LT
					|| compOverrideId == PRConstants.MAX_RET_HT_MAX_RET_LT) {
				if (range.getEndVal() != Constants.DEFAULT_NA) {

					if (compPriceEndValue > range.getEndVal()) {
						newRange.setEndVal(compPriceEndValue);
//						logger.debug("Comp override1 :" + itemDTO.getItemCode());
						itemDTO.setCompOverride(true);
					}
				}
				if (range.getStartVal() != Constants.DEFAULT_NA) {
					if (compPriceStartValue > range.getStartVal()) {
						newRange.setStartVal(compPriceStartValue);
//						logger.debug("Comp override 2 :" + itemDTO.getItemCode());
						itemDTO.setCompOverride(true);
					}
				}
			} else if (compOverrideId == PRConstants.MIN_RET_HT_MIN_RET_LT
					|| compOverrideId == PRConstants.MIN_RET_HT_MAX_RET_LT) {
				
			
				if (range.getEndVal() != Constants.DEFAULT_NA) {
					
					
					if (compPriceEndValue < range.getEndVal()) {
						newRange.setEndVal(compPriceEndValue);
//						logger.debug("Comp override 3 :" + itemDTO.getItemCode());
						itemDTO.setCompOverride(true);
					}
					
				}
				if (range.getStartVal() != Constants.DEFAULT_NA) {
					if (compPriceStartValue < range.getStartVal()) {
						newRange.setStartVal(compPriceStartValue);
//						logger.debug("Comp override  4:" + itemDTO.getItemCode());
						itemDTO.setCompOverride(true);
					}
				}

			}
		}

		else if (PRConstants.PRICE_GROUP_EXPR_LESSER_SYM.equals(operatorText)
				|| PRConstants.PRICE_GROUP_EXPR_BELOW.equals(operatorText)) {
		
			if (compOverrideId == PRConstants.MAX_RET_HT_MIN_RET_LT
					|| compOverrideId == PRConstants.MIN_RET_HT_MIN_RET_LT) {
				if (range.getEndVal() != Constants.DEFAULT_NA) {

					if (compPriceEndValue < range.getEndVal() &&  compPriceEndValue < relatedItemUnitPrice) {
						newRange.setEndVal(compPriceEndValue);
//						logger.debug("Comp override 1:" + itemDTO.getItemCode());
						itemDTO.setCompOverride(true);
					}
				}
				if (range.getStartVal() != Constants.DEFAULT_NA) {
					if (compPriceStartValue < range.getStartVal() && compPriceEndValue < relatedItemUnitPrice) {
						newRange.setStartVal(compPriceStartValue);
//						logger.debug("Comp override2 :" + itemDTO.getItemCode());
						itemDTO.setCompOverride(true);
					}
				}

			} else if (compOverrideId == PRConstants.MAX_RET_HT_MAX_RET_LT
					|| compOverrideId == PRConstants.MIN_RET_HT_MAX_RET_LT) {

				if (range.getEndVal() != Constants.DEFAULT_NA) {

					if (compPriceEndValue > range.getEndVal()  && compPriceEndValue < relatedItemUnitPrice) {
						newRange.setEndVal(compPriceEndValue);
//						logger.debug("Comp override 3 :" + itemDTO.getItemCode());
						itemDTO.setCompOverride(true);
					}
				}
				if (range.getStartVal() != Constants.DEFAULT_NA ) {
					if (compPriceStartValue > range.getStartVal() &&  compPriceEndValue < relatedItemUnitPrice) {
						newRange.setStartVal(compPriceStartValue);
//						logger.debug ("Comp override  4:" + itemDTO.getItemCode());
						itemDTO.setCompOverride(true);
					}
				}
			}

		}

	}

}
