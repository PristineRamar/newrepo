package com.pristine.dto.offermgmt.substitute;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.util.Constants;
import com.pristine.util.offermgmt.PRConstants;

public class SubstituteAdjRInputDTO {
	private int locationLevelId;
	private int locationId;
	private int productLevelId;
	private int productId;
	private List<SubjstituteAdjRMainItemDTO> mainItems = new ArrayList<SubjstituteAdjRMainItemDTO>();
	
	
	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}
	
	public List<SubjstituteAdjRMainItemDTO> getMainItems() {
		return mainItems;
	}

	public void setMainItems(List<SubjstituteAdjRMainItemDTO> mainItems) {
		this.mainItems = mainItems;
	}

	public int[] getIntArray(String propertyName) {
		int[] ret = new int[mainItems.size()];
		Iterator<SubjstituteAdjRMainItemDTO> iterator = mainItems.iterator();
		for (int i = 0; i < ret.length; i++) {
			if (propertyName.equals("MAIN_ITEM_CODE")) {
				ret[i] = iterator.next().getItemKey().getItemCodeOrRetLirId();
			} else if (propertyName.equals("MAIN_ITEM_REC_WEEK_REG_MULTIPLE")) {
				MultiplePrice multiplePrice = iterator.next().getRecWeekRegPrice();
				ret[i] = (multiplePrice != null && multiplePrice.multiple != null) ? multiplePrice.multiple : 0;
			} else if (propertyName.equals("MAIN_ITEM_REC_WEEK_SALE_MULTIPLE")) {
				MultiplePrice multiplePrice = iterator.next().getRecWeekSalePrice();
				ret[i] = (multiplePrice != null && multiplePrice.multiple != null) ? multiplePrice.multiple : 0;
			}
		}
		return ret;
	}

	public double[] getDoubleArray(String propertyName) {
		double[] ret = new double[mainItems.size()];
		Iterator<SubjstituteAdjRMainItemDTO> iterator = mainItems.iterator();
		for (int i = 0; i < ret.length; i++) {
			if (propertyName.equals("MAIN_ITEM_REC_WEEK_REG_PRICE")) {
				MultiplePrice multiplePrice = iterator.next().getRecWeekRegPrice();
				ret[i] = (multiplePrice != null && multiplePrice.price != null) ? multiplePrice.price : 0;
			} else if (propertyName.equals("MAIN_ITEM_REC_WEEK_SALE_PRICE")) {
				MultiplePrice multiplePrice = iterator.next().getRecWeekSalePrice();
				ret[i] = (multiplePrice != null && multiplePrice.price != null) ? multiplePrice.price : 0;
			} else if (propertyName.equals("MAIN_ITEM_REG_PRED")) {
				Double pred = iterator.next().getRegPrediction();
				ret[i] = pred == null ? 0 : pred;
			} else if (propertyName.equals("MAIN_ITEM_SALE_PRED")) {
				Double pred = iterator.next().getSalePrediction();
				ret[i] = pred == null ? 0 : pred;
			}
		}
		return ret;
	}

	public String[] getSubStringArray(String propertyName) {
		String[] ret = new String[mainItems.size()];
		Iterator<SubjstituteAdjRMainItemDTO> iterator = mainItems.iterator();
		for (int i = 0; i < ret.length; i++) {
			String out = "";
			List<SubstituteAdjRSubsItemDTO> subsItems = iterator.next().getSubstituteItems();
			for (SubstituteAdjRSubsItemDTO subsItem : subsItems) {
				if (propertyName == "SUB_ITEM_CODE") {
					out = out + "_" + subsItem.getItemKey().getItemCodeOrRetLirId();
				} else if (propertyName == "SUB_LEVEL_ID") {
					out = out + "_" + (subsItem.getItemKey().getLirIndicator() == PRConstants.NON_LIG_ITEM_INDICATOR
							? Constants.ITEM_LEVEL_CHILD_PRODUCT_LEVEL_ID : Constants.PRODUCT_LEVEL_ID_LIG);
				} else if (propertyName == "SUB_REC_WEEK_REG_MULTIPLE") {
					out = out + "_" + (subsItem.getRecWeekRegPrice() != null
							? (String.valueOf(subsItem.getRecWeekRegPrice().multiple == null ? 0 : subsItem.getRecWeekRegPrice().multiple)) : 0);
				} else if (propertyName == "SUB_REC_WEEK_REG_PRICE") {
					out = out + "_" + (subsItem.getRecWeekRegPrice() != null
							? (String.valueOf(subsItem.getRecWeekRegPrice().price == null ? 0 : subsItem.getRecWeekRegPrice().price)) : 0);
				} else if (propertyName == "SUB_REC_WEEK_SALE_MULTIPLE") {
					out = out + "_" + (subsItem.getRecWeekSalePrice() != null
							? (String.valueOf(subsItem.getRecWeekSalePrice().multiple == null ? 0 : subsItem.getRecWeekSalePrice().multiple)) : 0);
				} else if (propertyName == "SUB_REC_WEEK_SALE_PRICE") {
					out = out + "_" + (subsItem.getRecWeekSalePrice() != null
							? (String.valueOf(subsItem.getRecWeekSalePrice().price == null ? 0 : subsItem.getRecWeekSalePrice().price)) : 0);
				} else if (propertyName == "SUB_CUR_REG_MULTIPLE") {
					out = out + "_" + (subsItem.getCurRegPrice() != null
							? (String.valueOf(subsItem.getCurRegPrice().multiple == null ? 0 : subsItem.getCurRegPrice().multiple)) : 0);
				} else if (propertyName == "SUB_CUR_REG_PRICE") {
					out = out + "_" + (subsItem.getCurRegPrice() != null
							? (String.valueOf(subsItem.getCurRegPrice().price == null ? 0 : subsItem.getCurRegPrice().price)) : 0);
				} else if (propertyName == "SUB_CUR_SALE_MULTIPLE") {
					out = out + "_" + (subsItem.getCurSalePrice() != null
							? (String.valueOf(subsItem.getCurSalePrice().multiple == null ? 0 : subsItem.getCurSalePrice().multiple)) : 0);
				} else if (propertyName == "SUB_CUR_SALE_PRICE") {
					out = out + "_" + (subsItem.getCurSalePrice() != null
							? (String.valueOf(subsItem.getCurSalePrice().price == null ? 0 : subsItem.getCurSalePrice().price)) : 0);
				} else if (propertyName == "IMPACT_FACTOR") {
					out = out + "_" + String.valueOf(subsItem.getImpactFactor() == null ? 0 : subsItem.getImpactFactor());
				}
			}
			ret[i] = out.length() > 0 ? out.substring(1) : out;
		}
		return ret;
	}
	
	public boolean hasAtlteastOneSubsItem() {
		boolean hasAtlteastOneSubsItem = false; 
		String[] ret = new String[mainItems.size()];
		Iterator<SubjstituteAdjRMainItemDTO> iterator = mainItems.iterator();
		for (int i = 0; i < ret.length; i++) {
			List<SubstituteAdjRSubsItemDTO> subsItems = iterator.next().getSubstituteItems();
			if(subsItems.size() > 0) {
				hasAtlteastOneSubsItem = true;
				break;
			}
		}
		return hasAtlteastOneSubsItem;
	}
}
