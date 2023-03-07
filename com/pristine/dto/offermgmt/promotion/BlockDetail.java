/**
 * 
 */
package com.pristine.dto.offermgmt.promotion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.ProductKey;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.util.offermgmt.PRCommonUtil;

/**
 * @author dunagaraj
 *
 */
public class BlockDetail {

	private PageBlockNoKey pageBlockNoKey = null;
	private HashSet<ProductKey> categoryIds = new HashSet<ProductKey>();
	private PromoTypeLookup promoType = null;
	private Double minRegPrice = null;
	private Double maxRegPrice = null;
	private HashSet<Integer> brandIds = new HashSet<Integer>();
	private HashMap<Integer, BrandDetail> brands = new HashMap<Integer, BrandDetail>();
	private String weeklyAdStartDate = "";
	private PredictionMetrics regPricePredictionMetrics = new PredictionMetrics();
	private PredictionMetrics salePricePredictionMetrics = new PredictionMetrics();
	private long totalHHCnt = 0;
	private HashMap<Integer, String> departments = new HashMap<Integer, String>();
	private List<PromoItemDTO> items = new ArrayList<PromoItemDTO>();
	private HashSet<MultiplePrice> salePrices = new HashSet<MultiplePrice>();

	public HashSet<ProductKey> getCategoryIds() {
		return categoryIds;
	}

	public void setCategoryIds(HashSet<ProductKey> categoryIds) {
		this.categoryIds = categoryIds;
	}

	public PromoTypeLookup getPromoType() {
		return promoType;
	}

	public void setPromoType(PromoTypeLookup promoType) {
		this.promoType = promoType;
	}

	public Double getMinRegPrice() {
		return minRegPrice;
	}

	public void setMinRegPrice(Double minRegPrice) {
		this.minRegPrice = minRegPrice;
	}

	public Double getMaxRegPrice() {
		return maxRegPrice;
	}

	public void setMaxRegPrice(Double maxRegPrice) {
		this.maxRegPrice = maxRegPrice;
	}

	public String getWeeklyAdStartDate() {
		return weeklyAdStartDate;
	}

	public void setWeeklyAdStartDate(String weeklyAdStartDate) {
		this.weeklyAdStartDate = weeklyAdStartDate;
	}

	public PredictionMetrics getRegPricePredictionMetrics() {
		return regPricePredictionMetrics;
	}

	public void setRegPricePredictionMetrics(PredictionMetrics regPricePredictionMetrics) {
		this.regPricePredictionMetrics = regPricePredictionMetrics;
	}

	public PredictionMetrics getSalePricePredictionMetrics() {
		return salePricePredictionMetrics;
	}

	public void setSalePricePredictionMetrics(PredictionMetrics salePricePredictionMetrics) {
		this.salePricePredictionMetrics = salePricePredictionMetrics;
	}


	public long getTotalHHCnt() {
		return totalHHCnt;
	}

	public void setTotalHHCnt(long totalHHCnt) {
		this.totalHHCnt = totalHHCnt;
	}

	public HashMap<Integer, String> getDepartments() {
		return departments;
	}

	public void setDepartments(HashMap<Integer, String> departments) {
		this.departments = departments;
	}

	public HashMap<Integer, BrandDetail> getBrands() {
		return brands;
	}

	public void setBrands(HashMap<Integer, BrandDetail> brands) {
		this.brands = brands;
	}

	public HashSet<Integer> getBrandIds() {
		return brandIds;
	}

	public void setBrandIds(HashSet<Integer> brandIds) {
		this.brandIds = brandIds;
	}

	@Override
	public String toString() {
		return "BlockDetail [pageBlockNoKey=" + pageBlockNoKey + ", categoryIds=" + categoryIds + ", promoType=" + promoType + ", minRegPrice="
				+ minRegPrice + ", maxRegPrice=" + maxRegPrice + ", brandIds=" + brandIds + ", brands=" + brands + ", weeklyAdStartDate="
				+ weeklyAdStartDate + ", regPricePredictionMetrics=" + regPricePredictionMetrics + ", salePricePredictionMetrics="
				+ salePricePredictionMetrics + ", totalHHCnt=" + totalHHCnt + ", departments=" + departments 
				+ ", items=" + items + "]";
	}

	public List<PromoItemDTO> getItems() {
		return items;
	}

	public void setItems(List<PromoItemDTO> items) {
		this.items = items;
	}

	public PageBlockNoKey getPageBlockNoKey() {
		return pageBlockNoKey;
	}

	public void setPageBlockNoKey(PageBlockNoKey pageBlockNoKey) {
		this.pageBlockNoKey = pageBlockNoKey;
	}

	
	public double getSaleUnitsLiftPCTAgainstRegUnits() {
		return PRCommonUtil.getLiftPCT(this.regPricePredictionMetrics.getPredMov(), this.salePricePredictionMetrics.getPredMov());
	}
	public double getSaleRevLiftPCTAgainstRegRev() {
		return PRCommonUtil.getLiftPCT(this.regPricePredictionMetrics.getPredRev(), this.salePricePredictionMetrics.getPredRev());
	}
	public double getSaleMarLiftPCTAgainstRegMar() {
		return PRCommonUtil.getLiftPCT(this.regPricePredictionMetrics.getPredMar(), this.salePricePredictionMetrics.getPredMar());
	}

	public HashSet<MultiplePrice> getSalePrices() {
		return salePrices;
	}

	public void setSalePrices(HashSet<MultiplePrice> salePrices) {
		this.salePrices = salePrices;
	}
	
	public String getDepartmentName() {
		List<String> departmentNames = new ArrayList<>();
		this.getDepartments().values().forEach(depName -> {
			departmentNames.add(depName);
		});
		
		return PRCommonUtil.getCommaSeperatedStringFromStrArray(departmentNames);
	}
	
}
