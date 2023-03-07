package com.pristine.dto.offermgmt;

import java.util.ArrayList;
import java.util.List;

//import java.io.Serializable;

import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;
@IgnoreSizeOf
//public class PRPriceGroupRelatedItemDTO implements Serializable, Comparable<PRPriceGroupRelatedItemDTO>{
	public class PRPriceGroupRelatedItemDTO implements Comparable<PRPriceGroupRelatedItemDTO>, Cloneable {
	private int relatedItemCode;
	private double relatedItemSize;
	private String relatedUOMName = "";
	private int relatedItemBrandId;
	private int relatedItemBrandTierId;
	private String relatedItemBrandTier;
	private boolean isOverriddenRelatedItem = false;
//	private double relatedItemPrice;
	//29th Dec 2016, when related item is in multiple, its not taking unit price
	//while applying brand/size relation
	private MultiplePrice relatedItemPrice;
	private char relationType;
	private int brandPrecedence;
	private char htol = 'X';
	private boolean isLig = false;
	@Override
	public String toString() {
		return "PRPriceGroupRelatedItemDTO [relatedItemCode=" + relatedItemCode + ", relatedItemSize=" + relatedItemSize
				+ ", relatedUOMName=" + relatedUOMName + ", relatedItemBrandId=" + relatedItemBrandId
				+ ", relatedItemBrandTierId=" + relatedItemBrandTierId + ", isOverriddenRelatedItem="
				+ isOverriddenRelatedItem + ", relatedItemPrice=" + relatedItemPrice + ", relationType=" + relationType
				+ ", brandPrecedence=" + brandPrecedence + ", htol=" + htol + ", isLig=" + isLig + ", isDependentItem="
				+ isDependentItem + ", listCost=" + listCost + ", priceRelation=" + priceRelation + "]";
	}

	private boolean isDependentItem = false;
	private Double listCost;
	private boolean isRelationOperatorChanged = false;
	@IgnoreSizeOf
	private PRPriceGroupRelnDTO priceRelation = null;
	@IgnoreSizeOf
	private List<PRPriceGroupRelnDTO> priceRelations = new ArrayList<PRPriceGroupRelnDTO>();
	
	public int getRelatedItemCode() {
		return relatedItemCode;
	}

	public void setRelatedItemCode(int relatedItemCode) {
		this.relatedItemCode = relatedItemCode;
	}

	public double getRelatedItemSize() {
		return relatedItemSize;
	}

	public void setRelatedItemSize(double relatedItemSize) {
		this.relatedItemSize = relatedItemSize;
	}

	public int getRelatedItemBrandId() {
		return relatedItemBrandId;
	}

	public void setRelatedItemBrandId(int relatedItemBrandId) {
		this.relatedItemBrandId = relatedItemBrandId;
	}

	public int getRelatedItemBrandTierId() {
		return relatedItemBrandTierId;
	}

	public void setRelatedItemBrandTierId(int relatedItemBrandTierId) {
		this.relatedItemBrandTierId = relatedItemBrandTierId;
	}
	
	public MultiplePrice getRelatedItemPrice() {
		return relatedItemPrice;
	}

	public void setRelatedItemPrice(MultiplePrice relatedItemPrice) {
		this.relatedItemPrice = relatedItemPrice;
	}

	public PRPriceGroupRelnDTO getPriceRelation() {
		return priceRelation;
	}

	public void setPriceRelation(PRPriceGroupRelnDTO priceRelation) {
		this.priceRelation = priceRelation;
	}
	
	public char getRelationType() {
		return relationType;
	}

	public void setRelationType(char relationType) {
		this.relationType = relationType;
	}

	public int getBrandPrecedence() {
		return brandPrecedence;
	}

	public void setBrandPrecedence(int brandPrecedence) {
		this.brandPrecedence = brandPrecedence;
	}

	public char getHtol() {
		return htol;
	}

	public void setHtol(char htol) {
		this.htol = htol;
	}
	
	public int compareTo(PRPriceGroupRelatedItemDTO dto)
	{
	     if(getBrandPrecedence() == dto.getBrandPrecedence())
	    	 return 0;
	     else if(getBrandPrecedence() < dto.getBrandPrecedence())
	    	 return -1;
	     else
	    	 return 1;
	}

	public String getRelatedUOMName() {
		return relatedUOMName;
	}

	public void setRelatedUOMName(String relatedUOMName) {
		this.relatedUOMName = relatedUOMName;
	}


	public boolean getIsLig() {
		return isLig;
	}

	public void setIsLig(boolean isLig) {
		this.isLig = isLig;
	}

	public boolean isOverriddenRelatedItem() {
		return isOverriddenRelatedItem;
	}

	public void setOverriddenRelatedItem(boolean isOverriddenRelatedItem) {
		this.isOverriddenRelatedItem = isOverriddenRelatedItem;
	}
	
	public void copy(PRPriceGroupRelatedItemDTO prPriceGroupRelatedItemDTO){
		this.relatedItemCode =prPriceGroupRelatedItemDTO.getRelatedItemCode();
		this.relatedItemSize =prPriceGroupRelatedItemDTO.getRelatedItemSize();
		this.relatedUOMName = prPriceGroupRelatedItemDTO.getRelatedUOMName();
		this.relatedItemBrandId =prPriceGroupRelatedItemDTO.getRelatedItemBrandId();
		this.relatedItemBrandTierId =prPriceGroupRelatedItemDTO.getRelatedItemBrandTierId();
		this.isOverriddenRelatedItem = prPriceGroupRelatedItemDTO.isOverriddenRelatedItem;
		this.relatedItemPrice = prPriceGroupRelatedItemDTO.getRelatedItemPrice()  ;
		this.relationType =prPriceGroupRelatedItemDTO.getRelationType();
		this.brandPrecedence =prPriceGroupRelatedItemDTO.getBrandPrecedence();
		this.htol = prPriceGroupRelatedItemDTO.getHtol();
		this.isLig = prPriceGroupRelatedItemDTO.getIsLig();
		this.priceRelation = prPriceGroupRelatedItemDTO.getPriceRelation();
	}

	public boolean isDependentItem() {
		return isDependentItem;
	}

	public void setDependentItem(boolean isDependentItem) {
		this.isDependentItem = isDependentItem;
	}
	@Override
	public Object clone() throws CloneNotSupportedException {
		PRPriceGroupRelatedItemDTO cloned = (PRPriceGroupRelatedItemDTO) super.clone();

		if (cloned.getPriceRelation() != null)
			cloned.setPriceRelation(((PRPriceGroupRelnDTO) cloned.getPriceRelation().clone()));

		return cloned;
	}

	public Double getListCost() {
		return listCost;
	}

	public void setListCost(Double listCost) {
		this.listCost = listCost;
	}

	public String getRelatedItemBrandTier() {
		return relatedItemBrandTier;
	}

	public void setRelatedItemBrandTier(String relatedItemBrandTier) {
		this.relatedItemBrandTier = relatedItemBrandTier;
	}

	public boolean isRelationOperatorChanged() {
		return isRelationOperatorChanged;
	}

	public void setRelationOperatorChanged(boolean isRelationOperatorChanged) {
		this.isRelationOperatorChanged = isRelationOperatorChanged;
	}

	public List<PRPriceGroupRelnDTO> getPriceRelations() {
		return priceRelations;
	}

	public void setPriceRelations(List<PRPriceGroupRelnDTO> priceRelations) {
		this.priceRelations = priceRelations;
	}
	
	public void addPriceRelations(PRPriceGroupRelnDTO priceRelation) {
		this.priceRelations.add(priceRelation);
	}
}
