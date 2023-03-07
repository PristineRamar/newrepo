package com.pristine.dto.offermgmt;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.TreeMap;

import com.pristine.util.offermgmt.PRConstants;

import net.sf.ehcache.pool.sizeof.annotations.IgnoreSizeOf;

@IgnoreSizeOf
//public class PRPriceGroupDTO implements Serializable{
public class PRPriceGroupDTO {
	private int priceGroupId;
	private String priceGroupName;
	private int itemCode;
	private double itemSize;
	private String uomName = "";
	private int brandId;
	private int brandTierId;
	private int brandClassId;
	private char brandSizePrecedence;
	private char isPriceGroupLead;
	private char isSizeFamilyLead;
	private int sizeFamilyId;
	private String sizeRelationText = "";
	private boolean isLig = false;
	private String brandTierName;
	@IgnoreSizeOf
	// TreeMap<B-Brand/S-Size, List of Related Item Info>
	private TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList = new TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>>(); 

	public int getPriceGroupId() {
		return priceGroupId;
	}

	public void setPriceGroupId(int priceGroupId) {
		this.priceGroupId = priceGroupId;
	}

	public String getPriceGroupName() {
		return priceGroupName;
	}

	public void setPriceGroupName(String priceGroupName) {
		this.priceGroupName = priceGroupName;
	}

	public int getItemCode() {
		return itemCode;
	}

	public void setItemCode(int itemCode) {
		this.itemCode = itemCode;
	}

	public double getItemSize() {
		return itemSize;
	}

	public void setItemSize(double itemSize) {
		this.itemSize = itemSize;
	}

	public int getBrandId() {
		return brandId;
	}

	public void setBrandId(int brandId) {
		this.brandId = brandId;
	}

	public int getBrandTierId() {
		return brandTierId;
	}

	public void setBrandTierId(int brandTierId) {
		this.brandTierId = brandTierId;
	}

	public int getBrandClassId() {
		return brandClassId;
	}

	public void setBrandClassId(int brandClassId) {
		this.brandClassId = brandClassId;
	}
	
	public char getBrandSizePrecedence() {
		return brandSizePrecedence;
	}

	public void setBrandSizePrecedence(char brandSizePrecedence) {
		this.brandSizePrecedence = brandSizePrecedence;
	}

	public char getIsPriceGroupLead() {
		return isPriceGroupLead;
	}

	public void setIsPriceGroupLead(char isPriceGroupLead) {
		this.isPriceGroupLead = isPriceGroupLead;
	}

	public char getIsSizeFamilyLead() {
		return isSizeFamilyLead;
	}

	public void setIsSizeFamilyLead(char isSizeFamilyLead) {
		this.isSizeFamilyLead = isSizeFamilyLead;
	}

	public int getSizeFamilyId() {
		return sizeFamilyId;
	}

	public void setSizeFamilyId(int sizeFamilyId) {
		this.sizeFamilyId = sizeFamilyId;
	}
	
	public TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> getRelationList() {
		return relationList;
	}

	public void setRelationList(TreeMap<Character, ArrayList<PRPriceGroupRelatedItemDTO>> relationList) {
		this.relationList = relationList;
	}
	
	public void addBrandRelation(PRPriceGroupRelatedItemDTO brandRelation){
		ArrayList<PRPriceGroupRelatedItemDTO> tList = null;
		if(relationList.get(PRConstants.BRAND_RELATION) == null)
			tList = new ArrayList<PRPriceGroupRelatedItemDTO>();
		else
			tList = relationList.get(PRConstants.BRAND_RELATION);
		tList.add(brandRelation);
		relationList.put(PRConstants.BRAND_RELATION, tList);
	}
	
	public void addSizeRelation(PRPriceGroupRelatedItemDTO sizeRelation){
		ArrayList<PRPriceGroupRelatedItemDTO> tList = null;
		if(relationList.get(PRConstants.SIZE_RELATION) == null)
			tList = new ArrayList<PRPriceGroupRelatedItemDTO>();
		else
			tList = relationList.get(PRConstants.SIZE_RELATION);
		tList.add(sizeRelation);
		relationList.put(PRConstants.SIZE_RELATION, tList);
	}
	
	public void copy(PRPriceGroupDTO prPriceGroupDTO) {
		this.priceGroupId = prPriceGroupDTO.priceGroupId;
		this.priceGroupName = prPriceGroupDTO.priceGroupName;
		this.brandTierId = prPriceGroupDTO.brandTierId;
		this.brandClassId = prPriceGroupDTO.brandClassId;
		this.relationList = prPriceGroupDTO.relationList;
		this.itemSize = prPriceGroupDTO.itemSize;
		this.brandSizePrecedence = prPriceGroupDTO.brandSizePrecedence;
		this.sizeFamilyId = prPriceGroupDTO.sizeFamilyId;
		this.sizeRelationText = prPriceGroupDTO.sizeRelationText;
	}

	public String getSizeRelationText() {
		return sizeRelationText;
	}

	public void setSizeRelationText(String sizeRelationText) {
		this.sizeRelationText = sizeRelationText;
	}

	public String getUomName() {
		return uomName;
	}

	public void setUomName(String uomName) {
		this.uomName = uomName;
	}

	public boolean getIsLig() {
		return isLig;
	}

	public void setIsLig(boolean isLig) {
		this.isLig = isLig;
	}

	public String getBrandTierName() {
		return brandTierName;
	}

	public void setBrandTierName(String brandTierName) {
		this.brandTierName = brandTierName;
	}

	 
}
