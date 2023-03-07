package com.pristine.dto.offermgmt.oos;

public class OOSExpectationDTO {
	
	private int storeId;
	private int productId;
	private int productLevelId;
	private int itemLevelTrxCount;
	
	public int getStoreId() {
		return storeId;
	}
	public void setStoreId(int storeId) {
		this.storeId = storeId;
	}
	public int getProductId() {
		return productId;
	}
	public void setProductId(int productId) {
		this.productId = productId;
	}
	public int getProductLevelId() {
		return productLevelId;
	}
	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	public int getItemLevelTrxCount() {
		return itemLevelTrxCount;
	}
	public void setItemLevelTrxCount(int itemLevelTrxCount) {
		this.itemLevelTrxCount = itemLevelTrxCount;
	}
	public int getItemLevelUnitsCount() {
		return itemLevelUnitsCount;
	}
	public void setItemLevelUnitsCount(int itemLevelUnitsCount) {
		this.itemLevelUnitsCount = itemLevelUnitsCount;
	}
	public int getStoreLevelTrxCount() {
		return storeLevelTrxCount;
	}
	public void setStoreLevelTrxCount(int storeLevelTrxCount) {
		this.storeLevelTrxCount = storeLevelTrxCount;
	}
	private int itemLevelUnitsCount;
	private int storeLevelTrxCount;
}
