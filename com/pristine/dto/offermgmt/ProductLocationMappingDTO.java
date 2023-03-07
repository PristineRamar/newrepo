package com.pristine.dto.offermgmt;

public class ProductLocationMappingDTO implements Cloneable{
	private String category;
	private int primaryCompetitor;
	private String priceZone;
	private String store;
	public int productId;
	private int primaryCompetitorStrId;
	public int locationId;
	public int storeId;
	public int productLocationMappingId;
	public int productLevelId;
	public int locationLevelId;
	public String prcGrpCode;
	public int parentLocationLevelId;
	public int parentLocationId;
	public boolean isDSDZone;
	/**
	 * @return the category
	 */
	public String getCategory() {
		return category;
	}
	/**
	 * @param category the category to set
	 */
	public void setCategory(String category) {
		this.category = category;
	}
	/**
	 * @return the primaryCompetitor
	 */
	public int getPrimaryCompetitor() {
		return primaryCompetitor;
	}
	/**
	 * @param primaryCompetitor the primaryCompetitor to set
	 */
	public void setPrimaryCompetitor(int primaryCompetitor) {
		this.primaryCompetitor = primaryCompetitor;
	}
	/**
	 * @return the priceZone
	 */
	public String getPriceZone() {
		return priceZone;
	}
	/**
	 * @param priceZone the priceZone to set
	 */
	public void setPriceZone(String priceZone) {
		this.priceZone = priceZone;
	}
	/**
	 * @return the store
	 */
	public String getStore() {
		return store;
	}
	/**
	 * @param store the store to set
	 */
	public void setStore(String store) {
		this.store = store;
	}
	/**
	 * @return the storeId
	 */
	public int getStoreId() {
		return storeId;
	}
	/**
	 * @param storeId the storeId to set
	 */
	public void setStoreId(int storeId) {
		this.storeId = storeId;
	}
	/**
	 * @return the primaryCompetitorStrId
	 */
	public int getPrimaryCompetitorStrId() {
		return primaryCompetitorStrId;
	}
	/**
	 * @param primaryCompetitorStrId the primaryCompetitorStrId to set
	 */
	public void setPrimaryCompetitorStrId(int primaryCompetitorStrId) {
		this.primaryCompetitorStrId = primaryCompetitorStrId;
	}
	/**
	 * @return the productLocationMappingId
	 */
	public int getProductLocationMappingId() {
		return productLocationMappingId;
	}
	/**
	 * @param productLocationMappingId the productLocationMappingId to set
	 */
	public void setProductLocationMappingId(int productLocationMappingId) {
		this.productLocationMappingId = productLocationMappingId;
	}
	/**
	 * @return the productId
	 */
	public int getProductId() {
		return productId;
	}
	/**
	 * @param productId the productId to set
	 */
	public void setProductId(int productId) {
		this.productId = productId;
	}
	/**
	 * @return the locationId
	 */
	public int getLocationId() {
		return locationId;
	}
	/**
	 * @param locationId the locationId to set
	 */
	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}
	/**
	 * @return the productLevelId
	 */
	public int getProductLevelId() {
		return productLevelId;
	}
	/**
	 * @param productLevelId the productLevelId to set
	 */
	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}
	/**
	 * @return the locationLevelId
	 */
	public int getLocationLevelId() {
		return locationLevelId;
	}
	/**
	 * @param locationLevelId the locationLevelId to set
	 */
	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}
	
	@Override
	public ProductLocationMappingDTO clone() throws CloneNotSupportedException{
		return (ProductLocationMappingDTO) super.clone();
	}
	
}
