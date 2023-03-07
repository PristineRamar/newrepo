package com.pristine.dto;

public class VariationBaselineDTO implements IValueObject {
	private int baselineID;
	private int chainID;
	private int storeID;
	private int deptID;
	private String asOfDate;
	private float aiv;
	private float sale;
	private float netReg;
	private float depth;
	private float regUp;
	private float regDown;
	private float aivUpperLimit;
	private float saleUpperLimit;
	private float netRegUpperLimit;
	private float depthUpperLimit;
	private float aivLowerLimit;
	private float saleLowerLimit;
	private float netRegLowerLimit;
	private float depthLowerLimit;
	private float regUpUpperLimit;
	private float regUpLowerLimit;
	private float regDownUpperLimit;
	private float regDownLowerLimit;
	private int numOfStores;
	private int numOfItems;
	
	public float getRegUp() {
		return regUp;
	}
	public void setRegUp(float regUp) {
		this.regUp = regUp;
	}
	public float getRegDown() {
		return regDown;
	}
	public void setRegDown(float regDown) {
		this.regDown = regDown;
	}
	public float getRegUpUpperLimit() {
		return regUpUpperLimit;
	}
	public void setRegUpUpperLimit(float regUpUpperLimit) {
		this.regUpUpperLimit = regUpUpperLimit;
	}
	public float getRegUpLowerLimit() {
		return regUpLowerLimit;
	}
	public void setRegUpLowerLimit(float regUpLowerLimit) {
		this.regUpLowerLimit = regUpLowerLimit;
	}
	public float getRegDownUpperLimit() {
		return regDownUpperLimit;
	}
	public void setRegDownUpperLimit(float regDownUpperLimit) {
		this.regDownUpperLimit = regDownUpperLimit;
	}
	public float getRegDownLowerLimit() {
		return regDownLowerLimit;
	}
	public void setRegDownLowerLimit(float regDownLowerLimit) {
		this.regDownLowerLimit = regDownLowerLimit;
	}


	
	public float getAiv() {
		return aiv;
	}
	public void setAiv(float aiv) {
		this.aiv = aiv;
	}
	public float getAivLowerLimit() {
		return aivLowerLimit;
	}
	public void setAivLowerLimit(float aivLowerLimit) {
		this.aivLowerLimit = aivLowerLimit;
	}
	public float getAivUpperLimit() {
		return aivUpperLimit;
	}
	public void setAivUpperLimit(float aivUpperLimit) {
		this.aivUpperLimit = aivUpperLimit;
	}
	public String getAsOfDate() {
		return asOfDate;
	}
	public void setAsOfDate(String asOfDate) {
		this.asOfDate = asOfDate;
	}
	public int getBaselineID() {
		return baselineID;
	}
	public void setBaselineID(int baselineID) {
		this.baselineID = baselineID;
	}
	public int getChainID() {
		return chainID;
	}
	public void setChainID(int chainID) {
		this.chainID = chainID;
	}
	public float getDepth() {
		return depth;
	}
	public void setDepth(float depth) {
		this.depth = depth;
	}
	public float getDepthLowerLimit() {
		return depthLowerLimit;
	}
	public void setDepthLowerLimit(float depthLowerLimit) {
		this.depthLowerLimit = depthLowerLimit;
	}
	public float getDepthUpperLimit() {
		return depthUpperLimit;
	}
	public void setDepthUpperLimit(float depthUpperLimit) {
		this.depthUpperLimit = depthUpperLimit;
	}
	public int getDeptID() {
		return deptID;
	}
	public void setDeptID(int deptID) {
		this.deptID = deptID;
	}
	public float getNetReg() {
		return netReg;
	}
	public void setNetReg(float netReg) {
		this.netReg = netReg;
	}
	public float getNetRegLowerLimit() {
		return netRegLowerLimit;
	}
	public void setNetRegLowerLimit(float netRegLowerLimit) {
		this.netRegLowerLimit = netRegLowerLimit;
	}
	public float getNetRegUpperLimit() {
		return netRegUpperLimit;
	}
	public void setNetRegUpperLimit(float netRegUpperLimit) {
		this.netRegUpperLimit = netRegUpperLimit;
	}
	public float getSale() {
		return sale;
	}
	public void setSale(float sale) {
		this.sale = sale;
	}
	public float getSaleLowerLimit() {
		return saleLowerLimit;
	}
	public void setSaleLowerLimit(float saleLowerLimit) {
		this.saleLowerLimit = saleLowerLimit;
	}
	public float getSaleUpperLimit() {
		return saleUpperLimit;
	}
	public void setSaleUpperLimit(float saleUpperLimit) {
		this.saleUpperLimit = saleUpperLimit;
	}
	public int getStoreID() {
		return storeID;
	}
	public void setStoreID(int storeID) {
		this.storeID = storeID;
	}
	public int getNumOfItems() {
		return numOfItems;
	}
	public void setNumOfItems(int numOfItems) {
		this.numOfItems = numOfItems;
	}
	public int getNumOfStores() {
		return numOfStores;
	}
	public void setNumOfStores(int numOfStores) {
		this.numOfStores = numOfStores;
	}
	
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("Store Id = ").append(storeID).append(',');
		sb.append("Num of Stores = ").append(this.numOfStores).append(',');
		sb.append("Item Count = ").append(numOfItems).append(',');
		sb.append(" AIV = ").append(aiv);
		sb.append(" ( ").append(aivLowerLimit).append(", ").append(aivUpperLimit ).append("), ");
		sb.append(" Reg = ").append(this.netReg);
		sb.append(" ( ").append(netRegLowerLimit).append(", ").append(netRegUpperLimit ).append("), ");
		sb.append(" Sale = ").append(this.sale);
		sb.append(" ( ").append(saleLowerLimit).append(", ").append(saleUpperLimit ).append("), ");
		sb.append(" Depth = ").append(this.depth);
		sb.append(" ( ").append(depthLowerLimit).append(", ").append(depthUpperLimit ).append("), ");
		sb.append(" Regular up = ").append(this.regUp);
		sb.append(" ( ").append(regUpLowerLimit).append(", ").append(regUpUpperLimit ).append("), ");
		sb.append(" Regular down = ").append(this.regDown);
		sb.append(" ( ").append(regDownLowerLimit).append(", ").append(regDownUpperLimit ).append("), ");	

		return sb.toString();
		
	}
}

