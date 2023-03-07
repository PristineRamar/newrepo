package com.pristine.dto.offermgmt;

//import org.apache.log4j.Logger;

public class PRConstraintMinMax  implements  Cloneable{
//	private static Logger logger = Logger.getLogger("PRConstraintMinMax");
	
	private long cMinMaxId;
	private long cId;
	private double minValue;
	private double maxValue;
	private int quantity;
	
	public long getcMinMaxId() {
		return cMinMaxId;
	}
	public void setcMinMaxId(long cMinMaxId) {
		this.cMinMaxId = cMinMaxId;
	}
	public long getcId() {
		return cId;
	}
	public void setcId(long cId) {
		this.cId = cId;
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
	public int getQuantity() {
		return quantity;
	}
	public void setQuantity(int quantity) {
		this.quantity = quantity;
	}
	
	/**
	 * Methods that returns Price Range for Min Max Constraint
	 * @return
	 */
	public PRRange getPriceRange(){
		PRRange range = new PRRange();
		if(minValue > 0){
			range.setStartVal(minValue/quantity);
		}
		if(maxValue > 0){
			range.setEndVal(maxValue/quantity);
		}
		return range;
	}
	
	/**
	 * Methods that returns Price Range for Min Max Constraint
	 * @return
	 */
	public  static PRRange getPriceRange(double minValue, double maxValue){
		PRRange range = new PRRange();
		if(minValue > 0){
			range.setStartVal(minValue);
		}
		if(maxValue > 0){
			range.setEndVal(maxValue);
		}
		return range;
	}
	
	@Override
	protected Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
