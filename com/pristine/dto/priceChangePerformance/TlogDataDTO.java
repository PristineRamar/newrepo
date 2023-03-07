package com.pristine.dto.priceChangePerformance;

import java.time.LocalDate;


public class TlogDataDTO {
			
	private int DayCalendarId;
	private int WeekCalendarId;
	private int StoreId;
	private int ProductId;
	private int ProductLevelId;
	private boolean LigMember;
	private float UnitPrice;
	private int Quantity;
	//private float Weight;
	private float NetAmount;
	//private String SaleType;
	private long CustomerId;
	private float RegAmount;
	private LocalDate DayStartDate;
	private LocalDate WeekStartDate;
	private String CustomerType;
	private float NetRegAmt;
	private float Discount;
	
	//Item Info
	private String RetLIRName;
	private String RetailerItemCode;
	
	//Price Data Fields
	private float LY_RegularPrice;
	private float LY_ListCost;
	
	//IMS DATA Fields
	private float RegularPrice;
	private float Regular_M_Price;
	private int Regular_M_Pack;
	//private float Finalprice;
	private float ListCost;
	private double TotalRevenue;
	private double TotalMovement;
	private double NetMargin;
	//private int CalendarId;
	//private float SalePrice;
	//private float Sale_M_Price;
	//private int Sale_M_Pack;
	private boolean TotalMovementNull;
	private boolean TotalRevenueNull;
	
	//Extra
	private int Total_Mov;
	//private boolean ListCostNull;
	//private boolean RegularPriceNull;
	
	//PERCENTAGE
	private float Reg_Perc;
	private float Reg_Mov;
	private float Reg_Mar;
	private float Reg_Rev;
	
	private float Sil_Perc;
	private float Sil_Mov;
	private float Sil_Mar;
	private float Sil_Rev;
	
	private float Gold_Perc;
	private float Gold_Mov;
	private float Gold_Mar;
	private float Gold_Rev;
	
	private float Sale_Perc;
	
	private float No_Card_Perc;
	
	
	
	//Prediction
	private float Prediction; 
	private float PredictedSale;
	private float PredictedMargin;
	private boolean IsPredPresent; 
	
	private boolean IsDeleteForMulUPC;
	
	public boolean isPredPresent() {
		return IsPredPresent;
	}
	public void setIsPredPresent(boolean isPredPresent) {
		IsPredPresent = isPredPresent;
	}
	public float getReg_Perc() {
		return Reg_Perc;
	}
	public void setReg_Perc(float reg_Perc) {
		Reg_Perc = reg_Perc;
	}
	public float getSil_Perc() {
		return Sil_Perc;
	}
	public void setSil_Perc(float sil_Perc) {
		Sil_Perc = sil_Perc;
	}
	public float getGold_Perc() {
		return Gold_Perc;
	}
	public void setGold_Perc(float gold_Perc) {
		Gold_Perc = gold_Perc;
	}
	public float getSale_Perc() {
		return Sale_Perc;
	}
	public void setSale_Perc(float sale_Perc) {
		Sale_Perc = sale_Perc;
	}
	
	public String getCustomerType() {
		return CustomerType;
	}
	public void setCustomerType(String customerType) {
		CustomerType = customerType;
	}
	public float getNetRegAmt() {
		return NetRegAmt;
	}
	public void setNetRegAmt(float netRegAmt) {
		NetRegAmt = netRegAmt;
	}
	public float getDiscount() {
		return Discount;
	}
	public void setDiscount(float discount) {
		Discount = discount;
	}
	
	
	public LocalDate getWeekStartDate() {
		return WeekStartDate;
	}
	public void setWeekStartDate(LocalDate weekStartDate) {
		WeekStartDate = weekStartDate;
	}
	public int getDayCalendarId() {
		return DayCalendarId;
	}
	public void setDayCalendarId(int calendarId) {
		DayCalendarId = calendarId;
	}
	public int getStoreId() {
		return StoreId;
	}
	public void setStoreId(int storeId) {
		StoreId = storeId;
	}
	public int getProductId() {
		return ProductId;
	}
	public void setProductId(int productId) {
		ProductId = productId;
	}
	public float getUnitPrice() {
		return UnitPrice;
	}
	public void setUnitPrice(float unitPrice) {
		UnitPrice = unitPrice;
	}
	public int getQuantity() {
		return Quantity;
	}
	public void setQuantity(int quantity) {
		Quantity = quantity;
	}
	/*
	public float getWeight() {
		return Weight;
	}
	public void setWeight(float weight) {
		Weight = weight;
	}
	*/
	public float getNetAmount() {
		return NetAmount;
	}
	public void setNetAmount(float netAmount) {
		NetAmount = netAmount;
	}
	/*public String getSaleType() {
		return SaleType;
	}
	public void setSaleType(String saleType) {
		SaleType = saleType;
	}*/
	public long getCustomerId() {
		return CustomerId;
	}
	public void setCustomerId(long customerId) {
		CustomerId = customerId;
	}
	
	public float getRegAmount() {
		return RegAmount;
	}
	public void setRegAmount(float regAmount) {
		RegAmount = regAmount;
	}
	public LocalDate getDayStartDate() {
		return DayStartDate;
	}
	public void setDayStartDate(LocalDate startDayDate) {
		DayStartDate = startDayDate;
	}
	public float getLY_RegularPrice() {
		return LY_RegularPrice;
	}
	public void setLY_RegularPrice(float lY_RegularPrice) {
		LY_RegularPrice = lY_RegularPrice;
	}
	public float getLY_ListCost() {
		return LY_ListCost;
	}
	public void setLY_ListCost(float lY_ListCost) {
		LY_ListCost = lY_ListCost;
	}
	public float getRegularPrice() {
		return RegularPrice;
	}
	public void setRegularPrice(float regularPrice) {
		RegularPrice = regularPrice;
	}
	public float getRegular_M_Price() {
		return Regular_M_Price;
	}
	public void setRegular_M_Price(float regular_M_Price) {
		Regular_M_Price = regular_M_Price;
	}
	public int getRegular_M_Pack() {
		return Regular_M_Pack;
	}
	public void setRegular_M_Pack(int regular_M_Pack) {
		Regular_M_Pack = regular_M_Pack;
	}
	//public float getFinalprice() {
		//return Finalprice;
	//}
	//public void setFinalprice(float finalprice) {
		//Finalprice = finalprice;
	//}
	public float getListCost() {
		return ListCost;
	}
	public void setListCost(float listCost) {
		ListCost = listCost;
	}
	public double getTotalRevenue() {
		return TotalRevenue;
	}
	public void setTotalRevenue(double totalRevenue) {
		TotalRevenue = totalRevenue;
	}
	public double getTotalMovement() {
		return TotalMovement;
	}
	public void setTotalMovement(double totalMovement) {
		TotalMovement = totalMovement;
	}
	public double getNetMargin() {
		return NetMargin;
	}
	public void setNetMargin(double netMargin) {
		NetMargin = netMargin;
	}
	//public int getCalendarId() {
		//return CalendarId;
	//}
	//public void setCalendarId(int calendarId) {
		//CalendarId = calendarId;
	//}
//	public float getSalePrice() {
	//	return SalePrice;
	//}
	//public void setSalePrice(float salePrice) {
		//SalePrice = salePrice;
	//}
	//public float getSale_M_Price() {
		//return Sale_M_Price;
	//}
	//public void setSale_M_Price(float sale_M_Price) {
		//Sale_M_Price = sale_M_Price;
	//}
	//public int getSale_M_Pack() {
		//return Sale_M_Pack;
//	}
	//public void setSale_M_Pack(int sale_M_Pack) {
		//Sale_M_Pack = sale_M_Pack;
	//}
	
	public String getRetLIRName() {
		return RetLIRName;
	}
	public void setRetLIRName(String retLIRName) {
		RetLIRName = retLIRName;
	}
	public String getRetailerItemCode() {
		return RetailerItemCode;
	}
	public void setRetailerItemCode(String string) {
		RetailerItemCode = string;
	}
	
	public int getTotal_Mov() {
		return Total_Mov;
	}
	public void setTotal_Mov(int total_Mov) {
		Total_Mov = total_Mov;
	}
	public float getPrediction() {
		return Prediction;
	}
	public void setPrediction(float prediction) {
		Prediction = prediction;
	}
	
	
	public boolean IsEqual(TlogDataDTO t, String Type ,boolean ConsiderWeekStartDate ,boolean isPredictionAdded) {
		if(this.getProductId()!=t.getProductId()) {
			return false;
		}
		
		if(this.getProductLevelId()!=t.getProductLevelId()) {
			return false;
		}
		
		if(this.getRetailerItemCode()!=t.getRetailerItemCode())
		{	
			return false;
		}
		if(ConsiderWeekStartDate&&(!this.getWeekStartDate().equals(t.getWeekStartDate())))
		{
			return false;
		}
		if(!this.getRetLIRName().equals(t.getRetLIRName())) {
			
			return false;
		}
		if(this.getTotalMovement()!=t.getTotalMovement())
		{
			return false;
		}
		if(this.getTotalRevenue()!=t.getTotalRevenue())
		{
			return false;
		}
		if(this.getNetMargin()!=t.getNetMargin()) {
			return false;
		}
		if(this.getGold_Perc()!=t.getGold_Perc()) {
			return false;
		}
		if(this.getSil_Perc()!=t.getSil_Perc()) {
			return false;
		}
		if(this.getReg_Perc()!=t.getReg_Perc()) {
			return false;
		}
		if(this.getSale_Perc()!=t.getSale_Perc()) {
			return false;
		}
		if(Type=="NEW") {
			if(this.getRegularPrice()!=t.getRegularPrice()) {
				return false;
			}
			if(this.getListCost()!=t.getListCost()) {
				return false;
			}
			if(isPredictionAdded&&this.getPrediction()!=t.getPrediction()) {
				return false;
			}
		}else if(Type=="OLD") {
			if(this.getLY_RegularPrice()!=t.getLY_RegularPrice()) {
				return false;
			}
			if(this.getLY_ListCost()!=t.getLY_ListCost()) {
				return false;
			}
			
		}
		
		
		
		return true;
		
	}
	public boolean isTotalMovementNull() {
		return TotalMovementNull;
	}
	public void setTotalMovementNull(boolean totalMovementNull) {
		TotalMovementNull = totalMovementNull;
	}
	public boolean isTotalRevenueNull() {
		return TotalRevenueNull;
	}
	public void setTotalRevenueNull(boolean totalRevenueNull) {
		TotalRevenueNull = totalRevenueNull;
	}
/*	public boolean isListCostNull() {
		return ListCostNull;
	}
	public void setListCostNull(boolean listCostNull) {
		ListCostNull = listCostNull;
	}
	public boolean isRegularPriceNull() {
		return RegularPriceNull;
	}
	public void setRegularPriceNull(boolean regularPriceNull) {
		RegularPriceNull = regularPriceNull;
	}
	*/
	public int getWeekCalendarId() {
		return WeekCalendarId;
	}
	public void setWeekCalendarId(int weekCalendarId) {
		WeekCalendarId = weekCalendarId;
	}
	public float getReg_Mov() {
		return Reg_Mov;
	}
	public void setReg_Mov(float reg_Mov) {
		Reg_Mov = reg_Mov;
	}
	public float getReg_Mar() {
		return Reg_Mar;
	}
	public void setReg_Mar(float reg_Mar) {
		Reg_Mar = reg_Mar;
	}
	public float getReg_Rev() {
		return Reg_Rev;
	}
	public void setReg_Rev(float reg_Rev) {
		Reg_Rev = reg_Rev;
	}
	public float getSil_Mov() {
		return Sil_Mov;
	}
	public void setSil_Mov(float sil_Mov) {
		Sil_Mov = sil_Mov;
	}
	public float getSil_Mar() {
		return Sil_Mar;
	}
	public void setSil_Mar(float sil_Mar) {
		Sil_Mar = sil_Mar;
	}
	public float getSil_Rev() {
		return Sil_Rev;
	}
	public void setSil_Rev(float sil_Rev) {
		Sil_Rev = sil_Rev;
	}
	public float getGold_Mov() {
		return Gold_Mov;
	}
	public void setGold_Mov(float gold_Mov) {
		Gold_Mov = gold_Mov;
	}
	public float getGold_Mar() {
		return Gold_Mar;
	}
	public void setGold_Mar(float gold_Mar) {
		Gold_Mar = gold_Mar;
	}
	public float getGold_Rev() {
		return Gold_Rev;
	}
	public void setGold_Rev(float gold_Rev) {
		Gold_Rev = gold_Rev;
	}
	public int getProductLevelId() {
		return ProductLevelId;
	}
	public void setProductLevelId(int productLevelId) {
		ProductLevelId = productLevelId;
	}
	public boolean isLigMember() {
		return LigMember;
	}
	public void setLigMember(boolean ligMember) {
		LigMember = ligMember;
	}
	public float getNo_Card_Perc() {
		return No_Card_Perc;
	}
	public void setNo_Card_Perc(float no_Card_Perc) {
		No_Card_Perc = no_Card_Perc;
	}
	public float getPredictedSale() {
		return PredictedSale;
	}
	public void setPredictedSale(float predictedSale) {
		PredictedSale = predictedSale;
	}
	public float getPredictedMargin() {
		return PredictedMargin;
	}
	public void setPredictedMargin(float predictedMargin) {
		PredictedMargin = predictedMargin;
	}
	public boolean isIsDeleteForMulUPC() {
		return IsDeleteForMulUPC;
	}
	public void setIsDeleteForMulUPC(boolean isDeleteForMulUPC) {
		IsDeleteForMulUPC = isDeleteForMulUPC;
	}
}
