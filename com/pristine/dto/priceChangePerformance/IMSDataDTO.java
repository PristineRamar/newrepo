package com.pristine.dto.priceChangePerformance;

import java.time.LocalDate;

public class IMSDataDTO {

	private int ProductId;
	private float RegularPrice;
	private float Regular_M_Price;
	private int Regular_M_Pack;
	private float Finalprice;
	private float ListCost;
	private double TotalRevenue;
	private double TotalMovement;
	private double NetMargin;
	private int CalendarId;
	private float SalePrice;
	private float Sale_M_Price;
	private int Sale_M_Pack;
	private LocalDate WeekStartDate;
	private LocalDate WeekEndDate;
	private boolean TotalMovementNull;
	private boolean TotalRevenueNull;
	
	
	
	
	
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
	public int getProductId() {
		return ProductId;
	}
	public void setProductId(int productId) {
		ProductId = productId;
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
	public float getFinalprice() {
		return Finalprice;
	}
	public void setFinalprice(float finalprice) {
		Finalprice = finalprice;
	}
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
	public int getCalendarId() {
		return CalendarId;
	}
	public void setCalendarId(int calendarId) {
		CalendarId = calendarId;
	}
	public float getSalePrice() {
		return SalePrice;
	}
	public void setSalePrice(float salePrice) {
		SalePrice = salePrice;
	}
	public float getSale_M_Price() {
		return Sale_M_Price;
	}
	public void setSale_M_Price(float sale_M_Price) {
		Sale_M_Price = sale_M_Price;
	}
	public int getSale_M_Pack() {
		return Sale_M_Pack;
	}
	public void setSale_M_Pack(int sale_M_Pack) {
		Sale_M_Pack = sale_M_Pack;
	}
	public LocalDate getWeekStartDate() {
		return WeekStartDate;
	}
	public void setWeekStartDate(LocalDate firstWeekDate) {
		WeekStartDate = firstWeekDate;
	}
	public LocalDate getWeekEndDate() {
		return WeekEndDate;
	}
	public void setWeekEndDate(LocalDate lastWeekDate) {
		WeekEndDate = lastWeekDate;
	}
	
	
	
}
