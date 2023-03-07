package com.pristine.dto.priceChangePerformance;

import java.time.LocalDate;
import java.util.List;

public class WeekGraphDataDTO {
private int WeekCalendarID;
private int IsFutureWeek;
private LocalDate WeekStartDate;
private List<Integer> RetailerItemCodes;
private double NewMovement;
private double NewRevenue;
private double NewMargin;
private double Prediction_Movement;
private double Prediction_Sales;
private double Prediction_Margin;

private double OldMovement;
private double OldRevenue;
private double OldMargin;
public int getWeekCalendarID() {
	return WeekCalendarID;
}
public void setWeekCalendarID(int weekCalendarID) {
	WeekCalendarID = weekCalendarID;
}
public List<Integer> getRetailerItemCodes() {
	return RetailerItemCodes;
}
public void setRetailerItemCodes(List<Integer> retailerItemCodes) {
	RetailerItemCodes = retailerItemCodes;
}
public double getNewMovement() {
	return NewMovement;
}
public void setNewMovement(double newMovement) {
	NewMovement = newMovement;
}
public double getNewRevenue() {
	return NewRevenue;
}
public void setNewRevenue(double newRevenue) {
	NewRevenue = newRevenue;
}
public double getNewMargin() {
	return NewMargin;
}
public void setNewMargin(double newMargin) {
	NewMargin = newMargin;
}
public double getPrediction_Movement() {
	return Prediction_Movement;
}
public void setPrediction_Movement(double prediction) {
	Prediction_Movement = prediction;
}
public double getOldMovement() {
	return OldMovement;
}
public void setOldMovement(double oldMovement) {
	OldMovement = oldMovement;
}
public double getOldRevenue() {
	return OldRevenue;
}
public void setOldRevenue(double oldRevenue) {
	OldRevenue = oldRevenue;
}
public double getOldMargin() {
	return OldMargin;
}
public void setOldMargin(double oldMargin) {
	OldMargin = oldMargin;
}
public LocalDate getWeekStartDate() {
	return WeekStartDate;
}
public void setWeekStartDate(LocalDate weekStartDate) {
	WeekStartDate = weekStartDate;
}
public double getPrediction_Sales() {
	return Prediction_Sales;
}
public void setPrediction_Sales(double prediction_Sales) {
	Prediction_Sales = prediction_Sales;
}
public double getPrediction_Margin() {
	return Prediction_Margin;
}
public void setPrediction_Margin(double prediction_Margin) {
	Prediction_Margin = prediction_Margin;
}
public int getIsFutureWeek() {
	return IsFutureWeek;
}
public void setIsFutureWeek(int isFutureWeek) {
	IsFutureWeek = isFutureWeek;
}


}
