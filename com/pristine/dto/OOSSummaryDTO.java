package com.pristine.dto;

import org.apache.log4j.Logger;

public class OOSSummaryDTO {
	public int scheduleId;
	public int dayofweekId;
	public int specialEventId;
	public int specialFactId;
	public int timeOfDayId;
	public int itemCode;
	public boolean onSale = false;
	
	public float totalQty = 0;
	public float regQty = 0;
	public float saleQty =0;
	
	public double totalRev =0;
	public double regRev =0;
	public double saleRev =0;
	
	public double revOpp =0;

	public int visitCount = 0;
	public int regVisitCount = 0;
	public int saleVisitCount = 0;
	public int totalCustVisitCount = 0;
	
	public double margin = 0;
	public double regMargin = 0;
	public double saleMargin = 0;
	public double marginOpp=0;

	public float minRegQty = 0;
	public float minSaleQty = 0;
	
	public float maxRegQty = 0;
	public float maxSaleQty = 0;
	
	public int noOfObs = 0;
	public int noOfSaleObs = 0;
	
	public float modalQtyPerVisit = 0;
	public float modal2QtyPerVisit = 0;
	
	public int minDurationBtwVisit = 0;
	public int maxDurationBtwVisit = 0;
	public int avgDurationBtwVisit = 0;
	public float avgQtyPerVisit = 0;
	public int min95PctDurationBtwVisit = 0;
	public int max95PctDurationBtwVisit = 0;
	
	public float movVisitRatio;
	public float regMovVisitRatio;
	public float saleMovVisitRatio;
	
	public float unitRegPrice;
	public float unitSalePrice;
	
	public float maxQtyPerVisit = 0;
	
	public String ossInd = "N";
	public int ossClassification;
	public int ossScore= 0;
	public double prevdayexp =0;
	public double last7dayexp =0;
	public int last7dayobs= 0;
	public double last7dayavgexp =0;
	public int last7dayavgobs= 0;
	public double last28dayexp =0;
	public int last28dayobs= 0;
	public double last180dayexp =0;
	public int last180dayobs= 0;
	public int last7dayDaysMoved =0;
	public int last28dayDaysMoved =0;
	public int last180dayDaysMoved =0;
	public int qtyMode = 0;
	public float dailyQty = 0;
	
	public double expWithAd =0;
	public double sigmaWithAd =0;
	
	public String moved2PlusInAllDaysInPrevNdays ="N";
	
	public String deptName;
	public String catName;
	public String itemName;
	public String retailerItemCode;
	public String upc;

	public String timeOfDayStr;
	public String dayOfWeekStr;
	public String ossType;
	public String priority;
	public int storeId;
	public String oosMethod;
	public double mape=-1;
	public double sigmaUsed;
	public Double predictedExp=null;
	public String expMethodWithAd="";
	public int last7dayOused=0;
	public int last28dayOused=0;
	public int last180dayOused=0;
	public int last7dayavgOused=0;
	public String focusItem="N";
	public String lastMovementTS="";
	public boolean movementInLastHour=false;
	
	public void print(Logger logger) {
		logger.debug("ScheduleId, timeOfDay, TotalQty, visitCount, " +
				"{ minVisit, maxVisit, min95PctDuration, max95PctDuration}, {AvgQty, Modal Qty 1 , 2 }- " + 
				scheduleId + ", " + timeOfDayId + ", " + totalQty + ", " +  visitCount + ", { Duration - " + 
				avgDurationBtwVisit + ", " + minDurationBtwVisit + ", " + maxDurationBtwVisit + ", "  + min95PctDurationBtwVisit + ", " + max95PctDurationBtwVisit +
				"}, {Avg/Mode -" + avgQtyPerVisit+ ", " + modalQtyPerVisit + ", " +  modal2QtyPerVisit + "}"  +
				"{ Rev  " + regRev +  ", " +saleRev +  ", " + totalRev  + "}" + 
				"{ Qty  " + regQty +  ", " +saleQty +  ", " + totalQty + "}");
	}

	public void setOssScore(int i) {
		// TODO Auto-generated method stub
		ossScore = i;
	}

	public void checkAndSetOOSclassification(int c){
		if (ossClassification==0 || ossClassification > c){
			ossClassification = c;
		}
	}

}
