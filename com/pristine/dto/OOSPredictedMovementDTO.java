package com.pristine.dto;

import org.apache.log4j.Logger;


public class OOSPredictedMovementDTO {
	public int timeOfDayId;
	public int itemCode;
	
	public double prevdayexp =0;
	
	public double last7dayexp =0;
	public int last7dayobs= 0;
	public int last7dayobsUsed= 0;
	public int last7dayDaysMoved =0;

	public double last7dayavgexp =0;
	public int last7dayavgobs= 0;
	public int last7dayavgobsUsed= 0;
	
	public double last28dayexp =0;
	public int last28dayobs= 0;
	public int last28dayobsUsed= 0;
	public int last28dayDaysMoved =0;
	
	public double last180dayexp =0;
	public int last180dayobs= 0;
	public int last180dayobsUsed= 0;
	public int last180dayDaysMoved =0;
	
	public double last7daysigma=0;
	public double last28daysigma=0;
	public double last180daysigma=0;
	public double last7dayavgsigma=0;
	
	public double sigmaUsed=0;

	public double expWithAd =0;
	public double sigmaWithAd =0;
	public Double predictedExp;
	public String expMethodWithAd="";
	
}
