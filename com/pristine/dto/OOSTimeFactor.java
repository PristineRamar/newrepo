package com.pristine.dto;

import java.util.Date;

public class OOSTimeFactor implements IValueObject {

	public int todId;
	public int dateFactor;
	public String timeOfDay;
	
	public OOSTimeFactor (int timeOfDayId, int factor, String timeOfDayFactor){
		todId = timeOfDayId;
		dateFactor = factor;
		timeOfDay = timeOfDayFactor;
	}
}
