/*
 * Author : Naimish Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.SuspectItemDTO;

public class GenericUtil
{
	private GenericUtil()
	{
		// TODO
	}

	private static Logger	logger	= Logger.getLogger(GenericUtil.class);

	public static void initialiseLogger(String logFilePath)
	{
		PropertyConfigurator.configureAndWatch(logFilePath);
	}

	public static void logMessage(String msg)
	{
		logger.debug(msg);
	}

	public static void logMessage(String msg, Throwable t)
	{
		logger.debug(msg, t);
	}

	public static void logMessage(CompetitiveDataDTO competitiveDataDTO, String msg)
	{
		if( logger.isDebugEnabled())
			logger.debug(msg + " --> Schedule ID: " + competitiveDataDTO.getScheduleID().toString() + " Item Code: " + competitiveDataDTO.getItemCode().toString());
	}

	public static void logMessageSuspect(SuspectItemDTO suspectItemDTO, String msg)
	{
		if( logger.isDebugEnabled())	
			logger.debug(msg + " --> Schedule ID: " + suspectItemDTO.getSchedule_ID().toString() + " Item Code: " + suspectItemDTO.getItem_ID().toString());
	}

	public static void logError(String msg)
	{
		logger.error("**" + msg);
	}

	public static void logError(Throwable ex)
	{
		logError("**", ex);
	}

	public static void logError(String msg, Throwable ex)
	{
		logger.error("**" + msg, ex);
	}

	public static void logError(CompetitiveDataDTO competitiveDataDTO, String msg)
	{
		logger.error("**" + msg + " --> Schedule ID: " + competitiveDataDTO.getScheduleID().toString() + " Item Code: " + competitiveDataDTO.getItemCode().toString());
	}

	public static Float getMinimum(Float arr[], int startIndex)
	{
		Float min = 0f;

		List<Float> floatList = Arrays.asList(arr);

		Collections.sort(floatList);

		for (int iCounter = startIndex; iCounter < floatList.size(); iCounter++)
		{
			if (iCounter == startIndex)
				min = floatList.get(iCounter).floatValue();

			if (floatList.get(iCounter).floatValue() != 0 && floatList.get(iCounter).floatValue() < min)
				min = floatList.get(iCounter).floatValue();
		}

		return min;
	}

	public static Float getMaximum(Float arr[], int startIndex)
	{
		Float max = 0f;

		List<Float> floatList = Arrays.asList(arr);

		Collections.sort(floatList);

		for (int iCounter = startIndex; iCounter < floatList.size(); iCounter++)
		{
			if (iCounter == startIndex)
				max = floatList.get(iCounter).floatValue();

			if (floatList.get(iCounter).floatValue() != 0 && floatList.get(iCounter).floatValue() > max)
				max = floatList.get(iCounter).floatValue();
		}

		return max;
	}
	
 
	
	public static Double Round(double Value, int index) {

		double ret = 0;

		if (!(Double.isInfinite(Value)) || (Double.isNaN(Value))) {

				double p = (double) Math.pow(10, index);
				Value = Value * p;
				double tmp = Math.round(Value);
				ret = (double) tmp / p;
		}

		if (Double.isInfinite(ret) || Double.isNaN(ret)) {
			return 0.0;
		} else {
			return ret;
		}
	}
	
}