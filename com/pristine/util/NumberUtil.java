
package com.pristine.util;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;

public class NumberUtil
{
	private static Logger	logger = Logger.getLogger(NumberUtil.class);

	public static float RoundFloat(float num, int decimals)
	{
		BigDecimal bd = new BigDecimal(num);
		bd = bd.setScale(decimals, BigDecimal.ROUND_HALF_UP);

		return bd.floatValue();
	}

	public static double RoundDouble(double num, int decimals)
	{
		BigDecimal bd = new BigDecimal(num);
		bd = bd.setScale(decimals, BigDecimal.ROUND_HALF_UP);

		return bd.doubleValue();
	}

	
	public static int compareFloat(float f1, float f2){
		int retval;
		if( Math.abs(f1 - f2) <= 0.01f )
			retval = 0;
		else if ((f1 - f2) > 0)
			retval = 1;
		else 
			retval = -1;
		return retval;
		
	}
	
	public static int [] mode(int[] T) {
		int number = -1;
		int numberTwo = -1;
		int count = 1;
		int countTwo = 1;
		for (int i = 0; i < T.length; i++) {
			int tempCount = 0;
			int tempNum = T[i];
			for (int a = 0; a < T.length; a++) {
				if (tempNum == T[a]) {
					tempCount++;
				}
			}
			if (tempCount > count) {
				countTwo = count;	
				numberTwo = number;
				count = tempCount;
				number = tempNum;
			}
			if (tempNum != number &&  tempCount > countTwo)
			{	countTwo = tempCount;	numberTwo = tempNum;}
		}
		logger.debug("Mode Numbers are " + number + " " + numberTwo);
		if (count == countTwo) {
			
			logger.debug("No Majority");
		} else {
			logger.debug("The Number occurring most times is: "
					+ number);
		}
		int modeArr [] = new int[2];
		modeArr[0] = number;
		modeArr[1] = numberTwo;
		return modeArr;
		
	}
	
	
	 public static double mode(double [] arr)
	    {
	        HashMap<Double,Integer> arrayVals = new HashMap<Double,Integer> ();
	        int maxOccurences = 1;
	        double mode = arr[0];

	        for(int i = 0; i<arr.length; i++)
	        {   
	            double currentIndexVal = arr[i];
	            if(arrayVals.containsKey(currentIndexVal)){
	                int currentOccurencesNum = arrayVals.get(currentIndexVal);
	                currentOccurencesNum++;
	                arrayVals.put(currentIndexVal, currentOccurencesNum );
	                if(currentOccurencesNum >= maxOccurences)
	                {
	                    mode = currentIndexVal;
	                    maxOccurences = currentOccurencesNum;
	                }
	            }
	            else{
	                arrayVals.put(arr[i], 1);
	            }
	        }


	        return mode;
	    }
	 
	 
	 public static double mean(double[] data) {  
	    double sum = 0;

	    for (int i=0; i < data.length; i++) 
	    	sum = sum + data[i]; 
	    
	    double average = sum / data.length;;
	    
	    return average;
	 }
	 
	 public static double median(double[] m) {
		 
		Arrays.sort(m);
	    int middle = m.length/2;
	    if (m.length%2 == 1) {
	        return m[middle];
	    } else {
	        return (m[middle-1] + m[middle]) / 2.0;
	    }
	}

	public static double round(double value, int places) {
		if (places < 0)
			throw new IllegalArgumentException();

		long factor = (long) Math.pow(10, places);
		value = value * factor;
		long tmp = Math.round(value);
		return (double) tmp / factor;
	}		 
	
	public static double parseStringToDouble(String value, double defaultValue) {
	    return value == null || value.isEmpty() ? defaultValue : Double.parseDouble(value);
	}
	
	public static int parseStringToInteger(String value, int defaultValue) {
	    return value == null || value.isEmpty() ? defaultValue : Integer.parseInt(value);
	}
}
