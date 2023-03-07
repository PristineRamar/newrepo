package com.pristine.customer;

import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFSheet;

import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PropertyManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.customer.DayCountDTO;


public class Util
{
	static public StringBuffer getCommaList (List<String> list)
	{
		StringBuffer sb = new StringBuffer();
		for ( int ii = 0; ii < list.size(); ii++ ) {
			if ( ii > 0 ) sb.append(',');
			sb.append(list.get(ii));
		}
		return sb;
	}
	
	static public int[] get52WeeklyVisitCounts ()
	{
		return getIntArray(52);
	}
	
	static public int[] getIntArray (int size)
	{
		int[] array = new int[size];
		resetArray (array);
		return array;
	}
	
	static public void resetArray (int[] array)
	{
		for ( int ii = 0; ii < array.length; ii++ )
			array[ii] = 0;
	}
	
	static public Map<Integer, Integer> setupFrequencies (int[] dailyCounts, StringBuffer sb)
	{
		Map<Integer, Integer> frequencyCounts = new HashMap<Integer, Integer>();

		int freq = 0;
		boolean foundFirst = false;
		for ( int ii = 0; ii < dailyCounts.length; ii++ )
		{
			if ( dailyCounts[ii] > 0 )
			{
				if ( sb != null )
					sb.append(' ').append(ii);
				if ( !foundFirst ) {
					foundFirst = true;
				}
				else {
					freq = ii - freq;
					int count;
					try {
						count = frequencyCounts.get(freq);
					}
					catch (Exception e) {
						count = 0;
					}
					frequencyCounts.put(freq, ++count);
				}
				
				freq = ii;
			}
		}
		if ( sb != null )
			sb.append('.');
		
		return frequencyCounts;
	}
	
	static public StringBuffer getFrequencyCounts (Map<Integer, Integer> frequencyCounts)
	{
		StringBuffer sb = new StringBuffer();
		Set<Integer> keys = frequencyCounts.keySet();
		Iterator<Integer> keyIter = keys.iterator();
		while ( keyIter.hasNext() )
		{
			int freq = keyIter.next();
			int count = frequencyCounts.get(freq);
			sb.append(',').append(freq).append('=').append(count);
		}
		return sb;
	}
	
	static public int getTotalFrequencyCount (Map<Integer, Integer> frequencyCounts)
	{
		Set<Integer> keys = frequencyCounts.keySet();
		Iterator<Integer> keyIter = keys.iterator();
		int totalCount = 0;
		while ( keyIter.hasNext() )
		{
			int freq = keyIter.next();
			int count = frequencyCounts.get(freq);
			totalCount += count;
		}
		return totalCount;
	}
	
	static public int getMaxCountFrequency (Map<Integer, Integer> frequencyCounts)
	{
		Set<Integer> keys = frequencyCounts.keySet();
		Iterator<Integer> keyIter = keys.iterator();
		//countIter.
		int maxCount = 0;
		int maxCountFreq = 0;
		while ( keyIter.hasNext() )
		{
			int freq = keyIter.next();
			int count = frequencyCounts.get(freq);
			if ( count > maxCount ) {
				maxCount = count;
				maxCountFreq = freq;
			}
		}
		
		return maxCountFreq;
	}
	
	public static void saveToFile (HSSFWorkbook workBook, String fileName) throws GeneralException
	{
		try
		{
			String filePath = "C:/Projects/Pristine/Customer Analytics/" + fileName + ".xls";
			FileOutputStream fileOut = new FileOutputStream(filePath, true);	// true to append
			workBook.write(fileOut);
			fileOut.close();
		}
		catch (IOException ioe) {
			throw new GeneralException("Error in Excel File write... ", ioe);
		}

	}
	
	public static void writeToCell (HSSFRow row, int colCount, Object val) 
	{
		if( val != null)
		{
			if( val instanceof String  ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_STRING);
				cell.setCellValue((String)val);
			}
			else if ( val instanceof Integer ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_NUMERIC);
				Integer intObj = (Integer)val;
				cell.setCellValue(intObj.doubleValue());
			}
			else if ( val instanceof Double ){
				HSSFCell cell   = row.createCell(colCount,HSSFCell.CELL_TYPE_NUMERIC);
				Double dblObj = (Double)val;
				cell.setCellValue(dblObj.doubleValue());
			}
		}
	}

	public static List<DayCountDTO> getSevenDTOs ()
	{
		List<DayCountDTO> list = new ArrayList<DayCountDTO>();
		for ( int ii = 0; ii < 7; ii++ )
		{
			DayCountDTO dto = new DayCountDTO();
			dto.day = ii;
			dto.count = 0;
			dto.spend = 0;
			list.add(dto);
		}
		return list;
	}

	public static List<DayCountDTO> duplicateList (List<DayCountDTO> dayCountList)
	{
		List<DayCountDTO> listCopy = new ArrayList<DayCountDTO>();
		for ( int ii = 0; ii < dayCountList.size(); ii++ ) {
			DayCountDTO dto = new DayCountDTO (dayCountList.get(ii));
			listCopy.add(dto);
		}
		return listCopy;
	}
	
}
