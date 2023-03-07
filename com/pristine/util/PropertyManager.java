/*
 * Author : Suresh Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.util;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import org.apache.log4j.Logger;

public class PropertyManager
{
	static Logger		logger	= Logger.getLogger(PropertyManager.class);
	static Properties	props = new Properties();

	public static void initialize(String fileName)
	{
		try
		{			
						
			FileInputStream in = new FileInputStream(fileName);

			props.load(in);
			in.close();
		}
		catch (Exception e)
		{
			logger.error("Error in reading property file", e);
		}
	}

	public static void initialize(InputStream in)
	{
		try
		{			
			props.load(in);

			in.close();
		}
		catch (Exception e)
		{
			logger.error("Error in reading property file", e);
		}
	}

	public static String getProperty(String key)
	{
		return props.getProperty(key);
	}

	public static String getProperty(String key, String defaultVal)
	{
		return props.getProperty(key, defaultVal);
	}
}