package com.pristine.dataload.offermgmt.promotion;

import java.util.Properties;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;

/**
 * Keep variables that will be accessed by more than one class
 * @author Nagarajan
 *
 */
public class BasePromotionEngine {
	
	
	protected void setLog4jProperties(String logName) throws GeneralException {
		String logTypes = PropertyManager.getProperty("log4j.rootLogger");
		String appender = PropertyManager.getProperty("log4j.appender.logFile");
		String logPath = PropertyManager.getProperty("log4j.appender.logFile.File");
		String maxFileSize = PropertyManager.getProperty("log4j.appender.logFile.MaxFileSize");
		String maxBackupIndex = PropertyManager.getProperty("log4j.appender.logFile.MaxBackupIndex");
		String patternLayout = PropertyManager.getProperty("log4j.appender.logFile.layout");
		String conversionPattern = PropertyManager.getProperty("log4j.appender.logFile.layout.ConversionPattern");
		
		String appenderConsole = PropertyManager.getProperty("log4j.appender.console");
		String appenderConsoleLayout = PropertyManager.getProperty("log4j.appender.console.layout");
		String appenderConsoleLayoutPattern = PropertyManager.getProperty("log4j.appender.console.layout.ConversionPattern");
		
		
		logPath = logPath + "/" + logName + ".log";

		Properties props = new Properties();
		props.setProperty("log4j.rootLogger", logTypes);
		props.setProperty("log4j.appender.logFile", appender);
		props.setProperty("log4j.appender.logFile.File", logPath);
		props.setProperty("log4j.appender.logFile.MaxFileSize", maxFileSize);
		props.setProperty("log4j.appender.logFile.layout", patternLayout);
		props.setProperty("log4j.appender.logFile.layout.ConversionPattern", conversionPattern);
		props.setProperty("log4j.appender.logFile.MaxBackupIndex", maxBackupIndex);
		 
		props.setProperty("log4j.appender.console", appenderConsole);
		props.setProperty("log4j.appender.console.layout", appenderConsoleLayout);
		props.setProperty("log4j.appender.console.layout.ConversionPattern", appenderConsoleLayoutPattern);
		PropertyConfigurator.configure(props);
	}
	
	
}
