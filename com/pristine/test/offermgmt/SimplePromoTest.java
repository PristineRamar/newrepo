package com.pristine.test.offermgmt;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dataload.tops.AdplexLoader;
import com.pristine.dataload.tops.DisplayTypeLoader;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PropertyManager;

public class SimplePromoTest {
	String rootPath = "";
	String weeklyAdFileRelativePath = "AdPlexLoader";
	String displayFileRelativePath = "DisplayLoader";
	String weeklyAdAndDisplayFileSourceRelativePath = "WeeklyRevenueCorrection";
	List<Date> processingWeeks = null;
	private static Logger logger = Logger.getLogger("SimplePromoTest");
	
	public static void main(String[] args) throws GeneralException, Exception, OfferManagementException {
		SimplePromoTest simplePromoTest = new SimplePromoTest();
		PropertyConfigurator.configure("log4j-testing.properties");
		PropertyManager.initialize("analysis.properties");
		
		simplePromoTest.rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		
		simplePromoTest.processingWeeks = new ArrayList<Date>();
		
		DateFormat formatterMMddyy = new SimpleDateFormat("MM/dd/yy");
		Date tempWeekDate = formatterMMddyy.parse("01/09/16");
		simplePromoTest.processingWeeks.add(tempWeekDate);
		tempWeekDate = formatterMMddyy.parse("01/16/16");
		simplePromoTest.processingWeeks.add(tempWeekDate);
		
		simplePromoTest.loadAdAndDisplay();
	}
	
	private void loadAdAndDisplay() {
		logger.info("Loading of Weekly Ad and Display is Started...");

		String adAndDisplaySourcePath = rootPath + "/" + weeklyAdAndDisplayFileSourceRelativePath;
		String adInputPath = rootPath + "/" + weeklyAdFileRelativePath;
		String displayInputPath = rootPath + "/" + displayFileRelativePath;
		List<String> zipFilesForAdAndDisplay = new ArrayList<String>();

		try {
			ArrayList<String> allSourceZipFiles = PrestoUtil.getAllFilesInADirectory(adAndDisplaySourcePath, "zip");
			for (Date processingWeekStartDate : processingWeeks) {
				SimpleDateFormat nf = new SimpleDateFormat("MM_dd_yy");
				String dateToLookFor = nf.format(processingWeekStartDate);
				boolean isFilePresent = false;
				
				for (String sourceZipFile : allSourceZipFiles) {
					//Check if the file is present in source folder
					isFilePresent = PrestoUtil.checkIfFileIsPresentInAZipFile(sourceZipFile, dateToLookFor);
					if(isFilePresent) {
						zipFilesForAdAndDisplay.add(sourceZipFile);
						break;
					}
				}

				if (!isFilePresent) {
					logger.warn("No matching Ad file found for week: " + dateToLookFor);
				}
			}

		} catch (Exception | GeneralException ex) {
			logger.error("Error while finding Weekly Ad or Display File");
		}

		try {
			if (zipFilesForAdAndDisplay.size() > 0) {
				//Copy the files to ad and display input folders
				for (String sourceAdPath : zipFilesForAdAndDisplay) {
					String destPath = "";
					File sourceFilePath = new File(sourceAdPath);
					File tempFile = new File(sourceAdPath);
					
					//Copy to Ad loader input folder
					destPath = adInputPath + "/" + tempFile.getName();
					File destFilePath = new File(destPath);
					FileUtils.copyFile(sourceFilePath, destFilePath);
					
					//Copy to Display loader input folder
					destPath = "";
					destPath = displayInputPath + "/" + tempFile.getName();
					destFilePath = new File(destPath);
					FileUtils.copyFile(sourceFilePath, destFilePath);
				}

				// Call the Weekly Ad Loader
				String argsAd[] = { "ADPLEX_FILE_PATH=", weeklyAdFileRelativePath };
				AdplexLoader.main(argsAd);

				// Call the Display Ad Loader
				String argsDisplay[] = { displayFileRelativePath };
				DisplayTypeLoader.main(argsDisplay);
			} else {
				logger.info("No Ad or Display file found...");
			}
		} catch (Exception ex) {
			logger.error("Error while loading Weekly Ad or Display File");
		}

		logger.info("Loading of Weekly Ad and Display is Completed...");
	}
}
