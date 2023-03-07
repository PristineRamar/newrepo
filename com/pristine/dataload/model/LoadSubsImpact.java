package com.pristine.dataload.model;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.csvreader.CsvReader;
import com.pristine.exception.GeneralException;
import com.pristine.util.NumberUtil;
import com.pristine.util.PropertyManager;
import com.pristine.dto.SubstitutionImpactDTO;


/* Pending - Use a custome log file and DB inserts */
public class LoadSubsImpact {

	private static Logger logger = Logger.getLogger("LoadSubstitutionImpact");
	private static String locationLevel = "6";
	private static String location;
	private static String productLevel = "4";
	private static String modelLevel = "2"; //2 for Week Level model and 4 for Period level model
	private static int LIR_PRODUCT_LEVEL = 11;
	private List<SubstitutionImpactDTO> subsImpactList = new  ArrayList<SubstitutionImpactDTO> ();
	
	
	public static void main(String[] args) {
		PropertyConfigurator.configure("log4j.properties");
		PropertyManager.initialize("analysis.properties");

		
		logger.info("main() - Started");
		 
		for (int ii = 0; ii < args.length; ii++) {
			
			String arg = args[ii]; 
			
			if (arg.startsWith("LOCATION_LEVEL_ID")) {
				locationLevel = arg.substring("LOCATION_LEVEL_ID=".length());
			}
			
			if (arg.startsWith("LOCATION")) {
				location = arg.substring("LOCATION=".length());
			}

			
			if (arg.startsWith("PRODUCT_LEVEL_ID")) {
				productLevel = arg.substring("LOCATION=".length());
			}
			
			if (arg.startsWith("MODEL_LEVEL")) {
				modelLevel = arg.substring("MODEL_LEVEL=".length());
			}
		}
		
		if( location == null ){
			logger.error("Location not passed, cannot proceed further");
			return;
		}
		
		LoadSubsImpact subsLoader = new LoadSubsImpact();
		try {
			subsLoader.loadSubsImpactInfo();
		}catch(GeneralException ge){
			logger.error("General Exception while gathering Substitution Impact", ge);
		}
		catch(Throwable t){
			logger.error("Unexpected Exception while gathering Substitution Impact", t);	
		}
		
		logger.info("main() - Ended");
	}


	private void loadSubsImpactInfo() throws GeneralException {
		
		String modelRootPath = PropertyManager.getProperty("MODEL.ROOTPATH");
		if( modelRootPath == null ){
			logger.error("Model Root path not passed, cannot proceed further");	
			return;
		}
		
		String directoryPath = modelRootPath + "/OUTPUT/Model/" + locationLevel + "-" + location; 
		logger.info("Loading subs impact from " + directoryPath);
		
		String beginsWithPattern = productLevel+"-";
		String endsWithPattern = "-" + modelLevel;
		File file = new File(directoryPath);
		
		String[] names = file.list();

		
		if( names == null){
			logger.info ("No products available to check impact");
			return;
		}
		for(String name : names)
		{
		    String subFolder = directoryPath + "/" + name;
		    File modelDirectory = new File(subFolder);
			if (modelDirectory.isDirectory() && name.endsWith(endsWithPattern) && name.startsWith(beginsWithPattern))
		    {
		        logger.debug("Looking for Subs in " + subFolder);
		        gatherSubsImpact(subFolder);
		    }
		}
		
		//Print the Subs Impact
		String subOutputPath = modelRootPath + "/OUTPUT/SUBS_IMPACT/"+ locationLevel + "-" + location + ".csv";
		logger.info("No of pairs of item having Impact is " + subsImpactList.size());
		logger.info("writing O/p to " + subOutputPath );
		writeOutputToCSV(subOutputPath);		

	}


	private void gatherSubsImpact(String subFolder) throws GeneralException {
		
		try {
			String substitutionFolder = subFolder +"/SUBS";
			File subsFile = new File(substitutionFolder);
			
			String[] subsFileNames = subsFile.list();
			
			if( subsFileNames == null ){
				logger.info ("No SubstitutionFolder available -  " + substitutionFolder);
				return;
			}
			
			if( subsFileNames.length == 0 ){
				logger.info ("No SUBS available for " + substitutionFolder);
				return;
			}
			
			for(String subsFileName : subsFileNames)
			{
			    String subsImpactFullFileName = substitutionFolder + "/" + subsFileName;
			    File subsImpactFile = new File(subsImpactFullFileName);
				if (subsImpactFile.isFile() && subsFileName.endsWith(".csv") )
			    {
			        logger.debug("Looking for Subs impact in " + subsFileName);
			        
			        CsvReader reader = new CsvReader(new FileReader(subsImpactFullFileName));
			        String separator = ",";
			        reader.setDelimiter(separator.charAt(0));
			        //Read the file
			        // Load the impact to a DTO
			        int lineCount = -1;
					while (reader.readRecord()) {
						String [] line = reader.getValues();
						lineCount++;
						if( lineCount == 0 ){
							continue; //skips the header
						}
						
						if( line.length < 7 ) {
							continue;
						}
						SubstitutionImpactDTO subsImpactDTO = new SubstitutionImpactDTO ();
						
						subsImpactDTO.setMainProductId(Integer.parseInt(line[1]));
						subsImpactDTO.setMainProductLevelId(LIR_PRODUCT_LEVEL);
						subsImpactDTO.setSubsProductLevelId(LIR_PRODUCT_LEVEL);
						subsImpactDTO.setSubsProductId(Integer.parseInt(line[0]));
						
						float minImpactRate = Float.parseFloat(line[6]) - 1;
						float maxImpactRate = Float.parseFloat(line[5]) - 1 ;
						subsImpactDTO.setMaxImpact(maxImpactRate);
						subsImpactDTO.setMinImpact(minImpactRate);
						
						subsImpactList.add(subsImpactDTO);
						
					}
					reader.close();
			    }
			}
		}  catch (FileNotFoundException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (NumberFormatException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (IOException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		}  
		//read the file and calculate the impact
		
	}

	private  void writeOutputToCSV(String fileName) throws GeneralException {
		
		try {
			FileOutputStream fileOut = new FileOutputStream(fileName);
			StringBuffer sbHeader = new StringBuffer( "Main Product Level, Main Product Id, Subs Product Level, Subs Product Id,");
			sbHeader.append("Min Rate Impact %, Max Rate Impact %");
			writeRowToFile(fileOut,sbHeader);
			
			for ( SubstitutionImpactDTO subsImpactDto : subsImpactList ){
				StringBuffer sb = new StringBuffer();
				sb.append(subsImpactDto.getMainProductLevelId());
				sb.append(",");
				sb.append(subsImpactDto.getMainProductId());
				sb.append(",");
				sb.append(subsImpactDto.getSubsProductLevelId());
				sb.append(",");
				sb.append(subsImpactDto.getSubsProductId());
				sb.append(",");
				float minRateImpact = NumberUtil.RoundFloat(subsImpactDto.getMinImpact() * 100,2);
				if( minRateImpact > 0 )
					minRateImpact = 0;
				sb.append(minRateImpact);
				sb.append(",");
				float maxRateImpact = NumberUtil.RoundFloat(subsImpactDto.getMaxImpact() *100,2);
				if( maxRateImpact < 0)
					maxRateImpact = 0;
				sb.append(maxRateImpact);
				writeRowToFile(fileOut,sb);
			}
			fileOut.close();
			
			
		}catch (FileNotFoundException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (NumberFormatException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		} catch (IOException e) {
			throw new GeneralException("Error while executing compDataLoad", e);
		}  
	}
	
	private void writeRowToFile(FileOutputStream fileOut, StringBuffer sb) throws IOException{
		
		fileOut.write(sb.toString().getBytes());
		fileOut.write('\n');
		
	}
}
