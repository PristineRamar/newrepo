/*
 * Title: Summary Goal Weekly
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	27/10/2011	John Britto		New Creation
 *******************************************************************************
 */

package com.pristine.dataload;

import java.sql.Connection;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import com.pristine.dao.DBManager;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dao.StorePerformanceRollupDAO;
import com.pristine.dao.SummaryGoalDAO;
import com.pristine.dto.StorePerformanceRollupSummaryDTO;
import com.pristine.dto.SummaryGoalDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.Constants;

public class SummaryGoalWeekly {

	private static Logger  logger = Logger.getLogger("SummaryGoalWeekly");
	private Connection 	_Conn = null;
	private String _xlsFilePath;
	private HashMap<String, Integer> _storeIdList;
	
	private int _rows_total_count = 1;
	private int _rows_inserted_count = 0;
	private int _rows_skipped_count = 0;
	
	private HashMap<String, Integer> _weekEndDates;
	
	final int CELL_STORE_NUMBER = 0;
	final int CELL_WEEK_END = 1;
	final int CELL_BUDGET = 2;
	final int CELL_FORE_CAST = 3;
	
	
	final private int INPUT_FILE_COL_COUNT = 4;
	final private int LEVEL_TYPE_STORE = 5;
	final private int LEVEL_TYPE_DISTRICT = 4;
	final private int LEVEL_TYPE_REGION = 3;
	final private int LEVEL_TYPE_DIVISION = 2;
	final private int LEVEL_TYPE_CHAIN = 1;	
	
	final private int EXCEL_MIN_ROW = 1;
	final private double VALUE_DEFAULT_NULL = -99;
	final private int VALUE_PROCESS_LOG = 100;
	
	/*
	 *****************************************************************
	 * Class constructor
	 * Argument : null
	 * @throws Exception
	 * ****************************************************************
	 */	
	public SummaryGoalWeekly ()
	{
        try
		{
        	//Create DB connection
        	_Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);
	    }
		catch (GeneralException gex) {
	        logger.error(gex);
	    } catch (SQLException se) {
	        logger.error(se);
		}
	}


	/*
	 *****************************************************************
	 * Main method of Batch
	 * Argument 1: Excel File Name
	 * Argument 2: Excel Sheet Number
	 * ****************************************************************
	 */	
	
	public static void main(String[] args) throws Exception 
	{
		PropertyConfigurator.configure("log4j-summary-goal-week.properties");

        PropertyManager.initialize("analysis.properties");
        
        //Get the file path to locate the Excel file
        String excelFilePath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		
        if( !excelFilePath.equals("")) {
			excelFilePath = excelFilePath +"/";
	        //logger.debug("Excel Path       " + excelFilePath);
        }
        else {
			logger.error("Invalid configuration: Excel file path missing");
			System.exit(0);        	
        }
		
		String xlsFileName = "";
		int xlsSheetNumber = 0;
		
		//Get Excel file name command line argument, this is the first parameter
		if (args.length > 0) {
			
			xlsFileName = args[0];
			logger.info("Excel File Name  " + xlsFileName);

		} else {
			logger.error("Invalid input: Excel file name missing");
			System.exit(0);
		}

		/*Get Excel sheet number, this is the first parameter
		If the Sheet number is not provided than take 0 as default*/
		if (args.length > 1) {

			xlsSheetNumber = Integer.parseInt(args[1]);
			logger.info("XLS Sheet Number  " + xlsSheetNumber);
		
		} else {

			logger.warn("Invalid input: Excel sheet number missing");

			//Assign first sheet as default
			xlsSheetNumber = 0;
		}		

		//Create object for class
		SummaryGoalWeekly objSummaryGoal = new SummaryGoalWeekly();
		
		objSummaryGoal._xlsFilePath = excelFilePath;
		
		//Call method to perform Summary Goal calculation
		objSummaryGoal.computeSummaryGoal(xlsFileName, xlsSheetNumber);
		
		// Close the connection
		PristineDBUtil.close(objSummaryGoal._Conn);
	}
	
    /*
	 *****************************************************************
	 * Function to get process the summary goal data
	 * Argument 1: Excel file name with extension
	 * Argument 2: Excel Sheet number 
	 * Return : void 
	 *****************************************************************
	 */
	private void computeSummaryGoal(String xlsFileName, int xlsSheetNumber) {
		//Object for summary goal data
		SummaryGoalDAO objSummaryGoalDAO;
		
		try {
			objSummaryGoalDAO = new SummaryGoalDAO();
			
			//process the Summary Goal calculation for Store
			logger.info("Processing Summary Goal for Store");
			boolean goalProcess =  processSummaryGoalCreation(xlsFileName, xlsSheetNumber, objSummaryGoalDAO);
		
			//Process roll up only if at least one excel row is processed
			if (_rows_inserted_count > 0) {
				//Process Summary Goal for District
				logger.info("Processing Summary Goal for District");
				ProcessSummaryGoalForDistrict(objSummaryGoalDAO);
	
				//Process Summary Goal for Region
				logger.info("Processing Summary Goal for Region");
				ProcessSummaryGoalForRegion(objSummaryGoalDAO);
	
				//Process Summary Goal for Division (Banner)
				logger.info("Processing Summary Goal for Division");
				ProcessSummaryGoalForDivision(objSummaryGoalDAO);
				
				//Process Summary Goal for Chain
				logger.info("Processing Summary Goal for Chain");
				ProcessSummaryGoalForChain(objSummaryGoalDAO);				
				
			}

			
			if (goalProcess) {
				//Move source excel file completed or bad folder
				logger.info("Processing Excel File Moving");
				moveSourceFile(xlsFileName);
				logger.info("Summary goal Weekly end Sucessfully ");
			}
			else {
				logger.info("Summary goal Weekly end unsucessfully ");
			}
			
			
			
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

	}
	
	
    /*
	 *****************************************************************
	 * Function to get process the summary goal data from excel file
	 * Argument 1: Excel file name with extension
	 * Argument 2: Excel Sheet number 
	 * Return : void 
     * @throws GeneralException, SQLException, IOException 
	 *****************************************************************
	 */
	private boolean processSummaryGoalCreation(String xlsFileName, int xlsSheetNumber, SummaryGoalDAO objSummaryGoalDAO) throws IOException {
		
		//Object for competitive store data
		CompetitiveDataDAO 	objcompDataDAO;
		//Variable to hold the excel file name with path
		String xlsFile = _xlsFilePath + xlsFileName;
		//logger.debug("File Name " + xlsFile);		
		Boolean processStatus = true;
		
		try {
			//Instance for file
			FileInputStream objXlsFile = new FileInputStream(xlsFile);
			
			//object for excel work book
			HSSFWorkbook objWbook = new HSSFWorkbook(objXlsFile);
			
			//Object for excel sheet
			HSSFSheet objSheet = objWbook.getSheetAt(xlsSheetNumber);

			objcompDataDAO = new CompetitiveDataDAO(_Conn);


			_rows_total_count = objSheet.getPhysicalNumberOfRows();
			logger.info("Total Excel Rows to process " + String.valueOf(objSheet.getPhysicalNumberOfRows()));

			if (_rows_total_count > EXCEL_MIN_ROW) {
				
				_storeIdList = new HashMap<String, Integer>();
				_weekEndDates = new HashMap<String, Integer>();
				
				//Run loop for every row in excel sheet 
				for (int irow=1; irow<_rows_total_count; irow++)	{

					//logger.debug("Processing Row   " + irow);
					//Assign the current row in a object for processing
					HSSFRow row = objSheet.getRow(irow);
		
					//Number of columns in every row should be four
					if (row.getPhysicalNumberOfCells() >=INPUT_FILE_COL_COUNT) {

						//initialize store id
						int storeId = -1;
						
						//Get the 1st cell value as store number
						String storeNumber = getStoreNumber(row.getCell(CELL_STORE_NUMBER));

						//Get the store ID for the store number 
						if (null != storeNumber) {
							storeId = getCompStore(storeNumber, objcompDataDAO);								
							//logger.debug("Store ID         " + String.valueOf(storeId));
						}

						/*if store id exist, proceed further
						  otherwise skip the current row and proceed to next */
						if (storeId > 0 ) {					
							boolean rowStatus = processRowData(row, storeId, objSummaryGoalDAO);
							
							if (rowStatus) {
								_rows_inserted_count = _rows_inserted_count +1;
							}
							else {
								_rows_skipped_count =_rows_skipped_count + 1;
								logger.warn("Process terminated for Row     " + String.valueOf(irow));
							}
						}
						else {
							
							logger.warn("Invalid Store Number           "  + storeNumber);
							logger.warn("Process terminated for Row     " + String.valueOf(irow));
							_rows_skipped_count =_rows_skipped_count + 1;
						}
						
					}
					else {
						logger.warn("Insufficient number of columns ");
						logger.warn("Process terminated for Row     " + String.valueOf(irow));						
						_rows_skipped_count =_rows_skipped_count + 1;
					}
				
    				int proRecSize = irow;
    				if ( (proRecSize % VALUE_PROCESS_LOG) == 0 ) {
    					logger.info( "Rows processed....." + String.valueOf(proRecSize));
    				}
				
				}
				logger.info("Processed Rows..." + String.valueOf(_rows_total_count));
				logger.info("Inserted Rows...." + String.valueOf(_rows_inserted_count));
				logger.info("Skipped Rows....." + String.valueOf(_rows_skipped_count));

			}
			else {
				logger.error("No data found in Excel Sheet......" + objSheet.getSheetName());
			}			
		
			objSheet = null;
			objWbook = null;
			objXlsFile.close();
			
		} 
		catch (FileNotFoundException fe) {
			logger.error("Invalid input : Excel file missing " + xlsFileName);
			processStatus = false;
		}
		catch (IOException ie) {
			logger.error(ie.getLocalizedMessage());
			processStatus = false;
		}
		catch (IllegalArgumentException ae) {
			logger.error(ae.getLocalizedMessage());
			processStatus = false;
		}			
		catch (GeneralException ge) {
			logger.error(ge.getMessage());
		}
	return processStatus;
	}
	
	/*
	 *****************************************************************
	 * Function to process row for goal data manipulation 
	 * Argument 1: Excel Row
	 * Return : boolean (completion status) 
     * @throws GeneralException, SQLException 
	 *****************************************************************
	 */
	@SuppressWarnings("deprecation")
	private boolean processRowData(HSSFRow row, int storeId, SummaryGoalDAO objSummaryGoalDAO) {
		boolean processStatus = true;
		String strEndDate = null;
		try {
			
			/*Get the 2nd cell value as Week end date
			  If the date is not valid then stop the processing of current row */
			String weekEndDate = row.getCell(CELL_WEEK_END).toString();
			
			try {
				
				DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy");
				strEndDate = formatter.format(new Date(weekEndDate)).toString();
			
			}
			catch (Exception e) {
				logger.error("Invalid Week end date " + weekEndDate);				
				processStatus = false;
			}
				
			/*Get the 3nd cell value as weekly budget
			If the budget is empty then assign budget as null
			If the budget is not valid then stop the processing of current row */
			Double budget = VALUE_DEFAULT_NULL;
			String strBudget = null;
		
			try {
				strBudget = row.getCell(CELL_BUDGET).toString();
				if (!strBudget.isEmpty()) {
					budget = Double.parseDouble(strBudget);
				}
			} 
			catch (NumberFormatException nfe) {
				logger.warn("Invalid input, Budget is not valid number: " + strBudget);
				processStatus = false;			
			}

			/*Get the 3nd cell value as weekly forecast
			If the forecast is empty then assign forecast as 0
			If the forecast is not valid then stop the processing of current row */
			Double foreCast = VALUE_DEFAULT_NULL;							
			String strForeCast = null;
			
			try {
				strForeCast = row.getCell(CELL_FORE_CAST).toString();
				
				if (!strForeCast.isEmpty()) {
					foreCast = Double.parseDouble(strForeCast);
				}
			} 
			catch (NumberFormatException nfe) {
				logger.warn("Invalid input, Forecast is not valid number: " + strForeCast);
				processStatus = false;
			}

			//If both forecast and budget are null then skip this row from processing
			if ((foreCast==VALUE_DEFAULT_NULL) && (budget==VALUE_DEFAULT_NULL))
			{
				processStatus = false;
				logger.warn("Budget & Forecast are Null ");
			}
			
			//If all the preconditions are OK then proceed with DB update
			if (processStatus) {
				
				//Create DTO and assign values
				SummaryGoalDTO objgoalDTO = new SummaryGoalDTO();
				objgoalDTO.setWeekEndDate(strEndDate);
				objgoalDTO.setLevelTypeID(LEVEL_TYPE_STORE);
				objgoalDTO.setLevelId(storeId);
				objgoalDTO.setBudget(budget);
				objgoalDTO.setForecast(foreCast);
				//logger.debug("Week End Date    " + objgoalDTO.getWeekEndDate());
				//logger.debug("Level ID Type    " + objgoalDTO.getLevelTypeID());
				//logger.debug("Level ID         " + objgoalDTO.getLevelId());
				//logger.debug("Budget           " + objgoalDTO.getBudget());
				//logger.debug("Forecast         " + objgoalDTO.getForecast());

				//Remove the existing record
				objSummaryGoalDAO.deleteSummaryGoal(_Conn, objgoalDTO);
				
				//Insert Summary Goal into DB
				objSummaryGoalDAO.insertSummaryGoal(_Conn, objgoalDTO);
				
				//Keep the Week End Date in a collection for roll up
				logProcessedDate(objgoalDTO.getWeekEndDate());
			}
		} 
		catch (GeneralException e) {
			logger.error(e.getMessage());
			processStatus = false;
		}
		return processStatus;		
	} 
	
	
    /*
	 *****************************************************************
	 * Function to get calculate the Summary Goal for District
	 * Argument 1: SummaryGoalDAO
	 * Return : void 
	 *****************************************************************
	 */
	private void ProcessSummaryGoalForDistrict(SummaryGoalDAO objSummaryGoalDAO) {
		StorePerformanceRollupDAO objSPRDAO;

		try {
			objSPRDAO = new StorePerformanceRollupDAO(_Conn);
			
			//Get the district list from retail district
			ArrayList<StorePerformanceRollupSummaryDTO> districtList = objSPRDAO.getDistrictID(_Conn, 0);
			//logger.debug("Total Districts : " + districtList.size());

			
			//If District data exist then proceed with roll up
			if (districtList.size() > 0 ) {
				
				//Run loop for each District
				for (StorePerformanceRollupSummaryDTO retailData : districtList) {
					//logger.debug("Processing for District id :"	+ String.valueOf(retailData.leveID));

					//Get the list of Store Id for the processing district
					String storeList = objSPRDAO.getStoreID(_Conn,
							String.valueOf(retailData.getLevelID()));
					//logger.debug("Store List for district    :" + storeList);

					//Call the roll up for District
					processSummaryRollup(LEVEL_TYPE_STORE, LEVEL_TYPE_DISTRICT,
							storeList, retailData.getLevelID(), objSummaryGoalDAO);
				}
			}
			else {
				//logger.debug("There is no District data found to do Roll up");
			}
				
			
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
	}
	
    /*
	 *****************************************************************
	 * Function to get calculate the Summary Goal for Region
	 * Argument 1: SummaryGoalDAO
	 * Return : void 
	 *****************************************************************
	 */
	private void ProcessSummaryGoalForRegion(SummaryGoalDAO objSummaryGoalDAO) {
		StorePerformanceRollupDAO objSPRDAO;

		try {
			objSPRDAO = new StorePerformanceRollupDAO(_Conn);
			
			//Get the region data from retail region
			ArrayList<StorePerformanceRollupSummaryDTO> regionData = objSPRDAO.getRegions(_Conn, 0);
			//logger.debug("Total Regions : " + regionData.size());
			
			//If Region data exist then proceed with roll up
			if (regionData.size() > 0) {
				
				//Run loop for each region
				for (StorePerformanceRollupSummaryDTO regionList : regionData) {
					//logger.debug("Processing for Region :" + String.valueOf(regionList.leveID));

					//Get district data for processing region  
					ArrayList<StorePerformanceRollupSummaryDTO> districtData = objSPRDAO
							.getDistrictID(_Conn, regionList.getLevelID());

					//If District data exist then proceed further
					if (districtData.size() > 0) {

						StringBuilder distList = new StringBuilder();
						for (StorePerformanceRollupSummaryDTO districtList : districtData) {

							if (distList.length() > 0)
								distList.append(", ");
							distList.append(String.valueOf(districtList.getLevelID()));
						}
						//logger.debug("District list...." + distList.toString());
						
						//Call the roll up for Region
						processSummaryRollup(LEVEL_TYPE_DISTRICT,
								LEVEL_TYPE_REGION, distList.toString(),
								regionList.getLevelID(), objSummaryGoalDAO);
					} else {
						//logger.debug("No District data found for region "	+ String.valueOf(regionList.leveID));
					}
				}
			} else {
				//logger.debug("There is no Region data found to do Roll up");				
			}
			
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
	}

    /*
	 *****************************************************************
	 * Function to get calculate the Summary Goal for Division
	 * Argument 1: SummaryGoalDAO
	 * Return : void 
	 *****************************************************************
	 */
	private void ProcessSummaryGoalForDivision(SummaryGoalDAO objSummaryGoalDAO) {
		StorePerformanceRollupDAO objSPRDAO;

		try {
			objSPRDAO = new StorePerformanceRollupDAO(_Conn);

			//Get Division (Banner) from Retail Division
			ArrayList<StorePerformanceRollupSummaryDTO> divisionData = objSPRDAO.getBanner(_Conn);
			//logger.debug("Total Divisions : " + divisionData.size());
			
			//If Division data exist then proceed next
			if (divisionData.size() > 0) {
				
				//Run loop for each region
				for (StorePerformanceRollupSummaryDTO divisionList : divisionData) {
				
					//logger.debug("Processing for Division :" + String.valueOf(divisionList.leveID));

					//Get region for processing Division
					ArrayList<StorePerformanceRollupSummaryDTO> regionData = objSPRDAO
							.getRegions(_Conn, divisionList.getLevelID());
					
					StringBuilder regList = new StringBuilder();

					//If Region data exist then proceed further
					if (regionData.size() > 0) {
						for (StorePerformanceRollupSummaryDTO regionList : regionData) {
							if (regList.length() > 0)
								regList.append(" ,");
							regList.append(String.valueOf(regionList.getLevelID()));
						}
						//logger.debug("Region List...." + regList.toString());

						//Call the roll up for Division
						processSummaryRollup(LEVEL_TYPE_REGION,
								LEVEL_TYPE_DIVISION, regList.toString(),
								divisionList.getLevelID(), objSummaryGoalDAO);
					} else {
						//logger.debug("No Region data found for Division "	+ String.valueOf(divisionList.leveID));
					}
				}
			}
			else {
				//logger.debug("There is no Division data found to do Roll up");						
			} 
			
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
	}

	
	
	
	
	
    /*
	 *****************************************************************
	 * Function to get calculate the Summary Goal for Chain
	 * Argument 1: SummaryGoalDAO
	 * Return : void 
	 *****************************************************************
	 */
	private void ProcessSummaryGoalForChain(SummaryGoalDAO objSummaryGoalDAO) {
		StorePerformanceRollupDAO objSPRDAO;

		try {
			objSPRDAO = new StorePerformanceRollupDAO(_Conn);

			//Get Division (Banner) from Retail Division
			ArrayList<StorePerformanceRollupSummaryDTO> divisionData = objSPRDAO.getBanner(_Conn);
			//logger.debug("Total Divisions : " + divisionData.size());
			
			//If Division data exist then proceed next
			if (divisionData.size() > 0) {
				StringBuilder divList = new StringBuilder();
				
				for (StorePerformanceRollupSummaryDTO divisionList : divisionData) {
					if (divList.length() > 0)
						divList.append(" ,");
					divList.append(String.valueOf(divisionList.getLevelID()));
				}
				//logger.debug("Division List...." + divList.toString());

				StoreDAO objStoreDAO = new StoreDAO();
				int chainId = objStoreDAO.getSubscriberCompetitorChain(_Conn); 
				
				if (chainId > 0) {
				//Call the roll up for Division
				processSummaryRollup(LEVEL_TYPE_DIVISION,
						LEVEL_TYPE_CHAIN, divList.toString(),
						chainId, objSummaryGoalDAO);
				}
				else {
					//logger.debug("Chain Id not found");					
				}
			}
			else {
				//logger.debug("There is no Division data for chain");						
			} 
			
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
	}

	
	
	
	
	
	
	
	
	
	
	
	/*
	 *****************************************************************
	 * Function to get the summary data and create roll up data
	 * Argument 1: Level Type Id to get the roll update data
	 * Argument 2: Level Type Id to update the roll update data
	 * Argument 3: List of IDs (Store Id, District Id, Region Id)
	 * Argument 4: Level id
	 * Argument 5: Summary Goal DAO instance 
	 * Return : void 
	 *****************************************************************
	 */
	private void processSummaryRollup(int sourceLevelTypeId, int targetLevelTypeId, String rollupSourceIds, int levelId, SummaryGoalDAO objSummaryGoalDAO) {
		SummaryGoalDTO objgoalDTO = new SummaryGoalDTO();
		try {
			//Fetch the accumulated summary Goal data for district
			ArrayList<SummaryGoalDTO> listGoalData = objSummaryGoalDAO.getSummaryGoalData(_Conn, sourceLevelTypeId, rollupSourceIds, _weekEndDates);
			
			// Proceed only if goal data found 
			if (listGoalData.size() > 0) {
				
				//Do the process for each Week End Date
				for(int dcount=0; dcount<listGoalData.size(); dcount++) {
					
					
					objgoalDTO = listGoalData.get(dcount);
					objgoalDTO.setLevelTypeID(targetLevelTypeId);
					objgoalDTO.setLevelId(levelId);
					//logger.debug("Level Id      : " + objgoalDTO.getLevelId() );
					//logger.debug("Level Type Id : " + objgoalDTO.getLevelTypeID());					
					//logger.debug("Week End Date :"  + objgoalDTO.getWeekEndDate());
					//logger.debug("Budget        :"  + objgoalDTO.getBudget());
					//logger.debug("Fore Cast     :"  + objgoalDTO.getForecast());

					//Remove the existing record
					objSummaryGoalDAO.deleteSummaryGoal(_Conn, objgoalDTO);
	
					//Insert Summary Goal into DB
					objSummaryGoalDAO.insertSummaryGoal(_Conn, objgoalDTO);
				}
			}
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
		
	}
	
	
	/*
	 *****************************************************************
	 * Function to move source file to destination based on status
	 * Argument 1: Store Number
	 * Return : int (Store ID) 
     * @throws GeneralException, SQLException 
	 *****************************************************************
	 */
	private void moveSourceFile(String xlsFileName) {
		/*if all the records are processed successfully, then move excel to completed folder
		  if any of the row is skipped then move the excel to bad folder */
		if (_rows_total_count == (_rows_inserted_count + 1) ) {
			//logger.debug("Move the file " + xlsFileName + " to completed folder");
			//logger.debug("File path " + (_xlsFilePath + Constants.COMPLETED_FOLDER));
			PrestoUtil.moveFile(_xlsFilePath + xlsFileName, _xlsFilePath + Constants.COMPLETED_FOLDER);					
		}
		else {
			//logger.debug("Move the file " + xlsFileName + " to bad folder");
			//logger.debug("File path " + (_xlsFilePath + Constants.BAD_FOLDER));
			PrestoUtil.moveFile(_xlsFilePath + xlsFileName, _xlsFilePath + Constants.BAD_FOLDER);
		}
	}
	
    
	/*
	 *****************************************************************
	 * Function to get keep the processed dates in a collection
	 * Argument 1: processed week end date 
	 * Return : void
     * *****************************************************************
	 */
	private void logProcessedDate(String weekEndDate) {
		
		if ((_weekEndDates.size() < 1 ) || (!_weekEndDates.containsKey(weekEndDate))) {
			_weekEndDates.put(weekEndDate, 1);
			//logger.debug("Week End Date added in collection " + weekEndDate);
		}
		/*else {
			//logger.debug("Week End Date exist already " + weekEndDate);
		}*/
	}

	
	/*
	 *****************************************************************
	 * Function to get the Store Number either from Excel cell data  
	 * Argument 1: Excel Cell
	 * Return : String (Store Number) 
	 *****************************************************************
	 */
	private String getStoreNumber(HSSFCell cellStore) {

		String strStoreNumber = null;
		
		//logger.debug("Cell Type        " + cellStore.getCellType());		
		
		if (cellStore.getCellType() == HSSFCell.CELL_TYPE_NUMERIC) {
		
			int storeNumber = (int) cellStore.getNumericCellValue();
			strStoreNumber = String.valueOf(storeNumber);
		
		}
		else if (cellStore.getCellType() == HSSFCell.CELL_TYPE_STRING) {
		
			strStoreNumber = cellStore.getStringCellValue();
		
		}
		
		strStoreNumber = String.format("%4s", strStoreNumber).replace(' ', '0');
		//logger.debug("Store Number     " + strStoreNumber);
		return strStoreNumber;
		
	}
	
	
    /*
	 *****************************************************************
	 * Function to get competitor Store IDs
	 * Argument 1: Store Number
	 * Return : int (Store ID) 
     * @throws GeneralException, SQLException 
	 *****************************************************************
	 */
    private int getCompStore(String StoreNum, CompetitiveDataDAO objcomp)
    {
    	int storeID = -1;
		try {
			
			/*Check the input Store Number exist in the collection 
			  if exist then get the Store ID from the collection */
			if ((_storeIdList.size() > 0 ) && (_storeIdList.containsKey(StoreNum))) {
				
				storeID = _storeIdList.get(StoreNum);
				//logger.debug("Get Store Number form collection....");
				
			}
			else {
				
				//logger.debug("Get store Number from DB............");
				//Call the DAO to get the Store ID 
				ArrayList<String> storeIdList = objcomp.getStoreIdList(_Conn, StoreNum);
				if (storeIdList.size() >0 ) {
					//Assign the Store ID into private variable
					storeID = Integer.parseInt(storeIdList.get(0));
					//Store the Store Id in the collection with Store Number as key
					_storeIdList.put(StoreNum, storeID);
				}
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}    		
    return storeID;
    }
}