/*
 * Title: TOPS  Repair Batch process..
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	03/07/2012	Dinesh Kumar	Initial Version 
 * Version 0.2  26/09/2012  Dinesh Kumar    Code refactored
 *******************************************************************************
 */

package com.pristine.dataload.salesanalysis;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import com.pristine.business.entity.SalesaggregationbusinessV2;
import com.pristine.dao.CompStoreDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.salesanalysis.RepairDao;
import com.pristine.dao.salesanalysis.SalesAggregationCtdDao;
import com.pristine.dao.salesanalysis.SalesAggregationDailyDao;
import com.pristine.dao.salesanalysis.SalesAggregationDailyRollupDao;
import com.pristine.dao.salesanalysis.SalesAggregationDao;
import com.pristine.dao.salesanalysis.SalesAggregationRollupDao;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.salesanalysis.SpecialCriteriaDTO;
import com.pristine.dto.salesanalysis.SummaryDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PropertyManager;

import java.text.ParseException;

public class RepairBatch {
	
	static Logger logger = Logger.getLogger("RepairBatch");
	private Date _startDate 	   = null;              // hold the input start date
	private Date _endDate   	   = null;              // hold the input end date
	private Connection _conn       = null;              // hold the Connection Object
	private String _repairMode     = null;              // hold the input _repairMode Object
	private String _storeNumber    = null;             // hold the input district Id.
	private String _district       = null;
	private String _region         = null;
	private String _division       = null;
	private String _chain          = null;
	private int _locationId        = 0;
	
		
	CompetitiveDataDTO 				_competitorDetails            = null;
	HashMap<String, SummaryDataDTO> _beforeSummarydailyDetails    = null;
	HashMap<String, SummaryDataDTO> _afterSummarydailyDetails     = null;
	HashMap<String, SummaryDataDTO> _beforeSummarydistrictDetails = null;
	HashMap<String, SummaryDataDTO> _afterSummarydistrictDetails  = null;
	HashMap<String, SummaryDataDTO> _beforeSummaryRegionDetails   = null;
	HashMap<String, SummaryDataDTO> _afterSummaryRegionDetails    = null;
	HashMap<String, SummaryDataDTO> _beforeSummaryDivisionDetails = null;
	HashMap<String, SummaryDataDTO> _afterSummaryDivisionDetails  = null;
	HashMap<String, SummaryDataDTO> _beforeSummaryChainDetails    = null;
	HashMap<String, SummaryDataDTO> _afterSummaryChainDetails     = null;
	
	DateFormat _formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	// Object for RetailCalendarDao
	RetailCalendarDAO _objCalendarDao = new RetailCalendarDAO();

	// Object for CompStoreDAO
	CompStoreDAO _objCompStoreDao = new CompStoreDAO();

	// Object for SpecialCriteriaDTO
	SpecialCriteriaDTO _objsplCriteriaDTO = new SpecialCriteriaDTO();

	// Object for SummaryDailyDao
	SalesAggregationDao _objSummaryDao = new SalesAggregationDao();
	
	// Object for SalesAggregationDao
	SalesAggregationDao _objSalesDao = new SalesAggregationDao();
	
	// Object for SalesAggregationDailyDao
	SalesAggregationDailyDao _objSalesDailyDao = new SalesAggregationDailyDao();
	
	// Object for RepairDao
	RepairDao _objRepairDao = new RepairDao();
		
	// object for SummaryRollUpDao
	SalesAggregationDailyRollupDao _objSDRollupDao = new SalesAggregationDailyRollupDao();
		
	// object for SalesAggregationRollupDao
	SalesAggregationRollupDao _objSalesAggregationDao = new SalesAggregationRollupDao();
	
	// Object for SalesAggregationBusinesLogic
	SalesaggregationbusinessV2 _objSalesBusiness = new SalesaggregationbusinessV2();
	
	// Object for SalesAggregationCtd
	SalesAggregationCtdDao _objSalesCtd = new SalesAggregationCtdDao();
	
	// Object for SalesAggregationRollupDao
	SalesAggregationRollupDao _objSalesRollupDao = new SalesAggregationRollupDao();
	
	/**********************************************
	 * Class constructor 
	 * Argument 1 : Store Number
	 * Argument 2 : Start Date
	 * Argument 3 : End Date
	 * Process 1 : Input Validation
	 * Process 2 : Intialize the Db connection
	 * 
	 */
	
	public RepairBatch(String storeNo, Date startDate, String repairMode,
			String district, String region, String divison, String chain) {
		
		
		// input validation

	
		if (startDate == null) {
			logger.error(" Input Missing....... PROCESSDATE");
			System.exit(1);
		}
		
		if( repairMode ==null){
			logger.error(" Input Missing..........REPAIRLEVEL");
			System.exit(1);
		}

		if (repairMode.equalsIgnoreCase("STORE")) {
			if (storeNo == null && district == null) {
				logger.error(" Input Missing ...... STORE/DISTRICT");
				System.exit(1);
			}
			
			if( district !=null){
				storeNo = "";
			}
			else{
				district = "";
			}

		}
		
		if (repairMode.equalsIgnoreCase("DISTRICT")) {
			if (district == null) {

				logger.error(" Input Missing ...... DISTRICT");
				System.exit(1);
			}

		}
		
		if (repairMode.equalsIgnoreCase("REGION")) {
			if (region == null) {

				logger.error(" Input Missing ...... REGION");
				System.exit(1);
			}

		}
		

		if (repairMode.equalsIgnoreCase("DIVISION")) {
			if (divison == null) {

				logger.error(" Input Missing ...... DIVISION");
				System.exit(1);
			}

		}
		
		if (repairMode.equalsIgnoreCase("CHAIN")) {
			if (chain == null) {

				logger.error(" Input Missing ...... CHAIN");
				System.exit(1);
			}

		}

		// assign the input values to global variables
	 
		_startDate   = startDate;
		_endDate     = startDate;
		_repairMode  = repairMode;
		_storeNumber = storeNo;
		_district = district;
		_region = region;
		_division = divison;
		_chain = chain;
		 
		PropertyManager.initialize("analysis.properties");
		
		try{
			_conn = DBManager.getConnection();
		}catch(GeneralException gex){
			logger.error(" Connection Failed...." , gex);
			System.exit(1);
		}
		
		
	}

	/************************************************************************
	 * main method for the batch
	 * Argument 1 : Store Number
	 * Argument 2 : Start Date
	 * Argument 3 : End Date
	 * Batch mainly used to Recalculate the daily aggregation records and 
	 * Reinsert into all levels
	 * @throws GeneralException 
	 *  
	 */
	
	public static void main(String[] args) throws GeneralException{
		
		PropertyConfigurator.configure("log4j-repair-batch.properties");
		logCommand(args);
		String storeNo = null;     // hold the input store number
		String district = null;  // hold the input District Id
		Date processDate = null;   // hold the input start date
		String repairMode = null;  // hold the input repair mode object
		String region = null;       // hold the input region Id
		String divison = null;      // hold the input division Id
		String chain   = null;       // hold the input chain id
		DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
		try{
		
			// read the input parameters
			for (int ii = 0; ii < args.length; ii++) {

				String arg = args[ii];
				
				// for getting store number
				if (arg.startsWith("STORE")) {
					storeNo = arg.substring("STORE=".length());
				}
				
				// for getting process date
				else if (arg.startsWith("PROCESSDATE")) {
					String inputdate = arg.substring("PROCESSDATE=".length());
					try {
						processDate = dateFormat.parse(inputdate);
					} catch (ParseException par) {
						logger.error(" Input Date Error....PROCESSDATE");
						System.exit(1);
					}
				}
				
				// for getting repair level
				else if (arg.startsWith("REPAIRLEVEL")) {
					repairMode = arg.substring("REPAIRLEVEL=".length());
				}
				
				// for getting district Id
				else if (arg.startsWith("DISTRICT")) {
					district = arg.substring("DISTRICT=".length());
				} 
				
				// for getting region id
				else if (arg.startsWith("REGION")) {
					region = arg.substring("REGION=".length());
				}
				
				// for getting division id
				else if (arg.startsWith("DIVISION")) {
					divison = arg.substring("DIVISION=".length());
				}
				
				// for getting chain id
				else if (arg.startsWith("CHAIN")) {
					chain = arg.substring("CHAIN=".length());
				}
				
			}

			// call the class constructors
			RepairBatch objRepairBatch = new RepairBatch(storeNo, processDate,
					repairMode, district, region, divison, chain);
		
			// call the Repairprocess method 
			objRepairBatch.repairProcess();
		
		}catch(Exception gex){
			logger.error(" Main Method Error...... " , gex);
		}

	}
	
	/*************************************************************
	 *  Method used to get the basic records from " MOVEMENT_DAILY"
	 *  based on given input condition . 
	 *  Argument 1 : Start Date
	 *  Argument 2 : End Date
	 *  Argument 3 : Store Number
	 * @throws GeneralException 
	 * @throws SQLException 
	 * @throws ParseException 
	*/

	private void repairProcess() throws GeneralException, SQLException, ParseException {

		// get the calendar list from retail_calendar table.
		try {
			
		
			RetailCalendarDTO objCalendarDto = null;
			
			List<RetailCalendarDTO> calendarList = _objCalendarDao
					.RowTypeBasedCalendarList(_conn, _startDate, _endDate,
							Constants.CALENDAR_DAY);
			
			for( int cL= 0 ; cL< calendarList.size();cL++){
			
				objCalendarDto = calendarList.get(0);
				
			}
			
			
			if( _repairMode.equalsIgnoreCase("STORE")){
						 
			List<SummaryDataDTO> storeList = _objCompStoreDao.getStoreNumebrs(_conn,_district,_storeNumber);
			
			for( int ii=0; ii< storeList.size();ii++){
				
				
				SummaryDataDTO objStoreDto =  storeList.get(ii);
				
				_locationId = objStoreDto.getLocationId();
				
				SummaryDailyV2 objSummaryLoader = new SummaryDailyV2(_startDate,_endDate, _storeNumber , _district , "NORMAL", "MD", "N");
				
				logger.info(" get product group records begins..........");
				objSummaryLoader.getProductGroupData();
			
				logger.info(" Get Uom Details.....");
				objSummaryLoader.getUomData();
					
				
				_objsplCriteriaDTO = objSummaryLoader.loadSpecialCriteria();

					repairDaily(objSummaryLoader,  objCalendarDto , objStoreDto);
					repairWeekly(objCalendarDto.getStartDate());
					repairPeriod(objCalendarDto.getStartDate());
					repairQuarter(objCalendarDto.getStartDate());
					repairYear(objCalendarDto.getStartDate());
			}
				
			}else {
				
			SummaryDailyRollUp objSummaryRollupLoader = new SummaryDailyRollUp(	_startDate, _endDate, _district,
					_region,_division,_chain,"NORMAL", "N");
				
			repairDailyRollup(objSummaryRollupLoader, objCalendarDto);
			repairWeekly(objCalendarDto.getStartDate());
			repairPeriod(objCalendarDto.getStartDate());
			repairQuarter(objCalendarDto.getStartDate());
			repairYear(objCalendarDto.getStartDate());
			}
		
		} catch (GeneralException e) {

			logger.error(" Error in process repair ... ", e);
			throw new GeneralException(" Error in process repair ... ", e);
		}

	}
	
	
	
	/**
	 * Process 1 : Get the before aggregation records from "SALES_AGGR_DAILY" table
	 * Process 2 : call the summary daily batch for day level aggregation
	 * Process 3 : get the after aggregation records from "SALES_AGGR_DAILY" table
	 * Argument 1 :SummaryDailyV2 objSummaryLoader
	 * Argument 2 :SpecialCriteriaDTO splCriteriaDTO
	 * Argument 3 :RetailCalendarDTO objCalendarDto
	 * Argument 4 :SalesAggregationDaoobjSummaryDao
	 * @param objStoreDto 
	 * @throws GeneralException
	 */
	private void repairDaily(SummaryDailyV2 objSummaryLoader,RetailCalendarDTO objCalendarDto, SummaryDataDTO objStoreDto) 
			throws GeneralException {

		try {
			
			logger.info(" Get current aggregation details from SALES_AGGR_DAILY table.....");
			
			// before aggregation records
			 _beforeSummarydailyDetails = _objSummaryDao
					.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
							_locationId,
							Constants.STORE_LEVEL_ID ,"SALES_AGGR_DAILY");
			 
			 logger.info(" Before Summary Details Count.......... " + _beforeSummarydailyDetails.size());
			 
			 logger.info(" Repair Daily Aggregation Begins................................");

			// call the summary daily batch and aggregate in all levels
			try {
				objSummaryLoader.ComputeSummaryAggregationForDay(objCalendarDto,objStoreDto,_objSalesDailyDao,_objsplCriteriaDTO, null);
			} catch (ParseException e) {
				 logger.error(" Error While Parsing the Date....." , e);
				 throw new GeneralException(" Error While Parsing the Date....." , e);
			}

			
			logger.info(" Get the new aggregation details from SALES_AGGR_DAILY table.........");
			
			// after aggregation records
			 _afterSummarydailyDetails = _objSummaryDao
					.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
							_locationId,
							Constants.STORE_LEVEL_ID, "SALES_AGGR_DAILY");
			 
			 logger.info(" After Summary Details Count.......... " + _afterSummarydailyDetails.size());
			 
			 
		} /*catch (ParseException e) {
			 logger.error(" Error in date parsing....... " +e);
		} */catch (GeneralException e) {
			throw new GeneralException(" Error In Summary Daily Aggregation......." , e);

		}

	}
	
	
	/**
	 * Process 1 : Get the before aggregation records from "SALES_AGGR_DAILY_ROLLUP" table
	 * Process 2 : call the summary daily batch for day level aggregation
	 * Process 3 : get the after aggregation records from "SALES_AGGR_DAILY_ROLLUP" table
	 * Argument 1 :objSummaryRollupLoader
	 * Argument 2 :objCalendarDto
	 * Argument 3 :objCalendarDao
	 * Argument 4 :objSalesDao
	 * Argument 5 :objSDRollupDao
	 * @param objSummaryDao 
	 * @throws GeneralException 
	 */
	private void repairDailyRollup(SummaryDailyRollUp objSummaryRollupLoader,
			RetailCalendarDTO objCalendarDto) throws GeneralException {
		
		try {
			
			logger.info(" Repair the daily rollup proces begins..............");
			// before aggregation records
			
			if( _repairMode.equalsIgnoreCase("DISTRICT")){
			logger.info(" Get District Level Before aggregation proces........");
			 _beforeSummarydistrictDetails = _objSummaryDao
						.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
								Integer.parseInt(_district),
								Constants.DISTRICT_LEVEL_ID ,"SALES_AGGR_DAILY_ROLLUP");
			}
			 
			else if( _repairMode.equalsIgnoreCase("REGION")){
			 logger.info(" Get Region Level Before aggregation proces........");
			// before aggregation records
			 _beforeSummaryRegionDetails = _objSummaryDao
						.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
								Integer.parseInt(_region),
								Constants.REGION_LEVEL_ID , "SALES_AGGR_DAILY_ROLLUP");
			}
			 
			else if( _repairMode.equalsIgnoreCase("DIVISION")){
			 logger.info(" Get division Level Before aggregation proces........");
			  // before aggregation records
			 _beforeSummaryDivisionDetails = _objSummaryDao
						.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
								Integer.parseInt(_division),
								Constants.DIVISION_LEVEL_ID, "SALES_AGGR_DAILY_ROLLUP");
			}
			 
			else if( _repairMode.equalsIgnoreCase("CHAIN")){
			 logger.info(" Get chain Level Before aggregation proces........");
			 // before aggregation records
			 _beforeSummaryChainDetails = _objSummaryDao
						.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
								Integer.parseInt(_chain),
							Constants.CHAIN_LEVEL_ID,"SALES_AGGR_DAILY_ROLLUP");
			}
			 

			objSummaryRollupLoader.locationLevelAggregation(objCalendarDto, _objSDRollupDao, _objSalesDao, _objCalendarDao, null);
			 
			 
			 if( _repairMode.equalsIgnoreCase("DISTRICT")){
			 logger.info(" Get District Level after aggregation proces........");
			 // after aggregation records
			 _afterSummarydistrictDetails = _objSummaryDao
						.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
								Integer.parseInt(_district),
								Constants.DISTRICT_LEVEL_ID ,"SALES_AGGR_DAILY_ROLLUP");
			 }
			 
			 else if( _repairMode.equalsIgnoreCase("REGION")){
			 logger.info(" Get Region Level after aggregation proces........");
			// after aggregation records
			 _afterSummaryRegionDetails = _objSummaryDao
						.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
								Integer.parseInt(_region),
								Constants.REGION_LEVEL_ID , "SALES_AGGR_DAILY_ROLLUP");
			 }
			 
			 else if( _repairMode.equalsIgnoreCase("DIVISION")){
			 logger.info(" Get Division Level after aggregation proces........");
			  // after aggregation records
			 _afterSummaryDivisionDetails = _objSummaryDao
						.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
								Integer.parseInt(_division),
								Constants.DIVISION_LEVEL_ID, "SALES_AGGR_DAILY_ROLLUP");
			 }
			 
			 else if( _repairMode.equalsIgnoreCase("CHAIN")){
			 logger.info(" Get chain Level after aggregation proces........");
			 // after aggregation records
			 _afterSummaryChainDetails = _objSummaryDao
						.getSummaryDetails(_conn, objCalendarDto.getCalendarId(),
								Integer.parseInt(_chain),
								Constants.CHAIN_LEVEL_ID,"SALES_AGGR_DAILY_ROLLUP");
		 }

		} catch (GeneralException e) {
			throw new GeneralException(" Error while processing repair rollup level....",e);
		}

	}
	
	
	/**
	 * Process 1 : Get the weekly aggregation records from SALES_AGGR_WEEKLY
	 * Process 2 : Minus the weekly total and previousaggregation total
	 * Process 3 : Add the current aggregation total to weekly map
	 *    [ Weekly Total - Previous Day Total ] - Current Day Total
	 * Argument 1 : objCalendarDto
	 * Argument 2 : Process Date
	 * Argument 3 : objCalendarDao 
	 * @param objRepairDao 
	 * @throws GeneralException 
	 * @throws ParseException 
	 * @throws SQLException 
	 */
	private void repairWeekly(String processDate) throws GeneralException, SQLException, ParseException {
		
		
				
		logger.info(" Weekly Repair process begins................");
		
		RetailCalendarDTO weeklyCalendarDto = null;
		
		List<RetailCalendarDTO> dailyCalendarList = null;
		
		
		// get the week calendar Id
		try {
			// Convert the string to date
			computeProcessDate(processDate);
		} catch (ParseException e) {
			throw new GeneralException(" Date Convertion Error...........", e);
		}
		try {

			logger.info(" Get weekly Calendar Id process Begins........");
			weeklyCalendarDto = _objCalendarDao
					.week_period_quarter_yearCalendarList(_conn, _startDate,
							Constants.CALENDAR_WEEK);

			dailyCalendarList = _objCalendarDao.dayCalendarList(_conn,
					_startDate,
					_formatter.parse(weeklyCalendarDto.getEndDate()),
					Constants.CALENDAR_DAY);

		} catch (GeneralException e) {
			throw new GeneralException(" Get Week CalendarId Error...... ", e);

		}
			
		
		if (_repairMode.equalsIgnoreCase("STORE")) {
			logger.info(" Get Current Weekly Aggregation Records From SALES_AGGR_WEEKLY table.....");
			HashMap<String, SummaryDataDTO> previousweeklyAggregation = _objSummaryDao
					.getSummaryDetails(_conn,
							weeklyCalendarDto.getCalendarId(), _locationId,
							Constants.STORE_LEVEL_ID, "SALES_AGGR_WEEKLY");

			// itreate the weekly aggregation map and repair the process
			_objRepairDao.calculatRepairAggregation(previousweeklyAggregation,
					_beforeSummarydailyDetails, _afterSummarydailyDetails);

			try {
				_objSummaryDao.updateSalesAggr(_conn,
						previousweeklyAggregation, 0,
						weeklyCalendarDto.getCalendarId(), "SALES_AGGR_WEEKLY",
						"ACTUAL", null);
			} catch (SQLException e) {
				throw new GeneralException(" Excute Error...........", e);
			}

		}
	
		if (_repairMode.equalsIgnoreCase("DISTRICT")) {
			// repair the weekly district data

			logger.info(" get the Current District Weekly Aggregation records from SALES_AGGR_WEEKLY_ROLLUP..............");

			HashMap<String, SummaryDataDTO> previousDistrictWeekAggregation = _objSummaryDao
					.getSummaryDetails(_conn,
							weeklyCalendarDto.getCalendarId(),
							Integer.parseInt(_district),
							Constants.DISTRICT_LEVEL_ID,
							"SALES_AGGR_WEEKLY_ROLLUP");

			// itreate the weekly aggregation map and repair the process
			_objRepairDao
					.calculatRepairAggregation(previousDistrictWeekAggregation,
							_beforeSummarydistrictDetails,
							_afterSummarydistrictDetails);

			try {
				_objSalesAggregationDao.updateSalesRollup(_conn,
						previousDistrictWeekAggregation, 0,
						weeklyCalendarDto.getCalendarId(),
						"SALES_AGGR_WEEKLY_ROLLUP", "ACTUAL");
			} catch (GeneralException e) {
				throw new GeneralException(
						" Week District Level Rollup Error......... ", e);

			}
		}
		

		
		
		if ( _repairMode.equalsIgnoreCase("REGION")) {
			// repair the weekly Region data

			logger.info(" get the Current Region Weekly Aggregation records from SALES_AGGR_WEEKLY_ROLLUP..............");

			HashMap<String, SummaryDataDTO> previousRegionWeekAggregation = _objSummaryDao
					.getSummaryDetails(_conn,
							weeklyCalendarDto.getCalendarId(),
							Integer.parseInt(_region),
							Constants.REGION_LEVEL_ID,
							"SALES_AGGR_WEEKLY_ROLLUP");

			// itreate the weekly aggregation map and repair the process
			_objRepairDao.calculatRepairAggregation(
					previousRegionWeekAggregation, _beforeSummaryRegionDetails,
					_afterSummaryRegionDetails);

			try {
				_objSalesAggregationDao.updateSalesRollup(_conn,
						previousRegionWeekAggregation, 0,
						weeklyCalendarDto.getCalendarId(),
						"SALES_AGGR_WEEKLY_ROLLUP", "ACTUAL");
			} catch (GeneralException e) {
				throw new GeneralException(
						" Week Region Level Rollup Error......... ", e);

			}

		}
		
		
		if( _repairMode.equalsIgnoreCase("DIVISION")){
		// repair the weekly Divison data
		
			logger.info(" get the Current division Weekly Aggregation records from SALES_AGGR_WEEKLY_ROLLUP..............");
			HashMap<String, SummaryDataDTO> previousDivisionWeekAggregation = _objSummaryDao
					.getSummaryDetails(_conn,
							weeklyCalendarDto.getCalendarId(),
							Integer.parseInt(_division),
							Constants.DIVISION_LEVEL_ID,
							"SALES_AGGR_WEEKLY_ROLLUP");

			// itreate the weekly aggregation map and repair the process
			_objRepairDao
					.calculatRepairAggregation(previousDivisionWeekAggregation,
							_beforeSummaryDivisionDetails,
							_afterSummaryDivisionDetails);
			try {
				_objSalesAggregationDao.updateSalesRollup(_conn,
						previousDivisionWeekAggregation, 0,
						weeklyCalendarDto.getCalendarId(),
						"SALES_AGGR_WEEKLY_ROLLUP", "ACTUAL");
			} catch (GeneralException e) {
				throw new GeneralException(
						" Week division Level Rollup Error......... ", e);

			}
		}			
		
		if( _repairMode.equalsIgnoreCase("CHAIN")){
		// repair the weekly Chain data
		
			logger.info(" get the Current Chain Weekly Aggregation records from SALES_AGGR_WEEKLY_ROLLUP..............");

			HashMap<String, SummaryDataDTO> previousChainWeekAggregation = _objSummaryDao
					.getSummaryDetails(_conn,
							weeklyCalendarDto.getCalendarId(),
							Integer.parseInt(_chain), Constants.CHAIN_LEVEL_ID,
							"SALES_AGGR_WEEKLY_ROLLUP");

			// itreate the weekly aggregation map and repair the process
			_objRepairDao.calculatRepairAggregation(
					previousChainWeekAggregation, _beforeSummaryChainDetails,
					_afterSummaryChainDetails);

			try {
				_objSalesAggregationDao.updateSalesRollup(_conn,
						previousChainWeekAggregation, 0,
						weeklyCalendarDto.getCalendarId(),
						"SALES_AGGR_WEEKLY_ROLLUP", "ACTUAL");
			} catch (GeneralException e) {
				throw new GeneralException(
						" Week chain Level Rollup Error......... ", e);

			}
		}		
		
		//ctdProcess(dailyCalendarList,processDate, weeklyCalendarDto, Constants.CTD_WEEK ,_repairMode);
	 
	}
	
	
	private void ctdProcess(List<RetailCalendarDTO> dailyCalendarList,
			String processDate, RetailCalendarDTO weeklyCalendarDto, int ctdConstants, String __repairMode)
			throws NumberFormatException, SQLException, ParseException,
			GeneralException {
		
		
		if( _repairMode.equalsIgnoreCase("STORE")){
		logger.info(" Store Level Ctd Process Starts........");
		// weekly ctd process for store
		_objRepairDao.ctdRepairAggregation(_conn, dailyCalendarList,
				_objSalesBusiness, _objSalesDao, _objSalesCtd,
				_locationId, Constants.STORE_LEVEL_ID,
				ctdConstants, processDate, weeklyCalendarDto.getEndDate(),"SALES_AGGR_DAILY", _objSalesRollupDao);
		}
		
		
		if( _repairMode.equalsIgnoreCase("DISTRICT")){
		logger.info(" District Level Ctd Process Starts........");
		// weekly ctd process for District
		_objRepairDao.ctdRepairAggregation(_conn, dailyCalendarList,
				_objSalesBusiness, _objSalesDao, _objSalesCtd,
				Integer.parseInt(_district), Constants.DISTRICT_LEVEL_ID,
				ctdConstants, processDate,  weeklyCalendarDto.getEndDate(), "SALES_AGGR_DAILY_ROLLUP", _objSalesRollupDao);
		}
		
		if( _repairMode.equalsIgnoreCase("REGION")){
		logger.info(" Region Level Ctd Process Starts........");
		// weekly ctd process for Region
		_objRepairDao.ctdRepairAggregation(_conn, dailyCalendarList,
				_objSalesBusiness, _objSalesDao, _objSalesCtd,
				Integer.parseInt(_region), Constants.REGION_LEVEL_ID,
				ctdConstants, processDate,  weeklyCalendarDto.getEndDate(), "SALES_AGGR_DAILY_ROLLUP", _objSalesRollupDao);
		}
		
		
		if( _repairMode.equalsIgnoreCase("DIVISION")){
		logger.info(" Division Level Ctd Process Starts........");
		// weekly ctd process for Division
				_objRepairDao.ctdRepairAggregation(_conn, dailyCalendarList,
						_objSalesBusiness, _objSalesDao, _objSalesCtd,
						Integer.parseInt(_division), Constants.DIVISION_LEVEL_ID,
						ctdConstants, processDate, weeklyCalendarDto.getEndDate(), "SALES_AGGR_DAILY_ROLLUP", _objSalesRollupDao);
		}
		
		if( _repairMode.equalsIgnoreCase("CHAIN")){
		logger.info(" Chain Level Ctd Process Starts........");
		// weekly ctd process for chain
				_objRepairDao.ctdRepairAggregation(_conn, dailyCalendarList,
						_objSalesBusiness, _objSalesDao, _objSalesCtd,
						Integer.parseInt(_chain), Constants.CHAIN_LEVEL_ID,
						ctdConstants, processDate,  weeklyCalendarDto.getEndDate(), "SALES_AGGR_DAILY_ROLLUP", _objSalesRollupDao);
				
		}
		 
		
	}

	
	private void repairPeriod(String startDate) throws GeneralException, ParseException, SQLException {
		
		logger.info(" Period Repair process begins................");
		
		RetailCalendarDTO periodCalendarId = null;
		
		List<RetailCalendarDTO> dailyCalendarList = null ;
			
		// get the Period calendar Id
		try {

			// Convert the string to date
			computeProcessDate(startDate);
		} catch (ParseException e) {

			throw new GeneralException(" Date Convertion Error...........", e);
		}
		try {
			
			logger.info(" Get period Calendar Id process Begins........");
			periodCalendarId = _objCalendarDao
					.week_period_quarter_yearCalendarList(_conn, _startDate,Constants.CALENDAR_PERIOD);
			
			dailyCalendarList = _objCalendarDao.dayCalendarList(_conn, _startDate, _formatter.parse(periodCalendarId.getEndDate()), Constants.CALENDAR_DAY);
			
		} catch (GeneralException e) {
			throw new GeneralException(" Get period CalendarId Error...... ", e);

		}
		
		
		
		if(  _repairMode.equalsIgnoreCase("STORE")){
		logger.info(" Get Current Period Aggregation Records From SALES_AGGR table.....");
		HashMap<String, SummaryDataDTO> previousAggregation = _objSummaryDao
				.getSummaryDetails(_conn, periodCalendarId.getCalendarId(),
						_locationId, Constants.STORE_LEVEL_ID ,"SALES_AGGR");
		
		
		// itreate the weekly aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousAggregation , _beforeSummarydailyDetails, _afterSummarydailyDetails);
		
		try {
			_objSummaryDao.updateSalesAggr(_conn, previousAggregation, 0, periodCalendarId.getCalendarId(), "SALES_AGGR" ,"ACTUAL",null);
		} catch (SQLException e) {
			 throw new GeneralException(" Excute Error..........." , e);
		} 
			}
	 
		if( _repairMode.equalsIgnoreCase("DISTRICT")){		
		// repair the period district data
		logger.info(" get the Current District Period Aggregation records from SALES_AGGR_ROLLUP..............");
		
		HashMap<String, SummaryDataDTO> previousDistrictAggregation = _objSummaryDao
				.getSummaryDetails(_conn, periodCalendarId.getCalendarId(),
						Integer.parseInt(_district), Constants.DISTRICT_LEVEL_ID ,"SALES_AGGR_ROLLUP");
		
		
		// itreate the period aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousDistrictAggregation , _beforeSummarydistrictDetails, _afterSummarydistrictDetails);
		
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousDistrictAggregation, 0, periodCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Period District Level Rollup Error......... ", e);

		}
		}
	 
		
		if(  _repairMode.equalsIgnoreCase("REGION")){
		// repair the Period Region data
		logger.info(" get the Current Region Period Aggregation records from SALES_AGGR_ROLLUP..............");
		
		HashMap<String, SummaryDataDTO> previousRegionAggregation = _objSummaryDao
				.getSummaryDetails(_conn, periodCalendarId.getCalendarId(),
						Integer.parseInt(_region), Constants.REGION_LEVEL_ID ,"SALES_AGGR_ROLLUP");
			 
		// itreate the weekly aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousRegionAggregation , _beforeSummaryRegionDetails, _afterSummaryRegionDetails);
	
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousRegionAggregation, 0, periodCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Period Region Level Rollup Error......... ", e);

		}
		}
	 
		if( _repairMode.equalsIgnoreCase("DIVISION")){
		// repair the weekly Divison data
		
		logger.info(" get the Current division Period Aggregation records from SALES_AGGR_ROLLUP..............");
				
				 
		HashMap<String, SummaryDataDTO> previousDivisionAggregation = _objSummaryDao
						.getSummaryDetails(_conn, periodCalendarId.getCalendarId(),
								Integer.parseInt(_division), Constants.DIVISION_LEVEL_ID ,"SALES_AGGR_ROLLUP");
				
					
		// itreate the Period aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousDivisionAggregation , _beforeSummaryDivisionDetails, _afterSummaryDivisionDetails);
				
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousDivisionAggregation, 0, periodCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Period division Level Rollup Error......... ", e);

		}
		}
		
		
		 
		if( _repairMode.equalsIgnoreCase("CHAIN")){
		// repair the period Chain data
		
		logger.info(" get the Current Chain Period Aggregation records from SALES_AGGR_ROLLUP..............");
						
						 
		HashMap<String, SummaryDataDTO> previousChainAggregation = _objSummaryDao
								.getSummaryDetails(_conn, periodCalendarId.getCalendarId(),
										Integer.parseInt(_chain), Constants.CHAIN_LEVEL_ID ,"SALES_AGGR_ROLLUP");
		 			
		// itreate the Period aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousChainAggregation , _beforeSummaryChainDetails, _afterSummaryChainDetails);
						
		try{
			 _objSalesAggregationDao.updateSalesRollup(_conn, previousChainAggregation, 0, periodCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Period chain Level Rollup Error......... ", e);

		}
		}

		//ctdProcess(dailyCalendarList,startDate, periodCalendarId, Constants.CTD_PERIOD, _repairMode);
	}

	
	private void repairQuarter(String startDate) throws GeneralException, SQLException, ParseException {
		
		logger.info(" Quarter Repair process begins................");
		
		RetailCalendarDTO QuarterCalendarId = null;
		
		
		List<RetailCalendarDTO> dailyCalendarList = null;
	
		// get the Quarter calendar Id
		try {

			// Convert the string to date
			computeProcessDate(startDate);
		} catch (ParseException e) {

			throw new GeneralException(" Date Convertion Error...........", e);
		}
		try {
			
			logger.info(" Get Quarter Calendar Id process Begins........");
			QuarterCalendarId = _objCalendarDao
					.week_period_quarter_yearCalendarList(_conn, _startDate,Constants.CALENDAR_QUARTER);
			
			dailyCalendarList = _objCalendarDao.dayCalendarList(_conn, _startDate, _formatter.parse(QuarterCalendarId.getEndDate()), Constants.CALENDAR_DAY);
			
		} catch (GeneralException e) {
			throw new GeneralException(" Get Quarter CalendarId Error...... ", e);

		}
		
		if(  _repairMode.equalsIgnoreCase("STORE")){
		logger.info(" Get Current Quarter Aggregation Records From SALES_AGGR table.....");
		HashMap<String, SummaryDataDTO> previousAggregation = _objSummaryDao
				.getSummaryDetails(_conn, QuarterCalendarId.getCalendarId(),
						_locationId, Constants.STORE_LEVEL_ID ,"SALES_AGGR");
		
		
		// itreate the weekly aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousAggregation , _beforeSummarydailyDetails, _afterSummarydailyDetails);
		
		
		try {
			_objSummaryDao.updateSalesAggr(_conn, previousAggregation, 0, QuarterCalendarId.getCalendarId(), "SALES_AGGR" ,"ACTUAL",null);
		} catch (SQLException e) {
			 throw new GeneralException(" Excute Error..........." , e);
		}
		}
		 
		
		if( _repairMode.equalsIgnoreCase("DISTRICT")){
		// repair the weekly district data
		
		logger.info(" get the Current District Quarter Aggregation records from SALES_AGGR_ROLLUP..............");
		
	
		HashMap<String, SummaryDataDTO> previousDistrictAggregation = _objSummaryDao
				.getSummaryDetails(_conn, QuarterCalendarId.getCalendarId(),
						Integer.parseInt(_district), Constants.DISTRICT_LEVEL_ID ,"SALES_AGGR_ROLLUP");
		
			
		// itreate the weekly aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousDistrictAggregation , _beforeSummarydistrictDetails, _afterSummarydistrictDetails);
		
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousDistrictAggregation, 0, QuarterCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" , "ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Quarter District Level Rollup Error......... ", e);

		}
		
		}
	 
		if(  _repairMode.equalsIgnoreCase("REGION")){
		// repair the Quarter Region data
		
		logger.info(" get the Current Region Quarter Aggregation records from SALES_AGGR_ROLLUP..............");
		
		 
		HashMap<String, SummaryDataDTO> previousRegionAggregation = _objSummaryDao
				.getSummaryDetails(_conn, QuarterCalendarId.getCalendarId(),
						Integer.parseInt(_region), Constants.REGION_LEVEL_ID ,"SALES_AGGR_ROLLUP");
		
		 
		
		// itreate the weekly aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousRegionAggregation , _beforeSummaryRegionDetails, _afterSummaryRegionDetails);
		
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousRegionAggregation, 0, QuarterCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Quarter Region Level Rollup Error......... ", e);

		}
		
		}
	 
		
		if( _repairMode.equalsIgnoreCase("DIVISION")){
		// repair the weekly Divison data
		
		logger.info(" get the Current division Quarter Aggregation records from SALES_AGGR_ROLLUP..............");
				
				 
		HashMap<String, SummaryDataDTO> previousDivisionAggregation = _objSummaryDao
						.getSummaryDetails(_conn, QuarterCalendarId.getCalendarId(),
								Integer.parseInt(_division), Constants.DIVISION_LEVEL_ID ,"SALES_AGGR_ROLLUP");
				
	 
				
				// itreate the Quarter aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousDivisionAggregation , _beforeSummaryDivisionDetails, _afterSummaryDivisionDetails);
				
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousDivisionAggregation, 0, QuarterCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Quarter division Level Rollup Error......... ", e);

		}
		
		}
	 
		if( _repairMode.equalsIgnoreCase("CHAIN")){
		// repair the weekly Chain data
		
		logger.info(" get the Current Chain Quarter Aggregation records from SALES_AGGR_ROLLUP..............");
						
						 
		HashMap<String, SummaryDataDTO> previousChainAggregation = _objSummaryDao
								.getSummaryDetails(_conn, QuarterCalendarId.getCalendarId(),
										Integer.parseInt(_chain), Constants.CHAIN_LEVEL_ID ,"SALES_AGGR_ROLLUP");
						
								
			// itreate the Quarter aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousChainAggregation , _beforeSummaryChainDetails, _afterSummaryChainDetails);
						
		try{
			 _objSalesAggregationDao.updateSalesRollup(_conn, previousChainAggregation, 0, QuarterCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Quarter chain Level Rollup Error......... ", e);

		}
		
		}
		
		//ctdProcess(dailyCalendarList,startDate, QuarterCalendarId, Constants.CTD_QUARTER , _repairMode);
		
		 
	}
	
	private void repairYear(String startDate) throws GeneralException, ParseException, NumberFormatException, SQLException {
		
		logger.info(" Year Repair process begins................");
		
		RetailCalendarDTO YearCalendarId = null;
		
		List<RetailCalendarDTO> dailyCalendarList = null;
		
		// get the Year calendar Id
		try {
		 // Convert the string to date
		 computeProcessDate(startDate);
		} catch (ParseException e) {

			throw new GeneralException(" Date Convertion Error...........", e);
		}
		try {
			
			logger.info(" Get Year Calendar Id process Begins........");
			YearCalendarId = _objCalendarDao
					.week_period_quarter_yearCalendarList(_conn, _startDate,Constants.CALENDAR_YEAR);
			
			dailyCalendarList = _objCalendarDao.dayCalendarList(_conn, _startDate, _formatter.parse(YearCalendarId.getEndDate()), Constants.CALENDAR_DAY);
		} catch (GeneralException e) {
			throw new GeneralException(" Get Year CalendarId Error...... ", e);

		}
		
		
		if(  _repairMode.equalsIgnoreCase("STORE")){
		logger.info(" Get Current Year Aggregation Records From SALES_AGGR table.....");
		HashMap<String, SummaryDataDTO> previousAggregation = _objSummaryDao
				.getSummaryDetails(_conn, YearCalendarId.getCalendarId(),
						_locationId, Constants.STORE_LEVEL_ID ,"SALES_AGGR");
		
		
		// itreate the weekly aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousAggregation , _beforeSummarydailyDetails, _afterSummarydailyDetails);
		
	 
		try {
			_objSummaryDao.updateSalesAggr(_conn, previousAggregation, 0, YearCalendarId.getCalendarId(), "SALES_AGGR" ,"ACTUAL",null);
		} catch (SQLException e) {
			 throw new GeneralException(" Excute Error..........." , e);
		} 
		
		}
		// repair the weekly district data
		
		if( _repairMode.equalsIgnoreCase("DISTRICT")){
		logger.info(" get the Current District Year Aggregation records from SALES_AGGR_ROLLUP..............");
		
	
		HashMap<String, SummaryDataDTO> previousDistrictAggregation = _objSummaryDao
				.getSummaryDetails(_conn, YearCalendarId.getCalendarId(),
						Integer.parseInt(_district), Constants.DISTRICT_LEVEL_ID ,"SALES_AGGR_ROLLUP");
		
	 
		
		// itreate the weekly aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousDistrictAggregation , _beforeSummarydistrictDetails, _afterSummarydistrictDetails);
		
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousDistrictAggregation, 0, YearCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Year District Level Rollup Error......... ", e);

		}
		
		}

		// repair the Year Region data
		if( _repairMode.equalsIgnoreCase("REGION")){
		logger.info(" get the Current Region Year Aggregation records from SALES_AGGR_ROLLUP..............");
		
		 
		HashMap<String, SummaryDataDTO> previousRegionAggregation = _objSummaryDao
				.getSummaryDetails(_conn, YearCalendarId.getCalendarId(),
						Integer.parseInt(_region), Constants.REGION_LEVEL_ID ,"SALES_AGGR_ROLLUP");
		
			
		// itreate the weekly aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousRegionAggregation , _beforeSummaryRegionDetails, _afterSummaryRegionDetails);
		
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousRegionAggregation, 0, YearCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP","ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Year Region Level Rollup Error......... ", e);

		}
		}
		
		// repair the weekly Divison data
		if( _repairMode.equalsIgnoreCase("DIVISION")){
		logger.info(" get the Current division Year Aggregation records from SALES_AGGR_ROLLUP..............");
				
				 
		HashMap<String, SummaryDataDTO> previousDivisionAggregation = _objSummaryDao
						.getSummaryDetails(_conn, YearCalendarId.getCalendarId(),
								Integer.parseInt(_division), Constants.DIVISION_LEVEL_ID ,"SALES_AGGR_ROLLUP");
				
			
		// itreate the Year aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousDivisionAggregation , _beforeSummaryDivisionDetails, _afterSummaryDivisionDetails);
				
		try{
		 _objSalesAggregationDao.updateSalesRollup(_conn, previousDivisionAggregation, 0, YearCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP","ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Year division Level Rollup Error......... ", e);

		}
		}
		
		// repair the weekly Chain data
		if( _repairMode.equalsIgnoreCase("CHAIN")){
		logger.info(" get the Current Chain Year Aggregation records from SALES_AGGR_ROLLUP..............");
						
						 
		HashMap<String, SummaryDataDTO> previousChainAggregation = _objSummaryDao
								.getSummaryDetails(_conn, YearCalendarId.getCalendarId(),
										Integer.parseInt(_chain), Constants.CHAIN_LEVEL_ID ,"SALES_AGGR_ROLLUP");
						
							
		// itreate the Year aggregation map and repair the process
		_objRepairDao.calculatRepairAggregation( previousChainAggregation , _beforeSummaryChainDetails, _afterSummaryChainDetails);
						
		try{
			 _objSalesAggregationDao.updateSalesRollup(_conn, previousChainAggregation, 0, YearCalendarId.getCalendarId(), "SALES_AGGR_ROLLUP" ,"ACTUAL");
		} catch (GeneralException e) {
			throw new GeneralException(" Year chain Level Rollup Error......... ", e);

		}
		}
		
		//ctdProcess(dailyCalendarList,startDate, YearCalendarId, Constants.CTD_YEAR ,_repairMode);
		 		
	}
	
	

	/**
	 *****************************************************************
	 * Static method to log the command line arguments
	 *****************************************************************
	 */	
    private static void logCommand (String[] args)  {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Repair Batch ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }

    /*
	 * ****************************************************************
	 *  Method mainly used to construct the date range 
	 * Argument 1 : startDate
	 * Argument 2 : Week Calendar Id
	 * throws startDate
	 * ****************************************************************
	 */
	private void computeProcessDate(String startDate) 
														throws ParseException {
		Calendar cal = Calendar.getInstance();
		// find the date with start time
			
		Date date = (Date) _formatter.parse(startDate);
		cal.setTime(date);
		cal.set(Calendar.HOUR_OF_DAY, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		_startDate = cal.getTime();
	
	}
	

}
