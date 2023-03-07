/*
 * Title: 12 Week Movement Calculation
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	07/12/2012	Janani		Initial Version
  *******************************************************************************
 */
package com.pristine.dataload.prestoload;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.text.SimpleDateFormat;
import java.sql.SQLException;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.Movement13WeeklyDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.DateUtil;

public class Movement12Week
{
	
	static Logger 		logger = Logger.getLogger("Movement12Week");
	private Connection 	_Conn = null;

	//variables to hold the start and dates
	private Date _week1StartDate = null;
	private Date _week1EndDate = null;	    	
	private Date _week2StartDate = null;
	private Date _week2EndDate = null;
	private Date _week12StartDate = null;
	private Date _week12EndDate = null;
	private Date _week13StartDate = null;
	private Date _week13EndDate = null;
	
	//variables for schedule ID for different movement weeks
	private String _week1ScheduleId; 
	private String _week2ScheduleId;
	private String _week12ScheduleId;
	private String _week13ScheduleId;
	
	//Variable to hold process details for item
	private int _itemTotal = 0;
	private int _itemUpdated = 0;
	private int _itemRejected = 0;
	private int _itemUpdateNotRequired = 0;	
	private int _item12WeekCalculated = 0;

	//Temporary variable for testing to hold department id
	private String _departmentCode;
	private String _categoryId;
	
	//Constant to hold the week numbers
	final private int CALC_WEEK_2 = 1;
	private int CALC_WEEK_12 = 11;
	private int CALC_WEEK_13 = 12;
	final private int VALUE_NULL_DEFAULT = -99;
	final private int VALUE_PROCESS_LOG = 1000;
	final private int CALL_SCHEDULE_SINGLE = 1;
	final private int CALL_SCHEDULE_MULTI = 2;
	
	final private String TARGET_TABLE_ITEM = "MOVEMENT_WEEKLY";
	
	//variables to store item list
	private List<Integer> _itemList = null;
	private List<Movement13WeeklyDTO> _update12WeekMovementList = null;
	
	private CompetitiveDataDAO 	_CompDataDAO;
	
	private static String ARG_STORE = "STORE=";
	private static String ARG_ZONE = "ZONE=";
	
	/**
	 * Constructor
	 */
	public Movement12Week ()
	{
		try
		{
	        PropertyManager.initialize("analysis.properties");
	        _Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);

	        _departmentCode = PropertyManager.getProperty("MOVEMENT_EXCLUDE_DEPARTMENT");
	        _categoryId = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");
	        
	        _CompDataDAO = new CompetitiveDataDAO(_Conn);
	        String checkUser = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_USER_ID", Constants.EXTERNAL_PRICECHECK_USER);
	        String checkList = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_LIST_ID", Constants.EXTERNAL_PRICECHECK_LIST);
	        _CompDataDAO.setParameters(checkUser, checkList);
	        
	        String movementWeeksStr = PropertyManager.getProperty("MOVEMENT_WEEKS");
	        if(movementWeeksStr != null){
	        	int movementWeeks = Integer.parseInt(movementWeeksStr);
	        	CALC_WEEK_12 = movementWeeks - 2;
	        	CALC_WEEK_13 = movementWeeks - 1;
	        }
	    }
		catch (GeneralException gex) {
	        logger.error("Error initializing", gex);
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
	}
	
	/**
	 * Main method
	 * @param args	0 - Store Number, 1 - Week 1 End Date
	 */
    public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-movement-12week.properties");
		logCommand (args);

		try {
			//Week 1 weekend date
			Date week1EndDate = null;
			//Date format
			String dateFormat = "MM/dd/yyyy";
			//Store Number to process
			String storeNum = null;
			String zoneNum = null;
			int processLevelTypeId = -1;
			String endDateStr = null;
			//Store ID for the store number
			int storeId=-1;
			
			boolean resultUpdate = false;
			
			//Get the Store number from the command line argument 
			if (args.length > 0) {
				if(args[0].startsWith(ARG_STORE)){
					storeNum = args[0].substring(ARG_STORE.length());
					processLevelTypeId = Constants.STORE_LEVEL_TYPE_ID;
					logger.info("Store Number " + storeNum);
				}else if(args[0].startsWith(ARG_ZONE)){
					zoneNum = args[0].substring(ARG_ZONE.length());;
					processLevelTypeId = Constants.ZONE_LEVEL_TYPE_ID;
					logger.info("Zone Number " + zoneNum);
				}
			}
			else
			{
				logger.error("Invalid input: Store id is missing");
				System.exit(0);
			}
			
			//Get the end date from the command line argument 
			if (args.length > 1) {
				endDateStr = args[1];
				week1EndDate = DateUtil.toDate(endDateStr, dateFormat);
				logger.debug("Target End date " + week1EndDate.toString());
			}
			else {
				logger.warn("Invalid input : Target End date missing");
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(new Date());
        		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        		cal.add(Calendar.DATE, -1);
        		week1EndDate = cal.getTime();
				logger.debug("Target End date " + week1EndDate.toString());		
			}
			
    		//Create object for movement 12 week
    		Movement12Week objMovement = new Movement12Week();
    		
    		//Get the competitor store Id
    		if(processLevelTypeId == Constants.STORE_LEVEL_TYPE_ID)
    			storeId = objMovement.getCompStore(storeNum);
    		else{
    			storeId = objMovement.getPriceZone(zoneNum);
    		}
    		
			if (storeId != -1) {    		

				logger.debug("Store ID is " + String.valueOf(storeId));
				
				//Call function to calculate the movement dates 
				objMovement.setWeeklyMovementDates(week1EndDate);

	    		//weekly. Get and assign schedule ID for different movement dates
	    		boolean processStatus = objMovement.setMovementtWeekScheudleId(storeId, processLevelTypeId);
				
				if (processStatus) {
					
					//Call function to process movement 12 week item level
					resultUpdate = objMovement.ProcessMovement12Week(storeId);
					
		    		if (resultUpdate)
					{
						logger.info("Movement 12 Week updation for Item completed");
					}
					else {
						logger.info("Movement 12 Week updation for Item not completed");
					}
				}
			}

			PristineDBUtil.commitTransaction(objMovement._Conn, "Movement 12 Week data commi for store " + storeNum);
			
    		// Close the connection
    		PristineDBUtil.close(objMovement._Conn);
			
		} catch (GeneralException ex) {
			logger.error(ex);
		}		
	}

 	 /**
 	  * Method to process 12 week movement for a given store
 	  * @param storeId	Store Id
 	  * @return	boolean	Processing success/failure
 	  */
    private boolean ProcessMovement12Week(int storeId)
	{
    	//Variable declaration
    	Movement13WeeklyDTO movementDtoW2 = new Movement13WeeklyDTO();
    	Movement13WeeklyDTO movementDtoW13 = new Movement13WeeklyDTO();
		MovementDAO movementDao = new MovementDAO();

		HashMap<Integer, Movement13WeeklyDTO> hashMapWeek1 = new HashMap<Integer, Movement13WeeklyDTO>();
		HashMap<Integer, Movement13WeeklyDTO> hashMapWeek2 = new HashMap<Integer, Movement13WeeklyDTO>();
		HashMap<Integer, Movement13WeeklyDTO> hashMapWeek13 = new HashMap<Integer, Movement13WeeklyDTO>();    		
		
    	//Get the data initialize status 
		boolean initStatus=false;
		int itemCode;
		
      	try	{

    		//Update initial data for 12 week movement
    		logger.debug("Initializing 12 week data ");   
    		long initialUpdateStartTime = System.currentTimeMillis();
    		initStatus = movementDao.updateWeeklyMovement12WeekInitialData(_Conn , _week1ScheduleId, TARGET_TABLE_ITEM, _departmentCode, _categoryId);
    		long initialUpdateEndTime = System.currentTimeMillis();
    		logger.debug("Time taken for initial update - " + (initialUpdateEndTime - initialUpdateStartTime) + " ms");
    		
    		//Proceed only if initialize is happened properly
    		if (initStatus) {
				// Fetching of movement data for week 1
				logger.debug("Getting Week 1 movement data");
				long week1MovStartTime = System.currentTimeMillis();
				hashMapWeek1 = movementDao.get12WeekMovementDataMap(_Conn,
						_week1ScheduleId, _departmentCode, _categoryId);
				long week1MovEndTime = System.currentTimeMillis();
				logger.debug("Time taken to retrieve week 1 movement - " + (week1MovEndTime - week1MovStartTime) + " ms");
				
				_itemTotal = hashMapWeek1.size();
				logger.debug("Week 1 Movement data count "
						+ String.valueOf(_itemTotal));
				//Check for the data existence for week 1. Proceed only if data exist 
				if (hashMapWeek1.size() > 0) {

					// Fetching of movement data for week 2			
					logger.debug("Fetching Data for Week 2");
					long week2MovStartTime = System.currentTimeMillis();
					hashMapWeek2 = movementDao.get12WeekMovementDataMap(_Conn,
							_week2ScheduleId, _departmentCode, _categoryId);
					long week2MovEndTime = System.currentTimeMillis();
					logger.debug("Time taken to retrieve week 2 movement - " + (week2MovEndTime - week2MovStartTime) + " ms");
					
					logger.debug("Fetching Data for Week 12");
					long week13MovStartTime = System.currentTimeMillis();
					hashMapWeek13 = movementDao.get12WeekMovementDataMap(_Conn,
							_week13ScheduleId, _departmentCode, _categoryId);
					long week13MovEndTime = System.currentTimeMillis();
					logger.debug("Time taken to retrieve week 12 movement - " + (week13MovEndTime - week13MovStartTime) + " ms");
				}
			}
    		
			//Calculate the 12 week movement 
    		
    		logger.debug("Processing 12 week movement for Item.....");
    		_itemList = new ArrayList<Integer>();
    		_update12WeekMovementList = new ArrayList<Movement13WeeklyDTO>();
    		
    		//Loop to process 12 week movement for each item in Week1 data
    		int proRecSize = 0;
    		for (Movement13WeeklyDTO movementDtoW1: hashMapWeek1.values()) {
    			
    			itemCode = movementDtoW1.getItemCode();
    			if (hashMapWeek2.containsKey(itemCode)) {
					movementDtoW2 = hashMapWeek2.get(itemCode);
				}else{
					movementDtoW2 = null;
				}
    			
    			if (hashMapWeek13.containsKey(itemCode)) {
					movementDtoW13 = hashMapWeek13.get(itemCode);
				}else{
					movementDtoW13 = null;
				}
    			
    			CalculateMovement12WeekData(movementDtoW1, movementDtoW2, movementDtoW13, storeId);
    			
    			//Write info log after completion of every VALUE_PROCESS_LOG records
    	    	proRecSize = proRecSize+1;
    	    	if ( (proRecSize % VALUE_PROCESS_LOG) == 0 ) {
    	    		logger.info( "Items processed....." + String.valueOf(proRecSize));
    	    	}
    	    			
    	    }
    	    			
    		long get12WeekMovStartTime = System.currentTimeMillis();
    		logger.info("Item List Size" + _itemList.size());
    		if(_itemList.size() > 0){
    			Get12WeekMovementForItems();
    		}
    		long get12WeekMovEndTime = System.currentTimeMillis();
    		logger.debug("Time taken to calculate 12 week movements - " + (get12WeekMovEndTime - get12WeekMovStartTime) + " ms");
    		
    		logger.info("Update Item List Size - " + _update12WeekMovementList.size());
    		long update12WeekMovStartTime = System.currentTimeMillis();
    		if(_update12WeekMovementList.size() > 0){
    			List<Movement13WeeklyDTO> tempList = new ArrayList<Movement13WeeklyDTO>();
				int limitcount = 0;
				for(Movement13WeeklyDTO movDto:_update12WeekMovementList){
					Movement13WeeklyDTO tempDto = hashMapWeek1.get(movDto.getItemCode());
					movDto.setCheckDataId(tempDto.getCheckDataId());
					tempList.add(movDto);
					limitcount++;
					if( limitcount > 0 && (limitcount%Constants.BATCH_UPDATE_COUNT == 0)){
						int count[] = movementDao.updateMovement12WeekData(_Conn, tempList, TARGET_TABLE_ITEM);
						for(Integer cnt:count){
							if(cnt != Statement.EXECUTE_FAILED){
								_itemUpdated++;
							}else{
								_itemRejected++;
							}
						}
						tempList.clear();
					}
				}
				if(tempList.size() > 0){
					int count[] = movementDao.updateMovement12WeekData(_Conn, tempList, TARGET_TABLE_ITEM);
					for(Integer cnt:count){
						if(cnt != Statement.EXECUTE_FAILED){
							_itemUpdated++;
						}else{
							_itemRejected++;
						}
					}
				}
    		}
    		long update12WeekMovEndTime = System.currentTimeMillis();
    		logger.debug("Time taken to update 12 week movements - " + (update12WeekMovEndTime - update12WeekMovStartTime) + " ms");

    		logger.debug("Processing of 12 week movement calculation is done");
    		logger.info ("Total Items processed.................... " + _itemTotal);
    		logger.info ("W12 movement calculated items............ " + _item12WeekCalculated);
    		logger.info ("Total updated Items...................... " + _itemUpdated);
    		logger.info ("Item Rejected from W12 calculation....... " + _itemRejected);
        	
    	}
    	catch (GeneralException gex) {
    		gex.printStackTrace();
    		logger.error(gex.getMessage());    		
    	}
    	catch (Exception ex) {
    		ex.printStackTrace();
    		logger.error(ex.getMessage());
    	}
    	finally{
        	movementDtoW2 = null;
        	movementDtoW13 = null;
        	hashMapWeek1 = null;
        	hashMapWeek2 = null;
        	hashMapWeek13 = null;
        	_itemList = null;
    		_update12WeekMovementList = null;
    		_itemTotal = 0;
    		_item12WeekCalculated = 0;
    		_itemUpdated = 0;
    		_itemRejected = 0;
    	}
    	return true;
	}
        
    /**
     * Method to calculate 13 week movement data
     * @param week1		Week 1 DTO
     * @param week2		Week 2 DTO
     * @param week13	Week 13 DTO
     * @param storeId	Store Id
     */
    private void CalculateMovement12WeekData(Movement13WeeklyDTO week1, Movement13WeeklyDTO week2, Movement13WeeklyDTO week13, int storeId) {
    	double week12QuantityRegular  =0;
    	double week12QuantitySale = 0; 
    	int checkDataID = -1;
    	int itemCode = -1;
    	boolean updateStatus = false;
    	
    	checkDataID = week1.getCheckDataId();
    	itemCode = week1.getItemCode();
    	
    	//Case 1 - If 12 week movement data for week 2 are null
	    if (week2 == null || ((week2.getQuantitySale12Week() == VALUE_NULL_DEFAULT ) || (week2.getQuantityRegular12Week() == VALUE_NULL_DEFAULT))) {
	    		
	    	_itemList.add(week1.getItemCode());
	   	} 
 	   	//Case 2 - If 12 week movement data for week 2 is 0 and current week movement data is 0
    	//No need to update 12 week movement data, keep the 12 week moment data as 0
	    else if ((week2.getQuantitySale12Week() == 0 ) && (week2.getQuantityRegular12Week() == 0) 
	    			&& (week1.getQuantityRegular() == 0) && (week1.getQuantitySale()==0)) {
	    	//Increment the update not required count
			_itemUpdateNotRequired = _itemUpdateNotRequired + 1;
			
			updateStatus = false;
	    
	    }
    	//Case 3 - If 12 week movement data for week 2 is 0 and week 1 movement data is not null
	    else if ((week2.getQuantitySale12Week() == 0 ) && (week2.getQuantityRegular12Week() == 0) 
	    		&& ((week1.getQuantityRegular() > 0) || (week1.getQuantitySale() > 0))) {
	    		
	    	week12QuantityRegular = week1.getQuantityRegular()/CALC_WEEK_13;
	       	week12QuantitySale = week1.getQuantitySale()/CALC_WEEK_13;
	        	
	    	//Update required
	       	updateStatus = true;
	    	
	    }
    	//Case 4 - If 12 week movement data for week 2 is greater than 0
	    else if ((week2.getQuantitySale12Week() > 0 ) || (week2.getQuantityRegular12Week() > 0)) {	
	    	if(week13 != null){
		    	week12QuantityRegular = ((week2.getQuantityRegular12Week() * CALC_WEEK_13) + week1.getQuantityRegular() - week13.getQuantityRegular())/CALC_WEEK_13;
		       	week12QuantitySale = ((week2.getQuantitySale12Week() * CALC_WEEK_13) + week1.getQuantitySale() - week13.getQuantitySale())/CALC_WEEK_13;
	        }else{
	        	week12QuantityRegular = ((week2.getQuantityRegular12Week() * CALC_WEEK_13) + week1.getQuantityRegular())/CALC_WEEK_13;
		       	week12QuantitySale = ((week2.getQuantitySale12Week() * CALC_WEEK_13) + week1.getQuantitySale())/CALC_WEEK_13;
	        }
    		updateStatus = true;
	    	
	    }

	    if (updateStatus) {	    		
	    		//Assign values to DTO
	    		Movement13WeeklyDTO movement12Dto = new Movement13WeeklyDTO();
	    		movement12Dto.setItemCode(itemCode);
	        	movement12Dto.setCheckDataId(checkDataID);
	    		movement12Dto.setQuantityRegular12Week(week12QuantityRegular);
	    		movement12Dto.setQuantitySale12Week(week12QuantitySale);
	    		
	    		_update12WeekMovementList.add(movement12Dto);		
	    } 
    }
    
    
    /**
     * Method to query 12 week movement taking into account all 12 schedules
     * @throws GeneralException
     */
    private void Get12WeekMovementForItems() throws GeneralException{
    	MovementDAO movementDao = new MovementDAO();
    	
    	HashMap<Integer, Movement13WeeklyDTO> week12Movements = movementDao.getWeeklyMovementData(_Conn , _week12ScheduleId, _itemList);
    	
    	double week12QuantityRegular = 0;
    	double week12QuantitySale = 0;
    	
    	for(Movement13WeeklyDTO mvDTO:week12Movements.values()){
    		_item12WeekCalculated = _item12WeekCalculated + 1;
			
			if ((mvDTO.getQuantityRegular() > 0) || (mvDTO.getQuantitySale() > 0)) {
    			week12QuantityRegular = mvDTO.getQuantityRegular()/CALC_WEEK_13;
    			week12QuantitySale = mvDTO.getQuantitySale()/CALC_WEEK_13;    			
    		}else if((mvDTO.getQuantityRegular() == 0) && (mvDTO.getQuantitySale() == 0)){
    			week12QuantityRegular = 0;
    			week12QuantitySale = 0;
    		}else{
    			week12QuantityRegular = VALUE_NULL_DEFAULT;
    			week12QuantitySale = VALUE_NULL_DEFAULT;
    		}
    		mvDTO.setQuantityRegular12Week(week12QuantityRegular);
    		mvDTO.setQuantitySale12Week(week12QuantitySale);
    		_update12WeekMovementList.add(mvDTO);
    	}
	}

    /**
     * Method to calculate different movement dates
     * @param week1EndDate	Input Weekend Date
     */
    private void setWeeklyMovementDates(Date week1EndDate) {
    	
		//Calculate the Week 1 start date
		try {
		_week1StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, 0));
		_week1EndDate = week1EndDate;				
		
		//Calculate Week 2 start and end dates
		_week2StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, CALC_WEEK_2));
		_week2EndDate = DateUtil.toDate(DateUtil.getWeekEndDate(_week2StartDate));
		
		//Calculate Week 12 start and end dates
		_week12StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, CALC_WEEK_12));
		_week12EndDate = _week1EndDate;
		
		//Calculate Week 13 start and end dates
		_week13StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, CALC_WEEK_13));
		_week13EndDate = DateUtil.toDate(DateUtil.getWeekEndDate(_week13StartDate));    			
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
    }
    
    
    /**
     * Method to set Schedule Id for different movement dates
     * @param storeId	Store Id
     * @return boolean	Schedules exist or not
     */
    private boolean setMovementtWeekScheudleId(int storeId, int processLevelTypeId){
    	
    	boolean schduleIdExist = true;

    	_week1ScheduleId = getMovementtWeekScheudleId(_week1StartDate, _week1EndDate, storeId, CALL_SCHEDULE_SINGLE, processLevelTypeId);
    	if ((null == _week1ScheduleId) || (_week1ScheduleId == "")) {
    		schduleIdExist = false;
    	}
    		
    	_week2ScheduleId = getMovementtWeekScheudleId(_week2StartDate, _week2EndDate, storeId, CALL_SCHEDULE_SINGLE, processLevelTypeId);

    	_week12ScheduleId = getMovementtWeekScheudleId(_week12StartDate, _week12EndDate, storeId, CALL_SCHEDULE_MULTI, processLevelTypeId);
    	if ((null == _week12ScheduleId) || (_week12ScheduleId == "")) {
    		schduleIdExist = false;
    	}
    	
    	_week13ScheduleId = getMovementtWeekScheudleId(_week13StartDate, _week13EndDate, storeId, CALL_SCHEDULE_SINGLE, processLevelTypeId);
    	
    	return schduleIdExist;
    }

    
    /**
     * Method to fetch Schedule IDs for the given duration
     * @param weekStartDate		Week Start Date
     * @param weekEndDate		Week End Date
     * @param storeId			Store Id
     * @param callType			Single/Multiple
     * @return	Schedule Id
     */
    private String getMovementtWeekScheudleId(Date weekStartDate, Date weekEndDate, int storeId, int callType, int processLevelTypeId)
    {
     	ScheduleDAO objScheduleDao = new ScheduleDAO();
     	SimpleDateFormat dft = new SimpleDateFormat("MM/dd/yyyy");
     	String startDate = dft.format(weekStartDate);
     	String endDate = dft.format(weekEndDate);
     	String schCSV = null;     	
     	
     	try {
				//logger.debug("Fetch Schedule id for duration between " + startDate + " and " + endDate);
     			ArrayList <ScheduleInfoDTO> schIdList = null;
     			
     			if(processLevelTypeId == Constants.ZONE_LEVEL_TYPE_ID)
     				schIdList = objScheduleDao.getSchedulesForZone(_Conn, storeId, -1, startDate , endDate);
     			else
     				schIdList = objScheduleDao.getSchedulesForStore(_Conn, storeId, -1, startDate , endDate);
	
	     		if (schIdList.size() > 0)
	     		{
		     		if (callType == 1)
		     		{
			     		//Get the Schedule id - Array should return always one records
			     		schCSV = String.valueOf(schIdList.get(0).getScheduleId());
		     		}
		     		else
		     		{
			     		//Get the Schedule IDs
			     		schCSV = schIdList.size() > 0 ? ScheduleDAO.getScheduleIdCSV (schIdList) :  null;
		     		}
	     		
		        	logger.info("Schedule ID for duration " + startDate + " and " + endDate + " ....." + schCSV);
	     		
	     		}
	     		
	     		schIdList = null;
	     		
     	} catch (GeneralException e) {
 			logger.error(e.getMessage());
 		}

     	return schCSV;
    }
    
    
    /**
     * Function to get competitor Store IDs
     * @param StoreNum	Store Number
     * @return int	Store ID
     */
    private int getCompStore(String StoreNum)
    {
    	int storeID = -1;
		try {
			_CompDataDAO.setSubscriber(_Conn, true);
			//Call the DAO to get the Store ID 
			ArrayList<String> storeIdList = _CompDataDAO.getStoreIdList(_Conn, StoreNum);
			
			//Assign the Store ID into private variable
			storeID = Integer.parseInt(storeIdList.get(0));
		} catch (SQLException e) {
			logger.error(e.getMessage());
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}    		
    return storeID;
    }
    
    private int getPriceZone(String zoneNum) throws GeneralException
    {
    	int zoneId = -1;
    	RetailPriceZoneDAO retailPriceZoneDAO = new RetailPriceZoneDAO();
		PriceZoneDTO priceZoneDTO = new PriceZoneDTO();
		priceZoneDTO = retailPriceZoneDAO.getRetailPriceZone(_Conn, zoneNum);
		zoneId = priceZoneDTO.getPriceZoneId();
	 
    	return zoneId;
    }

    /**
     * Method to log command line arguments
     * @param args	Arguments
     */
    private static void logCommand (String[] args)
    {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Movement12Week ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }
     	
}