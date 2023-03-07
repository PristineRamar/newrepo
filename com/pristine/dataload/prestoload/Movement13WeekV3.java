/*
 * Title: 13 Week Movement Calculation
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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.sql.SQLException;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.Movement13WeeklyDTO;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.DateUtil;
import com.pristine.business.util.*;

public class Movement13WeekV3
{
	
	static Logger 		logger = Logger.getLogger("Movement13Week");
	private Connection 	_Conn = null;

	//variables to hold the start and end dates
	private Date _week1StartDate = null;
	private Date _week1EndDate = null;	    	
	private Date _week2StartDate = null;
	private Date _week2EndDate = null;
	private Date _week3StartDate = null;
	private Date _week3EndDate = null;
	private Date _week13StartDate = null;
	private Date _week13EndDate = null;
	
	//variables for schedule ID for different movement weeks
	private String _week1ScheduleId; 
	private String _week2ScheduleId;
	private String _week3ScheduleId;
	private String _week13ScheduleId;

	//Temporary variable for testing to hold department id
	private String _departmentCode;
	private String _categoryId;
	
	//Constant to hold the week numbers
	final private int CALC_WEEK_2 = 1;
	final private int CALC_WEEK_3 = 2;
	private int CALC_WEEK_12 = 12;
	private int CALC_WEEK_13 = 13;
	
	final private int VALUE_NULL_DEFAULT = -99;
	final private int VALUE_PROCESS_LOG = 1000;
	final private int CALL_SCHEDULE_SINGLE = 1;
	final private int CALL_SCHEDULE_MULTI = 2;
	
	final private String TARGET_TABLE_ITEM = "MOVEMENT_WEEKLY";
	final private String TARGET_TABLE_LIG = "MOVEMENT_WEEKLY_LIG";
	
	private CompetitiveDataDAO 	_CompDataDAO;
	
	//variables to store item list
	private List<Integer> _itemList = null;
	private List<Movement13WeeklyDTO> _update13WeekMovementList = null;
	private List<Movement13WeeklyDTO> _update13WeekLIGMovementList = null;
	
	private static String ARG_STORE = "STORE=";
	private static String ARG_ZONE = "ZONE=";
	private static String ARG_LOCATION = "LOCATION_ID=";
	private static String ARG_LOCATION_LEVEL = "LOCATION_LEVEL_ID=";
	
	
	/**
	 * Constructor
	 */
	public Movement13WeekV3 ()
	{
		try
		{
	        PropertyManager.initialize("analysis.properties");
	        _Conn = DBManager.getConnection();
	        //_Conn.setAutoCommit(true);

	        _departmentCode = PropertyManager.getProperty("MOVEMENT_EXCLUDE_DEPARTMENT");
	        _categoryId = PropertyManager.getProperty("MOVEMENT_EXCLUDE_CATEGORY");
	        
	        _CompDataDAO = new CompetitiveDataDAO(_Conn);
	        String checkUser = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_USER_ID", Constants.EXTERNAL_PRICECHECK_USER);
	        String checkList = PropertyManager.getProperty("SUBSCRIBER_DATALOAD.CHECK_LIST_ID", Constants.EXTERNAL_PRICECHECK_LIST);
	        _CompDataDAO.setParameters(checkUser, checkList);
	        
	        String movementWeeksStr = PropertyManager.getProperty("MOVEMENT_WEEKS");
	        if(movementWeeksStr != null){
	        	int movementWeeks = Integer.parseInt(movementWeeksStr);
	        	CALC_WEEK_12 = movementWeeks - 1;
	        	CALC_WEEK_13 = movementWeeks;
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
	 * @throws SQLException 
	 */
    public static void main(String[] args) throws SQLException
	{
		PropertyConfigurator.configure("log4j-movement-13week.properties");
		logCommand (args);

		try {
			//Week 1 weekend date
			Date week1EndDate = null;
			//Date format
			String dateFormat = "MM/dd/yyyy";
			DateFormat df = new SimpleDateFormat("MM/dd/yyyy"); 
			//Store Number to process
			String storeNum = null;
			String zoneNum = null;
			String locationIdStr = null;
			int locationLevelId = -1;
			String endDateStr = null;
			//Store ID for the store number
			int locationId=-1;
			RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
			boolean resultUpdate = false;
			Movement13WeekV3 objMovement = new Movement13WeekV3();
			//Get the Store number from the command line argument 
			if (args.length > 0) {
				if (args[0].startsWith(ARG_STORE)) {
					storeNum = args[0].substring(ARG_STORE.length());
					locationLevelId = Constants.STORE_LEVEL_ID;
					logger.info("Store Number " + storeNum);
				} else if (args[0].startsWith(ARG_ZONE)) {
					zoneNum = args[0].substring(ARG_ZONE.length());
					locationLevelId = Constants.ZONE_LEVEL_ID;
					logger.info("Zone Number " + zoneNum);
				} else if (args[0].startsWith(ARG_LOCATION_LEVEL)) {
					locationLevelId = Integer.parseInt(args[0].substring(ARG_LOCATION_LEVEL.length()));
					if (args[1].startsWith(ARG_LOCATION)) {
						locationIdStr = args[1].substring(ARG_LOCATION.length());
						logger.info("Location level Id - " + locationLevelId + ", Location Id - " + locationIdStr);
					}
				}
			}
			else
			{
				logger.error("Invalid input: Store id is missing");
				System.exit(0);
			}
			
			//Get the end date from the command line argument 
			if (args.length > 1 && !args[0].startsWith(ARG_LOCATION_LEVEL)) {
				endDateStr = args[1];
				week1EndDate = DateUtil.toDate(endDateStr, dateFormat);
				logger.info("Target End date " + week1EndDate.toString());
			}else if(args[0].startsWith(ARG_LOCATION_LEVEL) && args.length > 2){
				endDateStr = args[2];
				week1EndDate = DateUtil.toDate(endDateStr, dateFormat);
				logger.info("Target End date " + week1EndDate.toString());
			}
			else {
				logger.warn("Invalid input : Target End date missing");
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(new Date());
        		//cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        		cal.add(Calendar.DATE, -7);
        		week1EndDate = cal.getTime();
        		String week1EndDateStr = df.format(week1EndDate);
        		try {
					RetailCalendarDTO calendarDTO = retailCalendarDAO.
							getCalendarId(objMovement._Conn, week1EndDateStr, Constants.CALENDAR_WEEK);
					week1EndDate = df.parse(calendarDTO.getEndDate());
				} catch (GeneralException | Exception e) {
					logger.error("Error", e);
				}
				logger.info("Target End date " + week1EndDate.toString());		
			}
			
    		//Create object for movement 13 week
    		
    		
    		//Get the competitor store Id
    		if(locationLevelId == Constants.STORE_LEVEL_ID)
    			locationId = objMovement.getCompStore(storeNum);
    		else if(locationLevelId != Constants.STORE_LEVEL_ID
    				&& locationLevelId != Constants.ZONE_LEVEL_ID){
    			locationId = Integer.parseInt(locationIdStr);
    		}
    		else{
    			locationId = objMovement.getPriceZone(zoneNum);
    		}
    		
			if (locationId != -1) {    		

				logger.info("Store ID is " + String.valueOf(locationId));
				
				//Call function to calculate the movement dates 
				objMovement.setWeeklyMovementDates(week1EndDate);

	    		//weekly. Get and assign schedule ID for different movement dates
	    		boolean processStatus = objMovement.setMovementtWeekScheudleId(locationId, locationLevelId);
				
				if (processStatus) {
					
					//Call function to process movement 13 week item level
					resultUpdate = objMovement.ProcessMovement13Week(locationId);
					
		    		if (resultUpdate)
					{
						logger.info("Movement 13 Week updation for Item completed");
					}
					else {
						logger.info("Movement 13 Week updation for Item not completed");
					}
					
		    		//Call function to process movement 13 week item level LIG
					resultUpdate = objMovement.ProcessMovement13WeekLIG(locationId);
		    		
					if (resultUpdate)
					{
						logger.info("Movement 13 Week updation for LIG completed");
					}
					else {
						logger.info("Movement 13 Week updation for LIG not completed");
					}
				
				}
			}

			PristineDBUtil.commitTransaction(objMovement._Conn, "Movement 13Week computed successfully");
			
    		// Close the connection
    		PristineDBUtil.close(objMovement._Conn);
		
		} catch (GeneralException ex) {
			logger.error(ex);
		}		
	}

    /**
     * Method to process 13 week movement for a given store
     * @param storeId	Store Id
     * @return	boolean	Processing success/failure
     */
    private boolean ProcessMovement13Week(int storeId)
	{
    	//Variable declaration
    	int _itemTotal = 0;
    	int _itemUpdated = 0;
    	
    	Movement13WeeklyDTO movementDtoW2 = new Movement13WeeklyDTO();
    	Movement13WeeklyDTO movementDtoW3 = new Movement13WeeklyDTO();
    	MovementDAO movementDao = new MovementDAO();

    	HashMap<Integer, Movement13WeeklyDTO> hashMapWeek1 = new HashMap<Integer, Movement13WeeklyDTO>();
		HashMap<Integer, Movement13WeeklyDTO> hashMapWeek2 = new HashMap<Integer, Movement13WeeklyDTO>();
		HashMap<Integer, Movement13WeeklyDTO> hashMapWeek3 = new HashMap<Integer, Movement13WeeklyDTO>();
		
    	//Get the data initialize status 
		boolean initStatus=false;
		int itemCode;
		
    	try	{

    		//Update initial data for 13 week movement
    		logger.debug("Initializing 13 week data ");  
    		long initialUpdateStartTime = System.currentTimeMillis();
    		initStatus = movementDao.updateWeeklyMovement13WeekInitialData(_Conn , _week1ScheduleId, TARGET_TABLE_ITEM, _departmentCode, _categoryId);
    		long initialUpdateEndTime = System.currentTimeMillis();
    		logger.debug("Time taken for initial update - " + (initialUpdateEndTime - initialUpdateStartTime) + " ms");
    		
    		//Proceed only if initialize is happened properly
    		if (initStatus) {
				// Fetching of movement data for week 1
				logger.debug("Getting Week 1 movement data");
				hashMapWeek1 = movementDao.get12WeekMovementDataMap(_Conn,
						_week1ScheduleId, _departmentCode, _categoryId);
				_itemTotal = hashMapWeek1.size();
				logger.debug("Week 1 Movement data count "
						+ String.valueOf(_itemTotal));
				//Check for the data existence for week 1. Proceed only if data exist 
				if (hashMapWeek1.size() > 0) {

					// Fetching of movement data for week 2			
					logger.debug("Fetching Data for Week 2");
					hashMapWeek2 = movementDao.get12WeekMovementDataMap(_Conn,
							_week2ScheduleId, _departmentCode, _categoryId);
					
					logger.debug("Fetching Data for Week 3");
					hashMapWeek3 = movementDao.get12WeekMovementDataMap(_Conn,
							_week3ScheduleId, _departmentCode, _categoryId);
				}
			}
    		
			//Calculate the 13 week movement 
    		
    		logger.info("Processing 13 week movement for Item.....");
    		_itemList = new ArrayList<Integer>();
    		_update13WeekMovementList = new ArrayList<Movement13WeeklyDTO>();
    		
    		//Loop to process 13 week movement for each item in Week1 data
    		int proRecSize = 0;
    		for (Movement13WeeklyDTO movementDtoW1: hashMapWeek1.values()) {
    			
    			itemCode = movementDtoW1.getItemCode();
    			
    			if (hashMapWeek2.containsKey(itemCode)) {
					movementDtoW2 = hashMapWeek2.get(itemCode);
				}else{
					movementDtoW2 = null;
				}
    			
    			if (hashMapWeek3.containsKey(itemCode)) {
					movementDtoW3 = hashMapWeek3.get(itemCode);
				}else{
					movementDtoW3 = null;
				}
    			
    			CalculateMovement13WeekData(movementDtoW1, movementDtoW2, movementDtoW3, storeId);
    		      			
    			//Write info log after completion of every VALUE_PROCESS_LOG records
    			proRecSize = proRecSize+1;
    			if ( (proRecSize % VALUE_PROCESS_LOG) == 0 ) {
    				logger.info( "Items processed....." + String.valueOf(proRecSize));
    			}
    		}
    		long get13WeekMovStartTime = System.currentTimeMillis();
    		logger.info("Item List Size" + _itemList.size());
    		if(_itemList.size() > 0){
    			Get13WeekMovementForItems();
    		}
    		long get13WeekMovEndTime = System.currentTimeMillis();
    		logger.debug("Time taken to calculate 13 week movements - " + (get13WeekMovEndTime - get13WeekMovStartTime) + " ms");
    		
    		logger.info("Update Item List Size - " + _update13WeekMovementList.size());
    		long update13WeekMovStartTime = System.currentTimeMillis();
    			if(_update13WeekMovementList.size() > 0){
    				List<Movement13WeeklyDTO> tempList = new ArrayList<Movement13WeeklyDTO>();
    				int limitcount = 0;
    				for(Movement13WeeklyDTO movDto:_update13WeekMovementList){
    					Movement13WeeklyDTO tempDto = hashMapWeek1.get(movDto.getItemCode());
    					movDto.setCheckDataId(tempDto.getCheckDataId());
    					tempList.add(movDto);
    					limitcount++;
    					if( limitcount > 0 && (limitcount%Constants.BATCH_UPDATE_COUNT == 0)){
    						int count[] = movementDao.updateMovement13WeekData(_Conn, tempList, TARGET_TABLE_ITEM);
    						for(Integer cnt:count){
    							if(cnt != Statement.EXECUTE_FAILED){
    								_itemUpdated++;
    							}
    						}
    						tempList.clear();
    					}
    				}
    				if(tempList.size() > 0){
    					int count[] = movementDao.updateMovement13WeekData(_Conn, tempList, TARGET_TABLE_ITEM);
    					for(Integer cnt:count){
    						if(cnt != Statement.EXECUTE_FAILED){
    							_itemUpdated++;
    						}
    					}
    				}
    			}
    			long update13WeekMovEndTime = System.currentTimeMillis();
    			logger.debug("Time taken to update 13 week movements - " + (update13WeekMovEndTime - update13WeekMovStartTime) + " ms");
    		
    		logger.info("Processing of 13 week movement calculation is done");
    		logger.info ("Total Items processed.................... " + _itemTotal);
    		logger.info ("Total updated Items...................... " + _itemUpdated);   		
        	
    	}
    	catch (GeneralException gex) {
    		logger.error(gex.getMessage()); 
    		gex.printStackTrace();
    	}
    	catch (Exception ex) {
    		logger.error(ex.getMessage());
    		ex.printStackTrace();
    	}
    	finally{
        	movementDtoW2 = null;
        	hashMapWeek2 = null;
        	_update13WeekMovementList = null;
        	_itemList = null;
    	}
    	return true;
	}
    
    /**
     * Method to query 13 week movement taking into account all 12 schedules
     * @throws GeneralException
     */
    private void Get13WeekMovementForItems() throws GeneralException{
    	MovementDAO movementDao = new MovementDAO();
    	
    	HashMap<Integer, Movement13WeeklyDTO> week13Movements = movementDao.getWeeklyMovementData(_Conn , _week13ScheduleId, _itemList);
    	
    	double week13QuantityRegular = 0;
    	double week13QuantitySale = 0;
    	
    	for(Movement13WeeklyDTO mvDTO:week13Movements.values()){
			
			if ((mvDTO.getQuantityRegular() > 0) || (mvDTO.getQuantitySale() > 0)) {
    			week13QuantityRegular = mvDTO.getQuantityRegular()/CALC_WEEK_13;
    			week13QuantitySale = mvDTO.getQuantitySale()/CALC_WEEK_13;    			
    		}else if((mvDTO.getQuantityRegular() == 0) && (mvDTO.getQuantitySale() == 0)){
    			week13QuantityRegular = 0;
    			week13QuantitySale = 0;
    		}else{
    			week13QuantityRegular = VALUE_NULL_DEFAULT;
    			week13QuantitySale = VALUE_NULL_DEFAULT;
    		}
    		mvDTO.setQuantityRegular13Week(week13QuantityRegular);
    		mvDTO.setQuantitySale13Week(week13QuantitySale);
    		_update13WeekMovementList.add(mvDTO);
    	}
	}
        
	/**
     * Method to calculate 13 week movement data
     * @param week1		Week 1 DTO
     * @param week2		Week 2 DTO
     * @param week2		Week 3 DTO
     * @param storeId	Store Id
     */
    private void CalculateMovement13WeekData(Movement13WeeklyDTO week1, Movement13WeeklyDTO week2, Movement13WeeklyDTO week3,int storeId) {
    	double week13QuantityRegular  =0;
    	double week13QuantitySale = 0; 
    	int checkDataID = -1;
    	int itemCode = -1;
    	boolean updateStatus = false;
    	
    	checkDataID = week1.getCheckDataId();
    	itemCode = week1.getItemCode();
		
    	//Case 1 - If 12 week movement data for week 2 are null
    	if (week2 == null || ((week2.getQuantitySale12Week() == VALUE_NULL_DEFAULT ) && (week2.getQuantityRegular12Week() == VALUE_NULL_DEFAULT))) {
	    	
	    	// Check if item is present in Week 3
	    	// If 12 week movement data for week 3 are null, calculate 13 week movement
	    	if (week3 == null || ((week3.getQuantitySale12Week() == VALUE_NULL_DEFAULT ) && (week3.getQuantityRegular12Week() == VALUE_NULL_DEFAULT))) {
	    		_itemList.add(week1.getItemCode());	   
	    	}
	    	else if ((week3.getQuantitySale12Week() == 0 ) && (week3.getQuantityRegular12Week() == 0) 
		    		&& (week1.getQuantityRegular() == 0) && (week1.getQuantitySale()==0)) {
				updateStatus = false;
		    }
	    	else if ((week3.getQuantitySale12Week() == 0 ) && (week3.getQuantityRegular12Week() == 0) 
		    		&& ((week1.getQuantityRegular() > 0) || (week1.getQuantitySale() > 0))) {
		    	week13QuantityRegular = week1.getQuantityRegular()/CALC_WEEK_13;
		       	week13QuantitySale = week1.getQuantitySale()/CALC_WEEK_13;
		       	updateStatus = true;
		    }
	    	else if ((week3.getQuantitySale12Week() > 0 ) || (week3.getQuantityRegular12Week() > 0)) {	
		    	week13QuantityRegular = ((week3.getQuantityRegular12Week() * CALC_WEEK_12) + week1.getQuantityRegular())/CALC_WEEK_13;
		       	week13QuantitySale = ((week3.getQuantitySale12Week() * CALC_WEEK_12) + week1.getQuantitySale())/CALC_WEEK_13;
	    		updateStatus = true;
		    }
	    } 
 	    //Case 2 - If 12 week movement data for week 2 is 0 and current week movement data is 0
    	//No need to update 13 week movement data, keep the 13 week moment data as 0
	    else if ((week2.getQuantitySale12Week() == 0 ) && (week2.getQuantityRegular12Week() == 0) 
	    		&& (week1.getQuantityRegular() == 0) && (week1.getQuantitySale()==0)) {
			
			//Update not required	
			updateStatus = false;
	    		
	    }
    	//Case 3 - If 12 week movement data for week 2 is 0 and week 1 movement data is not null
	    else if ((week2.getQuantitySale12Week() == 0 ) && (week2.getQuantityRegular12Week() == 0) 
	    		&& ((week1.getQuantityRegular() > 0) || (week1.getQuantitySale() > 0))) {
	    	
	    	week13QuantityRegular = week1.getQuantityRegular()/CALC_WEEK_13;
	       	week13QuantitySale = week1.getQuantitySale()/CALC_WEEK_13;
	        	
	    	//Update required
	       	updateStatus = true;
	    	
	    }
    	//Case 4 - If 12 week movement data for week 2 is greater than 0
	    else if ((week2.getQuantitySale12Week() > 0 ) || (week2.getQuantityRegular12Week() > 0)) {	
	        	
	    	week13QuantityRegular = ((week2.getQuantityRegular12Week() * CALC_WEEK_12) + week1.getQuantityRegular())/CALC_WEEK_13;
	       	week13QuantitySale = ((week2.getQuantitySale12Week() * CALC_WEEK_12) + week1.getQuantitySale())/CALC_WEEK_13;
	       	
    		updateStatus = true;
	    	
	    }

	    if (updateStatus) {	    		
	   		//Assign values to DTO
	   		Movement13WeeklyDTO movement13Dto = new Movement13WeeklyDTO(); 
	   		movement13Dto.setItemCode(itemCode);
	       	movement13Dto.setCheckDataId(checkDataID);
	   		movement13Dto.setQuantityRegular13Week(week13QuantityRegular);
	   		movement13Dto.setQuantitySale13Week(week13QuantitySale);
	   		
	    		
	   		_update13WeekMovementList.add(movement13Dto);
	    }
    }

    /**
     * Method to process 13 week movement for LIG items for a given store
     * @param storeId	Store Id
     * @return	boolean	Processing success/failure
     * @throws SQLException 
     */
     private boolean ProcessMovement13WeekLIG(int storeId) throws SQLException {
    	//Variable to hold process details for item
    	int _itemTotal = 0;
    	int _itemUpdated = 0;
    	int _itemRejected = 0;
    	 
    	 ArrayList<Movement13WeeklyDTO> listWeek13Source = new ArrayList<Movement13WeeklyDTO>();
    	 Movement13WeeklyDTO movementDtoW13 = new Movement13WeeklyDTO();
    	 MovementDAO movementDao = new MovementDAO();
    	 boolean processStatus=false; 		
    	 _update13WeekLIGMovementList = new ArrayList<Movement13WeeklyDTO>();
    	 
		try {
			//Update initial data for 13 week movement for LIG
			logger.debug("Update initial data for LIG Item");
			processStatus = movementDao.updateWeeklyMovement13WeekInitialData(_Conn , _week1ScheduleId, TARGET_TABLE_LIG, _departmentCode, _categoryId);
			
			if (processStatus) {
				logger.debug("Fetching of Movement 13 weekly data for LIG");
				listWeek13Source = movementDao.getMovement13WeeklyDataForLIG(_Conn, _week1ScheduleId, _departmentCode, _categoryId); 
			}
			
			if (listWeek13Source.size()>0) {
				
				double regularWeek13Sum = 0;
				double saleWeek13Sum = 0;
				int checkDataId = -1;
				int nextLig; 
			
				Movement13WeeklyDTO updateWeek13LIGDTO = null;
				
				//Create object for LIG key look up 
				LIGKeyLookup objLIGUtil = new LIGKeyLookup(_Conn, Integer.parseInt(_week1ScheduleId));
				
    			for (int itemCount = 0; itemCount < listWeek13Source.size(); itemCount++) {    				
    				updateWeek13LIGDTO = new Movement13WeeklyDTO();

    				movementDtoW13 = listWeek13Source.get(itemCount);

    				regularWeek13Sum = regularWeek13Sum + movementDtoW13.getQuantityRegular13Week();
    				saleWeek13Sum = saleWeek13Sum + movementDtoW13.getQuantitySale13Week();

    				//Get the LIG code for next item 
    				if (itemCount == listWeek13Source.size()-1) {
    					nextLig = -1;
    				}
    					else {
    						nextLig = listWeek13Source.get(itemCount+1).getRetLIRId();
    				}

    				//Check where the current and next item's LIR code
    				//If both are different then update LIG moment for current LIR code
    				if ((itemCount >= 0) && (nextLig != movementDtoW13.getRetLIRId())) {
    					
    					//Get the check data id
    					checkDataId = objLIGUtil.getCheckDataId(movementDtoW13.getRetLIRItemCode());

       					_itemTotal = _itemTotal + 1;
       					
    					updateWeek13LIGDTO.setCheckDataId(checkDataId);
    					updateWeek13LIGDTO.setQuantityRegular13Week(regularWeek13Sum);
    					updateWeek13LIGDTO.setQuantitySale13Week(saleWeek13Sum);

    					_update13WeekLIGMovementList.add(updateWeek13LIGDTO);

    					//Reset the variable to initial values 
    					regularWeek13Sum = 0;
    					saleWeek13Sum = 0;
    					checkDataId = 0;
    					nextLig = 0;    					

    				}

    				//Write info log after completion of every VALUE_PROCESS_LOG records
    				int proRecSize = itemCount+1;
    				if ( (proRecSize % VALUE_PROCESS_LOG) == 0 ) {
    					logger.info("Items processed....." + String.valueOf(proRecSize));
    				}  			    				

    			}
			}
			
			long update13WeekMovStartTime = System.currentTimeMillis();
			if(_update13WeekLIGMovementList.size() > 0){
				List<Movement13WeeklyDTO> tempList = new ArrayList<Movement13WeeklyDTO>();
				int limitcount = 0;
				for(Movement13WeeklyDTO movDto:_update13WeekLIGMovementList){
					tempList.add(movDto);
					limitcount++;
					if( limitcount > 0 && (limitcount%Constants.BATCH_UPDATE_COUNT == 0)){
						int count[] = movementDao.updateMovement13WeekData(_Conn, tempList, TARGET_TABLE_LIG);
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
					int count[] = movementDao.updateMovement13WeekData(_Conn, tempList, TARGET_TABLE_LIG);
					for(Integer cnt:count){
						if(cnt != Statement.EXECUTE_FAILED){
							_itemUpdated++;
						}else{
							_itemRejected++;
						}
					}
				}
			}
			long update13WeekMovEndTime = System.currentTimeMillis();
			logger.info("Time taken to update 13 week movements - " + (update13WeekMovEndTime - update13WeekMovStartTime) + " ms");
			
			logger.info ("LIG Processed Items........ " + listWeek13Source.size());
			logger.info ("LIG Processed Items LIG.... " + _itemTotal);
			logger.info ("LIG Sucessful Item......... " + _itemUpdated);	        		
			logger.info ("LIG Rejected Item.......... " + _itemRejected);			
			
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}finally{
			_update13WeekLIGMovementList = null;
		}
    	
    	 return true;
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
		
		//Calculate Week 3 start and end dates
		_week3StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, CALC_WEEK_3));
		_week3EndDate = DateUtil.toDate(DateUtil.getWeekEndDate(_week3StartDate));
		
		//Calculate Week 13 start and end dates
		_week13StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, CALC_WEEK_12));
		_week13EndDate = _week1EndDate;
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
    }
    
    
    /**
     * Method to set Schedule Id for different movement dates
     * @param locationId	Store Id
     * @return boolean	Schedules exist or not
     */
    private boolean setMovementtWeekScheudleId(int locationId, int locationLevelId){
    	
    	boolean schduleIdExist = true;

    	_week1ScheduleId = getMovementtWeekScheudleId(_week1StartDate, _week1EndDate, locationId, CALL_SCHEDULE_SINGLE, locationLevelId);
    	if ((null == _week1ScheduleId) || (_week1ScheduleId == "")) {
    		schduleIdExist = false;
    	}
    		
    	_week2ScheduleId = getMovementtWeekScheudleId(_week2StartDate, _week2EndDate, locationId, CALL_SCHEDULE_SINGLE, locationLevelId);
    	
    	_week3ScheduleId = getMovementtWeekScheudleId(_week3StartDate, _week3EndDate, locationId, CALL_SCHEDULE_SINGLE, locationLevelId);
    	
    	_week13ScheduleId = getMovementtWeekScheudleId(_week13StartDate, _week13EndDate, locationId, CALL_SCHEDULE_MULTI, locationLevelId);
    	if ((null == _week13ScheduleId) || (_week13ScheduleId == "")) {
    		schduleIdExist = false;
    	}

     	return schduleIdExist;
    }

    
    /**
     * Method to fetch Schedule IDs for the given duration
     * @param weekStartDate		Week Start Date
     * @param weekEndDate		Week End Date
     * @param locationId			Store Id
     * @param callType			Single/Multiple
     * @return	Schedule Id
     */
    private String getMovementtWeekScheudleId(Date weekStartDate, Date weekEndDate, int locationId, int callType, int locationLevelId)
    {
     	ScheduleDAO objScheduleDao = new ScheduleDAO();
     	SimpleDateFormat dft = new SimpleDateFormat("MM/dd/yyyy");
     	String startDate = dft.format(weekStartDate);
     	String endDate = dft.format(weekEndDate);
     	String schCSV = null;     	
     	
     	try {
				ArrayList <ScheduleInfoDTO> schIdList = null;
				
				if(locationLevelId == Constants.ZONE_LEVEL_ID)
					schIdList = objScheduleDao.getSchedulesForZone(_Conn, locationId, -1, startDate , endDate);
				else if(locationLevelId != Constants.STORE_LEVEL_ID
	    				&& locationLevelId != Constants.ZONE_LEVEL_ID){
					schIdList = objScheduleDao.getSchedulesForChainV2(_Conn, locationLevelId, locationId, -1, startDate , endDate);
				}
				else
					schIdList = objScheduleDao.getSchedulesForStore(_Conn, locationId, -1, startDate , endDate);
	
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
			//Call the DAO to get the Store ID
			_CompDataDAO.setSubscriber(_Conn, true);
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
		StringBuffer sb = new StringBuffer("Command: Movement13WeekV3 ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }
     	
}