/*
 * Title: TOPS Movement 13 week
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	10/10/2011	John Britto		New Creation
 * Version 0.2	10/17/2011  John Britto		Review comments fixed
 * Version 0.3	10/19/2011  John Britto		Source for LIG added
 * Version 0.4	02/09/2012  John Britto		W13 calculation calling issue fixed
  *******************************************************************************
 */
package com.pristine.dataload;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import java.sql.Connection;
import java.util.*;
import java.text.SimpleDateFormat;
import java.sql.SQLException;
import com.pristine.dao.DBManager;
import com.pristine.dao.MovementDAO;
import com.pristine.dao.ScheduleDAO;
import com.pristine.dto.Movement13WeeklyDTO;
import com.pristine.dto.ScheduleInfoDTO;
import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.DateUtil;
import com.pristine.business.util.*;

public class Movement13WeekV2
{
	
	static Logger 		logger = Logger.getLogger("Movement13Week");
	private Connection 	_Conn = null;

	//variables to hold the start and dates
	private Date _week1StartDate = null;
	private Date _week1EndDate = null;	    	
	private Date _week2StartDate = null;
	private Date _week2EndDate = null;
	private Date _week13StartDate = null;
	private Date _week13EndDate = null;
	private Date _week14StartDate = null;
	private Date _week14EndDate = null;
	
	//variables for schedule ID for different movement weeks
	private String _week1ScheduleId; 
	private String _week2ScheduleId;
	private String _week14ScheduleId;
	private String _week13ScheduleId;
	
	//Variable to hold process details for item
	private int _itemTotal = 0;
	private int _itemUpdated = 0;
	private int _itemUpdatedInit = 0;
	private int _itemTermindated = 0;	
	private int _itemRejected = 0;
	private int _itemUpdateNotRequired = 0;	
	private int _item13WeekCalculated = 0;

	//Temporary variable for testing to hold department id
	private String _departmentCode;
	private String _categoryId;
	
	//Constant to hold the week numbers
	final private int CALC_WEEK_2 = 1;
	final private int CALC_WEEK_13 = 12;
	final private int CALC_WEEK_14 = 13;
	final private int VALUE_NULL_DEFAULT = -99;
	final private int VALUE_PROCESS_LOG = 1000;
	final private int CALL_SCHEDULE_SINGLE = 1;
	final private int CALL_SCHEDULE_MULTI = 2;
	
	final private String TARGET_TABLE_ITEM = "MOVEMENT_WEEKLY";
	final private String TARGET_TABLE_LIG = "MOVEMENT_WEEKLY_LIG";
	
	private CompetitiveDataDAO 	_CompDataDAO;
	
	
	/*
	 *****************************************************************
	 * Class constructor
	 * Argument 1: Target end date
	 * @throws Exception
	 * ****************************************************************
	 */	
	public Movement13WeekV2 ()
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
	    }
		catch (GeneralException gex) {
	        logger.error("Error initializing", gex);
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
	}
	
	/*
	 *****************************************************************
	 * Main method of Batch
	 * Argument 1: Store Number
	 * Argument 2: Week 1 End Date
	 * ****************************************************************
	 */	
	
    public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-movement-13week.properties");
		logCommand (args);

		try {
			//Week 1 weekend date
			Date week1EndDate = null;
			//Date format
			String dateFormat = "MM/dd/yyyy";
			//Store Number to process
			String storeNum = null;
			String endDateStr = null;
			//Store ID for the store number
			int storeId=-1;
			
			boolean resultUpdate = false;
			
			//Get the Store number from the command line argument 
			if (args.length > 0) {
				storeNum = args[0];
				logger.info("Store Number " + storeNum);
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
				logger.info("Target End date " + week1EndDate.toString());
			}
			else {
				logger.warn("Invalid input : Target End date missing");
        		Calendar cal = Calendar.getInstance();
        		cal.setTime(new Date());
        		cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        		cal.add(Calendar.DATE, -1);
        		week1EndDate = cal.getTime();
				logger.info("Target End date " + week1EndDate.toString());		
			}
			
    		//Create object for movement 13 week
    		Movement13WeekV2 objMovement = new Movement13WeekV2();
    		
    		//Get the competitor store Id
    		storeId = objMovement.getCompStore(storeNum);
    		
			if (storeId != -1) {    		

				logger.info("Store ID is " + String.valueOf(storeId));
				
				//Call function to calculate the movement dates 
				objMovement.setWeeklyMovementDates(week1EndDate);

	    		//weekly. Get and assign schedule ID for different movement dates
	    		//logger.debug("Getting schedule ID for different movement dates");
	    		boolean processStatus = objMovement.setMovementtWeekScheudleId(storeId);
				
				if (processStatus) {
					
					//Call function to process movement 13 week item level
					//logger.debug("Begin movement 13 process for Item");	
		    		resultUpdate = objMovement.ProcessMovement13Week(storeId);
					
		    		if (resultUpdate)
					{
						logger.info("Movement 13 Week updation for Item completed");
					}
					else {
						logger.info("Movement 13 Week updation for Item not completed");
					}
					
		    		//Call function to process movement 13 week item level LIG
					//logger.debug("Begin movement 13 process for LIG");
		    		resultUpdate = objMovement.ProcessMovement13WeekLIG(storeId);
		    		
					if (resultUpdate)
					{
						logger.info("Movement 13 Week updation for LIG completed");
					}
					else {
						logger.info("Movement 13 Week updation for LIG not completed");
					}
				
				}
			}

    		// Close the connection
    		PristineDBUtil.close(objMovement._Conn);
			//logger.debug("DB connection closed");
		
		} catch (GeneralException ex) {
			logger.error(ex);
		}		
	}

    /*
	 *****************************************************************
	 * Main function to process movement 13 week for given store Number 
 	 * Argument 1: Store ID
 	 * Return: boolean
	 ***************************************************************** 	 
 	 */
    private boolean ProcessMovement13Week(int storeId)
	{
    	//Variable declaration
    	ArrayList<Movement13WeeklyDTO> listWeek1 = new ArrayList<Movement13WeeklyDTO>();
    	Movement13WeeklyDTO movementDtoW1 = new Movement13WeeklyDTO();
    	Movement13WeeklyDTO movementDtoW2 = new Movement13WeeklyDTO();
    	Movement13WeeklyDTO movementDtoW14 = new Movement13WeeklyDTO();
		MovementDAO movementDao = new MovementDAO();

		HashMap<Integer, Movement13WeeklyDTO> hashMapWeek2 = new HashMap<Integer, Movement13WeeklyDTO>();
		HashMap<Integer, Movement13WeeklyDTO> hashMapWeek14 = new HashMap<Integer, Movement13WeeklyDTO>();    		
		
    	//Get the data initialize status 
		boolean initStatus=false;
		
    	//Get the W2 data existence status 
		boolean processW2Status = false;
    	
    	//Get the W14 data existence status
		boolean processW14Status = false;

    	boolean updateM13Week;
		
		int itemCode;

    	try	{

    		//Update initial data for 13 week movement
    		logger.info("Initializing 13 week data ");    			
    		initStatus = movementDao.updateWeeklyMovement13WeekInitialData(_Conn , _week1ScheduleId, TARGET_TABLE_ITEM, _departmentCode, _categoryId);

    		//Proceed only if initialize is happened properly
    		if (initStatus) {
				// Fetching of movement data for week 1
				logger.info("Getting Week 1 movement data");
				listWeek1 = movementDao.getWeeklyMovementDataList(_Conn,
						_week1ScheduleId, _departmentCode, _categoryId);
				_itemTotal = listWeek1.size();
				logger.info("Week 1 Movement data count "
						+ String.valueOf(_itemTotal));
				//Check for the data existence for week 1. Proceed only if data exist 
				if (listWeek1.size() > 0) {

					// Fetching of movement data for week 2			
					logger.info("Fetching Data for Week 2");
					hashMapWeek2 = movementDao.getWeeklyMovementDataMap(_Conn,
							_week2ScheduleId, _departmentCode, _categoryId);

					//Check data existence for week2
					if (hashMapWeek2 != null) {
						processW2Status = true;
						logger.info("Week 2 Movement data count "
								+ String.valueOf(hashMapWeek2.size()));
						
						// Fetching of movement data for week 14
						logger.info("Fetching Data for Week 14");
						hashMapWeek14 = movementDao.getWeeklyMovementDataMap(_Conn,
								_week14ScheduleId, _departmentCode, _categoryId);
						
						//Check data existence for week14
						if (hashMapWeek14 != null) {
							processW14Status = true;
							logger.info("Week 14 Movement data count "
									+ String.valueOf(hashMapWeek14.size()));
						
						} else {
							logger.error("NO Movement Weekly data found for Week 14");
							processW14Status = false;
						}
												
					} else {
						logger.error("NO Movement Weekly data found for Week 2");
						processW2Status = false;
					}
				}
			}
    		
			//Calculate the 13 week movement 
    		
    		//Check data existence for week2 and week 14
			//Proceed only if data exist for both weeks 
    		if (processW2Status && processW14Status) {

	    		Movement13WeeklyDTO movement13Dto = new Movement13WeeklyDTO();
	    		
	    		logger.info("Processing 13 week movement for Item.....");
    			//Loop to process 13 week movement for each item in Week1 data
    			for (int itemCount = 0; itemCount < listWeek1.size(); itemCount++) {
    				
    				updateM13Week = false;
    				movementDtoW1 = listWeek1.get(itemCount);
    				itemCode = movementDtoW1.getItemCode();
    				//logger.debug("Processing for item " + String.valueOf(itemCode));
    				
    				//Check for item data existence in week 2
    				if (hashMapWeek2.containsKey(itemCode)) {
    					movementDtoW2 = hashMapWeek2.get(itemCode);
    					updateM13Week = true;
    
    					//Check for item data existence in week 14
        				if (hashMapWeek14.containsKey(itemCode)) {
        					movementDtoW14 = hashMapWeek14.get(itemCode);
        					updateM13Week = true;
        				}
        				else {
        					updateM13Week = false;
        				}
    				}
    				else {
    					updateM13Week = false;
    				}
    				
    				//If data exist in week 2 or week 14, then proceed with movement 13 week calculations
    				//If data not exist in week 2 or week 14, then reset to initial values    				
    				if (updateM13Week) {
    					//logger.debug("Processing of 13 week movement calculation and update");
    					CalculateMovement13WeekData(movementDtoW1, movementDtoW2, movementDtoW14, storeId);
    				}
    				else {
       		    		//logger.debug("TERMINATED ITEM");
    					//Increment the W13 calculation terminated record count
    		    		_itemTermindated = _itemTermindated + 1;
    		    		//Reset to initial value
    					movement13Dto.setCheckDataId(movementDtoW1.getCheckDataId());
    		    		movement13Dto.setQuantityRegular13Week(VALUE_NULL_DEFAULT);
    		    		movement13Dto.setQuantitySale13Week(VALUE_NULL_DEFAULT);
    		    		boolean updateStatus = movementDao.updateMovement13WeekData(_Conn, movement13Dto, TARGET_TABLE_ITEM);
    		    		
    		    		if (updateStatus) {
    		    			_itemUpdatedInit = _itemUpdatedInit + 1;    		    			
    		    		}
    				}
    			
    				//Write info log after completion of every VALUE_PROCESS_LOG records
    				int proRecSize = itemCount+1;
    				if ( (proRecSize % VALUE_PROCESS_LOG) == 0 ) {
    					logger.info( "Items processed....." + String.valueOf(proRecSize));
    				}
    				
    			//logger.debug("--------------------------------------------");
    			}
        		logger.info("Processing of 13 week movement calculation is done");
    			logger.info ("Total Items processed.................... " + _itemTotal);
    			logger.info ("W13 movement calculated items............ " + _item13WeekCalculated);
    			logger.info ("Total updated Items...................... " + _itemUpdated);
    			logger.info ("Items updated with initial data.......... " + _itemUpdatedInit);    			
    			logger.info ("Update not required item count........... " + _itemUpdateNotRequired);	    			
    			logger.info ("Item Terminated from W13 calculation..... " + _itemTermindated);
    			logger.info ("Item Rejected from W13 calculation....... " + _itemRejected);
    		}
        	
    	}
    	catch (GeneralException gex) {
    		logger.error(gex.getMessage());    		
    	}
    	catch (Exception ex) {
    		logger.error(ex.getMessage());
    	}
    	finally{
        	movementDtoW1 = null;
        	movementDtoW2 = null;
        	movementDtoW14 = null;
        	hashMapWeek2 = null;
        	hashMapWeek14 = null;
    	}
    	return (processW2Status & processW14Status);
	}
        
	/*
	 *****************************************************************
 	 * Function to calculate the 13 week movement data 
 	 * Argument 1: Week 1 DTO
 	 * Argument 1: Week 2 DTO
 	 * Argument 1: Week 14 DTO 
 	 * Return: boolean
	 *****************************************************************
	 */
    private boolean CalculateMovement13WeekData(Movement13WeeklyDTO week1, Movement13WeeklyDTO week2, Movement13WeeklyDTO week14, int storeId) {
    	MovementDAO movementDao = new MovementDAO();
    	double week13QuantityRegular  =0;
    	double week13QuantitySale = 0; 
    	int checkDataID = -1;
    	boolean updateStatus = false;
    	
    	try {
    		
    		/*logger.debug("* Processing Item              " + week1.getItemCode());
    		logger.debug("* Week 1 QuantityRegular       " + week1.getQuantityRegular());
    		logger.debug("* Week 1 QuantitySale          " + week1.getQuantitySale());
    		logger.debug("* Week 1 QuantityRegular13Week " + week1.getQuantityRegular13Week());
    		logger.debug("* Week 1 QuantitySale13Week    " + week1.getQuantitySale13Week());
    		logger.debug("* Week 2 QuantityRegular13Week " + week2.getQuantityRegular13Week());
    		logger.debug("* Week 2 QuantitySale13Week    " + week2.getQuantitySale13Week());
    		logger.debug("* Week 14 QuantityRegular      " + week14.getQuantityRegular());
    		logger.debug("* Week 14 QuantitySale         " + week14.getQuantitySale());*/
    		
			checkDataID = week1.getCheckDataId();
			
    		//Case 1 - If 13 week movement data for week 2 are null
	    	if ((week2.getQuantitySale13Week() == VALUE_NULL_DEFAULT ) || (week2.getQuantityRegular13Week() == VALUE_NULL_DEFAULT)) {
	    		//logger.debug("W13 CALCULATION ");
	    		
	    		//Calculate 13 week movement
	    		Movement13WeeklyDTO mvDTO = movementDao.getWeeklyMovements13WeekData(_Conn , _week13ScheduleId, week1.getItemCode());

	        	//Increment 13 week calculation required item count  
	        	_item13WeekCalculated = _item13WeekCalculated + 1;
    			
    			//Check the record count, if it is 13 then proceed 13 movement calculation
    			//If it is not 13, then rest the 13 week moment data as null
	    		if (mvDTO.getWeek13RecordCount() == CALC_WEEK_14) {
	    			
	    			if ((mvDTO.getQuantityRegular13Week() > 0) || (mvDTO.getQuantitySale13Week() > 0)) {
	    				week13QuantityRegular = mvDTO.getQuantityRegular13Week();
	    				week13QuantitySale = mvDTO.getQuantitySale13Week();
	    				
	    				//Update required
		    			updateStatus = true;
	    			
	    			}
	    			else {
    		    		//logger.debug("REJECTED - Count less than 13");
	    				//Increment the rejected count
	    				_itemRejected = _itemRejected + 1;
	    				
		    			week13QuantityRegular = VALUE_NULL_DEFAULT;
		    			week13QuantitySale = VALUE_NULL_DEFAULT;

		    			//Update required
		    			updateStatus = true;
	    			}
	    		}
	    		else
	    		{
	    			week13QuantityRegular = VALUE_NULL_DEFAULT;
	    			week13QuantitySale = VALUE_NULL_DEFAULT;

	    			//Update required
	    			updateStatus = true;
	    		}

	    	} 
 	    	//Case 2 - If 13 week movement data for week 2 is 0 and current week movement data is 0
    		//No need to update 13 week movement data, keep the 13 week moment data as 0
	    	else if ((week2.getQuantitySale13Week() == 0 ) && (week2.getQuantityRegular13Week() == 0) 
	    			&& (week1.getQuantityRegular() == 0) && (week1.getQuantitySale()==0)) {
	    		//logger.debug("UPDATE NOT REQUIRED - All values are 0");
				//Increment the update not required count
				_itemUpdateNotRequired = _itemUpdateNotRequired + 1;
				
				//Update not required	
				updateStatus = false;
	    		
	    	}
    		//Case 3 - If 13 week movement data for week 2 is 0 and week 1 movement data is not null
	    	else if ((week2.getQuantitySale13Week() == 0 ) && (week2.getQuantityRegular13Week() == 0) 
	    			&& ((week1.getQuantityRegular() > 0) || (week1.getQuantitySale() > 0))) {
	    		
	    		week13QuantityRegular = week1.getQuantityRegular();
	        	week13QuantitySale = week1.getQuantitySale();
	        	
	    		//Update required
	        	updateStatus = true;
	    	
	    	}
    		//Case 4 - If 13 week movement data for week 2 is greater than 0
	    	else if ((week2.getQuantitySale13Week() > 0 ) || (week2.getQuantityRegular13Week() > 0)) {	
	        	
	    		week13QuantityRegular = week2.getQuantityRegular13Week() + week1.getQuantityRegular() - week14.getQuantityRegular();
	        	week13QuantitySale = week2.getQuantitySale13Week() + week1.getQuantitySale() - week14.getQuantitySale();
	        	
    			updateStatus = true;
	    	
	    	}

	    	if (updateStatus) {	    		
	        	//logger.debug("Week 13 Week13QuantityRegular..." + week13QuantityRegular);
	        	//logger.debug("Week 13 week13QuantitySale......" + week13QuantitySale);
	        	//logger.debug("checkDataID....................." + checkDataID);	        	

	    		//Assign values to DTO
	    		Movement13WeeklyDTO movement13Dto = new Movement13WeeklyDTO(); 
	        	movement13Dto.setCheckDataId(checkDataID);
	    		movement13Dto.setQuantityRegular13Week(week13QuantityRegular);
	    		movement13Dto.setQuantitySale13Week(week13QuantitySale);
	    		
	    		//logger.debug("Update 13 week movement data");	    		
				updateStatus = movementDao.updateMovement13WeekData(_Conn, movement13Dto, TARGET_TABLE_ITEM);

	        	//Write status log
	        	if (updateStatus) {
	        		if ((week13QuantityRegular == VALUE_NULL_DEFAULT) || (week13QuantitySale == VALUE_NULL_DEFAULT)) {
    		    		//logger.debug("UPDATE INIT");
		        		//Increment the W3 movement data count with initial count
		        		_itemUpdatedInit = _itemUpdatedInit + 1;
		        		
	        		}
	        		else {
    		    		//logger.debug("UPDATE W13 ");
		        		//Increment the W3 movement data count
	        			_itemUpdated = _itemUpdated + 1;
	        		}
	        	} 
	    	}
	    	
		} catch (GeneralException ex) {
			logger.error(ex.getMessage());
		}

    	return updateStatus;
    }

 
    /*
 	 *****************************************************************
 	 * Main function to process movement 13 week for given store Number 
  	 * Argument 1: Store ID
  	 * Return: boolean
 	 ***************************************************************** 	 
  	 */
     private boolean ProcessMovement13WeekLIG(int storeId) {
    	 
    	 ArrayList<Movement13WeeklyDTO> listWeek13Source = new ArrayList<Movement13WeeklyDTO>();
    	 Movement13WeeklyDTO movementDtoW13 = new Movement13WeeklyDTO();
    	 MovementDAO movementDao = new MovementDAO();
    	 boolean processStatus=false; 		
    	 
		try {
			//Update initial data for 13 week movement for LIG
			logger.info("Update initial data for LIG Item");
			processStatus = movementDao.updateWeeklyMovement13WeekInitialData(_Conn , _week1ScheduleId, TARGET_TABLE_LIG, _departmentCode, _categoryId);
			
			if (processStatus) {
				logger.info("Fetching of Movement 13 weekly data for LIG");
				listWeek13Source = movementDao.getMovement13WeeklyDataForLIG(_Conn, _week1ScheduleId, _departmentCode, _categoryId); 
			}
			
			if (listWeek13Source.size()>0) {
				
				_itemUpdated = 0;
				_itemRejected = 0;
				_itemTotal = 0;
				
				double regularWeek13Sum = 0;
				double saleWeek13Sum = 0;
				int checkDataId = -1;
				int nextLig; 
				boolean updateWeek13LIGStatus = false;
				Movement13WeeklyDTO updateWeek13LIGDTO = new Movement13WeeklyDTO();
				
				//Create object for LIG key look up 
				LIGKeyLookup objLIGUtil = new LIGKeyLookup(_Conn, Integer.parseInt(_week1ScheduleId));
				
    			//logger.debug("Processing of LIG calculation begins");
   			
    			for (int itemCount = 0; itemCount < listWeek13Source.size(); itemCount++) {

    				movementDtoW13 = listWeek13Source.get(itemCount);

    				regularWeek13Sum = regularWeek13Sum + movementDtoW13.getQuantityRegular13Week();
    				saleWeek13Sum = saleWeek13Sum + movementDtoW13.getQuantitySale13Week();

    				/*Logic to fix the check_data_id for update - code 
    				commented because of incorrect data
    				
					if (movementDtoW13.getRetLIRItemCode() == movementDtoW13.getItemCode()) {
    					checkDataId = movementDtoW13.getCheckDataId();
    				}*/

    				//logger.debug("RetLIRId              " + movementDtoW13.getRetLIRId() );
    				//logger.debug("ItemCode              " + movementDtoW13.getItemCode() );
    				//logger.debug("RetLIRItemCode        " + movementDtoW13.getRetLIRItemCode() );
    				//logger.debug("QuantityRegular13Week " + movementDtoW13.getQuantityRegular13Week());
    				//logger.debug("QuantitySale13Week    " + movementDtoW13.getQuantitySale13Week() );
    				//logger.debug("CheckDataId original  " + movementDtoW13.getCheckDataId());
    				//logger.debug("CheckDataId to update " + checkDataId);
    				
					//logger.debug("Process Item count " + itemCount);
    				
    				//Get the LIG code for next item 
    				if (itemCount == listWeek13Source.size()-1) {
    					nextLig = -1;
    				}
    					else {
    						nextLig = listWeek13Source.get(itemCount+1).getRetLIRId();
    				}

    				//Check where the current and next item's LIR code
    				//If both are different then update LIG moment for current LIR code
    				if ((itemCount > 0) && (nextLig != movementDtoW13.getRetLIRId())) {
    					
    					//Get the check data id
    					checkDataId = objLIGUtil.getCheckDataId(movementDtoW13.getRetLIRItemCode());

       					_itemTotal = _itemTotal + 1;
       					
    					updateWeek13LIGDTO.setCheckDataId(checkDataId);
    					updateWeek13LIGDTO.setQuantityRegular13Week(regularWeek13Sum);
    					updateWeek13LIGDTO.setQuantitySale13Week(saleWeek13Sum);

    					//Update movement 13 week data into DB
    					//logger.debug("Update the LIG data");
    					
    					//if (checkDataId != 64543076) {
    						updateWeek13LIGStatus = movementDao.updateMovement13WeekData(_Conn, updateWeek13LIGDTO, TARGET_TABLE_LIG);        					
    					//}

    					//logger.debug("Process Group count " + _itemTotal);    					
    					
    					if (updateWeek13LIGStatus){
    						_itemUpdated = _itemUpdated + 1;
    						//logger.debug("Update count " + _itemUpdated);
    					}
    					else {
    						_itemRejected = _itemRejected + 1;
    						//logger.debug("Rejected Count" + _itemRejected);
    					}
    					
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

    				//logger.debug("------------------------------------------");
    			}
			}

			logger.info ("LIG Processed Items........ " + listWeek13Source.size());
			logger.info ("LIG Processed Items LIG.... " + _itemTotal);
			logger.info ("LIG Sucessful Item......... " + _itemUpdated);	        		
			logger.info ("LIG Rejected Item.......... " + _itemRejected);			
			
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
    	
    	 return _itemUpdated > 0;
     }
    
    /*
	 *****************************************************************
 	 * Function to calculate the different movement dates 
 	 * Argument 1: Week 1 End Date
 	 * Return: void
	 *****************************************************************
 	 */
    private void setWeeklyMovementDates(Date week1EndDate) {
    	
		//Calculate the Week 1 start date
		try {
		_week1StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, 0));
		_week1EndDate = week1EndDate;				
		//logger.debug("Week 1 Start Date is " + _week1StartDate + " End Date is " + _week1EndDate);
		
		//Calculate Week 2 start and end dates
		_week2StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, CALC_WEEK_2));
		_week2EndDate = DateUtil.toDate(DateUtil.getWeekEndDate(_week2StartDate));
		//logger.debug("Week 2 Start Date is " + _week2StartDate + " End Date is " + _week2EndDate);

		//Calculate Week 13 start and end dates
		_week13StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, CALC_WEEK_13));
		_week13EndDate = _week1EndDate;
		//logger.debug("Week 13 Start Date is " + _week13StartDate + " End Date is " + _week13EndDate);
		
		//Calculate Week 14 start and end dates
		_week14StartDate = DateUtil.toDate(DateUtil.getWeekStartDate(week1EndDate, CALC_WEEK_14));
		_week14EndDate = DateUtil.toDate(DateUtil.getWeekEndDate(_week14StartDate));    			
		//logger.debug("Week 14 Start Date is " + _week14StartDate + " End Date is " + _week14EndDate);    			
		} catch (GeneralException e) {
			logger.error(e.getMessage());
		}
    }
    
    
    /*
	 *****************************************************************
 	 * Function to set schedule id for different movement dates 
 	 * Argument: Store id
 	 * Return: boolean
	 ***************************************************************** 	 
 	 */
    private boolean setMovementtWeekScheudleId(int storeId){
    	
    	boolean schduleIdExist = true;

    	_week1ScheduleId = getMovementtWeekScheudleId(_week1StartDate, _week1EndDate, storeId, CALL_SCHEDULE_SINGLE);
    	if ((null == _week1ScheduleId) || (_week1ScheduleId == "")) {
    		schduleIdExist = false;
    	}
    		
    	_week2ScheduleId = getMovementtWeekScheudleId(_week2StartDate, _week2EndDate, storeId, CALL_SCHEDULE_SINGLE);
    	if ((null == _week2ScheduleId) || (_week2ScheduleId == "")) {
    		schduleIdExist = false;
    	}

    	_week13ScheduleId = getMovementtWeekScheudleId(_week13StartDate, _week13EndDate, storeId, CALL_SCHEDULE_MULTI);
    	if ((null == _week13ScheduleId) || (_week13ScheduleId == "")) {
    		schduleIdExist = false;
    	}
    	
    	_week14ScheduleId = getMovementtWeekScheudleId(_week14StartDate, _week14EndDate, storeId, CALL_SCHEDULE_SINGLE);
    	if ((null == _week14ScheduleId) || (_week14ScheduleId == "")) {
    		schduleIdExist = false;
    	}
    	
    	return schduleIdExist;
    }

    
    /*
	 *****************************************************************
	 * Function to fetch Schedule IDs for given duration 
 	 * Argument 1: Start Date
 	 * Argument 2: End Date
 	 * Return: String (Schedule ID)
	 *****************************************************************
	 */
    private String getMovementtWeekScheudleId(Date weekStartDate, Date weekEndDate, int storeId, int callType)
    {
     	ScheduleDAO objScheduleDao = new ScheduleDAO();
     	SimpleDateFormat dft = new SimpleDateFormat("MM/dd/yyyy");
     	String startDate = dft.format(weekStartDate);
     	String endDate = dft.format(weekEndDate);
     	String schCSV = null;     	
     	
     	try {
				//logger.debug("Fetch Schedule id for duration between " + startDate + " and " + endDate);
	     		ArrayList <ScheduleInfoDTO> schIdList = objScheduleDao.getSchedulesForStore(_Conn, storeId, -1, startDate , endDate);
	
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
    
    
    /*
	 *****************************************************************
	 * Function to get competitor Store IDs
	 * Argument 1: Store Number
	 * Return : int (Store ID) 
     * @throws GeneralException, SQLException 
	 *****************************************************************
	 */
    private int getCompStore(String StoreNum)
    {
    	int storeID = -1;
		try {
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
    
    /**
	 *****************************************************************
	 * Static method to log the command line arguments
	 *****************************************************************
	 */	
    private static void logCommand (String[] args)
    {
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: Movement13Week ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		
		logger.info(sb.toString());
    }
     	
}