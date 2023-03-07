package com.pristine.oos;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.CostDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.OOSCalcsDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.StoreDAO;
import com.pristine.dataload.RDSDataLoad;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.dto.CostDTO;
import com.pristine.dto.MovementDTO;
import com.pristine.dto.OOSPredictedMovementDTO;
import com.pristine.dto.OOSSummaryDTO;
import com.pristine.dto.OOSTimeFactor;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.NumberUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class OOSDataLoad {
	private static Logger  logger = Logger.getLogger("OOSStats");

	private OOSTimeFactor timeSlots [];
	// Global variables initialized
	Connection conn = null;
	private final int STOPLIMIT = -1;
	private String storeNum = null;
	private int dow = 0;
	private int scheduleId;
	private String analysisDate;
	private int strId;
	private int calendarWeekId;
	private OOSCalcsDAO ossCalcDao = new OOSCalcsDAO ();
	private final String OSS_DATE_TIME_FORMAT = "MM/dd/yyyy hh:mm a";
	private boolean adjustForPartialDay = false;
	private int uptoTS = 0;
	private boolean analyze = false;
	private boolean preProcess = false;
	private CachedRowSet preProcessItemList = null;
	private OOSAnalysis oosAnalyzer = new OOSAnalysis();
	private CompetitiveDataDAO compDao;
	private CostDAO costDao;
	private boolean usePreBuiltExpectation = false;
	private boolean persistPredictedMovement = false;

	
	private HashMap<Integer,HashSet<OOSPredictedMovementDTO>> predictedMovementCache = new HashMap<Integer,HashSet<OOSPredictedMovementDTO>>(); 
	private HashMap<Integer,HashMap<Integer, Double>> predictionExpCache = new HashMap<Integer,HashMap<Integer, Double>>(); // from the Prediction Model 
	private HashMap<Integer,HashMap<Integer, Double>> predictionExpFocusCache = new HashMap<Integer,HashMap<Integer, Double>>(); // from the Prediction Model 
	private HashMap<Integer,HashMap<Integer, Double>> predictionExpHighMoverCache = new HashMap<Integer,HashMap<Integer, Double>>(); // from the Prediction Model 
	private HashMap<Integer,String> weeklyAdCache = new HashMap<Integer,String>(); 
	private HashMap<Integer, String> lastMovementCache = new HashMap<Integer,String>();
	
	HashMap<String, RetailPriceDTO> priceDataMap = new HashMap<String, RetailPriceDTO>(); 
	
	private static boolean useArchive = false;
	
	private HashMap<Integer,Integer> visitList = new HashMap<Integer,Integer> ();

	
	private static final int OOS_CLASSIFICATION_1_WS = 1;
	private static final int OOS_CLASSIFICATION_1_NS = 2;
	private static final int OOS_CLASSIFICATION_2_WS = 3;
	private static final int OOS_CLASSIFICATION_2_NS = 4;
	private static final int OOS_CLASSIFICATION_3_WS = 5;
	private static final int OOS_CLASSIFICATION_3_NS = 6;
	private static final int OOS_CLASSIFICATION_4_WS = 7;
	private static final int OOS_CLASSIFICATION_4_NS = 8;

	private static final String OOS_METHOD_7DAY = "last 7 days";
	private static final String OOS_METHOD_28DAY = "last 28 days";
	private static final String OOS_METHOD_180DAY = "last 120 days";
	private static final String OOS_METHOD_PREVDAY = "prev day";
	private static final String OOS_METHOD_7DAY_AVG = "last 42 days avg";

	final private float OOS_OPP_MOVEMENT_FACTOR = 0.7f;
	
	final private float OOS_DAY_LEVEL_MOVEMENT_FACTOR = 0.5f;
	final private int  OSS_DAY_LEVEL_MIN_QUANTITY_DIFF = 3;  // Check this
	
	final private float OOS_TOD_MOVEMENT_FACTOR = 0.5f;	
	final private float OOS_DAILY_MOVEMENT_FACTOR = 0.7f; // daily level check *for* day part
	final private int  OSS_TOD_MIN_QUANTITY_DIFF = 2;  // Check this

	private Boolean oldProcess = false;

	private HashMap<Integer, Integer> tsHash = new HashMap<Integer, Integer>();

	private boolean useAdForExp = false;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		//Inputs - Store Id
		//Date
 		PropertyConfigurator.configure("log4j-oos.properties");
		
		PropertyManager.initialize("analysis.properties");
		OOSDataLoad oosCalcs = new OOSDataLoad ();
		
		if( args.length == 2){
			//Use Previous date....
			String prevDate = DateUtil.getDateFromCurrentDate(-1);
			logger.info("Previous Date." + prevDate);
			oosCalcs.performOOSSummary(args[0], args[1], prevDate);
		}
		else if( args.length == 3){
			oosCalcs.performOOSSummary(args[0], args[1], args[2]);
		}
		else if( args.length >= 4 ){
			try {
			if( args.length >= 5){
				useArchive = Boolean.valueOf(args[4]);
			}
			
			Date startDate = new Date();
			Date endDate = new Date();
			if(args[2].compareToIgnoreCase("PREVIOUSDAY")==0){
				startDate = DateUtil.toDate(DateUtil.getDateFromCurrentDate(-1));
				endDate = DateUtil.toDate(DateUtil.getDateFromCurrentDate(-1));
			} else 	if(args[2].compareToIgnoreCase("CURRENTDAY")==0){
				// Do NOTHING THE CURRENT DATE IS SET AS DEFAULT
			
			} else{

			startDate = DateUtil.toDate(args[2]);
			endDate = DateUtil.toDate(args[3]);
			}
			Date dtAnalysisDate = startDate;
			String analysisDateStr = DateUtil.dateToString(dtAnalysisDate, Constants.APP_DATE_FORMAT);
			if( args.length >= 6){
				oosCalcs.preProcess = Boolean.valueOf(args[5]);
			}
			if(oosCalcs.preProcess){
				oosCalcs.preProcess(args[0],analysisDateStr);

				Calendar c = Calendar.getInstance();
				c.setTime(dtAnalysisDate);
				c.add(Calendar.DATE, -42);
				startDate = c.getTime();

				c.setTime(dtAnalysisDate);
				c.add(Calendar.DATE, -1);
				endDate = c.getTime();

				dtAnalysisDate = startDate;
				
			}
			if( args.length >= 7){
				oosCalcs.oldProcess  = Boolean.valueOf(args[6]);
				if(!oosCalcs.oldProcess){
					oosCalcs.setupDynamicList(args[0],analysisDateStr	);
				}
			}
			if( args.length >= 8){
				if(args[7].compareToIgnoreCase("false")!=0){
					oosCalcs.adjustForPartialDay  = true;
					oosCalcs.uptoTS = Integer.valueOf(args[7]);
				}
			}
			if( args.length >= 9){
				if(args[8].compareToIgnoreCase("WITHAD")==0){
					oosCalcs.useAdForExp  = true;
				}
			}
			if( args.length >= 10){
				if(args[9].compareToIgnoreCase("true")==0){
					oosCalcs.persistPredictedMovement  = true;
				}
			}
			if( args.length >= 11){
				if(args[10].compareToIgnoreCase("true")==0){
					oosCalcs.usePreBuiltExpectation  = true;
				}
			}
			while ( DateUtil.getDateDiff(endDate, dtAnalysisDate)>=0)
			{
				analysisDateStr = DateUtil.dateToString(dtAnalysisDate, Constants.APP_DATE_FORMAT);
				logger.debug("Analysis Date " + analysisDateStr);
				
				oosCalcs.performOOSSummary(args[0], args[1], analysisDateStr);
				
				dtAnalysisDate = DateUtil.incrementDate(dtAnalysisDate, 1);

			}
			}catch(Exception ge){
				logger.error("Error in Date Manipulation ....", ge);
			}catch(GeneralException ge){
				logger.error("Error in Date Manipulation ....", ge);
			}
			
		}
		else{ 
			logger.info("Insufficient Arguments - OOSDataLoad StoreNumber [true/false] for Analyze  StartDate(MM/DD/YYYY) [optional End Date] " +
					"[Archive  true/false] [Prerocess  true/false]  [OldProcess  true/false] " +
					"[adjust for partial day TimeslotID/false ] [WITHAD/false] [PersistPredictions true/false] [Use Prebuilt Exp true/false]");
			System.exit(1);
		}
		
	}

	
	private void preProcess(String storeNum, String analysisDate) throws Exception {
		// TODO Auto-generated method stub
		try{
			conn = DBManager.getConnection();
			
			//Get Store Id
			StoreDAO strdao = new StoreDAO();
			strId = strdao.getStoreID(conn, storeNum,null,-1);
			if( strId <= 0){
				throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNum );
			}

			preProcessItemList = ossCalcDao.getPreProcessItemList(conn, storeNum, strId, analysisDate);
		}catch(Exception  e){
			throw new Exception(e);
		}catch( GeneralException e){
			throw new Exception(e);
		}
		
	}


	private void setupDynamicList(String storeNum, String analysisDate) throws Exception {
		// TODO Auto-generated method stub
		try{
			conn = DBManager.getConnection();
			
			//Get Store Id
			StoreDAO strdao = new StoreDAO();
			strId = strdao.getStoreID(conn, storeNum,null,-1);
			if( strId <= 0){
				throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNum );
			}

			preProcessItemList = ossCalcDao.getDynamicItemList(conn,  strId, analysisDate);
		}catch(Exception  e){
			throw new Exception(e);
		}catch(GeneralException e){
			throw new Exception(e);
		}
		
	}

	
	/* Time of the Day Lookup
	 * 
4	12:00 AM - 7:00 AM
5	7:00 AM - 9:00 AM
6	9:00 AM - 11:00 AM
7	11:00 AM - 2:00 PM
8	2:00 PM - 4:00 PM
9	4:00 PM - 7:00 PM
10	7:00 PM - 9:00 PM
11	9:00 PM - 12:00 AM
	*/
	
	/* Day of Week Lookup
	 * 1	Sunday
	 * 2	Monday
	 * 3	Tuesday
	 * 4	Wednesday
	 * 5	Thursday
	 * 6	Friday
	 * 7	Saturday
	 * 0	Select
	 */
	
	private void doInitialization(String storeNo, String analyzeFlag, String inpDate) {
			
		timeSlots = oosAnalyzer.prepareTimeSlots();
		
		int i=0;
		for(OOSTimeFactor ts:timeSlots){
			
			tsHash .put(ts.todId,i);
			i++;
		}
		storeNum = storeNo;
		
		analysisDate = inpDate;
		if( analyzeFlag.equalsIgnoreCase("true"))
			analyze = true;
		else
			analyze = false;
	}
	
	public void performOOSSummary( String storeNo, String analyzeFlag, String inpDate) {
	
		logger.info("Analysis Started for Store - " + storeNo + " - " + inpDate);
		
		
		doInitialization(storeNo, analyzeFlag, inpDate);
		try {
			if(preProcess){
				analyze = false;
				oldProcess = false;
			}
			
			// get DB Connection
			conn = DBManager.getConnection();
			
			compDao = new CompetitiveDataDAO(conn);
			costDao = new CostDAO();
			RetailCalendarDAO calendarDao = new RetailCalendarDAO();  

			//Get Store Id
			StoreDAO strdao = new StoreDAO();
			strId = strdao.getStoreID(conn, storeNum,null,-1);
			if( strId <= 0){
				throw new GeneralException ("Invalid Store Number, Store Number passed = " + storeNum );
			}
			
			//Get the check list
			int checkListId = Integer.parseInt(PropertyManager.getProperty("OOS_ANALYSIS.CHECKLIST_ID", "222"));
			
			
			//Get day of the week
			
			dow = DateUtil.getdayofWeek( analysisDate, Constants.APP_DATE_FORMAT);
			//Find the schedule id
			scheduleId= getScheduleId( conn, strId, analysisDate);

			String excludeDeptId = PropertyManager.getProperty( "PI_REVENUE_EXCLUDE_DEPARTMENTS", "");
			int slotLen = timeSlots.length;
		    String startPeriod = setupPeriod( analysisDate, timeSlots[0].dateFactor, timeSlots[0].timeOfDay);
		    String endPeriod = setupPeriod( analysisDate, timeSlots[slotLen-1].dateFactor, timeSlots[slotLen-1].timeOfDay);
		    //Get for TodId of 4 - All Day

			int calendarId = (calendarDao.getCalendarId(conn, inpDate, Constants.CALENDAR_DAY)).getCalendarId();

			visitList = ossCalcDao.getVisitCount(conn, strId, startPeriod, endPeriod, excludeDeptId, useArchive,analysisDate,calendarId);
			
					
			calendarWeekId = (calendarDao.getCalendarId(conn, inpDate, Constants.CALENDAR_WEEK)).getCalendarId();
			
			if(analyze){
				oosAnalyzer.initialize(conn, strId, storeNo, inpDate, dow);
				predictionExpCache = ossCalcDao.loadPredictedExpCache(conn,strId,calendarId);
				predictionExpFocusCache = ossCalcDao.loadPredictedExpFocusCache(conn,strId,calendarId);
				predictionExpHighMoverCache = ossCalcDao.loadPredictedExpHighMoverCache(conn,strId,calendarId);
				lastMovementCache = ossCalcDao.loadLastMovementDateTime(conn, strId, analysisDate);
				logger.info("Prediction model exp size: "+predictionExpCache.size());
				logger.info("Prediction model exp (focus) size: "+predictionExpFocusCache.size());
				if(useAdForExp){
					weeklyAdCache = ossCalcDao.loadWeeklyAdCache(conn, storeNo, calendarWeekId);
				}
				logger.info("weeekly ad cache size:"+weeklyAdCache.size());
				if(usePreBuiltExpectation){
					logger.info("Begin load predicted Movement");
					predictedMovementCache = ossCalcDao.loadPredictedMovementCache(conn,strId,calendarWeekId,dow);
					logger.info("Endload predicted Movement:cache size" + predictedMovementCache.size());
				}
			}
			
			
			// load movemet data for the day into cache
			ossCalcDao.loadMovementDataCache(conn,storeNum,calendarId,useArchive);

			
			//Get the list of items for the check list by retId
			CachedRowSet crs=null;

			if(oldProcess){
				if( checkListId <= 0){
					throw new GeneralException ("Invalid CheckList Id, please set the OOS_ANALYSIS.CHECKLIST_ID property " );
				}
				crs = ossCalcDao.getNonLIGItemList(conn, checkListId);
				
			}else{
				crs = preProcessItemList;
				
			}
			
			if(!preProcess){
			//Delete any previous calculations
				ossCalcDao.deleteOSSCalcs( conn, scheduleId, dow);
				if(persistPredictedMovement){
				ossCalcDao.deleteMovementExpectation( conn, strId, calendarWeekId,dow);
				}

			}
			
			logger.info("Item list Count :"+ crs.size());
			logger.info("Begin Load Price list");
			loadPriceListCache(conn,crs, calendarId);
			logger.info("End Load Price list");
			performOOSCalcsForNonLIG(crs);

			if( analyze){
				oosAnalyzer.performCloseOperation();
			}

			
			// clear movement data from cache
			ossCalcDao.clearMovementDataCache();
			
			

		}catch (GeneralException ge) {
			logger.error("Error in OOS Calculation", ge);
			PristineDBUtil.rollbackTransaction(conn, "OOS Data Load");
			System.exit(1);
		}catch (Exception e) {
			logger.error("Error in OOS Calculation", e);
			PristineDBUtil.rollbackTransaction(conn, "OOS Data Load");
			System.exit(1);
		} 

		try {
			if (PropertyManager.getProperty("DATALOAD.COMMIT", "")
					.equalsIgnoreCase("TRUE")) {
				logger.info("Committing transacation");
				PristineDBUtil.commitTransaction(conn, "OOS Data Load");
			} else {
				logger.info("Rolling back transacation");
				PristineDBUtil.rollbackTransaction(conn, "OOS Data Load");
			}
		} catch (GeneralException ge) {
			logger.error("Error in commit", ge);
			System.exit(1);
		}finally {
			PristineDBUtil.close(conn);
		}
		logger.info("Analysis Completed for Store - " + storeNo + " - " + inpDate);
		
		
	}
	
	private void loadPriceListCache(Connection conn, CachedRowSet itemListCrs, int calendarId) throws GeneralException {
		PriceDataLoad objPriceLoad = new PriceDataLoad();
		try {
		List<String> itemCodeList = new ArrayList<String>();
		itemListCrs.beforeFirst();
		while( itemListCrs.next()){
			
			Integer currentItemCode  = itemListCrs.getInt("ITEM_CODE");
			itemCodeList.add(currentItemCode.toString());
		}
		itemListCrs.beforeFirst();
		priceDataMap = objPriceLoad
				.getRetailPrice(conn, storeNum,
						itemCodeList, calendarId, null, Constants.STORE_LEVEL_TYPE_ID);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			throw new GeneralException(e.getMessage());
		};
	}
	
	private void performOOSCalcsForNonLIG(CachedRowSet itemListCrs) throws GeneralException{
		
		try{
			ArrayList<Integer> itemCodeList = new ArrayList<Integer> ();
			int count = 0;
			itemListCrs.beforeFirst();
			while( itemListCrs.next()){
				count++;
				int newLirId  = itemListCrs.getInt("RET_LIR_ID");;
				int newFamilyItemCode = itemListCrs.getInt("RET_LIR_ITEM_CODE");
				int currentItemCode  = itemListCrs.getInt("ITEM_CODE");
				itemCodeList.clear();
//				logger.info("begin for an item");
				itemCodeList.add(currentItemCode);
				processOOSStats(itemCodeList, newFamilyItemCode);
//				logger.info("end for an item");
				if( STOPLIMIT > 0 && count >= STOPLIMIT) break;
			}
			
		}catch( SQLException sqle){
			throw new GeneralException( "Cached Rowset access exception", sqle);
		}
	}

	private void processOOSStats( ArrayList<Integer> itemCodeList, int familyItemCode) 
			throws GeneralException, SQLException {
		
		//build in item list
		int i = 0;
		boolean validData=false;
		String itemGroup = "";
		Iterator<Integer> itr = itemCodeList.iterator();
	    while (itr.hasNext()) {
	    	if( i > 0) itemGroup +=",";
	    	i++;
	    	itemGroup += itr.next();
	    }
	    boolean itemOOSforAtleastOneTS=false;
	    String startPeriod = setupPeriod( analysisDate, timeSlots[0].dateFactor, timeSlots[0].timeOfDay);
	    int slotLen = timeSlots.length;
	    String endPeriod = setupPeriod( analysisDate, timeSlots[slotLen-1].dateFactor, timeSlots[slotLen-1].timeOfDay);
	    
	    //get the Movement daily data for the timeslot period.
	    
	    ArrayList <MovementDTO> movementList =  ossCalcDao.getMovementData(conn, storeNum, itemGroup, startPeriod, endPeriod, useArchive);
	    logger.debug("item data size is " + movementList.size() );

	    String lastMovementTS = "";
	    
//	    SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy hh:mm aa");
//	    Date lastMovementDate = null;
//	    for(MovementDTO m: movementList){
//	    	try{
//	    	String ts = m.getTranTimeStamp();
//	    	
//	    	Date tsD = df.parse(ts);
//	    	if(lastMovementDate==null || tsD.after(lastMovementDate)){
//	    		lastMovementDate = tsD;
//	    		lastMovementTS = ts;
//	    	}
//	    	//MM/DD/YYYY HH:MI AM
//	    	
//	    	}catch(Exception e){
//	    		e.printStackTrace();
//	    	}
//	    }
	    
	    if(lastMovementCache.containsKey(familyItemCode)){
	    	lastMovementTS = lastMovementCache.get(familyItemCode);
	    }
	    
	    String saleInd = "N";
	    float regPrice = 0;
	    float salePrice =0;
	    CostDTO costInfo = new CostDTO();
	    
	    // Try and Get price from Price list
	    
	    String itemCode = itemCodeList.get(0).toString();
  
	    if(priceDataMap.containsKey(itemCode)){
	    	RetailPriceDTO price = priceDataMap.get(itemCode);
	    	saleInd = price.getPromotionFlag();
	    	regPrice = price.getRegPrice();
	    	salePrice = price.getSalePrice();
	    	if(saleInd.compareToIgnoreCase("Y")==0 && salePrice ==0 && price.getSaleQty()>0){
	    		salePrice = price.getSaleMPrice()/price.getSaleQty();
	    	}
	    	validData=true;
	    }else{ 

    	CompetitiveDataDTO compData = compDao.getCompDataForItem(conn, scheduleId, familyItemCode, true);
    	updateSaleInfo(movementList,compData);
    	regPrice = compData.regPrice;
    	salePrice = compData.fSalePrice;
    	if( compData.saleInd != null && !compData.saleInd.equals("")){  
    		saleInd = compData.saleInd;
    	}
    	costInfo = costDao.getCompDataForItem(conn, compData.checkItemId);
    	validData=true;
	    }
	    salePrice = NumberUtil.RoundFloat(salePrice,2);
	    regPrice = NumberUtil.RoundFloat(regPrice,2);
	    float prc = (saleInd.compareToIgnoreCase("Y")==0)?salePrice:regPrice;
	    // get last 7 days expectation
	    Hashtable<Integer,Double> prevdayExp = new Hashtable<Integer,Double>();
	    Hashtable<Integer,Double> last7dayExp = new Hashtable<Integer,Double>();
	    Hashtable<Integer,Double> last28dayExp = new Hashtable<Integer,Double>();
	    Hashtable<Integer,Double> last180dayExp = new Hashtable<Integer,Double>();
	    Hashtable<Integer,Double> last7dayAvgExp = new Hashtable<Integer,Double>();
	    Hashtable<Integer,Double> usingAdExp = new Hashtable<Integer,Double>();
	    String focusItem = "N";
	    HashMap<Integer,Double> predictionModelExp = new HashMap<Integer,Double>(); // PredictionExpCache
	    if(predictionExpCache.containsKey(familyItemCode)){
	    	predictionModelExp = predictionExpCache.get(familyItemCode);

	    }
	    if(predictionExpFocusCache.containsKey(familyItemCode)){
	    	predictionModelExp = predictionExpFocusCache.get(familyItemCode);
	    }
	    if(predictionExpHighMoverCache.containsKey(familyItemCode)){
	    	focusItem = "Y";
	    }
	    if(predictionModelExp == null){
	    	predictionModelExp = new HashMap<Integer,Double>();
	    }
	    
	    HashSet<OOSPredictedMovementDTO> predictedMov = new HashSet<OOSPredictedMovementDTO>();
	    if(usePreBuiltExpectation){
	    	predictedMov = predictedMovementCache.get(familyItemCode);
	    }
	    
	    Hashtable<Integer,Double> mape = new Hashtable<Integer,Double>();
	    Hashtable<Integer,Double> mase = new Hashtable<Integer,Double>();

	    String displayType = null;
	    
	    displayType = weeklyAdCache.get(familyItemCode);
	    
	    List<Integer> last7dayObs =new ArrayList<Integer>();
	    List<Integer> last28dayObs =new ArrayList<Integer>();
	    List<Integer> last180dayObs =new ArrayList<Integer>();
	    List<Integer> last7dayAvgObs =new ArrayList<Integer>();

	    List<Integer> withAdObs =new ArrayList<Integer>();
	    
	    Hashtable<Integer,Double> last7dayStdD =new Hashtable<Integer, Double>();
	    Hashtable<Integer, Double> last28dayStdD =new Hashtable<Integer, Double>();
	    Hashtable<Integer, Double> last180dayStdD =new Hashtable<Integer, Double>();
	    Hashtable<Integer, Double> last7dayAvgStdD =new Hashtable<Integer, Double>();

	    Hashtable<Integer, Double> withAdStdD =new Hashtable<Integer, Double>();
	    
	    int last7dayO =0;
	    int last28dayO = 0;
	    int last180dayO=0;
	    int last7dayAvgO =0;

	    int last7dayOused =0;
	    int last28dayOused = 0;
	    int last180dayOused=0;
	    int last7dayAvgOused =0;

	    
	    int last7dayDaysMoved =0;
	    int last28dayDaysMoved = 0;
	    int last180dayDaysMoved=0;

	    int last7dayDaysToIgnore =0;
	    int last28dayDaysToIgnore = 0;
	    int last180dayDaysToIgnore=0;
	    int last7dayAvgDaysToIgnore=0;

	    int last7dayDaysToIgnoreWithAd =0;
	    int last28dayDaysToIgnoreWithAd = 0;
	    int last180dayDaysToIgnoreWithAd=0;
	    int last7dayAvgDaysToIgnoreWithAd=0;
	    
	    String adExpMethod="";
	    
	    boolean moved2PlusInAllDaysInPrevNdays=false;

	    if(analyze == true){
	    	if(usePreBuiltExpectation){
	    		
	    		if(predictedMov !=null){
	    		for(OOSPredictedMovementDTO p: predictedMov){
	    			
			    	last7dayDaysMoved = p.last7dayDaysMoved;
			    	last28dayDaysMoved = p.last28dayDaysMoved;
			    	last180dayDaysMoved = p.last180dayDaysMoved;
		    		
			    	if(p.prevdayexp != 0){
			    	prevdayExp.put(p.timeOfDayId, p.prevdayexp);
			    	}

			    	last7dayExp.put(p.timeOfDayId, p.last7dayexp);
			    	last28dayExp.put(p.timeOfDayId, p.last28dayexp);
			    	last180dayExp.put(p.timeOfDayId, p.last180dayexp);
			    	last7dayAvgExp.put(p.timeOfDayId, p.last7dayavgexp);
			    	
			    	usingAdExp.put(p.timeOfDayId, p.expWithAd)  ;
			    	withAdStdD.put(p.timeOfDayId,p.sigmaWithAd);
			    	adExpMethod = p.expMethodWithAd;
			    	
			    	// need to persist separate sigma eventually
			    	last7dayStdD.put(p.timeOfDayId, p.sigmaUsed);
			    	last28dayStdD.put(p.timeOfDayId, p.sigmaUsed);
			    	last180dayStdD.put(p.timeOfDayId, p.sigmaUsed);
			    	last7dayAvgStdD.put(p.timeOfDayId, p.sigmaUsed);

					last7dayO= p.last7dayobs;
					last28dayO= p.last28dayobs;
					last180dayO= p.last180dayobs;
					last7dayAvgO= p.last7dayavgobs;

					last7dayOused= p.last7dayobsUsed;
					last28dayOused= p.last28dayobsUsed;
					last180dayOused= p.last180dayobsUsed;
					last7dayAvgOused= p.last7dayavgobsUsed;

	    		}
	    		}

	    	} else {

    	// Load predicted expectations

//	    	mape = getMape(familyItemCode);	
	    		
	    	last7dayDaysMoved = ossCalcDao.getPrevNdayDaysMoved(conn,7, scheduleId, dow, analysisDate, strId, familyItemCode);
	    	last28dayDaysMoved = ossCalcDao.getPrevNdayDaysMoved(conn,28, scheduleId, dow, analysisDate, strId, familyItemCode);
	    	last180dayDaysMoved = ossCalcDao.getPrevNdayDaysMoved(conn,120, scheduleId, dow, analysisDate, strId, familyItemCode);
	    	
//	    	moved2PlusInAllDaysInPrevNdays = ossCalcDao.getMoved2plusInAllDaysInPrevNdays(conn, 42, scheduleId,dow,analysisDate, strId,familyItemCode);

	    	last7dayDaysToIgnore = ossCalcDao.getPrevNdayDaysToIgnore(conn, 7, scheduleId,dow,analysisDate, strId,familyItemCode,saleInd, prc);
	    	last28dayDaysToIgnore = ossCalcDao.getPrevNdayDaysToIgnore(conn, 28, scheduleId,dow,analysisDate, strId,familyItemCode,saleInd, prc);
	    	last180dayDaysToIgnore = ossCalcDao.getPrevNdayDaysToIgnore(conn, 120, scheduleId,dow,analysisDate, strId,familyItemCode,saleInd, prc);
	    	last7dayAvgDaysToIgnore = ossCalcDao.getPrevNdayDaysToIgnoreZeroMovement(conn, 42, scheduleId,dow,analysisDate, strId,familyItemCode,saleInd, prc);

	    	prevdayExp = getPrevdayExpectation(analysisDate,familyItemCode,saleInd, prc,displayType,false);

	    	last7dayExp = getNdayExpectation(OOS_METHOD_7DAY,analysisDate,familyItemCode,saleInd, prc,last7dayObs,last7dayStdD,last7dayDaysMoved, last7dayDaysToIgnore,displayType,false);
	    	last28dayExp = getNdayExpectation(OOS_METHOD_28DAY,analysisDate,familyItemCode,saleInd, prc,last28dayObs,last28dayStdD,last28dayDaysMoved, last28dayDaysToIgnore,displayType,false);
	    	last180dayExp = getNdayExpectation(OOS_METHOD_180DAY,analysisDate,familyItemCode,saleInd, prc,last180dayObs,last180dayStdD,last180dayDaysMoved, last180dayDaysToIgnore,displayType,false);
	    	last7dayAvgExp = getNdayExpectation(OOS_METHOD_7DAY_AVG,analysisDate,familyItemCode,saleInd, prc,last7dayAvgObs,last7dayAvgStdD,0, last7dayAvgDaysToIgnore,displayType,false);

    		// WITH AD
	    	int withAdO = 0;
	    	int withAdOused = 0;
	    	
	    	if(useAdForExp){

	    	last7dayDaysToIgnoreWithAd = ossCalcDao.getPrevNdayDaysToIgnoreWithAd(conn, 7, scheduleId,dow,analysisDate, strId,familyItemCode,saleInd, prc,displayType,calendarWeekId);
	    	usingAdExp = getNdayExpectation(OOS_METHOD_7DAY,analysisDate,familyItemCode,saleInd, prc,withAdObs,withAdStdD,last7dayDaysMoved, last7dayDaysToIgnoreWithAd,displayType,true);
			if(withAdObs!=null && withAdObs.size()>0){
				withAdO= withAdObs.get(0);
				if(withAdObs.size()>1){
					withAdOused= withAdObs.get(1);
				}
			}
			adExpMethod = "last 7 days";
	    	if(withAdOused <3){
		    	last28dayDaysToIgnoreWithAd = ossCalcDao.getPrevNdayDaysToIgnoreWithAd(conn, 28, scheduleId,dow,analysisDate, strId,familyItemCode,saleInd, prc,displayType,calendarWeekId);
	    		usingAdExp = getNdayExpectation(OOS_METHOD_28DAY,analysisDate,familyItemCode,saleInd, prc,withAdObs,withAdStdD,last28dayDaysMoved, last28dayDaysToIgnoreWithAd,displayType,true);
	    		if(withAdObs!=null && withAdObs.size()>0){
	    			withAdO= withAdObs.get(0);
	    			if(withAdObs.size()>1){
	    				withAdOused= withAdObs.get(1);
	    			}
	    		}
				adExpMethod = "last 28 days";

	    		if(withAdOused <7){
	    	    	last180dayDaysToIgnoreWithAd = ossCalcDao.getPrevNdayDaysToIgnoreWithAd(conn, 120, scheduleId,dow,analysisDate, strId,familyItemCode,saleInd, prc,displayType,calendarWeekId);
	    			usingAdExp = getNdayExpectation(OOS_METHOD_180DAY,analysisDate,familyItemCode,saleInd, prc,withAdObs,withAdStdD,last180dayDaysMoved, last180dayDaysToIgnoreWithAd,displayType,true);
	    			if(withAdObs!=null && withAdObs.size()>0){
	    				withAdO= withAdObs.get(0);
	    				if(withAdObs.size()>1){
	    					withAdOused= withAdObs.get(1);
	    				}
	    			}
	    			adExpMethod = "last 120 days";

	    			if(withAdOused < 14){
	    		    	prevdayExp = getPrevdayExpectation(analysisDate,familyItemCode,saleInd, prc,displayType,true);
	    				
	    				if(prevdayExp!=null && prevdayExp.size()>0 && prevdayExp.get(4) !=null && prevdayExp.get(4) > 0){
	    					usingAdExp = prevdayExp;
	    					withAdStdD = new Hashtable<Integer,Double>();
	    					adExpMethod = "prev day";
	    				} else {
	    				
	    				
	    				usingAdExp = last7dayAvgExp;
	    				withAdStdD = last7dayAvgStdD;
	    				adExpMethod = "last 42 days avg";
	    				}

	    			}
	    		}
	    	}
	    	} // end if Use Ad
   	
			if(last7dayObs!=null && last7dayObs.size()>0){
				last7dayO= last7dayObs.get(0);
				if(last7dayObs.size()>1){
					last7dayOused= last7dayObs.get(1);
				}
			}
			if(last28dayObs!=null && last28dayObs.size()>0){
				last28dayO= last28dayObs.get(0);
				if(last28dayObs.size()>1){
					last28dayOused= last28dayObs.get(1);
				}
			}
			if(last180dayObs!=null && last180dayObs.size()>0){
				last180dayO= last180dayObs.get(0);
				if(last180dayObs.size()>1){
					last180dayOused= last180dayObs.get(1);
				}
			}
			if(last7dayAvgObs!=null && last7dayAvgObs.size()>0){
				last7dayAvgO= last7dayAvgObs.get(0);
				if(last7dayAvgObs.size()>1){
					last7dayAvgOused= last7dayAvgObs.get(1);
				}
			}

	    	}
	    }
    		if(!validData){
    			return;
    		}
    		
    		float dailyRevOpp=0;
    		float dailyMarginOpp=0;
    		
	    	startPeriod = setupPeriod( analysisDate, 0, timeSlots[0].timeOfDay);
	    	endPeriod = setupPeriod( analysisDate, 1, timeSlots[slotLen-1].timeOfDay);
	    	OOSSummaryDTO oosSummaryDaily = computeOOSStats( familyItemCode, movementList, startPeriod, endPeriod,4);
	    	oosSummaryDaily.onSale = saleInd.equalsIgnoreCase("Y");
    		oosSummaryDaily.unitRegPrice = regPrice;
    		oosSummaryDaily.unitSalePrice = salePrice;
    		oosSummaryDaily.totalCustVisitCount = visitList.get(Constants.TIME_OF_DAY_ALLDAY);
    		oosSummaryDaily.ossInd = "E";
    		oosSummaryDaily.mape = -1;
    		oosSummaryDaily.lastMovementTS = lastMovementTS;
    		if(moved2PlusInAllDaysInPrevNdays){
    			oosSummaryDaily.moved2PlusInAllDaysInPrevNdays ="Y" ;
    		}
    		if(analyze == true ){
    			if(usingAdExp.containsKey(oosSummaryDaily.timeOfDayId)){
    				oosSummaryDaily.expWithAd = usingAdExp.get(oosSummaryDaily.timeOfDayId);
    				oosSummaryDaily.expMethodWithAd = adExpMethod ;
    			}
    			if(withAdStdD.get(oosSummaryDaily.timeOfDayId)!=null){
    			oosSummaryDaily.sigmaWithAd = withAdStdD.get(oosSummaryDaily.timeOfDayId);
    			}
		    	if(mape.containsKey(oosSummaryDaily.timeOfDayId)){
		    		oosSummaryDaily.mape =mape.get(oosSummaryDaily.timeOfDayId); 
		    	}
				
		    	if(prevdayExp.containsKey(oosSummaryDaily.timeOfDayId)){
		    		oosSummaryDaily.prevdayexp =prevdayExp.get(oosSummaryDaily.timeOfDayId); 
		    	}
		    	if(predictionModelExp.containsKey(oosSummaryDaily.timeOfDayId)){
		    		oosSummaryDaily.predictedExp = predictionModelExp.get(oosSummaryDaily.timeOfDayId);
		    		oosSummaryDaily.focusItem = focusItem;
		    	}
    			
		    	oosSummaryDaily.last7dayobs = last7dayO;
    			oosSummaryDaily.last28dayobs = last28dayO;
    			oosSummaryDaily.last180dayobs = last180dayO;
    			oosSummaryDaily.last7dayavgobs = last7dayAvgO;

		    	oosSummaryDaily.last7dayOused = last7dayOused;
    			oosSummaryDaily.last28dayOused = last28dayOused;
    			oosSummaryDaily.last180dayOused = last180dayOused;
    			oosSummaryDaily.last7dayavgOused = last7dayAvgOused;
    			
    			if(last7dayExp.containsKey(oosSummaryDaily.timeOfDayId)){
	    			oosSummaryDaily.last7dayexp =last7dayExp.get(oosSummaryDaily.timeOfDayId); 
	    		}
				if(last28dayExp.containsKey(oosSummaryDaily.timeOfDayId)){
	    			oosSummaryDaily.last28dayexp =last28dayExp.get(oosSummaryDaily.timeOfDayId); 
	    		}
				if(last180dayExp.containsKey(oosSummaryDaily.timeOfDayId)){
	    			oosSummaryDaily.last180dayexp =last180dayExp.get(oosSummaryDaily.timeOfDayId); 
	    		}
				if(last7dayAvgExp.containsKey(oosSummaryDaily.timeOfDayId)){
	    			oosSummaryDaily.last7dayavgexp =last7dayAvgExp.get(oosSummaryDaily.timeOfDayId); 
	    		}
	    		double sigma = 0.0;
				if(last7dayOused>=3){
					if(last7dayStdD.get(oosSummaryDaily.timeOfDayId)!=null) sigma = last7dayStdD.get(oosSummaryDaily.timeOfDayId);
	    			oosSummaryDaily.sigmaUsed = sigma;
	    		}else if(last28dayOused>=7){
					if(last28dayStdD.get(oosSummaryDaily.timeOfDayId)!=null) sigma = last28dayStdD.get(oosSummaryDaily.timeOfDayId);
	    			oosSummaryDaily.sigmaUsed = sigma;

	    		}else if(last180dayOused>=28){
					if(last180dayStdD.get(oosSummaryDaily.timeOfDayId)!=null) sigma = last180dayStdD.get(oosSummaryDaily.timeOfDayId);
	    			oosSummaryDaily.sigmaUsed = sigma;

	    		}else if(prevdayExp.size()>0){
	    			oosSummaryDaily.sigmaUsed = 0;

	    		}else if(last7dayAvgOused>=21){
					if(last7dayAvgStdD.get(oosSummaryDaily.timeOfDayId)!=null) sigma = last7dayAvgStdD.get(oosSummaryDaily.timeOfDayId);
	    			oosSummaryDaily.sigmaUsed = sigma;

	    		}

	    		oosSummaryDaily.last7dayDaysMoved=last7dayDaysMoved; 
	    		oosSummaryDaily.last28dayDaysMoved=last28dayDaysMoved;
	    		oosSummaryDaily.last180dayDaysMoved=last180dayDaysMoved;

			}
    		
    		List<OOSSummaryDTO> oosSummaryList = new ArrayList<OOSSummaryDTO>(); 
    		List<OOSSummaryDTO> oosSummaryListTemp = new ArrayList<OOSSummaryDTO>();
    		int slotsWithMovement =0;
		    for (int j = 0; j < slotLen - 1; j++){
		    	if(adjustForPartialDay && (timeSlots[j].todId>uptoTS) && (timeSlots[j].todId != 11)){
		    		continue;
		    	}
		    	startPeriod = setupPeriod( analysisDate, timeSlots[j].dateFactor, timeSlots[j].timeOfDay);
		    	endPeriod = setupPeriod( analysisDate, timeSlots[j+1].dateFactor, timeSlots[j+1].timeOfDay);
		    	OOSSummaryDTO oosSummary = computeOOSStats( familyItemCode, movementList, startPeriod, endPeriod,timeSlots[j].todId );
		    	if(oosSummary.timeOfDayId!=4){
		    		if(oosSummary.totalQty>0){
		    			slotsWithMovement++;
		    		}
		    	}
	    		if(moved2PlusInAllDaysInPrevNdays){
	    			oosSummary.moved2PlusInAllDaysInPrevNdays ="Y" ;
	    		}

		    	oosSummary.onSale = saleInd.equalsIgnoreCase("Y");
		    	//Override the visit count with true visit count for timeslot
		    	if(visitList.get(timeSlots[j].todId)==null){
		    		visitList.put(timeSlots[j].todId, 0);
		    	}
		    	oosSummary.totalCustVisitCount = visitList.get(timeSlots[j].todId); 
	    		oosSummary.lastMovementTS = lastMovementTS;

		    	if( oosSummary.totalCustVisitCount> 0)
				    	oosSummary.movVisitRatio = (float)(oosSummary.regQty + oosSummary.saleQty)/oosSummary.totalCustVisitCount;
				else
				    	oosSummary.movVisitRatio = 0;
				{
		    		oosSummary.unitRegPrice = regPrice;
		    		oosSummary.unitSalePrice = salePrice;
		    		oosSummary.ossInd = "E";
		    		
		    		if( analyze ){
		    			if(usingAdExp.containsKey(oosSummary.timeOfDayId)){
		    				oosSummary.expWithAd = usingAdExp.get(oosSummary.timeOfDayId);
		    				oosSummary.expMethodWithAd = adExpMethod ;
		    			}
		    			if(withAdStdD.get(oosSummary.timeOfDayId)!=null){
						oosSummary.sigmaWithAd = withAdStdD.get(oosSummary.timeOfDayId);
		    			}
				    	if(mape.containsKey(oosSummary.timeOfDayId)){
				    		oosSummary.mape =mape.get(oosSummary.timeOfDayId); 
				    	}
			    		if(prevdayExp.containsKey(oosSummary.timeOfDayId)){
			    			oosSummary.prevdayexp =prevdayExp.get(oosSummary.timeOfDayId); 
			    		}
				    	if(predictionModelExp.containsKey(oosSummary.timeOfDayId)){
				    		oosSummary.predictedExp = predictionModelExp.get(oosSummary.timeOfDayId);
				    		oosSummary.focusItem = focusItem;
				    	}
	    				oosSummary.last7dayobs = last7dayO;
	    				oosSummary.last28dayobs = last28dayO;
	    				oosSummary.last180dayobs = last180dayO;
		    			oosSummary.last7dayavgobs = last7dayAvgO;

				    	oosSummary.last7dayOused = last7dayOused;
		    			oosSummary.last28dayOused = last28dayOused;
		    			oosSummary.last180dayOused = last180dayOused;
		    			oosSummary.last7dayavgOused = last7dayAvgOused;

		    			
		    			if(last7dayExp.containsKey(oosSummary.timeOfDayId)){
			    			oosSummary.last7dayexp =last7dayExp.get(oosSummary.timeOfDayId); 
			    		}
			    		if(last28dayExp.containsKey(oosSummary.timeOfDayId)){
			    			oosSummary.last28dayexp =last28dayExp.get(oosSummary.timeOfDayId); 
			    		}
			    		if(last180dayExp.containsKey(oosSummary.timeOfDayId)){
			    			oosSummary.last180dayexp =last180dayExp.get(oosSummary.timeOfDayId); 
			    		}
						if(last7dayAvgExp.containsKey(oosSummary.timeOfDayId)){
			    			oosSummary.last7dayavgexp =last7dayAvgExp.get(oosSummary.timeOfDayId); 
			    		}
						double sigma =0.0;
						if(last7dayOused>=3){
							if(last7dayStdD.get(oosSummary.timeOfDayId)!=null) sigma = last7dayStdD.get(oosSummary.timeOfDayId);
			    			oosSummary.ossInd = compareWithExp(oosSummary,oosSummaryDaily,last7dayExp.get(oosSummary.timeOfDayId),sigma);
			    			oosSummary.oosMethod = OOS_METHOD_7DAY;
			    			oosSummary.sigmaUsed = sigma;
			    		}else if(last28dayOused>= 7){
							if(last28dayStdD.get(oosSummary.timeOfDayId)!=null) sigma = last28dayStdD.get(oosSummary.timeOfDayId);
			    			oosSummary.ossInd = compareWithExp(oosSummary,oosSummaryDaily,last28dayExp.get(oosSummary.timeOfDayId),sigma);
			    			oosSummary.oosMethod = OOS_METHOD_28DAY;
			    			oosSummary.sigmaUsed = sigma;
			    		}else if(last180dayOused>=28){
							if(last180dayStdD.get(oosSummary.timeOfDayId)!=null) sigma = last180dayStdD.get(oosSummary.timeOfDayId);
			    			oosSummary.ossInd = compareWithExp(oosSummary,oosSummaryDaily,last180dayExp.get(oosSummary.timeOfDayId),sigma);
			    			oosSummary.oosMethod = OOS_METHOD_180DAY;
			    			oosSummary.sigmaUsed = sigma;
			    		}else if(prevdayExp.size()>0){
			    			oosSummary.ossInd = compareWithExp(oosSummary,oosSummaryDaily,prevdayExp.get(oosSummary.timeOfDayId),sigma);
			    			oosSummary.oosMethod = OOS_METHOD_PREVDAY;
			    			oosSummary.sigmaUsed = 0.0;
			    		}else if(last7dayAvgOused>=21){
							if(last7dayAvgStdD.get(oosSummary.timeOfDayId)!=null) sigma = last7dayAvgStdD.get(oosSummary.timeOfDayId);
			    			oosSummary.ossInd = compareWithExp(oosSummary,oosSummaryDaily,last7dayAvgExp.get(oosSummary.timeOfDayId),sigma);
			    			oosSummary.oosMethod = OOS_METHOD_7DAY_AVG;
			    			oosSummary.sigmaUsed = sigma;

			    		}else{
			    			oosSummary.ossInd = "E";
			    			oosSummary.oosMethod = "NA";
			    		}
			    		if(oosSummary.ossInd.compareToIgnoreCase("Y")==0){
			    			if(oosSummary.movementInLastHour || oosSummary.prevdayexp==0 || (oosSummary.predictedExp!=null && oosSummary.totalQty >= oosSummary.predictedExp)){
			    				oosSummary.ossInd = "N";
			    			}
			    		}
						
			    		oosSummary.last7dayDaysMoved=last7dayDaysMoved;
			    		oosSummary.last28dayDaysMoved=last28dayDaysMoved;
			    		oosSummary.last180dayDaysMoved=last180dayDaysMoved;
		    			dailyRevOpp+=oosSummary.revOpp;
		    			dailyMarginOpp+=oosSummary.marginOpp;
		    		}
		    		
		    		if(oosSummary.ossInd.compareToIgnoreCase("Y")==0){
		    			itemOOSforAtleastOneTS = true;
		    		}
		    		if(oosSummary.totalQty>0 ||  oosSummary.ossInd.compareToIgnoreCase("Y")==0){
		    			oosSummaryList.add(oosSummary);
		    		}

		    		oosSummaryListTemp.add(oosSummary); // list for oos classification
		    	}
				
		    }


		    {
		    	//Setup for daily as well
				if( oosSummaryDaily.totalCustVisitCount> 0){
				    	oosSummaryDaily.movVisitRatio = (float)(oosSummaryDaily.regQty + oosSummaryDaily.saleQty)/oosSummaryDaily.totalCustVisitCount ;
				}
				else{
				    	oosSummaryDaily.movVisitRatio = 0;
				}
				if(analyze == true){
		    		double sigma = 0.0;
					if(last7dayOused>=3){
						if(last7dayStdD.get(oosSummaryDaily.timeOfDayId)!=null) sigma = last7dayStdD.get(oosSummaryDaily.timeOfDayId);
		    			oosSummaryDaily.ossInd = compareWithExpDayLevel(oosSummaryDaily,last7dayExp.get(oosSummaryDaily.timeOfDayId),sigma,itemOOSforAtleastOneTS);
		    			oosSummaryDaily.oosMethod = OOS_METHOD_7DAY;
		    			oosSummaryDaily.sigmaUsed = sigma;
		    		}else if(last28dayOused>=7){
						if(last28dayStdD.get(oosSummaryDaily.timeOfDayId)!=null) sigma = last28dayStdD.get(oosSummaryDaily.timeOfDayId);
		    			oosSummaryDaily.ossInd = compareWithExpDayLevel(oosSummaryDaily,last28dayExp.get(oosSummaryDaily.timeOfDayId),sigma,itemOOSforAtleastOneTS);
		    			oosSummaryDaily.oosMethod = OOS_METHOD_28DAY;
		    			oosSummaryDaily.sigmaUsed = sigma;

		    		}else if(last180dayOused>=28){
						if(last180dayStdD.get(oosSummaryDaily.timeOfDayId)!=null) sigma = last180dayStdD.get(oosSummaryDaily.timeOfDayId);
		    			oosSummaryDaily.ossInd = compareWithExpDayLevel(oosSummaryDaily,last180dayExp.get(oosSummaryDaily.timeOfDayId),sigma,itemOOSforAtleastOneTS);
		    			oosSummaryDaily.oosMethod = OOS_METHOD_180DAY;
		    			oosSummaryDaily.sigmaUsed = sigma;

		    		}else if(prevdayExp.size()>0){
		    			oosSummaryDaily.ossInd = compareWithExpDayLevel(oosSummaryDaily,prevdayExp.get(oosSummaryDaily.timeOfDayId),0.0,itemOOSforAtleastOneTS);
		    			oosSummaryDaily.oosMethod = OOS_METHOD_PREVDAY;
		    			oosSummaryDaily.sigmaUsed = 0;

		    		}else if(last7dayAvgOused>=21){
						if(last7dayAvgStdD.get(oosSummaryDaily.timeOfDayId)!=null) sigma = last7dayAvgStdD.get(oosSummaryDaily.timeOfDayId);
		    			oosSummaryDaily.ossInd = compareWithExpDayLevel(oosSummaryDaily,last7dayAvgExp.get(oosSummaryDaily.timeOfDayId),sigma,itemOOSforAtleastOneTS);
		    			oosSummaryDaily.oosMethod = OOS_METHOD_7DAY_AVG;
		    			oosSummaryDaily.sigmaUsed = sigma;

		    		}else{
		    			oosSummaryDaily.ossInd = "E";
		    			oosSummaryDaily.oosMethod = "NA";
		    		}
		    		if(oosSummaryDaily.ossInd.compareToIgnoreCase("Y")==0){
		    			if(oosSummaryDaily.prevdayexp==0 || (oosSummaryDaily.predictedExp !=null && oosSummaryDaily.totalQty >= oosSummaryDaily.predictedExp)){
		    				oosSummaryDaily.ossInd = "N";
		    			}
		    		}


				}
				oosSummaryDaily.revOpp =dailyRevOpp;
				oosSummaryDaily.marginOpp =dailyMarginOpp;
	    		if(oosSummaryDaily.totalQty>0 || oosSummaryDaily.ossInd.compareToIgnoreCase("Y")==0){
	    			oosSummaryList.add(oosSummaryDaily);
	    		}

	    		oosSummaryListTemp.add(oosSummaryDaily); // list for oos classification
		    }

		    if(analyze){
		    	classifyOOS(oosSummaryListTemp);
				if(persistPredictedMovement){
			    	ossCalcDao.insertMovementExpectation( conn, oosSummaryListTemp,strId, calendarWeekId,dow);
				}
		    }

		    if(itemOOSforAtleastOneTS || oosSummaryDaily.totalQty>0){
//				if(!persistPredictedMovement){

		    	ossCalcDao.insertOSSCalcs( conn, oosSummaryList, slotsWithMovement==slotLen-1);
//				}
		    }
    	
    	
	}




	private String compareWithExp(OOSSummaryDTO summary, OOSSummaryDTO summaryDaily,
			Double expQ, Double sD) {
		String oos = "N";
		if(expQ!=null && summary.totalQty >=0){
			if(
					(Math.round(expQ * summaryDaily.totalCustVisitCount) - summaryDaily.totalQty) > Math.round(summaryDaily.sigmaUsed * summaryDaily.totalCustVisitCount) 
					&& (summary.totalQty < OOS_TOD_MOVEMENT_FACTOR * Math.round(expQ * summary.totalCustVisitCount))
					&& ((Math.round(expQ * summary.totalCustVisitCount) - summary.totalQty) > OSS_TOD_MIN_QUANTITY_DIFF)
					&& (Math.round(expQ * summary.totalCustVisitCount) - summary.totalQty) > Math.round(1 * sD * summary.totalCustVisitCount)){
				oos = "Y";
			}
		}
		return oos;
	}

	private String compareWithExpDayLevel(OOSSummaryDTO summaryDaily,
			Double expQ, Double sD, boolean itemOOSforAtleastOneTS) {
		String oos = "N";
		if(expQ!=null && summaryDaily.totalQty >= 0){
			
			
			if(itemOOSforAtleastOneTS){
			if(((summaryDaily.totalQty < OOS_DAY_LEVEL_MOVEMENT_FACTOR * Math.round(expQ * summaryDaily.totalCustVisitCount)) &&
				(Math.round(expQ * summaryDaily.totalCustVisitCount) - summaryDaily.totalQty) > Math.round(1 * sD * summaryDaily.totalCustVisitCount)	)
					&& (Math.round(expQ * summaryDaily.totalCustVisitCount) - summaryDaily.totalQty) > OSS_DAY_LEVEL_MIN_QUANTITY_DIFF ){
				oos = "Y";
			}
			}
			
		
			
		}
		return oos;
	}

	
	private Hashtable<Integer, Double> getNdayExpectation(String expType, String analysisDate,int itemCode, String saleInd,float price, List<Integer> nDayCount, Hashtable<Integer,Double> stdD, int daysMoved, int daysToIgnore, String displayType, boolean useAd) 
	{
		Hashtable<Integer, Double> exp = new Hashtable<Integer, Double>();
		Hashtable<Integer,Double> qtyTotal= new Hashtable<Integer,Double>();
		HashSet<String> uDays = new HashSet<String>();
		try {
			CachedRowSet movement = null;
			
			if(useAd){
				if(expType.compareToIgnoreCase(OOS_METHOD_7DAY)==0){
					movement  = ossCalcDao.getPrevNdayMovementWithAd(conn, 7, scheduleId,dow,analysisDate, strId,itemCode,saleInd, price,displayType,calendarWeekId);
				}
				if(expType.compareToIgnoreCase(OOS_METHOD_7DAY_AVG)==0){
					movement  = ossCalcDao.get42dayAvgMovement(conn, scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
				}
				if(expType.compareToIgnoreCase(OOS_METHOD_28DAY)==0){
					movement  = ossCalcDao.getPrevNdayMovementWithAd(conn, 28,scheduleId,dow,analysisDate, strId,itemCode,saleInd, price,displayType,calendarWeekId);
				}
				if(expType.compareToIgnoreCase(OOS_METHOD_180DAY)==0){
					movement  = ossCalcDao.getPrevNdayMovementWithAd(conn, 120,scheduleId,dow,analysisDate, strId,itemCode,saleInd, price,displayType,calendarWeekId);
				}
				
			}else{
				if(expType.compareToIgnoreCase(OOS_METHOD_7DAY)==0){
					movement  = ossCalcDao.getPrevNdayMovement(conn, 7, scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
				}
				if(expType.compareToIgnoreCase(OOS_METHOD_7DAY_AVG)==0){
					movement  = ossCalcDao.get42dayAvgMovement(conn, scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
				}
				if(expType.compareToIgnoreCase(OOS_METHOD_28DAY)==0){
					movement  = ossCalcDao.getPrevNdayMovement(conn, 28,scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
				}
				if(expType.compareToIgnoreCase(OOS_METHOD_180DAY)==0){
					movement  = ossCalcDao.getPrevNdayMovement(conn, 120,scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
				}
				
			}

			
			if(movement ==null){
				return exp;
			}
			
			// remove days where it was OOS for more than one timeslot
			Hashtable<String,Integer> oosTS = new Hashtable<String, Integer>();
			while(movement.next()){
				Integer todid = movement.getInt(1);
				Integer dowid= movement.getInt(2);
				Integer scheduleId= movement.getInt(5);
				String key = scheduleId+":"+dowid;
  				String oosInd = movement.getString(6);
				if(todid !=4 && oosInd.compareToIgnoreCase("Y")==0){
					if(oosTS.containsKey(key)){
						oosTS.put(key, oosTS.get(key)+1);
					}else{
					oosTS.put(key, 1);
					}
				}
			}
			// Variables to deal with adjustment of Day level Exp when partial day is considered
			// for schedule and dowId totalt he visits and quantities separately for daylevel timeslot (4)
			// and for timeslots following the analysis time.
			
			Hashtable <String,Double> qtoIgnore = new Hashtable <String,Double>();
			Hashtable <String,Integer> vtoIgnore = new Hashtable <String,Integer>();
			Hashtable <String,Double> q4 = new Hashtable <String,Double>();
			Hashtable <String,Integer> v4 = new Hashtable <String,Integer>();
			
			
			movement.beforeFirst();
			Hashtable<Integer,List<Double>> ratioHash = new Hashtable<Integer,List<Double>>();
			int oosTScount =0;
			while( movement.next()){
				Integer todid = movement.getInt(1);
				Integer dowid= movement.getInt(2);
				Double qty = movement.getDouble(3);
				Integer tvc = movement.getInt(4);
				Integer scheduleId= movement.getInt(5);
				String key = scheduleId+":"+dowid;
				// for adjustment
				if(adjustForPartialDay){
					if(todid == 4){
						q4.put(key, qty);
						v4.put(key, tvc);
					}
					if(todid > uptoTS && todid <=10){
						if(qtoIgnore.containsKey(key)){
							qtoIgnore.put(key, qtoIgnore.get(key)+qty);
						}else{
							qtoIgnore.put(key, qty);
						}
						if(vtoIgnore.containsKey(key)){
							vtoIgnore.put(key, vtoIgnore.get(key)+tvc);
						}else{
							vtoIgnore.put(key, tvc);
						}
						
					}
				}
				
				if(oosTS.containsKey(key) && oosTS.get(key)>1){
					continue;
				}
				uDays.add(scheduleId+":"+dowid);
				if(qtyTotal.containsKey(todid)){
					qtyTotal.put(todid, qtyTotal.get(todid)+qty/tvc);
				}else{
					qtyTotal.put(todid, qty/tvc);
				}
				if(ratioHash.containsKey(todid)){
					ratioHash.get(todid).add(qty/tvc);
				}else{
					List<Double> l = new ArrayList<Double>();
					l.add(qty/tvc);
					ratioHash.put(todid, l);
				}
			}
			
			Enumeration<Integer> oosTSEnum = oosTS.elements();
			while(oosTSEnum.hasMoreElements()){
				Integer i = oosTSEnum.nextElement();
				if(i>1){
					oosTScount++;
				}
			}
			int dayCount = uDays.size();
			int obs =0;
			if(expType.compareToIgnoreCase(OOS_METHOD_7DAY_AVG)==0){
				dayCount = 42 - oosTScount - daysToIgnore;
				obs = dayCount + oosTScount;
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_7DAY)==0){
				dayCount = 7 - oosTScount - daysToIgnore;
				obs = dayCount + oosTScount;
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_28DAY)==0){
				dayCount = 28 - oosTScount - daysToIgnore;
				obs = dayCount + oosTScount;
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_180DAY)==0){
				dayCount = 120 - oosTScount - daysToIgnore;
				obs = dayCount + oosTScount;
			}
			nDayCount.clear();
			nDayCount.add(obs);
			nDayCount.add(dayCount);
			stdD.clear();

			if(dayCount ==0){
				return exp;
			}
			
			
			
			Enumeration<Integer> qtyE = qtyTotal.keys();
			while(qtyE.hasMoreElements()){
				Integer i = qtyE.nextElement();
				exp.put(i, qtyTotal.get(i)/dayCount);

				if(qtyTotal.get(i)!=null && dayCount > 1){
					Double mean = qtyTotal.get(i)/dayCount;
					Double dSum = 0.0;
					List<Double> l  = ratioHash.get(i);
					if(l!=null){
					for(Double r:l){
						dSum = dSum + (r-mean)*(r-mean);
					}

						stdD.put(i,Math.sqrt(dSum/(dayCount-1)));
					}
			}
			
			
			}
			
			if(adjustForPartialDay){
				Enumeration<String> sKeys = q4.keys();
				Double qtyTotal4 =0.0;
				while(sKeys.hasMoreElements()){
					String skey = sKeys.nextElement();
					Double qI = 0.0;
					Integer vI =0;
					Double qt = q4.get(skey);
					Integer vt = v4.get(skey);
					if(qtoIgnore.containsKey(skey)){
						qI = qtoIgnore.get(skey);
					}
					if(vtoIgnore.containsKey(skey)){
						vI = vtoIgnore.get(skey);
					}
					qtyTotal4 = qtyTotal4 + (qt-qI)/(vt-vI);
				}
				
				exp.put(4,qtyTotal4/dayCount);
			}

			
		} catch (GeneralException ge) {
			logger.error("Error loading last N day expectation:"+ ge.getMessage());
		} catch(SQLException se){
			logger.error("Error loading last N day expectation:"+ se.getMessage());
		} catch(Exception e){
			e.printStackTrace();
			logger.error(qtyTotal);
//			throw e;
		}
		return exp;
		
	}


	private Hashtable<Integer, Double> getNdayExpectationIgnoreVisits(String expType, String analysisDate,int itemCode, String saleInd,float price, List<Integer> nDayCount, Hashtable<Integer,Double> stdD, int daysMoved, int daysToIgnore) {
		Hashtable<Integer, Double> exp = new Hashtable<Integer, Double>();
		Hashtable<Integer,Double> qtyTotal= new Hashtable<Integer,Double>();
		HashSet<String> uDays = new HashSet<String>();
		try {
			CachedRowSet movement = null;
			if(expType.compareToIgnoreCase(OOS_METHOD_7DAY)==0){
				movement  = ossCalcDao.getPrevNdayMovement(conn, 7, scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_7DAY_AVG)==0){
				movement  = ossCalcDao.get42dayAvgMovement(conn, scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_28DAY)==0){
				movement  = ossCalcDao.getPrevNdayMovement(conn, 28,scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_180DAY)==0){
				movement  = ossCalcDao.getPrevNdayMovement(conn, 120,scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
			}
			
			if(movement ==null){
				return exp;
			}
			
			// remove days where it was OOS for more than one timeslot
			Hashtable<String,Integer> oosTS = new Hashtable<String, Integer>();
			while(movement.next()){
				Integer todid = movement.getInt(1);
				Integer dowid= movement.getInt(2);
				Integer scheduleId= movement.getInt(5);
				String key = scheduleId+":"+dowid;
  				String oosInd = movement.getString(6);
				if(todid !=4 && oosInd.compareToIgnoreCase("Y")==0){
					if(oosTS.containsKey(key)){
						oosTS.put(key, oosTS.get(key)+1);
					}else{
					oosTS.put(key, 1);
					}
				}
			}
			movement.beforeFirst();
			Hashtable<Integer,List<Double>> ratioHash = new Hashtable<Integer,List<Double>>();
			int oosTScount =0;
			while( movement.next()){
				Integer todid = movement.getInt(1);
				Integer dowid= movement.getInt(2);
				Double qty = movement.getDouble(3);
				Integer tvc = 1;  //   movement.getInt(4);
				Integer scheduleId= movement.getInt(5);
				String key = scheduleId+":"+dowid;
				if(oosTS.containsKey(key) && oosTS.get(key)>1){
					continue;
				}
				uDays.add(scheduleId+":"+dowid);
				if(qtyTotal.containsKey(todid)){
					qtyTotal.put(todid, qtyTotal.get(todid)+qty/tvc);
				}else{
					qtyTotal.put(todid, qty/tvc);
				}
				if(ratioHash.containsKey(todid)){
					ratioHash.get(todid).add(qty/tvc);
				}else{
					List<Double> l = new ArrayList<Double>();
					l.add(qty/tvc);
					ratioHash.put(todid, l);
				}
			}
			
			Enumeration<Integer> oosTSEnum = oosTS.elements();
			while(oosTSEnum.hasMoreElements()){
				Integer i = oosTSEnum.nextElement();
				if(i>1){
					oosTScount++;
				}
			}
			int dayCount = uDays.size();
			int obs =0;
			if(expType.compareToIgnoreCase(OOS_METHOD_7DAY_AVG)==0){
				dayCount = 42 - oosTScount - daysToIgnore;
				obs = dayCount + oosTScount;
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_7DAY)==0){
				dayCount = 7 - oosTScount - daysToIgnore;
				obs = dayCount + oosTScount;
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_28DAY)==0){
				dayCount = 28 - oosTScount - daysToIgnore;
				obs = dayCount + oosTScount;
			}
			if(expType.compareToIgnoreCase(OOS_METHOD_180DAY)==0){
				dayCount = 120 - oosTScount - daysToIgnore;
				obs = dayCount + oosTScount;
			}
			nDayCount.clear();
			nDayCount.add(obs);
			nDayCount.add(dayCount);
			stdD.clear();

			if(dayCount ==0){
				return exp;
			}
			Enumeration<Integer> qtyE = qtyTotal.keys();
			while(qtyE.hasMoreElements()){
				Integer i = qtyE.nextElement();
				exp.put(i, qtyTotal.get(i)/dayCount);

				if(qtyTotal.get(i)!=null && dayCount > 1){
					Double mean = qtyTotal.get(i)/dayCount;
					Double dSum = 0.0;
					List<Double> l  = ratioHash.get(i);
					if(l!=null){
					for(Double r:l){
						dSum = dSum + (r-mean)*(r-mean);
					}

						stdD.put(i,Math.sqrt(dSum/(dayCount-1)));
					}
			}
			
			
			}
			
		} catch (GeneralException ge) {
			logger.error("Error loading last N day expectation:"+ ge.getMessage());
		} catch(SQLException se){
			logger.error("Error loading last N day expectation:"+ se.getMessage());
		} catch(Exception e){
			e.printStackTrace();
			logger.error(qtyTotal);
//			throw e;
		}
		return exp;
		
	}

	

	
	private Hashtable<Integer, Double> getMape(int itemCode) {
		Hashtable<Integer, Double> exp = new Hashtable<Integer, Double>();
		try {
			CachedRowSet movement = ossCalcDao.getMapeScore(conn, strId,itemCode);

			while( movement.next()){
				Integer todid = movement.getInt(1);
				Double mape = movement.getDouble(2);
					exp.put(todid, mape);
				
			}	
			
		} catch (GeneralException ge) {
			logger.debug("Error loading Mape:"+ ge.getMessage());
		} catch(SQLException se){
			logger.debug("Error loading Mape:"+ se.getMessage());
		}catch(Exception e){
			logger.debug("Error loading Mape:"+ e.getMessage());
		}
		return exp;
		
	}

	
	private Hashtable<Integer, Double> getPrevdayExpectation(String analysisDate,int itemCode, String saleInd,float price, String displayType, boolean withAd) {
		Hashtable<Integer, Double> exp = new Hashtable<Integer, Double>();
		try {
			
			CachedRowSet movement =null;
			if(!withAd){
			movement= ossCalcDao.getPrevdayMovement(conn, scheduleId,dow,analysisDate, strId,itemCode,saleInd, price);
			}else{
			movement = ossCalcDao.getPrevdayMovementWithAd(conn, scheduleId,dow,analysisDate, strId,itemCode,saleInd, price, displayType,calendarWeekId);
			}

			while( movement.next()){
				Integer todid = movement.getInt(1);
				Double qty = movement.getDouble(3);
				Integer tvc = movement.getInt(4);
					exp.put(todid, qty/tvc);
				
			}	
			
		} catch (GeneralException ge) {
			logger.error("Error loading Prev day expectation:"+ ge.getMessage());
		} catch(SQLException se){
			logger.error("Error loading Prev day expectation:"+ se.getMessage());
		}
		return exp;
		
	}


	
	private void classifyOOS(List<OOSSummaryDTO> oosSummaryList) {
		// TODO Auto-generated method stub
		int ctr = 0;
		OOSSummaryDTO prev = null;
		boolean allDayLow = true;
		int size = oosSummaryList.size()-1; // ignoring the last item(daily summary)
		for(int i = 0;i<size;i++){
			OOSSummaryDTO s = oosSummaryList.get(i);
			OOSSummaryDTO p = null;
			OOSSummaryDTO n = null;
			if(i>0){
				p = oosSummaryList.get(i-1);
			}
			if(i<size-1){
				n = oosSummaryList.get(i+1);
			}
			// check 2WS
			if(s.ossInd.compareToIgnoreCase("Y")==0 && s.totalQty==0){
				if((p!=null && p.ossInd.compareToIgnoreCase("Y")==0 && p.totalQty==0) ||
						(n!=null && n.ossInd.compareToIgnoreCase("Y")==0 && n.totalQty==0)){
							s.ossClassification = OOS_CLASSIFICATION_2_WS;
							s.ossScore = s.ossScore + 2;
							continue;
				}
			}
			// check 3WS
			if(s.ossInd.compareToIgnoreCase("Y")==0 && s.totalQty == 0){
				if((p!=null && p.ossInd.compareToIgnoreCase("Y")==0 && p.totalQty>0) ||
						(n!=null && n.ossInd.compareToIgnoreCase("Y")==0 && n.totalQty>0)){
							s.ossClassification = OOS_CLASSIFICATION_3_WS;
							s.ossScore = s.ossScore + 1;
							continue;
				}
				
			}
			//or
			if(s.ossInd.compareToIgnoreCase("Y")==0 && s.totalQty > 0){
				if((p!=null && p.ossInd.compareToIgnoreCase("Y")==0 && p.totalQty>=0) ||
						(n!=null && n.ossInd.compareToIgnoreCase("Y")==0 && n.totalQty>=0)){
							s.ossClassification = OOS_CLASSIFICATION_3_WS;
							s.ossScore = s.ossScore + 1;
							continue;
				}
				
			}
			
			// check 4WS
			if(s.ossInd.compareToIgnoreCase("Y")==0 && s.totalQty >= 0){
				if((p!=null && p.ossInd.compareToIgnoreCase("Y")==0 ) &&
						(n!=null && n.ossInd.compareToIgnoreCase("Y")==0 )){
							s.ossClassification = OOS_CLASSIFICATION_4_WS;
				}
				
			}
			
			
		}
		// check 1WS
		OOSSummaryDTO s =oosSummaryList.get(size); 
		if(s.ossInd.compareToIgnoreCase("Y")==0 && s.totalQty == 0){
			for(OOSSummaryDTO s1:oosSummaryList){
				s1.checkAndSetOOSclassification(OOS_CLASSIFICATION_1_WS);
				s1.ossScore = s1.ossScore + 3;
			}
		}
		
	}


	private void updateSaleInfo(ArrayList <MovementDTO> movementList, CompetitiveDataDTO compData ){
		float saleQty = 0;
		float saleRev = 0;
		float regQty = 0;
		float regRev = 0;
		for( int i = 0; i < movementList.size(); i++ ){
			MovementDTO movementObj = movementList.get(i);
			if( movementObj.isSaleFlag() ){
				saleQty += movementObj.getExtnQty() + movementObj.getExtnWeight();
				saleRev += movementObj.getItemNetPrice();
			}
				
				regQty += movementObj.getExtnQty() + movementObj.getExtnWeight();
				regRev += movementObj.getExtendedGrossPrice();

		}
		if( saleQty > 0 ){
			compData.saleInd = "Y";
			compData.fSalePrice = NumberUtil.RoundFloat(saleRev/saleQty, 2);
		}
		if( regQty > 0){
			compData.regPrice = NumberUtil.RoundFloat(regRev/regQty, 2);
		}
		if((compData.regPrice - compData.fSalePrice)< .05){
			compData.saleInd = "N";
			compData.fSalePrice =0;
		}
    	

	}
	private OOSSummaryDTO computeOOSStats(int familyItemCode, ArrayList<MovementDTO> movementList, String startPeriod, 
			String endPeriod, int todId) throws GeneralException 
	{
		// TODO Auto-generated method stub
		
		/*	
		find the start and End index matching the period
		*/ 
		int startIndex = -1;
		int endIndex = -1;
		
		Date startPeriodDate =  DateUtil.toDate(startPeriod, OSS_DATE_TIME_FORMAT);
		Date endPeriodDate =  DateUtil.toDate(endPeriod, OSS_DATE_TIME_FORMAT);

		float dailyQty = 0; 
		OOSSummaryDTO oosSummary= buildOOSSummary(familyItemCode, todId);

		for( int i = 0; i < movementList.size(); i++ ){
			MovementDTO movementObj = movementList.get(i);
			String timeStamp = movementObj.getTranTimeStamp();
			if( isTimeWithInLastHourInTS( timeStamp, startPeriodDate, endPeriodDate) ) {
				oosSummary.movementInLastHour = true;
//				oosSummary.lastMovementTS = timeStamp;
			}
			if( isTimeWithInRange( timeStamp, startPeriodDate, endPeriodDate) ) {
				if( startIndex == -1 ){
					startIndex = i;
					endIndex = i;
				}else{
					endIndex = i;
				}
			}
			
			dailyQty += movementObj.getExtnQty() + movementObj.getExtnWeight();

		}
		
		
		oosSummary.dailyQty = dailyQty;
		if( startIndex > -1){
			for( int i = startIndex; i <=endIndex; i++ ){
				MovementDTO movementObj = movementList.get(i);
				if( movementObj.isSaleFlag() ){
					oosSummary.saleQty += movementObj.getExtnQty() + movementObj.getExtnWeight();
					oosSummary.saleRev += movementObj.getItemNetPrice();
					
	
				}else{
					oosSummary.regQty += movementObj.getExtnQty() + movementObj.getExtnWeight();
					oosSummary.regRev += movementObj.getItemNetPrice();
					
				}
					

				oosSummary.visitCount++;
				
			}
			oosSummary.totalQty = oosSummary.regQty + oosSummary.saleQty;
			oosSummary.totalRev = oosSummary.regRev + oosSummary.saleRev;
			
			int movementListLen = endIndex-startIndex+1;
			if( movementListLen > 1 ){
				int [] timeDurationArr = new int [movementListLen-1];
				int minDuration = 0;
				int maxDuration = 0;
				int totalDuration = 0;
				int j = 0;
				for (  int i = startIndex; i < endIndex; i++, j++ ){
					MovementDTO movementCurrent = movementList.get(i);
					MovementDTO movementNext = movementList.get(i+1);
					int actDuration = findDuration( movementCurrent.getTranTimeStamp(), movementNext.getTranTimeStamp());
					totalDuration += actDuration;
					timeDurationArr[j] = actDuration;
					if( minDuration == 0 ){
						minDuration = actDuration;
						maxDuration = actDuration;
					}else{
						if( actDuration < minDuration)
							minDuration = actDuration;
						else if ( actDuration > maxDuration)
							maxDuration = actDuration;
					}
				}
				oosSummary.minDurationBtwVisit = minDuration;
				oosSummary.maxDurationBtwVisit = maxDuration;
				oosSummary.avgDurationBtwVisit = totalDuration/timeDurationArr.length;
				//calculate the 95% values
				//Sort the array
				Arrays.sort(timeDurationArr);
				int min2point5Index = (int)(timeDurationArr.length * 2.5)/ 100;
				
				int max2point5Index = timeDurationArr.length - min2point5Index;
				
				if( min2point5Index > 0 && max2point5Index < timeDurationArr.length){
				    oosSummary.min95PctDurationBtwVisit=timeDurationArr[min2point5Index];
				    oosSummary.max95PctDurationBtwVisit=timeDurationArr[max2point5Index];				
				}
				
			}


			
			double totalQty = 0;
			int qtyArr [] = new int [movementListLen];
			
			for ( int i = startIndex, j = 0; i<=endIndex; i++, j++ ){
		    	MovementDTO movementObj = movementList.get(i);
		    	
		    	if( movementObj.getExtnQty() > 0 ){
		    		totalQty += movementObj.getExtnQty();
		    		qtyArr[j] = (int)movementObj.getExtnQty();
		    	}
		    	else{
		    		totalQty += movementObj.getExtnWeight();
		    		qtyArr[j] = (int)movementObj.getExtnWeight();
		    	}
		    	if( qtyArr[j] > oosSummary.maxQtyPerVisit)
		    		oosSummary.maxQtyPerVisit = qtyArr[j];
				
			}
			
		    oosSummary.avgQtyPerVisit = (float)totalQty/movementListLen;
		    int [] modeArr = NumberUtil.mode(qtyArr);
		    oosSummary.modalQtyPerVisit= modeArr[0];
		    oosSummary.modal2QtyPerVisit= modeArr[1];
		    oosSummary.print(logger);
		    

			
		}
		
		
		return oosSummary;
		//Insert the object 
		
		
	}


	private int findDuration(String tranTimeStampCurrent, String tranTimeStampNext) throws GeneralException {
		// TODO Auto-generated method stub
		Date posCurrentDate = DateUtil.toDate(tranTimeStampCurrent, OSS_DATE_TIME_FORMAT);
		Date posNextDate = DateUtil.toDate(tranTimeStampNext, OSS_DATE_TIME_FORMAT);
	
		long currentPeriodms = posCurrentDate.getTime();
		long nextPeriodms = posNextDate.getTime();
	
		int durationInMin = (int) (nextPeriodms - currentPeriodms)/ ( 1000* 60);
		
		return durationInMin;
	}


	private OOSSummaryDTO buildOOSSummary(int familyItemCode, int todId) {

		OOSSummaryDTO ossSummaryEmpty = new OOSSummaryDTO();
		ossSummaryEmpty.scheduleId = scheduleId;
		ossSummaryEmpty.dayofweekId = dow;
		ossSummaryEmpty.storeId = strId;
		ossSummaryEmpty.specialEventId =0;
		ossSummaryEmpty.specialFactId =0;
		ossSummaryEmpty.timeOfDayId = todId;
		ossSummaryEmpty.itemCode = familyItemCode;
		
		ossSummaryEmpty.totalQty =0;
		ossSummaryEmpty.regQty =0;
		ossSummaryEmpty.saleQty =0;
		ossSummaryEmpty.totalRev = 0;
		ossSummaryEmpty.regRev = 0;
		ossSummaryEmpty.saleRev = 0;
		ossSummaryEmpty.visitCount =0;
		
		ossSummaryEmpty.minDurationBtwVisit = 0;
		ossSummaryEmpty.maxDurationBtwVisit = 0;
		ossSummaryEmpty.avgDurationBtwVisit = 0;
		ossSummaryEmpty.avgQtyPerVisit = 0;
		ossSummaryEmpty.modalQtyPerVisit= 0;
		ossSummaryEmpty.modal2QtyPerVisit= 0;
		ossSummaryEmpty.min95PctDurationBtwVisit=0;
		ossSummaryEmpty.max95PctDurationBtwVisit=0;
		
		ossSummaryEmpty.movVisitRatio = 0;
		ossSummaryEmpty.unitRegPrice = 0;
		ossSummaryEmpty.unitSalePrice = 0;
		ossSummaryEmpty.maxQtyPerVisit = 0;
		ossSummaryEmpty.margin = 0;
		
		ossSummaryEmpty.marginOpp = 0;
		ossSummaryEmpty.revOpp = 0;
		return ossSummaryEmpty;
	}


	private boolean isTimeWithInRange(String posTimeStamp, Date startPeriod, Date endPeriod) throws GeneralException {
		// TODO Auto-generated method stub
		
		boolean retVal = false;
		Date posDate = DateUtil.toDate(posTimeStamp, OSS_DATE_TIME_FORMAT);
		long startPeriodms = startPeriod.getTime();
		long endPeriodms = endPeriod.getTime();
		long posPeriodms = posDate.getTime();
		if( (posPeriodms - startPeriodms) >= 0 &&  (endPeriodms - posPeriodms) > 0 )
			retVal = true;
		return retVal;
	}

	private boolean isTimeWithInLastHourInTS(String posTimeStamp, Date startPeriod, Date endPeriod) throws GeneralException {
		
		boolean retVal = false;
		Date posDate = DateUtil.toDate(posTimeStamp, OSS_DATE_TIME_FORMAT);
		long endPeriodms = endPeriod.getTime();
		long startPeriodms = endPeriodms - (3600000 * 2);  // Looking for TWO ours of no movement
		long posPeriodms = posDate.getTime();
		if( (posPeriodms - startPeriodms) >= 0 &&  (endPeriodms - posPeriodms) > 0 )
			retVal = true;
		return retVal;
	}

	

	private String setupPeriod( String analysisDate, int dateIncrementFactor, String timeOfDay) throws GeneralException {
		String period = analysisDate;
		
		if ( dateIncrementFactor != 0){
			Date tempDate = DateUtil.toDate(analysisDate);
			Date tempDate2 =  DateUtil.incrementDate(tempDate, dateIncrementFactor);
			period = DateUtil.dateToString(tempDate2, Constants.APP_DATE_FORMAT);
		}
		period = period + " " + timeOfDay;
		return period;
	}
	
	//Get the start date, end date and using Store Id/Store num we can find the scheduleId
	private int getScheduleId( Connection conn, int strId, String analysisDate) throws GeneralException{
		CompetitiveDataDTO compData = new  CompetitiveDataDTO ();
		CompetitiveDataDAO compDataDao = new CompetitiveDataDAO(conn);
		compData.compStrId = strId;
		compData.checkDate = analysisDate;
		RDSDataLoad.setupWeekStartEndDate(compData);
		return compDataDao.getScheduleID(conn, compData);
	}
	

	
}


