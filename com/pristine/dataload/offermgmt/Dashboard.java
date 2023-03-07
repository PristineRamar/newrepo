package com.pristine.dataload.offermgmt;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.LocationCompetitorMapDAO;
import com.pristine.dao.ProductGroupDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.DashboardDAO;
import com.pristine.dao.offermgmt.PricingEngineDAO;
import com.pristine.dao.pricingalert.PricingAlertDAO;
import com.pristine.dto.ItemDTO;
import com.pristine.dto.RetailCalendarDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.offermgmt.DashboardDTO;
import com.pristine.dto.offermgmt.LocationKey;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.ProductDTO;
import com.pristine.dto.offermgmt.PRStrategyDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.service.RetailCostServiceOptimized;
import com.pristine.service.offermgmt.ItemKey;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.PricingEngineService;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRConstants;

public class Dashboard {
	
	private static Logger logger = Logger.getLogger("Dashboard");

	private Connection conn = null;

	private String currentDateStr = null;
	private HashMap<String, Integer> calendarIdMap = new HashMap<String, Integer>();
	private RetailCalendarDAO calDAO = null;
	private DashboardDAO dao = null;
	private LocationCompetitorMapDAO locCompMapDAO = null;
	private String chainId = null;

	private PricingEngineDAO pricingEngineDAO = null;
	
	private boolean isCompChangeCntReqd = false;
	private boolean isCostChangeCntReqd = false;
	private boolean isNewItemCntReqd = false;
	private boolean isStrategyIdReqd = false;
	private boolean isRevenueReqd = false;
	private boolean isItemCntReqd = false;
	private boolean isUpdateDashboardFromRecTable = false;
	
	private static final String UPD_STRATEGY = "UPD_STRATEGY";
	private static final String UPD_COMP_CHG_CNT = "UPD_COMP_CHG_CNT";
	private static final String UPD_COST_CHG_CNT = "UPD_COST_CHG_CNT";
	private static final String UPD_NEW_ITEM_CNT = "UPD_NEW_ITEM_CNT";
	private static final String UPD_ITEM_CNT = "UPD_ITEM_CNT";
	private static final String UPD_REVENUE = "UPD_REVENUE";
	private static final String DASHBOARD_DATE = "DATE=";
	private static final String ZONE_ID = "ZONE_ID=";
	private static final String PRODUCT_LEVEL_ID = "PRODUCT_LEVEL_ID=";
	private static final String PRODUCT_ID = "PRODUCT_ID=";
	
	private int locationLevelId = -1;
	private int locationId = -1;
	private int productLevelId = -1;
	private int productId = -1;
	
	private HashMap<Integer, ItemDTO> itemMap = null;
	
	public Dashboard(){
		PropertyConfigurator.configure("log4j-pr-reg-dashboard.properties");
		//PropertyManager.initialize("analysis.properties");
		PropertyManager.initialize("recommendation.properties");
		try {
			conn = DBManager.getConnection();
		} catch (GeneralException exe) {
			logger.error("Error while connecting to DB:" + exe);
			System.exit(1);
		}
		calDAO = new RetailCalendarDAO();
		dao = new DashboardDAO(conn);
		pricingEngineDAO = new PricingEngineDAO();
		locCompMapDAO = new LocationCompetitorMapDAO();
		itemMap = new HashMap<Integer, ItemDTO>();
	}
	
	public Dashboard(Connection conn){
		this.conn = conn;
		calDAO = new RetailCalendarDAO();
		dao = new DashboardDAO(conn);
		pricingEngineDAO = new PricingEngineDAO();
	}
	
	public static void main(String[] args) {
		Dashboard d = new Dashboard();
		boolean isUpdateParameterPassed = false;
		String dashboardDate = null;
		
		if(args.length > 0){
			for(String arg : args){
				if(arg.startsWith(UPD_STRATEGY)){
					d.setStrategyIdReqd(true);
					isUpdateParameterPassed = true;
				}else if(arg.startsWith(UPD_COMP_CHG_CNT)){
					d.setCompChangeCntReqd(true);
					isUpdateParameterPassed = true;
				}else if(arg.startsWith(UPD_COST_CHG_CNT)){
					d.setCostChangeCntReqd(true);
					isUpdateParameterPassed = true;
				}else if(arg.startsWith(UPD_NEW_ITEM_CNT)){
					d.setNewItemCntReqd(true);
					isUpdateParameterPassed = true;
				}else if(arg.startsWith(UPD_ITEM_CNT)){
					d.setItemCntReqd(true);
					isUpdateParameterPassed = true;
				}else if(arg.startsWith(UPD_REVENUE)){
					d.setRevenueReqd(true);
					isUpdateParameterPassed = true;
				}else if(arg.startsWith(DASHBOARD_DATE)){
					dashboardDate = arg.substring(DASHBOARD_DATE.length());
				}else if(arg.startsWith(ZONE_ID)){
					d.setLocationLevelId(Constants.ZONE_LEVEL_ID);
					d.setLocationId(Integer.parseInt(arg.substring(ZONE_ID.length())));
				}else if(arg.startsWith(PRODUCT_LEVEL_ID)){
					d.setProductLevelId(Integer.parseInt(arg.substring(PRODUCT_LEVEL_ID.length())));
				}else if(arg.startsWith(PRODUCT_ID)){
					d.setProductId(Integer.parseInt(arg.substring(PRODUCT_ID.length())));
				}else if(arg.startsWith("UPD_DASHBOARD_FROM_LATEST_REC")){
					d.setIsUpdateDashboardFromRecTable(true);
				}
			}
		}
		
		if (d.getIsUpdateDashboardFromRecTable()) {
			d.updateDashboardFromRecTable();
		} else {
			// Update all attributes when no update parameters are passed
			if (!isUpdateParameterPassed) {
				d.setStrategyIdReqd(true);
				d.setCompChangeCntReqd(true);
				d.setCostChangeCntReqd(true);
				d.setNewItemCntReqd(true);
				d.setItemCntReqd(true);
				d.setRevenueReqd(true);
			}

			// Sets the date for which dashboard batch has to run
			d.setCurrentDate(dashboardDate);

			// Populate dashboard data
			try {
				d.populateDashboardData();
			} catch (OfferManagementException e) {
				e.printStackTrace();
				logger.error(e.toString(), e);
				logger.error("Error in populateDashboardData()");
			}
		}
	}
	
	public void updateDashboardFromRecTable() {
		PricingEngineService pricingEngineService = new PricingEngineService();
		PricingEngineDAO pricingEngineDAO = new PricingEngineDAO();
		try {
			Long latestRecRunId = 0l;
			latestRecRunId = pricingEngineDAO.getLatestRecommendationRunId(conn, getLocationLevelId(),
					getLocationId(), getProductLevelId(), getProductId());
			if (getLocationLevelId() > 0 && getLocationId() > 0 && getProductLevelId() > 0 && getProductId() > 0
					&& latestRecRunId > 0) {
				logger.info("Dashboard running for : Location Level Id: " + getLocationLevelId() + " ,Location Id: "
						+ getLocationId() + " ,Product Level Id: " + getProductLevelId() + " ,Product Id: "
						+ getProductId() + " ,Rec Run Id: " + latestRecRunId);
				pricingEngineService.updateDashboard(conn, getLocationLevelId(), getLocationId(), getProductLevelId(),
						getProductId(), latestRecRunId);
				PristineDBUtil.commitTransaction(conn, "Dashboard commit");
			} else {
				logger.error("Dashboard update failed. Please make sure following parameters have proper values "
						+ " ZONE_ID, PRODUCT_LEVEL_ID, PRODUCT_ID");
			}
		} catch (OfferManagementException | GeneralException e) {
			logger.debug("Error in updateDashboardFromRecTable()");
			PristineDBUtil.rollbackTransaction(conn, "Dashboard Rollback");
		}
	}
	
	/**
	 * Sets date for which dashboard program has to run
	 * @param dashboardDate		Sets specific date so that dashboard program can run for that date. Sets current date if it is passed as null
	 */
	public void setCurrentDate(String dashboardDate){
		try{
			String date = null;
			if(dashboardDate != null)
				date = DateUtil.dateToString(DateUtil.toDate(dashboardDate, Constants.APP_DATE_FORMAT), Constants.APP_DATE_FORMAT);
			else
				date = DateUtil.dateToString(DateUtil.toDate(DateUtil.now(), DateUtil.DATE_FORMAT_NOW), Constants.APP_DATE_FORMAT);
			
			setCurrentDateStr(date);
			logger.info("Dashboard Date is - " + date);
		}catch(GeneralException ge){
			logger.error("Error when setting dashboard date - " + ge.getMessage());
			logger.error("Date needs to be mentioned in MM/dd/yyyy format");
			System.exit(0);
		}
	}
	
	/**
	 * Populates data in dashboard table
	 * @throws OfferManagementException 
	 */
	public void populateDashboardData() throws OfferManagementException{
		RetailPriceZoneDAO rpzDAO = new RetailPriceZoneDAO();
		PricingAlertDAO paDAO = new PricingAlertDAO(); 
		ArrayList<DashboardDTO> insertList = new ArrayList<DashboardDTO>();
		ArrayList<DashboardDTO> updateList = new ArrayList<DashboardDTO>();
		try{
			// Retrieve existing dashboard data (To check if data needs to be inserted/updated in dashboard table
			HashMap<String, DashboardDTO> dashboardData = dao.getDashboardData();
						
			// Retrieves list of locations for which program has to run
			ArrayList<Integer> locationList = getLocationList();
			
			// Retrives list of products for which program has to run
			ArrayList<Integer> productList = getProductList();
						
			// Retrieves division id for every location
			HashMap<Integer, Integer> zoneDivisionMap = getZoneDivisionMap(locationList);
			
			// Retrieves all strategies active as on input date/current date
			HashMap<String, HashMap<String, Integer>> strategyMap = dao.getStrategies(getCurrentDateStr());
			logger.info("Size of strategy map - " + strategyMap.size());
			
			//  Retrieves parent level hierarchy for each category (Used in determining strategy assigned) 
			HashMap<Integer, ProductDTO> categoryHierarchy = dao.getCategoryHierarchy(0);
			logger.info("Size of category hierarchy - " + categoryHierarchy.size());
			chainId = new RetailPriceDAO().getChainId(conn);
			
			boolean getLastWeekRevenue = false;
			String inputDate = DateUtil.getWeekStartDate(DateUtil.toDate(getCurrentDateStr(), Constants.APP_DATE_FORMAT), 0);
			String today = DateUtil.getWeekStartDate(DateUtil.toDate(DateUtil.now(), DateUtil.DATE_FORMAT_NOW), 0);
			if(DateUtil.getDateDiff(DateUtil.toDate(today), DateUtil.toDate(inputDate)) <= 0){
				getLastWeekRevenue = true;
			}else{
				getLastWeekRevenue = false;
			}
			ItemService itemService = new ItemService(null);
			int locationLevelId = PRConstants.ZONE_LEVEL_TYPE_ID;
			int productLevelId = Constants.CATEGORYLEVELID;
			for(Integer locationId : locationList){
				Date currentDate = DateUtil.toDate(getCurrentDateStr());
				
				// Retrives Kvi List for Zone
				int kviListId = rpzDAO.getKviListForZone(conn, locationId);
				logger.info("KVI List Id - " + kviListId);
				// Retrieves List of KVI Items
				ArrayList<Integer> kviList = paDAO.getKVIItems(conn, kviListId);
				// Retrieves zone lists that contains the zone (Used in determining strategy assigned)
				ArrayList<Integer> zoneListId = dao.getLocationListId(locationLevelId, locationId);
				Integer divisionId = zoneDivisionMap.get(locationId);
				
				insertList = new ArrayList<DashboardDTO>();
				updateList = new ArrayList<DashboardDTO>();
				for(Integer productId : productList){
					List<Integer> priceZoneStores = new ArrayList<Integer>();
					List<String> priceAndStrategyZoneNos = new ArrayList<String>();
					String key = locationLevelId + "-" + locationId + "-" + productLevelId + "-" + productId;
					boolean isPriceZone = true;
					
					//If dashboard trying to run for non price zone, just update the active as no in dashboard table,
					//don't proceed further
					isPriceZone = pricingEngineDAO.isPriceZone(conn, productLevelId, productId, locationLevelId, locationId);
					if(!isPriceZone){
						//Update active
						dao.updateActiveStatus(locationLevelId, locationId, productLevelId, productId, String.valueOf(Constants.NO));
						continue;
					}
					
					//Separate query it written to get the store ids instead of in the same query, as the performance is good this way
					priceZoneStores = itemService.getPriceZoneStores(conn, productLevelId, productId, locationLevelId, locationId);
					
					//24th Mar 2017, item recent price and effective date is required as comp and cost change is calculated based on that
					
					PRStrategyDTO inputDTO = new PRStrategyDTO(); 
					List<PRItemDTO> allStoreItems = new ArrayList<PRItemDTO>();
					HashMap<ItemKey, PRItemDTO> itemDataMap = new HashMap<ItemKey, PRItemDTO>();
					RetailCalendarDAO retailCalendarDAO = new RetailCalendarDAO();
					PricingEngineWS pricingEngineWS = new PricingEngineWS();
					HashMap<String, RetailCalendarDTO> allWeekCalendarDetails = new HashMap<String, RetailCalendarDTO>();
					RetailCalendarDTO curCalDTO = null;

					allWeekCalendarDetails = retailCalendarDAO.getAllWeeks(conn);
					
					inputDTO.setLocationLevelId(locationLevelId);
					inputDTO.setLocationId(locationId);
					inputDTO.setProductLevelId(productLevelId);
					inputDTO.setProductId(productId);
					
					RetailCalendarDTO calDTO = pricingEngineWS.getLatestWeek(conn, inputDTO, retailCalendarDAO);
					
					// Week in which the batch runs
					curCalDTO = retailCalendarDAO.getCalendarId(conn, DateUtil.getWeekStartDate(0), Constants.CALENDAR_WEEK);
					
					allStoreItems = itemService.getAuthorizedItemsOfZoneAndStore(conn, inputDTO, priceZoneStores);

					priceAndStrategyZoneNos = itemService.getPriceAndStrategyZoneNos(allStoreItems);

					itemDataMap = itemService.populateAuthorizedItemsOfZone(conn, 0, inputDTO, allStoreItems);
					
					//Get strategy zone ids
//					boolean useProdLocationMapForZones = Boolean.parseBoolean
//							(PropertyManager.getProperty("USE_PRODUCT_LOCATION_FOR_ZONE_STORE_MAP", "FALSE"));
//					if(useProdLocationMapForZones){
//						priceAndStrategyZoneNos = pricingEngineDAO.getPriceZoneNos(conn, productLevelId, productId, locationLevelId,
//								locationId);
//					}else{
//						priceAndStrategyZoneNos = pricingEngineDAO.getPriceAndStrategyZoneNos(conn, productLevelId, productId, locationLevelId,
//								locationId);
//					}
					
					//Get all strategy zones
					
					/* Retrieves Item Map with Item code as key and ItemDTO populated with ret_lir_id as value 
					(This list will be used to populate total item count and different counts at LIG level in dashboard table */
					itemMap = new HashMap<Integer, ItemDTO>();
					itemMap = dao.getAllItems(locationId, productLevelId, productId, priceZoneStores,locationLevelId);
					
					pricingEngineDAO.getPriceDataOptimized(conn, Integer.parseInt(chainId), inputDTO, calDTO, null, 0, itemDataMap,
							null, null, true, priceAndStrategyZoneNos, priceZoneStores);
					
					// Retrieves competitor store info
					HashMap<Integer, LocationKey> compDataMap = locCompMapDAO.getCompetitors(conn, locationLevelId, locationId, productLevelId, productId);
					//NU:: 14th Jun 2017, to show the recent check date in the dashboard table
					String primaryCompRecentCheckDate = null;
					LocationKey compDetail = null;
					if (compDataMap.get(PRConstants.COMP_TYPE_1) != null) {
						compDetail = compDataMap.get(PRConstants.COMP_TYPE_1);
						logger.info("Primary Competitor - " + compDetail.toString());
						//get recent check date
						primaryCompRecentCheckDate = dao.getRecentCompCheckDate(productLevelId, productId, compDetail.getLocationId(),
								priceZoneStores, locationId, locationLevelId);
					} else {
						logger.warn("No primary competitor found for location - " + locationId);
						// continue;
					}
					
					if(dashboardData.get(key) != null){
						logger.info("Processing for key that already exists " + key);
						// Data exists for this combination already
						DashboardDTO dto = dashboardData.get(key);
						String resetDate = null;
						if(dto.getResetDate() != null)
							resetDate = dto.getResetDate();
						else
							resetDate = DateUtil.getWeekStartDate(currentDate, 13);
						int resetCalId = getCalendarId(resetDate);
						int curCalId = getCalendarId(getCurrentDateStr());	
						// Get comp change count
						if(isCompChangeCntReqd && compDetail != null)
							//getCompPriceChangeCountOld(locationId, primaryCompId, productLevelId, productId, resetDate, getCurrentDateStr(), kviList, dto);
							getCompPriceChangeCount(locationId, compDetail, productLevelId, productId, resetDate, getCurrentDateStr(),
									itemMap.values(), kviList, dto, itemDataMap);
						// Get cost change count
						if(isCostChangeCntReqd){
							//getCostChangeCount(locationId, productLevelId, productId, resetCalId, curCalId, kviList, dto);
							getCostChangeCountOptimized(locationId, productLevelId, productId, resetCalId, curCalId, kviList, dto, itemMap.keySet(),
									priceAndStrategyZoneNos, priceZoneStores, itemDataMap, inputDTO, allWeekCalendarDetails, curCalDTO);
						}
						// Get new item count
						if(isNewItemCntReqd)
							getNewItemCount(locationId, productLevelId, productId, resetDate, getCurrentDateStr(), kviList, dto, priceZoneStores, 
									locationLevelId);
						// Get Strategy id
						if(isStrategyIdReqd){
							dto.setStrategyId(getStrategyId(categoryHierarchy, strategyMap, zoneListId, divisionId, locationLevelId, locationId, productLevelId, productId));
							dto.setKviStrategyId(getStrategyId(categoryHierarchy, strategyMap, zoneListId, divisionId, locationLevelId, locationId, productLevelId, productId, kviListId));
						}
						// Get Item Count
						if(isItemCntReqd)
							getItemCount(productLevelId, productId, kviList, dto);
						// Get Revenue data
						if(isRevenueReqd)
							getRevenueData(productLevelId,productId, locationId, kviList, dto, getLastWeekRevenue, priceZoneStores);
						dto.setPrimaryCompRecentCheckDate(primaryCompRecentCheckDate);
						updateList.add(dto);
					}else{
						// First time data population
						logger.info("Processing for key " + key);
						String resetDate = DateUtil.getWeekStartDate(currentDate, 13);
						int resetCalId = getCalendarId(resetDate);
						int curCalId = getCalendarId(getCurrentDateStr());						
						
						DashboardDTO dto = new DashboardDTO();
						dto.setLocationLevelId(locationLevelId);
						dto.setLocationId(locationId);
						dto.setProductLevelId(productLevelId);
						dto.setProductId(productId);
						// Get Strategy id
						if(isStrategyIdReqd){
							dto.setStrategyId(getStrategyId(categoryHierarchy, strategyMap, locationList, divisionId, locationLevelId, locationId, productLevelId, productId));
							dto.setKviStrategyId(getStrategyId(categoryHierarchy, strategyMap, zoneListId, divisionId, locationLevelId, locationId, productLevelId, productId, kviListId));
						}
						// Get comp change count
						if(isCompChangeCntReqd && compDetail != null)
							//getCompPriceChangeCountOld(locationId, primaryCompId, productLevelId, productId, resetDate, getCurrentDateStr(), kviList, dto);
							getCompPriceChangeCount(locationId, compDetail, productLevelId, productId, resetDate, getCurrentDateStr(),
									itemMap.values(), kviList, dto, itemDataMap);
						// Get cost change count
						if(isCostChangeCntReqd){
							//getCostChangeCount(locationId, productLevelId, productId, resetCalId, curCalId, kviList, dto);
							getCostChangeCountOptimized(locationId, productLevelId, productId, resetCalId, curCalId, kviList, dto, itemMap.keySet(),
									priceAndStrategyZoneNos, priceZoneStores, itemDataMap, inputDTO, allWeekCalendarDetails, curCalDTO);
						}
						// Get new item count
						if(isNewItemCntReqd)
							getNewItemCount(locationId, productLevelId, productId, resetDate, getCurrentDateStr(), kviList, dto, priceZoneStores, 
									locationLevelId);
						// Get item count
						if(isItemCntReqd)
							getItemCount(productLevelId, productId, kviList, dto);
						// Get Revenue data
						if(isRevenueReqd)
							getRevenueData(productLevelId, productId, locationId, kviList, dto, getLastWeekRevenue, priceZoneStores);
						dto.setPrimaryCompRecentCheckDate(primaryCompRecentCheckDate);
						insertList.add(dto);
					}
				}
				try{
					// Insert dashboard data
					dao.insertDashboardData(insertList);
					// Update dashboard data
					dao.updateDashboardData(updateList, this);
					
					PristineDBUtil.commitTransaction(conn, "Dashboard commit");
				}catch(GeneralException ge){
					logger.error("Error in populateDashboardData " + ge.getMessage());
					PristineDBUtil.rollbackTransaction(conn, "Dashboard rollback");
				}
			}
			
		}catch(GeneralException | Exception ex){
			ex.printStackTrace();
			logger.error("Error in populateDashboardData " + ex.getMessage());
		} finally{
			PristineDBUtil.close(conn);
		}
	}

	/**
	 * Retrieves list of locations/price zone ids for which dashboard batch has to run
	 * @return
	 */
	private ArrayList<Integer> getLocationList() throws GeneralException{
		RetailPriceZoneDAO rpzDAO = new RetailPriceZoneDAO();
		ArrayList<Integer> locationList = null;
		if(getLocationId() == -1)
			locationList = rpzDAO.getZonesUnderSubscriber(conn);
		else{
			locationList = new ArrayList<Integer>();
			locationList.add(getLocationId());
		}
		rpzDAO = null;
		return locationList;
	}

	/**
	 * Retrieves division id for each location in the location list
	 * @param locationList		Retrieves division id for zones in the location list when zone id is specified in the input
	 * 							Retrieves division id for all zones when zone id is not specified in the input
	 * @return
	 * @throws GeneralException
	 */
	private HashMap<Integer, Integer> getZoneDivisionMap(ArrayList<Integer> locationList) throws GeneralException{
		RetailPriceZoneDAO rpzDAO = new RetailPriceZoneDAO();
		HashMap<Integer, Integer> zoneDivisionMap = null;
		if(getLocationId() == -1)
			zoneDivisionMap = rpzDAO.getZoneDivisionMap(conn, 0);
		else{
			zoneDivisionMap = new HashMap<Integer, Integer>();
			for(Integer zoneId : locationList){
				HashMap<Integer, Integer> tMap = rpzDAO.getZoneDivisionMap(conn, zoneId);
				zoneDivisionMap.putAll(tMap);
			}
		}
		rpzDAO = null;
		return zoneDivisionMap;
	}

	/**
	 * Returns list of all categories for which dashboard batch has to run
	 * @return
	 * @throws GeneralException
	 */
	private ArrayList<Integer> getProductList() throws GeneralException{
		ArrayList<Integer> productList = null;
		ProductGroupDAO pgDAO = new ProductGroupDAO();
		if(getProductLevelId() == -1 && getProductId() == -1)
			productList = pgDAO.getCategories(conn);
		else if(getProductLevelId() == Constants.CATEGORYLEVELID){
			// If product level id is category level or below use product id as defined in the input
			productList = new ArrayList<Integer>();
			productList.add(getProductId());
		}else
			productList = pgDAO.getCategories(conn, getProductLevelId(), getProductId());
		return productList;
	}
	
	/**
	 * Returns week calendar id for input date
	 * @param date
	 * @return
	 * @throws GeneralException
	 */
	public int getCalendarId(String date) throws GeneralException{
		int calId = 0;
		if(calendarIdMap.get(date) != null){
			calId = calendarIdMap.get(date) ;
		}else{
			RetailCalendarDTO resetCalDTO = calDAO.getCalendarId(conn, date, Constants.CALENDAR_WEEK);
			calId = resetCalDTO.getCalendarId();
		}
		return calId;
	}
	
	/**
	 * Sets comp price change count (All items, KVI, LIG count, LIG count for KVI)
	 * @param primaryCompId		Primary Comp Str Id
	 * @param productLevelId	Product Level Id	
	 * @param productId			Product Id
	 * @param resetDate			Date when dashboard data was reset
	 * @param currentDateStr	Date for which dashboard program is running
	 * @param kviList			KVI Item List
	 * @param dto
	 */
//	private void getCompPriceChangeCountOld(int zoneId, int primaryCompId, int productLevelId, Integer productId, String resetDate, String currentDateStr, ArrayList<Integer> kviList, DashboardDTO dto) {
//		ArrayList<Integer> compPriceChangeList = dao.getCompPriceChangeCountOld(zoneId, primaryCompId, productLevelId, productId, resetDate, currentDateStr);
//		int kviCompChange = 0;
//		int disCompChange = 0;
//		int disKviCompChange = 0;
//		
//		Set<Integer> ligRetLirId = new HashSet<Integer>();
//		Set<Integer> ligKviRetLirId = new HashSet<Integer>();
//		
//		if(compPriceChangeList != null){
//			dto.setCompChange(compPriceChangeList.size());
//			for(Integer itemCode : compPriceChangeList){
//				ItemDTO item = itemMap.get(itemCode);
//				if(kviList.contains(itemCode)){
//					kviCompChange++;
//					if(item !=null)
//						if(item.likeItemId > 0){
//							if(!ligKviRetLirId.contains(item.likeItemId)){
//								ligKviRetLirId.add(item.likeItemId);
//								disKviCompChange++;
//							}
//						}else{
//							disKviCompChange++;
//						}
//				}
//				
//				if(item !=null)
//					if(item.likeItemId > 0){
//						if(!ligRetLirId.contains(item.likeItemId)){
//							ligRetLirId.add(item.likeItemId);
//							disCompChange++;
//						}
//					}else{
//						disCompChange++;
//					}
//			}
//			dto.setKviCompChange(kviCompChange);
//			dto.setDistinctCompChange(disCompChange);
//			dto.setDistinctKviCompChange(disKviCompChange);
//		}
//	}
	
	/**
	 * Sets comp price change count (All items, KVI, LIG count, LIG count for KVI)
	 * @param primaryCompId		Primary Comp Str Id
	 * @param productLevelId	Product Level Id	
	 * @param productId			Product Id
	 * @param resetDate			Date when dashboard data was reset
	 * @param currentDateStr	Date for which dashboard program is running
	 * @param authorizedItems	Collection of authorized items
	 * @param kviList			KVI Item List
	 * @param dto
	 */
	private void getCompPriceChangeCount(int zoneId, LocationKey compDetail, int productLevelId, Integer productId, String resetDate,
			String currentDateStr, Collection<ItemDTO> authorizedItems, ArrayList<Integer> kviList, DashboardDTO dto,
			HashMap<ItemKey, PRItemDTO> itemDataMap) {
		ArrayList<Integer> compPriceChangeList = dao.getCompPriceChangeCount(zoneId, compDetail, productLevelId, productId, resetDate,
				currentDateStr, authorizedItems, itemDataMap);
		int kviCompChange = 0;
		int disCompChange = 0;
		int disKviCompChange = 0;

		Set<Integer> ligRetLirId = new HashSet<Integer>();
		Set<Integer> ligKviRetLirId = new HashSet<Integer>();

		if (compPriceChangeList != null) {
			dto.setCompChange(compPriceChangeList.size());
			for (Integer itemCode : compPriceChangeList) {
				ItemDTO item = itemMap.get(itemCode);
				if (kviList.contains(itemCode)) {
					kviCompChange++;
					if (item != null)
						if (item.likeItemId > 0) {
							if (!ligKviRetLirId.contains(item.likeItemId)) {
								ligKviRetLirId.add(item.likeItemId);
								disKviCompChange++;
							}
						} else {
							disKviCompChange++;
						}
				}

				if (item != null)
					if (item.likeItemId > 0) {
						if (!ligRetLirId.contains(item.likeItemId)) {
							ligRetLirId.add(item.likeItemId);
							disCompChange++;
						}
					} else {
						disCompChange++;
					}
			}
			dto.setKviCompChange(kviCompChange);
			dto.setDistinctCompChange(disCompChange);
			dto.setDistinctKviCompChange(disKviCompChange);
		}
	}
	
	/**
	 * Sets cost change count (All items, KVI, LIG count, LIG count for KVI)
	 * @param locationId		Price Zone Id
	 * @param productLevelId	Product Level Id
	 * @param productId			Product Id
	 * @param resetCalId		Calendar id when dashboard data was reset
	 * @param curCalId			Calendar id for the date for which dashboard program is running
	 * @param kviList			KVI Item List
	 * @param dto
	 * @throws GeneralException 
	 * @throws NumberFormatException 
	 */
//	private void getCostChangeCount(Integer locationId, int productLevelId, Integer productId, int resetCalId,
//			int curCalId, ArrayList<Integer> kviList, DashboardDTO dto) {
//		ArrayList<Integer> costChangeList = dao.getCostChangeCount(locationId, productLevelId, productId, resetCalId,
//				curCalId);
//		int kviCostChange = 0;
//		int disCostChange = 0;
//		int costChange = 0;
//		int disKviCostChange = 0;
//
//		Set<Integer> ligRetLirId = new HashSet<Integer>();
//		Set<Integer> ligKviRetLirId = new HashSet<Integer>();
//
//		if (costChangeList != null) {
//			// dto.setCostChange(costChangeList.size());
//			for (Integer itemCode : costChangeList) {
//				ItemDTO item = itemMap.get(itemCode);
//				if (kviList.contains(itemCode)) {
//					kviCostChange++;
//					if (item != null)
//						if (item.likeItemId > 0) {
//							if (!ligKviRetLirId.contains(item.likeItemId)) {
//								ligKviRetLirId.add(item.likeItemId);
//								disKviCostChange++;
//							}
//						} else {
//							disKviCostChange++;
//						}
//				}
//
//				if (item != null) {
//					if (item.likeItemId > 0) {
//						if (!ligRetLirId.contains(item.likeItemId)) {
//							ligRetLirId.add(item.likeItemId);
//							disCostChange++;
//						}
//					} else {
//						disCostChange++;
//					}
//					// logger.debug("Item whose cost changed: " +
//					// item.itemCode);
//					// Cost list may have unauthorized item, this block will
//					// have only authorized items
//					costChange++;
//				}
//			}
//			dto.setCostChange(costChange);
//			dto.setKviCostChange(kviCostChange);
//			dto.setDistinctCostChange(disCostChange);
//			dto.setDistinctKviCostChange(disKviCostChange);
//		}
//	}
	
	private void getCostChangeCountOptimized(Integer locationId, int productLevelId, Integer productId, int resetCalId,
			int curCalId, ArrayList<Integer> kviList, DashboardDTO dto, Set<Integer> itemCodeSet,
			List<String> strategyZoneNos, List<Integer> priceZoneStores, HashMap<ItemKey, PRItemDTO> itemDataMap,
			PRStrategyDTO inputDTO, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, RetailCalendarDTO curCalDTO) throws NumberFormatException, GeneralException {
//		ArrayList<Integer> costChangeList = dao.getCostChangeCountOptimized(Integer.parseInt(chainId), locationId,
//				productLevelId, productId, resetCalId, curCalId, itemCodeSet, strategyZoneNos, priceZoneStores);
		ArrayList<Integer> costChangeList = identityCostChgitems(itemCodeSet, itemDataMap, inputDTO, allWeekCalendarDetails, strategyZoneNos,
				priceZoneStores, curCalDTO);
		int kviCostChange = 0;
		int disCostChange = 0;
		int costChange = 0;
		int disKviCostChange = 0;
		
		Set<Integer> ligRetLirId = new HashSet<Integer>();
		Set<Integer> ligKviRetLirId = new HashSet<Integer>();
		
		if(costChangeList != null){
			//dto.setCostChange(costChangeList.size());
			for (Integer itemCode : costChangeList) {
				ItemDTO item = itemMap.get(itemCode);
				if (kviList.contains(itemCode)) {
					kviCostChange++;
					if (item != null)
						if (item.likeItemId > 0) {
							if (!ligKviRetLirId.contains(item.likeItemId)) {
								ligKviRetLirId.add(item.likeItemId);
								disKviCostChange++;
							}
						} else {
							disKviCostChange++;
						}
				}

				if (item != null) {
					if (item.likeItemId > 0) {
						if (!ligRetLirId.contains(item.likeItemId)) {
							ligRetLirId.add(item.likeItemId);
							disCostChange++;
						}
					} else {
						disCostChange++;
					}
					//logger.debug("Item whose cost changed: " + item.itemCode);
					// Cost list may have unauthorized item, this block will
					// have only authorized items
					costChange++;
				}
			}
			dto.setCostChange(costChange);
			dto.setKviCostChange(kviCostChange);
			dto.setDistinctCostChange(disCostChange);
			dto.setDistinctKviCostChange(disKviCostChange);
		}
	}
	/**
	 * Sets number of new items (KVI, LIG count, LIG count for KVI)
	 * @param productLevelId		Product Level Id
	 * @param productId				Product Id
	 * @param resetDate				Date when dashboard record was last reset
	 * @param currentDateStr		Current Date
	 * @param kviList				List containing KVI items
	 * @param dto
	 */
	private void getNewItemCount(int zoneId, int productLevelId, Integer productId, String resetDate,
			String currentDateStr, ArrayList<Integer> kviList, DashboardDTO dto, List<Integer> priceZoneStores, int locationLevelId) {
		ArrayList<Integer> newItemList = dao.getNewItemCount(zoneId, productLevelId, productId, resetDate,
				currentDateStr, priceZoneStores, locationLevelId);
		int kviNewItem = 0;
		int disNewItem = 0;
		int disKviNewItem = 0;
		
		Set<Integer> ligRetLirId = new HashSet<Integer>();
		Set<Integer> ligKviRetLirId = new HashSet<Integer>();
		
		if(newItemList != null){
			dto.setNewItem(newItemList.size());
			for(Integer itemCode : newItemList){
				ItemDTO item = itemMap.get(itemCode);
				if(kviList.contains(itemCode)){
					kviNewItem++;
					if(item !=null)
						if(item.likeItemId > 0){
							if(!ligKviRetLirId.contains(item.likeItemId)){
								ligKviRetLirId.add(item.likeItemId);
								disKviNewItem++;
							}
						}else{
							disKviNewItem++;
						}
				}
				
				if(item !=null)
					if(item.likeItemId > 0){
						if(!ligRetLirId.contains(item.likeItemId)){
							ligRetLirId.add(item.likeItemId);
							disNewItem++;
						}
					}else{
						disNewItem++;
					}
			}
			dto.setKviNewItem(kviNewItem);
			dto.setDistinctNewItem(disNewItem);
			dto.setDistinctKviNewItem(disKviNewItem);
		}
	}
	
	/**
	 * Sets total count of items under product (All items, KVI items, LIG count, LIG count for KVI)
	 * @param productLevelId	Product Level Id
	 * @param productId			Product Id of category
	 * @param kviList			KVI Item List
	 * @param dto
	 */
	private void getItemCount(int productLevelId, Integer productId, ArrayList<Integer> kviList, DashboardDTO dto) {
		int kviItem = 0;
		int disItem = 0;
		int disKviItem = 0;
		
		Set<Integer> ligRetLirId = new HashSet<Integer>();
		Set<Integer> ligKviRetLirId = new HashSet<Integer>();
		
		if(itemMap != null && itemMap.size() > 0){
			dto.setNoOfItems(itemMap.size());
			for(ItemDTO item : itemMap.values()){
				if(kviList.contains(item.itemCode)){
					kviItem++;
					if(item.likeItemId > 0){
						if(!ligKviRetLirId.contains(item.likeItemId)){
							ligKviRetLirId.add(item.likeItemId);
							disKviItem++;
						}
					}else{
						disKviItem++;
					}
				}
				
				if(item.likeItemId > 0){
					if(!ligRetLirId.contains(item.likeItemId)){
						ligRetLirId.add(item.likeItemId);
						disItem++;
					}
				}else{
					disItem++;
				}
			}
			dto.setNoOfKviItems(kviItem);
			dto.setDistinctNoOfItems(disItem);
			dto.setDistinctNoOfKviItems(disKviItem);
		}
	}
	
	/**
	 * Sets revenue (All items, KVI items)
	 * @param productId				Product Id of category
	 * @param locationId			Zone Id
	 * @param kviList				KVI Item List
	 * @param dto					DashboardDTO
	 * @param getLastWeekRevenue	Gets last week revenue is set to true. If not gets revenue as on week of input date
	 */
	private void getRevenueData(Integer productLevelId, Integer productId, Integer locationId,
			ArrayList<Integer> kviList, DashboardDTO dto, Boolean getLastWeekRevenue, List<Integer> priceZoneStores)
			throws GeneralException {
		double totRevenue = 0;
		double kviTotRevenue = 0;
		HashMap<Integer, Double> revenueMap = null;

		if (getLastWeekRevenue)
			revenueMap = dao.getRevenueData(locationId, productLevelId, productId,
					DateUtil.getWeekStartDate(DateUtil.toDate(getCurrentDateStr(), Constants.APP_DATE_FORMAT), 1),
					priceZoneStores, locationLevelId);
		else
			revenueMap = dao.getRevenueData(locationId, productLevelId, productId,
					DateUtil.getWeekStartDate(DateUtil.toDate(getCurrentDateStr(), Constants.APP_DATE_FORMAT), 0),
					priceZoneStores, locationLevelId);

		for (Map.Entry<Integer, Double> entry : revenueMap.entrySet()) {
			totRevenue = totRevenue + entry.getValue();
			if (kviList.contains(entry.getKey()))
				kviTotRevenue = kviTotRevenue + entry.getValue();
		}
		dto.setTotRevenue(totRevenue);
		dto.setKviTotRevenue(kviTotRevenue);
	}

	/**
	 * Sets strategy id for input location-product combination
	 * @param categoryHierarchy		Parent product hierarchy for input product
	 * @param strategyMap			Strategies active as on date for which program is running
	 * @param locationList			List of product lists containing the product
	 * @param divisionId			Division id for input location
	 * @param locationLevelId		Location Level Id
	 * @param locationId			Location Id (Price Zone Id)
	 * @param productLevelId		Product Level Id
	 * @param productId				Product Id (Category)
	 * @return
	 */
	public Integer getStrategyId(HashMap<Integer, ProductDTO> categoryHierarchy,
			HashMap<String, HashMap<String, Integer>> strategyMap, ArrayList<Integer> locationList, Integer divisionId,
			int locationLevelId, Integer locationId, int productLevelId, Integer productId) throws GeneralException {

		//PP:: 29th Jun 2017, 
		//TOPS : lead zone strategy should get reflected
		//in dashboard for dependent zones 
		int leadZoneId = 0;
		if(locationLevelId == PRConstants.ZONE_LEVEL_TYPE_ID){
			leadZoneId = new PricingEngineDAO().getLeadAndDependentZone(getConn(), locationId);
			//Updating division id if lead zone is present
			if(leadZoneId > 0) {
				divisionId = new RetailPriceZoneDAO().getDivisionIdForZone(getConn(), leadZoneId);
			}
		}
		
		Integer strategyId = null;
		ProductDTO pDTO = categoryHierarchy.get(productId);
		String locKey = locationLevelId + "-" + (leadZoneId > 0 ? leadZoneId : locationId);
		String proKey = productLevelId + "-" + productId;

		//Location, Prod -> Zone List, Prod -> Division, Prod -> Chain, Prod
		strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId);

		if (strategyId == null) {
			ArrayList<Long> pListId = pricingEngineDAO.getProductListId(getConn(), productLevelId, productId);
			for (Long pId : pListId) {
				proKey = PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID + "-" + pId;
				strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId);
				if (strategyId != null)
					break;
			}
		}

		if (strategyId == null) {
			proKey = Constants.PORTFOLIO + "-" + pDTO.getPortfolioId();
			//Location, Portfolio -> Zone List, Portfolio -> Division, Portfolio -> Chain, Portfolio
			strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId);

			if (strategyId == null) {
				ArrayList<Long> pListId = pricingEngineDAO.getProductListId(getConn(), Constants.PORTFOLIO,
						pDTO.getPortfolioId());
				for (Long pId : pListId) {
					proKey = PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID + "-" + pId;
					//Location, Portfolio List -> Zone List, Portfolio List -> Division, Portfolio List -> Chain, Portfolio List
					strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId);
					if (strategyId != null)
						break;
				}
			}
		}

		proKey = Constants.DEPARTMENTLEVELID + "-" + pDTO.getDeptId();
		if (strategyId == null) {
			//Location, Department -> Zone List, Department -> Division, Department -> Chain, Department
			strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId);
			logger.info("Dept Strategy Id " + strategyId);
			if (strategyId == null) {
				ArrayList<Long> pListId = pricingEngineDAO.getProductListId(getConn(), Constants.DEPARTMENTLEVELID,
						pDTO.getDeptId());
				for (Long pId : pListId) {
					proKey = PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID + "-" + pId;
					//Location, Department List -> Zone List, Department List -> Division, Department List -> Chain, Department List
					strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId);
					if (strategyId != null)
						break;
				}
			}
		}

		proKey = Constants.ALLPRODUCTS + "-" + 0;
		if (strategyId == null) {
			//Location, All Products -> Zone List, All Products -> Division, All Products -> Chain, All Products
			strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId);
		}

		logger.info("Strategy Id " + strategyId);
		return strategyId;
	}
	
	/**
	 * Gets strategy id for input location-product combination
	 * @param strategyMap		Strategies active as on date for which program is running
	 * @param locKey			Location Level Id + "-" + Location Id
	 * @param proKey			Product Level Id + "-" + Product Id
	 * @param locationList		Location List(s) containing input zone
	 * @param divisionId		Division Id for input location
	 * @return
	 */
	private Integer getStrategyId(HashMap<String, HashMap<String, Integer>> strategyMap, String locKey, String proKey, 
			ArrayList<Integer> locationList, Integer divisionId){
		Integer strategyId = null;
		if(strategyMap.get(locKey) != null){
			HashMap<String, Integer> tMap = strategyMap.get(locKey);
			if(tMap.get(proKey) != null){
				strategyId = tMap.get(proKey); 
			}
		}
		
		if(strategyId == null){
			for(Integer locationListId : locationList){
				locKey = PRConstants.ZONE_LIST_LEVEL_TYPE_ID + "-" + locationListId;
				if(strategyMap.get(locKey) != null){
					HashMap<String, Integer> tMap = strategyMap.get(locKey);
					if(tMap.get(proKey) != null){
						strategyId = tMap.get(proKey); 
					}
					if(strategyId != null) break;
				}
			}
		}
		if(strategyId == null){
			if(divisionId != null){
				locKey = Constants.DIVISION_LEVEL_ID + "-" + divisionId;
				if(strategyMap.get(locKey) != null){
					HashMap<String, Integer> tMap = strategyMap.get(locKey);
					if(tMap.get(proKey) != null){
						strategyId = tMap.get(proKey); 
					}
				}
			}
		}
		if(strategyId == null){
			locKey = Constants.CHAIN_LEVEL_ID + "-" + chainId;
			if(strategyMap.get(locKey) != null){
				HashMap<String, Integer> tMap = strategyMap.get(locKey);
				if(tMap.get(proKey) != null){
					strategyId = tMap.get(proKey); 
				}
			}
		}
		
		return strategyId;
	}
	
	/**
	 * Retrieves strategy id for location - product - price check list combination
	 * @param categoryHierarchy		Parent product hierarchy for input product
	 * @param strategyMap			Strategies active as on date for which program is running
	 * @param locationList			List of product lists containing the product
	 * @param divisionId			Division id for input location
	 * @param locationLevelId		Location Level Id
	 * @param locationId			Location Id (Price Zone Id)
	 * @param productLevelId		Product Level Id
	 * @param productId				Product Id (Category)
	 * @param priceCheckListId		Price Check List Id
	 * @return
	 */
	public Integer getStrategyId(HashMap<Integer, ProductDTO> categoryHierarchy, HashMap<String, HashMap<String, Integer>> strategyMap, ArrayList<Integer> locationList, Integer divisionId, 
			int locationLevelId, Integer locationId, int productLevelId, Integer productId, int priceCheckListId) {
		
		Integer strategyId = null;
		ProductDTO pDTO = categoryHierarchy.get(productId);
		String locKey = locationLevelId + "-" + locationId;
		String proKey = productLevelId + "-" + productId;
		
		strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId, priceCheckListId); 
		ArrayList<Long> pListId = pricingEngineDAO.getProductListId(conn, productLevelId, productId);
		if(strategyId == null)
			for(Long pId : pListId){
				proKey = PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID + "-" + pId;
				strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId, priceCheckListId);
				if(strategyId != null)
					break;
			}
		
		
		if(strategyId == null){
			proKey = Constants.PORTFOLIO + "-" + pDTO.getPortfolioId();
			strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId, priceCheckListId);
			
			if(strategyId == null)
				for(Long pId : pListId){
					proKey = PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID + "-" + pId;
					strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId, priceCheckListId);
					if(strategyId != null)
						break;
				}
		}
		
		proKey = Constants.DEPARTMENTLEVELID + "-" + pDTO.getDeptId();
		if(strategyId == null){
			strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId, priceCheckListId);
			logger.info("Dept Strategy Id " + strategyId);
			if(strategyId == null)
				for(Long pId : pListId){
					proKey = PRConstants.PRODUCT_LIST_LEVEL_TYPE_ID + "-" + pId;
					strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId, priceCheckListId);
					if(strategyId != null)
						break;
				}
		}
		
		proKey = Constants.ALLPRODUCTS + "-" + 0;
		if(strategyId == null){
			strategyId = getStrategyId(strategyMap, locKey, proKey, locationList, divisionId);
		}
		logger.info("Strategy Id " + strategyId);
		return strategyId;
	}
	
	/**
	 * Gets strategy id for input location-product-price check list combination
	 * @param strategyMap			Strategies active as on date for which program is running
	 * @param locKey				Location Level Id + "-" + Location Id
	 * @param proKey				Product Level Id + "-" + Product Id
	 * @param locationList			Location List(s) containing input zone
	 * @param divisionId			Division Id for input location
	 * @param priceCheckListId		Price Check List Id
	 * @return
	 */
	private Integer getStrategyId(HashMap<String, HashMap<String, Integer>> strategyMap, String locKey, String proKey, ArrayList<Integer> locationList, 
			Integer divisionId, int priceCheckListId){
		String kviProKey = proKey + "-" + priceCheckListId;
		Integer strategyId = null;
		if(strategyMap.get(locKey) != null){
			HashMap<String, Integer> tMap = strategyMap.get(locKey);
			
			if(priceCheckListId > 0 && tMap.get(kviProKey) != null){
				strategyId = tMap.get(kviProKey); 
			}
			if(strategyId == null && tMap.get(proKey) != null){
				strategyId = tMap.get(proKey); 
			}
		}
		
		if(strategyId == null){
			for(Integer locationListId : locationList){
				locKey = PRConstants.ZONE_LIST_LEVEL_TYPE_ID + "-" + locationListId;
				if(strategyMap.get(locKey) != null){
					HashMap<String, Integer> tMap = strategyMap.get(locKey);
					if(priceCheckListId > 0 && tMap.get(kviProKey) != null){
						strategyId = tMap.get(kviProKey); 
					}
					if(strategyId == null && tMap.get(proKey) != null){
						strategyId = tMap.get(proKey); 
					}
					if(strategyId != null) break;
				}
			}
		}
		if(strategyId == null){
			if(divisionId != null){
				locKey = Constants.DIVISION_LEVEL_ID + "-" + divisionId;
				if(strategyMap.get(locKey) != null){
					HashMap<String, Integer> tMap = strategyMap.get(locKey);
					if(priceCheckListId > 0 && tMap.get(kviProKey) != null){
						strategyId = tMap.get(kviProKey); 
					}
					if(strategyId == null && tMap.get(proKey) != null){
						strategyId = tMap.get(proKey); 
					}
				}
			}
		}
		if(strategyId == null){
			locKey = Constants.CHAIN_LEVEL_ID + "-" + chainId;
			if(strategyMap.get(locKey) != null){
				HashMap<String, Integer> tMap = strategyMap.get(locKey);
				if(priceCheckListId > 0 && tMap.get(kviProKey) != null){
					strategyId = tMap.get(kviProKey); 
				}
				if(strategyId == null && tMap.get(proKey) != null){
					strategyId = tMap.get(proKey); 
				}
			}
		}
		
		return strategyId;
	}
	
	private ArrayList<Integer> identityCostChgitems(Set<Integer> itemCodeSet, HashMap<ItemKey, PRItemDTO> itemDataMap,
			PRStrategyDTO inputDTO, HashMap<String, RetailCalendarDTO> allWeekCalendarDetails, List<String> priceAndStrategyZoneNos,
			List<Integer> priceZoneStores, RetailCalendarDTO curCalDTO) throws NumberFormatException, GeneralException {
		
		RetailCostServiceOptimized retailCostServiceOptimized = new RetailCostServiceOptimized(conn);
		double costChangeThreshold = Double.parseDouble(PropertyManager.getProperty("PR_DASHBOARD.COST_CHANGE_THRESHOLD", "0"));
		int costHistory = Integer.parseInt(PropertyManager.getProperty("PR_COST_HISTORY"));
		ArrayList<Integer> costChgItemCodes = new ArrayList<Integer>();
		
		// Get non-cached item's zone and store cost history
		HashMap<Integer, HashMap<String, List<RetailCostDTO>>> itemCostHistory = retailCostServiceOptimized.getCostHistory(Integer.parseInt(chainId),
				curCalDTO, costHistory, allWeekCalendarDetails, itemCodeSet, priceAndStrategyZoneNos, priceZoneStores);

		// Find latest cost of the item for zone
		retailCostServiceOptimized.getLatestCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, Integer.valueOf(chainId),
				inputDTO.getLocationId(), curCalDTO, costHistory, allWeekCalendarDetails);

		// Find if there is cost change for zone
		retailCostServiceOptimized.getPreviousCostOfZoneItems(itemCostHistory, itemCodeSet, itemDataMap, Integer.valueOf(chainId),
				inputDTO.getLocationId(), curCalDTO, costHistory, allWeekCalendarDetails, costChangeThreshold);

		for(Integer itemCode : itemCodeSet) {
			ItemKey itemKey = new ItemKey(itemCode, PRConstants.NON_LIG_ITEM_INDICATOR);
			if(itemDataMap.get(itemKey) != null) {
				PRItemDTO itemDTO = itemDataMap.get(itemKey);
				if(itemDTO.getCostChgIndicator() != 0) {
					costChgItemCodes.add(itemDTO.getItemCode());
				}
			}
		}
		return costChgItemCodes;
	}
	
	public String getCurrentDateStr() {
		return currentDateStr;
	}

	public void setCurrentDateStr(String currentDateStr) {
		this.currentDateStr = currentDateStr;
	}
	
	public boolean isCompChangeCntReqd() {
		return isCompChangeCntReqd;
	}

	public void setCompChangeCntReqd(boolean isCompChangeCntReqd) {
		this.isCompChangeCntReqd = isCompChangeCntReqd;
	}

	public boolean isCostChangeCntReqd() {
		return isCostChangeCntReqd;
	}

	public void setCostChangeCntReqd(boolean isCostChangeCntReqd) {
		this.isCostChangeCntReqd = isCostChangeCntReqd;
	}

	public boolean isNewItemCntReqd() {
		return isNewItemCntReqd;
	}

	public void setNewItemCntReqd(boolean isNewItemCntReqd) {
		this.isNewItemCntReqd = isNewItemCntReqd;
	}

	public boolean isStrategyIdReqd() {
		return isStrategyIdReqd;
	}

	public void setStrategyIdReqd(boolean isStrategyIdReqd) {
		this.isStrategyIdReqd = isStrategyIdReqd;
	}

	public boolean isRevenueReqd() {
		return isRevenueReqd;
	}

	public void setRevenueReqd(boolean isRevenueReqd) {
		this.isRevenueReqd = isRevenueReqd;
	}
	
	public Connection getConn() {
		return conn;
	}
	
	public String getChainId() {
		return chainId;
	}

	public void setChainId(String chainId) {
		this.chainId = chainId;
	}
	
	public boolean isItemCntReqd() {
		return isItemCntReqd;
	}

	public void setItemCntReqd(boolean isItemCntReqd) {
		this.isItemCntReqd = isItemCntReqd;
	}
	
	public int getLocationLevelId() {
		return locationLevelId;
	}

	public void setLocationLevelId(int locationLevelId) {
		this.locationLevelId = locationLevelId;
	}

	public int getLocationId() {
		return locationId;
	}

	public void setLocationId(int locationId) {
		this.locationId = locationId;
	}

	public int getProductLevelId() {
		return productLevelId;
	}

	public void setProductLevelId(int productLevelId) {
		this.productLevelId = productLevelId;
	}

	public int getProductId() {
		return productId;
	}

	public void setProductId(int productId) {
		this.productId = productId;
	}

	public boolean getIsUpdateDashboardFromRecTable() {
		return isUpdateDashboardFromRecTable;
	}

	public void setIsUpdateDashboardFromRecTable(boolean isUpdateDashboardFromRecTable) {
		this.isUpdateDashboardFromRecTable = isUpdateDashboardFromRecTable;
	}

	 
}