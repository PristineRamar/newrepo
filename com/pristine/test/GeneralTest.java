package com.pristine.test;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;

import org.apache.log4j.Logger;

import com.pristine.dao.CompetitiveDataDAO;
import com.pristine.dao.CostDAO;
import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.dao.RetailCostDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.ProductLocationMappingDAO;
import com.pristine.dataload.prestoload.PriceAndCostLoader;
import com.pristine.dataload.service.PriceAndCostService;
import com.pristine.dataload.tops.CostDataLoad;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.PriceZoneDTO;
import com.pristine.dto.RetailCostDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.StoreDTO;
import com.pristine.dto.customer.HouseHoldDetailsDTO;
import com.pristine.dto.offermgmt.PRRange;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
public class GeneralTest {

	Connection conn = null;
	private static Logger logger = Logger.getLogger("GeneralTest");
	CostDAO costDAO;
	public GeneralTest() {

		PropertyManager.initialize("analysis.properties");

		try {
			conn = DBManager.getConnection();
		} catch (GeneralException e) {
			logger.info("Error", e);
		}
	}

	public static void main(String args[]) {

		GeneralTest generalTest = new GeneralTest();

		//generalTest.testCostUnrollingForSA();

		//generalTest.testUnrollingPriceForPI();
		
		//generalTest.testUnrollingCostForPI();
		
		//generalTest.testPriceUnrollingForSA();
		
//		generalTest.testActiveItemfunction();
		
		try {
			generalTest.testCategoryMapping();
		} catch (GeneralException e) {
			e.printStackTrace();
		}
		
		
		

	}
	
	@SuppressWarnings("deprecation")
	private void testActiveItemfunction() throws ParseException, GeneralException{
//		HashMap<String, List<HouseHoldDetailsDTO>> activeItemsPerMonth = getActiveItemsPerMonth(conn);
//		for(Map.Entry<String, List<HouseHoldDetailsDTO>> itemDetailBasedOnMonth: activeItemsPerMonth.entrySet()){
//			String currentMonth = itemDetailBasedOnMonth.getKey().replace("-", "/");
//			currentMonth = "01/"+currentMonth;
			String currentMonth = "01/01/17";
			Date date = DateUtil.toDate(currentMonth,"dd/MM/yy");
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			cal.add(Calendar.MONTH, -1);
			Date date1 = cal.getTime();
			logger.info("Current date: "+date1);
			SimpleDateFormat format = new SimpleDateFormat("MM/YY");
			String actualMonth = format.format(date1);
			logger.info("After change date: "+actualMonth);
//			Calendar cal = Calendar(currentMonth);
// 			for(HouseHoldDetailsDTO houseHoldDetailsDTO: itemDetailBasedOnMonth.getValue()){
//				String itemId = houseHoldDetailsDTO.getItemId();
//			}
//		}
	}
	
	public HashMap<String, List<HouseHoldDetailsDTO>> getActiveItemsPerMonth(Connection conn){
		HashMap<String, List<HouseHoldDetailsDTO>> activeItemsDetails = new HashMap<String, List<HouseHoldDetailsDTO>>();
		PreparedStatement statement = null;
	    ResultSet rs = null;
		try{
			statement = conn.prepareStatement("select TO_CHAR(RC.START_DATE,'MM-yy') as MONTH_INFO, IMS.PRODUCT_ID as ITEM_ID, "
					+ "AVG(IMS.FINAL_PRICE) as FINAL_PRICE from IMS_WEEKLY_CHAIN IMS join RETAIL_CALENDAR RC on RC.CALENDAR_ID = IMS.CALENDAR_ID "
					+ "where IMS.LOCATION_ID = 50 and IMS.LOCATION_LEVEL_ID = 1 and TO_CHAR(RC.START_DATE,'MM-YY') like '10-16' "
					+ "and IMS.PRODUCT_ID < 100 group by TO_CHAR(RC.START_DATE,'MM-yy'), IMS.PRODUCT_ID");
	        rs = statement.executeQuery();
	        if(rs.next()){
	    	    HouseHoldDetailsDTO houseHoldDetailsDTO = new HouseHoldDetailsDTO();
	    	    List<HouseHoldDetailsDTO> houseHoldDetailsDTOs = new ArrayList<HouseHoldDetailsDTO>();
	    	    houseHoldDetailsDTO.setMonthInfo(rs.getString("MONTH_INFO"));
	    	    houseHoldDetailsDTO.setItemId(rs.getString("ITEM_ID"));
	    	    houseHoldDetailsDTO.setFinalPrice(rs.getDouble("FINAL_PRICE"));
	    	    if(activeItemsDetails.containsKey(houseHoldDetailsDTO.getMonthInfo())){
	    	    	houseHoldDetailsDTOs.addAll(activeItemsDetails.get(houseHoldDetailsDTO.getMonthInfo()));
	    	    }
	    	    houseHoldDetailsDTOs.add(houseHoldDetailsDTO);
	    	    activeItemsDetails.put(houseHoldDetailsDTO.getMonthInfo(), houseHoldDetailsDTOs);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getActiveItemsPerMonth()", e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return activeItemsDetails;
	}
	
/**
 * Added for testing sales analysis cost picking API
 * 
 * Finding store level cost using store_item_map table 
 * 
 * 	
 */
	public void testCostUnrollingForSA() {

		try {
			PriceAndCostService priceAndCostService = new PriceAndCostService();
			RetailCostDAO retailCostDAO = new RetailCostDAO();
			Set<String> itemCodeSet = new HashSet<String>();
			itemCodeSet.add("61401");

			int calId = 1980;
			HashMap<String, List<RetailCostDTO>> costRolledUpMap = retailCostDAO.getRetailCostInfo(conn, itemCodeSet,
					calId, false);

			StoreDTO storeInfo = new StoreDTO();
			storeInfo.strNum = "217";
			HashMap<String, List<RetailCostDTO>> unrolledMap = priceAndCostService.unrollAndFindGivenStoreCost(conn,
					costRolledUpMap, storeInfo, itemCodeSet, null);

			if (unrolledMap.get(storeInfo.strNum) != null) {
				List<RetailCostDTO> costList = unrolledMap.get(storeInfo.strNum);

				for (RetailCostDTO retailCostDTO : costList) {
					logger.info(retailCostDTO.toString());
				}
			}

		} catch (GeneralException e) {
			logger.info("Error", e);
		}
	}
	
	/**
	 * Added for testing price unrolling at store level 
	 */
	public void testPriceUnrollingForSA() {

		try {
			PriceAndCostService priceAndCostService = new PriceAndCostService();
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			Set<String> itemCodeSet = new HashSet<String>();
			itemCodeSet.add("968");

			int calId = 1990;
			HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = retailPriceDAO.getRetailPriceInfo(conn, itemCodeSet, calId, false);

			StoreDTO storeInfo = new StoreDTO();
			storeInfo.strNum = "6550";
			HashMap<String, List<RetailPriceDTO>> unrolledMap = priceAndCostService.unrollAndFindGivenStorePrice(conn,
					priceRolledUpMap, storeInfo, itemCodeSet, null);
			if (unrolledMap.get(storeInfo.strNum) != null) {
				List<RetailPriceDTO> priceList = unrolledMap.get(storeInfo.strNum);

				for (RetailPriceDTO retailPriceDTO : priceList) {
					logger.info(retailPriceDTO.toString());
				}
			}

		} catch (GeneralException e) {
			logger.info("Error", e);
		}
	}

	/**
	 * Added for testing banner level price
	 */
	
	public void testUnrollingPriceForPI() {

		try {
			RetailCostDAO retailCostDAO = new RetailCostDAO();
			PriceAndCostService priceAndCostService = new PriceAndCostService();
			RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
			RetailPriceZoneDAO priceZoneDAO = new RetailPriceZoneDAO();
			PriceAndCostLoader priceAndCostLoader = new PriceAndCostLoader();
			Set<String> itemCodeSet = new HashSet<String>();
			itemCodeSet.add("30363");

			List<Integer> calList = new ArrayList<>();
			calList.add(2020);

			HashMap<String, List<RetailPriceDTO>> priceRolledUpMap = retailPriceDAO.getRetailPriceInfoHistory(conn,
					itemCodeSet, calList);

			HashMap<String, HashMap<String, Integer>> itemMap = new HashMap<String, HashMap<String, Integer>>();

			int locationLevelId = 2;
			int locationId = 2;

			PriceZoneDTO chainDTO = new PriceZoneDTO();
			chainDTO.setLocationLevelId(locationLevelId);
			chainDTO.setLocationId(locationId);

			List<String> stores = priceZoneDAO.getStoresForLocation(conn, locationLevelId, locationId);
			if (stores != null && stores.size() > 0) {
				chainDTO.setCompStrNo(stores);
			} else {
				logger.error("No Stores for given location. Price and cost not loaded");
				System.exit(-1);
			}

			itemMap.put("30363", null);

			long startTime = System.currentTimeMillis();
			HashMap<String, HashMap<String, List<String>>> itemStoreMapping = retailCostDAO
					.getStoreItemMapAtZonelevel(conn, itemCodeSet, null);
			long endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve items from store_item_map - " + (endTime - startTime) + "ms");
			logger.info("store_item_map size - " + itemStoreMapping.size());
			HashMap<String, List<RetailPriceDTO>> bannerLevelRolledUpMap = priceAndCostService
					.identifyStoresAndFindCommonPrice(priceRolledUpMap, chainDTO, itemMap, itemStoreMapping, 2020);
			HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = priceAndCostLoader
					.unrollRetailPriceInfoAtBannerLevel(bannerLevelRolledUpMap, chainDTO, itemMap);

			for (List<RetailPriceDTO> unrolledPriceLst : unrolledPriceMap.values()) {
				for (RetailPriceDTO retailPriceDTO : unrolledPriceLst) {
					logger.info(retailPriceDTO.toString());
					if(!retailPriceDTO.isZonePriceDiff()){
						logger.info("Zone price not found");
					}else{
						logger.info("Zone price found");
					}
				}
			}

		} catch (GeneralException | Exception e) {
			logger.info("Error", e);
		}
	}

	/**
	 * Added for testing banner level cost
	 */
	
	public void testUnrollingCostForPI() {

		try {
			RetailCostDAO retailCostDAO = new RetailCostDAO();
			PriceAndCostService priceAndCostService = new PriceAndCostService();
			RetailPriceZoneDAO priceZoneDAO = new RetailPriceZoneDAO();
			PriceAndCostLoader priceAndCostLoader = new PriceAndCostLoader();
			Set<String> itemCodeSet = new HashSet<String>();
			itemCodeSet.add("290");

			List<Integer> calList = new ArrayList<>();
			calList.add(1990);

			HashMap<String, List<RetailCostDTO>> costRolledUpMap = retailCostDAO.getRetailCostInfoHistory(conn,
					itemCodeSet, calList);

			HashMap<String, HashMap<String, Integer>> itemMap = new HashMap<String, HashMap<String, Integer>>();

			int locationLevelId = 2;
			int locationId = 22;

			PriceZoneDTO chainDTO = new PriceZoneDTO();
			chainDTO.setLocationLevelId(locationLevelId);
			chainDTO.setLocationId(locationId);

			List<String> stores = priceZoneDAO.getStoresForLocation(conn, locationLevelId, locationId);
			if (stores != null && stores.size() > 0) {
				chainDTO.setCompStrNo(stores);
			} else {
				logger.error("No Stores for given location. Price and cost not loaded");
				System.exit(-1);
			}

			itemMap.put("290", null);

			long startTime = System.currentTimeMillis();
			HashMap<String, HashMap<String, List<String>>> itemStoreMapping = retailCostDAO
					.getStoreItemMapAtZonelevel(conn, itemCodeSet, null);
			long endTime = System.currentTimeMillis();
			logger.info("Time taken to retrieve items from store_item_map - " + (endTime - startTime) + "ms");
			logger.info("store_item_map size - " + itemStoreMapping.size());
			HashMap<String, List<RetailCostDTO>> bannerLevelRolledUpMap = priceAndCostService
					.identifyStoresAndFindCommonCost(costRolledUpMap, chainDTO, itemMap, itemStoreMapping);
			HashMap<String, List<RetailCostDTO>> unrolledCostMap = priceAndCostLoader
					.unrollRetailCostInfoAtBannerLevel(bannerLevelRolledUpMap, chainDTO, itemMap);

			for (List<RetailCostDTO> unrolledCostLst : unrolledCostMap.values()) {
				for (RetailCostDTO retailCostDTO : unrolledCostLst) {
					logger.info(retailCostDTO.toString());
				}
			}

		} catch (GeneralException | Exception e) {
			logger.info("Error", e);
		}
	}
	
	
	private void testGettingStoreId(){
		try {
			CompetitiveDataDAO competitiveDataDAO = new CompetitiveDataDAO(conn);
			String strNum="0108";
			
			List<String> storeList = competitiveDataDAO.getStoreIdList(conn, strNum);
			
			logger.info("Stores: " + PRCommonUtil.getCommaSeperatedStringFromStrArray(storeList));
			
			
		} catch (GeneralException | Exception e) {
			logger.info("Error", e);
		}
	}
	
	private void retrieveItemStoreMappingTest() throws GeneralException{
		HashMap<String, List<String>> storeItemMap = new HashMap<String, List<String>>();
		HashMap<String, List<String>> expectedStoreItemMap = new HashMap<String, List<String>>();
		List<String> temp = new ArrayList<String>();
		temp.add("30514");
		expectedStoreItemMap.put("2-5660", temp);
		expectedStoreItemMap.put("2-5989", temp);
		expectedStoreItemMap.put("1-4", temp);
		expectedStoreItemMap.put("2-5673", temp);
		costDAO = new CostDAO();
		List<String> itemLists = new ArrayList<String>();
		itemLists.add("30514");
		Object[] values = itemLists.toArray();
		try {
			costDAO.retrieveItemStoreMapping(conn,"03/12/2017",storeItemMap,values,"PRICE");
			for(Map.Entry<String, List<String>> entry: storeItemMap.entrySet()){
//				List<String> itemList = new ArrayList<String>();
//				itemList = entry.getValue();
				logger.info("Level type id and Level id :"+entry.getKey()+" And # of items: "+entry.getValue().size());
				if(entry.getKey().equals("2-5677")){
//					HashSet<String> tempList = new HashSet<String>(entry.getValue());
					List<String> tempList =new ArrayList<String>(new HashSet<String>(entry.getValue()));
					for(String items: tempList){
						logger.info("Active items code:"+items);
					}
				}
			}
			
			//1. Test equal, ignore order
	        assertEquals(expectedStoreItemMap, storeItemMap);
	        assertEquals(expectedStoreItemMap.get("2-5661"),storeItemMap.get("2-5660"));
		} catch (GeneralException e) {
			logger.error("Error in retriving item store mapping test",e);
			throw new GeneralException("Error in retriving item store mapping test", e);
		}
		
	}
//	 @Before
//	    public void setUp() {
//		 costDAO = EasyMock.createMock(CostDAO.class);
//	    }
//	 
//	    @After
//	    public void tearDown() {
//	    	costDAO = null;
//	    }
//	 
//	    @Test
//	    public void testMakeCoffe() throws NotEnoughException {
//	    	HashMap<String, List<String>> storeItemMap = new HashMap<String, List<String>>();
//	    	List<String> itemLists = new ArrayList<String>();
//			itemLists.add("30514");
//			Object[] values = itemLists.toArray();
//			 EasyMock.expect(mock.send(costDAO));
//	        EasyMock.expect(costDAO.retrieveItemStoreMapping(conn,"03/12/2017",storeItemMap,values,"PRICE")).andReturn(true);
//	        EasyMock.replay(coffeeContainer);
//	         
//	        EasyMock.expect(waterContainer.getPortion(Portion.LARGE)).andReturn(true);
//	        EasyMock.replay(waterContainer);
//	         
//	        assertTrue(coffeeMachine.makeCoffee(Portion.LARGE));
//	    }
	@Test
	private void unrollRetailPriceInfoWithStoreItemMapTest() throws GeneralException{
		HashMap<String, String> storeInfoMap = null;
	  	HashMap<String, List<String>> zoneStoreMap = null;
	  	costDAO = new CostDAO();
	  	
	  	RetailPriceDAO retailPriceDAO =new RetailPriceDAO();
		//priceRolledUpMapForItems setup
		HashMap<String, List<RetailPriceDTO>> priceRolledUpMapForItems = new HashMap<String, List<RetailPriceDTO>>();
		List<RetailPriceDTO> tempPriceList = new ArrayList<RetailPriceDTO>();
		RetailPriceDTO retailPriceDTO = new RetailPriceDTO();
		retailPriceDTO.setCalendarId(6814);
		retailPriceDTO.setItemcode("30514");
		retailPriceDTO.setLevelTypeId(0);
		retailPriceDTO.setLevelId("50");
		retailPriceDTO.setRegPrice(4.99f);
		retailPriceDTO.setRegEffectiveDate("03/30/2016");
		tempPriceList.add(retailPriceDTO);
		RetailPriceDTO retailPriceDTO1 = new RetailPriceDTO();
		retailPriceDTO1.setCalendarId(6814);
		retailPriceDTO1.setItemcode("30514");
		retailPriceDTO1.setLevelTypeId(1);
		retailPriceDTO1.setLevelId("0015");
		retailPriceDTO1.setRegPrice(5.99f);
		retailPriceDTO1.setRegEffectiveDate("03/30/2016");
		tempPriceList.add(retailPriceDTO1);
		
		priceRolledUpMapForItems.put("30514", tempPriceList);
		
		//storeInfoMap and storeZoneInfo setup
		String chainId = retailPriceDAO.getChainId(conn); 
		CostDataLoad costDataLoad = new CostDataLoad();
		storeInfoMap = costDAO.getStoreZoneInfo(conn, chainId);
		costDataLoad.castZoneNumbers(storeInfoMap);
		zoneStoreMap = costDataLoad.getZoneMapping(storeInfoMap);
		
		//itemStoreMapping
		HashMap<String, List<String>> storeItemMap = new HashMap<String, List<String>>();
		List<String> temp = new ArrayList<String>();
		temp.add("30514");
		storeItemMap.put("2-5660", temp);
		storeItemMap.put("2-5989", temp);
		storeItemMap.put("1-4", temp);
		storeItemMap.put("2-5673", temp);
		
		//Expected result hashMap
		HashMap<String, List<RetailPriceDTO>> expectedUnrolledPriceMap = new HashMap<String, List<RetailPriceDTO>>();
		List<RetailPriceDTO> tempUnrollList = new ArrayList<RetailPriceDTO>();
		RetailPriceDTO retailPriceDTO2 = new RetailPriceDTO();
		retailPriceDTO2.setCalendarId(6814);
		retailPriceDTO2.setItemcode("30514");
		retailPriceDTO2.setLevelTypeId(2);
		retailPriceDTO2.setLevelId("0233");
		retailPriceDTO2.setRegPrice(5.99f);
		retailPriceDTO2.setRegEffectiveDate("03/30/2016");
		tempUnrollList.add(retailPriceDTO2);
		expectedUnrolledPriceMap.put("0233", tempUnrollList);
		tempUnrollList = new ArrayList<RetailPriceDTO>();
		
		RetailPriceDTO retailPriceDTO3 = new RetailPriceDTO();
		retailPriceDTO3.setCalendarId(6814);
		retailPriceDTO3.setItemcode("30514");
		retailPriceDTO3.setLevelTypeId(2);
		retailPriceDTO3.setLevelId("0109");
		retailPriceDTO3.setRegPrice(4.99f);
		retailPriceDTO3.setRegEffectiveDate("03/30/2016");
		tempUnrollList.add(retailPriceDTO3);
		expectedUnrolledPriceMap.put("0109", tempUnrollList);
		tempUnrollList = new ArrayList<RetailPriceDTO>();
		
		RetailPriceDTO retailPriceDTO4 = new RetailPriceDTO();
		retailPriceDTO4.setCalendarId(6814);
		retailPriceDTO4.setItemcode("30514");
		retailPriceDTO4.setLevelTypeId(2);
		retailPriceDTO4.setLevelId("0206");
		retailPriceDTO4.setRegPrice(5.99f);
		retailPriceDTO4.setSalePrice(0f);
		retailPriceDTO4.setRegEffectiveDate("03/30/2016");
		tempUnrollList.add(retailPriceDTO4);
		expectedUnrolledPriceMap.put("0206", tempUnrollList);
		
		PriceDataLoad priceDataLoad = new PriceDataLoad(); 
		priceDataLoad.populateCalendarId("03/12/2017");
		priceDataLoad.setupObjects("03/12/2017", "DELTA");
		HashMap<String, List<RetailPriceDTO>> unrolledPriceMap = priceDataLoad.unrollRetailPriceInfoWithStoreItemMap(priceRolledUpMapForItems, storeInfoMap.keySet(), zoneStoreMap, retailPriceDAO, storeItemMap, storeInfoMap);
		logger.info("roll map Size"+unrolledPriceMap.size());
		assertEquals(expectedUnrolledPriceMap.get("0206").toString(), unrolledPriceMap.get("0206").toString());
//		assertEquals
		
		
	}
	
	
	private void testFetchCatogories() throws GeneralException{
		ProductLocationMappingDAO productLocationMappingDAO = new ProductLocationMappingDAO();
		
		productLocationMappingDAO.getCategoriesByPrcGrpCode(conn);
	}
	
	private void testDSDAndWarehouseZoneMapping() throws GeneralException{
		RetailCostDAO retailCostDAO = new RetailCostDAO();
		
		Set<String> itemCodeSet =  new HashSet<>();
		
		itemCodeSet.add("1234");
		
		retailCostDAO.getDSDandWHSEZoneMapping(conn, itemCodeSet);
	}
	
	private void testCategoryMapping() throws GeneralException{
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern(Constants.APP_DATE_FORMAT);
		LocalDate scanBack1StartDate = LocalDate.parse("05/10/2018", formatter);
		LocalDate scanBack1EndDate = LocalDate.parse("12/31/2018", formatter);
		

		long weeks = ChronoUnit.WEEKS.between(scanBack1StartDate, scanBack1EndDate);
		
		logger.info("Weeks: " + weeks);
		
	}
	

}
