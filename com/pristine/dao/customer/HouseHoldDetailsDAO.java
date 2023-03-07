package com.pristine.dao.customer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.customer.HouseHoldDetailsDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class HouseHoldDetailsDAO {
	static Logger logger = Logger.getLogger("HouseHoldDetailsDAO");
	public static final String GET_HOUSEHOLD_DETAILS = "SELECT RTL.LOYALTY_CARD_NO, RTL.STORE_ID AS STORE_ID, RTL.ITEM_ID AS ITEM_ID, "
			+ "HHNO_START_DATE.HH_FIRST_TRANS AS FIRST_TRANSACTION, RTL.MONTH_INFO 	AS MONTH_INFO, RTL.AMT_SPEND_USD AS AMT_SPEND_USD, "
			+ "RTL.QUANTITY AS QUANTITY, RTL.PL_BOUGHT_BY_HH AS PL_BOUGHT_BY_HH, RTL.WEIGHT AS WEIGHT FROM ("
			+ "(SELECT CLI.LOYALTY_CARD_NO as LOYALTY_CARD_NO, TL.STORE_ID AS STORE_ID, TL.ITEM_ID AS ITEM_ID, "
			+ "TO_CHAR(TL.TRX_TIME, 'MM-YY') AS MONTH_INFO,SUM(NET_AMT) AS AMT_SPEND_USD, SUM(QUANTITY) AS QUANTITY, "
			+ "COUNT(DISTINCT IL.ITEM_CODE) AS PL_BOUGHT_BY_HH, SUM(WEIGHT) AS WEIGHT "
			+ "FROM transaction_log TL "
			+ " JOIN CUSTOMER_LOYALTY_INFO CLI ON CLI.CUSTOMER_ID = TL.CUSTOMER_ID "
			+ "LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = TL.ITEM_ID AND IL.PRIVATE_LABEL_IND = 'Y' "
			+ "WHERE NET_AMT>=0 AND CLI.LOYALTY_CARD_NO NOT IN(0) AND TL.STORE_ID = ? AND TL.POS_DEPARTMENT_ID  BETWEEN ? AND ? "
			+ "GROUP BY CLI.LOYALTY_CARD_NO, TL.STORE_ID,TL.item_id, TO_CHAR(TL.TRX_TIME, 'MM-YY')) RTL "
			+ "JOIN (SELECT HHNO, TO_CHAR(START_DATE,'MON-YY') HH_FIRST_TRANS FROM (SELECT CLI.LOYALTY_CARD_NO HHNO, MIN(TL.TRX_TIME) "
			+ "START_DATE FROM transaction_log TL JOIN CUSTOMER_LOYALTY_INFO CLI ON CLI.CUSTOMER_ID = TL.CUSTOMER_ID "
			+ "GROUP BY CLI.LOYALTY_CARD_NO)) HHNO_START_DATE ON HHNO_START_DATE.HHNO = RTL.LOYALTY_CARD_NO)";
//	TL.CUSTOMER_ID IN (%CUSTOMER_ID%)
//	CLI.LOYALTY_CARD_NO= 43280238411 and tl.item_id in (2789,21108,27525,33082,47215) AND 
	public static final String GET_ACTIVE_ITEMS ="SELECT TO_CHAR(RC.START_DATE,'MM-YY') AS MONTH_INFO, IMS.PRODUCT_ID AS ITEM_ID, "
			+ "AVG(IMS.FINAL_PRICE) AS FINAL_PRICE from IMS_WEEKLY_CHAIN IMS join RETAIL_CALENDAR RC on "
			+ "RC.CALENDAR_ID = IMS.CALENDAR_ID where IMS.LOCATION_ID = 74 and IMS.LOCATION_LEVEL_ID = 1"
			+ " group by TO_CHAR(RC.START_DATE,'MM-YY'), IMS.PRODUCT_ID";
//	 product_id in (2789,21108,27525,33082,47215) and
	public static final String GET_ALL_ITEMS_BOUGHT_BY_HOUSEHOLD = "SELECT DISTINCT TL.ITEM_ID AS ITEM_ID, CLI.LOYALTY_CARD_NO AS LOYALTY_CARD_NO, "
			+ "TL.STORE_ID AS STORE_ID FROM transaction_log TL "
			+ "JOIN CUSTOMER_LOYALTY_INFO CLI ON CLI.CUSTOMER_ID = TL.CUSTOMER_ID WHERE TL.STORE_ID = ? ";
//	  AND TL.CUSTOMER_ID IN (%CUSTOMER_ID%)
	public static final String INSERT_INTO_HOUSEHOLD_DETAILS ="INSERT INTO HOUSEHOLD_MONTHLY_AGGR(HHNO,STORE_ID,FIRST_TRANS,MONTH_INFO,"
			+ "AMT_SPEND_USD,EST_AMT_PREV_MONTH,MONTHLY_PI,QUANTITY,WEIGHT,VISITS,PL_BOUGHT_BY_HH,ITEMS_BOUGHT_BY_HH_PM,ITEMS_BOUGHT_BY_HH, "
			+ "ACTIVE_ITEMS_WRT_HH_INFO) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
	public static final String GET_CUSTOMER_ID_FROM_TLOG ="SELECT DISTINCT CUSTOMER_ID FROM transaction_log "
			+ "WHERE STORE_ID = ? ";
//	 and customer_id = 1577538
	
	public static final String GET_NO_OF_VISITS_BASED_ON_HH = "SELECT RTL.LOYALTY_CARD_NO LOYALTY_CARD_NO, RTL.MONTH_INFO MONTH_INFO, COUNT(RTL.VISITS) AS VISITS FROM "
			+ "(SELECT DISTINCT TO_CHAR(TL.TRX_TIME,'dd-MM-YY') VISITS, TO_CHAR(TL.TRX_TIME,'MM-YY') MONTH_INFO, CLI.LOYALTY_CARD_NO LOYALTY_CARD_NO "
			+ "FROM transaction_log TL JOIN CUSTOMER_LOYALTY_INFO CLI ON TL.CUSTOMER_ID = CLI.CUSTOMER_ID WHERE "
			+ "TL.STORE_ID = ?) RTL "
			+ "GROUP BY LOYALTY_CARD_NO, MONTH_INFO";

	public HashMap<String, List<HouseHoldDetailsDTO>> getHoulseholdItemDetails(String storeId, Connection conn) throws GeneralException{
		HashMap<String, List<HouseHoldDetailsDTO>> householdDetails = new HashMap<String, List<HouseHoldDetailsDTO>>();
		PreparedStatement statement = null;
	    ResultSet rs = null;
		try{
		
			String query = new String(GET_HOUSEHOLD_DETAILS);
//			query = query.replaceAll("%CUSTOMER_ID%", customerIds);
			statement = conn.prepareStatement(query);
			statement.setFetchSize(200000);
			int counter=0;
			statement.setString(++counter, storeId);
			statement.setString(++counter, String.valueOf(PropertyManager.getProperty("SALES_ANALYSIS.MIN_REV_POS_DEPARTMENT")));
			statement.setString(++counter, String.valueOf(PropertyManager.getProperty("SALES_ANALYSIS.MAX_REV_POS_DEPARTMENT")));
	        rs = statement.executeQuery();
	        while(rs.next()){
	    	    HouseHoldDetailsDTO houseHoldDetailsDTO = new HouseHoldDetailsDTO();
	    	    List<HouseHoldDetailsDTO> houseHoldDetailsDTOs = new ArrayList<HouseHoldDetailsDTO>();
	    	    houseHoldDetailsDTO.setHouseholdNumber(rs.getString("LOYALTY_CARD_NO"));
	    	    houseHoldDetailsDTO.setHouseHoldFirstTrans(rs.getString("FIRST_TRANSACTION"));
	    	    houseHoldDetailsDTO.setStoreId(rs.getString("STORE_ID"));
	    	    houseHoldDetailsDTO.setItemId(rs.getString("ITEM_ID"));
	    	    houseHoldDetailsDTO.setMonthInfo(rs.getString("MONTH_INFO"));
	    	    houseHoldDetailsDTO.setAmountSpendByHH(rs.getDouble("AMT_SPEND_USD"));
	    	    houseHoldDetailsDTO.setQuantity(rs.getLong("QUANTITY"));
	    	    houseHoldDetailsDTO.setPlItems(rs.getLong("PL_BOUGHT_BY_HH"));
	    	    houseHoldDetailsDTO.setWeight(rs.getDouble("WEIGHT"));
	    	    if(householdDetails.containsKey(houseHoldDetailsDTO.getHouseholdNumber())){
	    	    	houseHoldDetailsDTOs.addAll(householdDetails.get(houseHoldDetailsDTO.getHouseholdNumber()));
	    	    }
	    	    houseHoldDetailsDTOs.add(houseHoldDetailsDTO);
	    	    householdDetails.put(houseHoldDetailsDTO.getHouseholdNumber(), houseHoldDetailsDTOs);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getHoulseholdDetails()", e);
			throw new GeneralException("Error while executing getActiveItemsPerMonth()", e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return householdDetails;
	}
	
	public List<HouseHoldDetailsDTO> getActiveItemsPerMonth(Connection conn) throws GeneralException{
		List<HouseHoldDetailsDTO> activeItemsDetails = new ArrayList<HouseHoldDetailsDTO>();
		PreparedStatement statement = null;
	    ResultSet rs = null;
		try{
			statement = conn.prepareStatement(GET_ACTIVE_ITEMS);
			statement.setFetchSize(200000);
	        rs = statement.executeQuery();
	        while(rs.next()){
	    	    HouseHoldDetailsDTO houseHoldDetailsDTO = new HouseHoldDetailsDTO();
	    	    houseHoldDetailsDTO.setMonthInfo(rs.getString("MONTH_INFO"));
	    	    houseHoldDetailsDTO.setItemId(rs.getString("ITEM_ID"));
	    	    houseHoldDetailsDTO.setFinalPrice(rs.getDouble("FINAL_PRICE"));
	    	    activeItemsDetails.add(houseHoldDetailsDTO);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getActiveItemsPerMonth()", e);
			throw new GeneralException("Error while executing getActiveItemsPerMonth()", e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return activeItemsDetails;
	}
	
	public HashMap<String, List<HouseHoldDetailsDTO>> getAllItemsBoughtByHH(String storeId, Connection conn) throws GeneralException{
		HashMap<String, List<HouseHoldDetailsDTO>> overallItemsBoughtByHH = new HashMap<String, List<HouseHoldDetailsDTO>>();
		PreparedStatement statement = null;
	    ResultSet rs = null;
		try{
			String query = new String(GET_ALL_ITEMS_BOUGHT_BY_HOUSEHOLD);
			//			query = query.replaceAll("%CUSTOMER_ID%", customerIds);
			statement = conn.prepareStatement(query);
			statement.setFetchSize(200000);
			int counter=0;
			statement.setString(++counter, storeId);
	        rs = statement.executeQuery();
	        while(rs.next()){
	    	    HouseHoldDetailsDTO houseHoldDetailsDTO = new HouseHoldDetailsDTO();
	    	    List<HouseHoldDetailsDTO> houseHoldDetailsDTOs = new ArrayList<HouseHoldDetailsDTO>();
	    	    houseHoldDetailsDTO.setHouseholdNumber(rs.getString("LOYALTY_CARD_NO"));
	    	    houseHoldDetailsDTO.setStoreId(rs.getString("STORE_ID"));
	    	    houseHoldDetailsDTO.setItemId(rs.getString("ITEM_ID"));
	    	    if(overallItemsBoughtByHH.containsKey(houseHoldDetailsDTO.getHouseholdNumber())){
	    	    	houseHoldDetailsDTOs.addAll(overallItemsBoughtByHH.get(houseHoldDetailsDTO.getHouseholdNumber()));
	    	    }
	    	    houseHoldDetailsDTOs.add(houseHoldDetailsDTO);
	    	    overallItemsBoughtByHH.put(houseHoldDetailsDTO.getHouseholdNumber(), houseHoldDetailsDTOs);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getAllItemsBoughtByHH()", e);
			throw new GeneralException("Error while executing getAllItemsBoughtByHH()", e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return overallItemsBoughtByHH;
	}
	
	public HashMap<String, List<HouseHoldDetailsDTO>> getNoOfVisitsBasedOnHH(String storeId, Connection conn) throws GeneralException{
		HashMap<String, List<HouseHoldDetailsDTO>> overallItemsBoughtByHH = new HashMap<String, List<HouseHoldDetailsDTO>>();
		PreparedStatement statement = null;
	    ResultSet rs = null;
		try{
			String query = new String(GET_NO_OF_VISITS_BASED_ON_HH);
//			query = query.replaceAll("%CUSTOMER_ID%", customerIds);
			statement = conn.prepareStatement(query);
			int counter=0;
			statement.setString(++counter, storeId);
			statement.setFetchSize(200000);

	        rs = statement.executeQuery();
	        while(rs.next()){
	    	    HouseHoldDetailsDTO houseHoldDetailsDTO = new HouseHoldDetailsDTO();
	    	    List<HouseHoldDetailsDTO> houseHoldDetailsDTOs = new ArrayList<HouseHoldDetailsDTO>();
	    	    houseHoldDetailsDTO.setHouseholdNumber(rs.getString("LOYALTY_CARD_NO"));
	    	    houseHoldDetailsDTO.setMonthInfo(rs.getString("MONTH_INFO"));
	    	    houseHoldDetailsDTO.setNoOfVisits(rs.getLong("VISITS"));
	    	    if(overallItemsBoughtByHH.containsKey(houseHoldDetailsDTO.getHouseholdNumber())){
	    	    	houseHoldDetailsDTOs.addAll(overallItemsBoughtByHH.get(houseHoldDetailsDTO.getHouseholdNumber()));
	    	    }
	    	    houseHoldDetailsDTOs.add(houseHoldDetailsDTO);
	    	    overallItemsBoughtByHH.put(houseHoldDetailsDTO.getHouseholdNumber(), houseHoldDetailsDTOs);
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getNoOfVisitsBasedOnHH()", e);
			throw new GeneralException("Error while executing getNoOfVisitsBasedOnHH()", e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return overallItemsBoughtByHH;
	}
	
	public void saveHouseHoldItemDetails(List<HouseHoldDetailsDTO> listOfHouseHoldDetails, Connection conn)
			throws GeneralException {
		int recordCount = 0;
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(INSERT_INTO_HOUSEHOLD_DETAILS);
			stmt.setFetchSize(200000);
			for (HouseHoldDetailsDTO houseHoldDetailsDTO : listOfHouseHoldDetails) {
				int counter = 0;
				stmt.setString(++counter, houseHoldDetailsDTO.getHouseholdNumber());
				stmt.setString(++counter, houseHoldDetailsDTO.getStoreId());
				stmt.setString(++counter, houseHoldDetailsDTO.getHouseHoldFirstTrans());
				stmt.setString(++counter, houseHoldDetailsDTO.getMonthInfo());
				stmt.setDouble(++counter, houseHoldDetailsDTO.getAmountSpendByHH());
				stmt.setDouble(++counter, houseHoldDetailsDTO.getEstimatedItemPrice());
				stmt.setDouble(++counter, houseHoldDetailsDTO.getItemsPI());
				stmt.setLong(++counter, houseHoldDetailsDTO.getQuantity());
				stmt.setDouble(++counter, houseHoldDetailsDTO.getWeight());
				stmt.setLong(++counter, houseHoldDetailsDTO.getNoOfVisits());
				stmt.setLong(++counter, houseHoldDetailsDTO.getPlItems());
				stmt.setLong(++counter, houseHoldDetailsDTO.getNoOfItemsInMonth());
				stmt.setLong(++counter, houseHoldDetailsDTO.getTotalItemsByHH());
				stmt.setLong(++counter, houseHoldDetailsDTO.getActiveItemsPerMonth());
				stmt.addBatch();
				recordCount++;
				if(recordCount % Constants.LIMIT_COUNT == 0){
					stmt.executeBatch();
					stmt.clearBatch();
					recordCount = 0;
				}
			}
			if(recordCount > 0){
				stmt.executeBatch();
				stmt.clearBatch();
			}
		} catch (SQLException exception) {
			logger.error("Error when inserting household details - " + exception);
			throw new GeneralException("Error in inserting household details", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	public List<String> getCustomerIdList(String storeId, Connection conn) throws GeneralException{
		List<String> listOfCustomerID = new ArrayList<String>();
		PreparedStatement statement = null;
	    ResultSet rs = null;
		try{
			statement = conn.prepareStatement(GET_CUSTOMER_ID_FROM_TLOG);
			int counter=0;
			statement.setString(++counter, storeId);
	        rs = statement.executeQuery();
	        while(rs.next()){
	        	listOfCustomerID.add(rs.getString("CUSTOMER_ID"));
	        }
		}
		catch (SQLException e)
		{
			logger.error("Error while executing getCustomerIdList()", e);
			throw new GeneralException("Error in inserting household details", e);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}
		return listOfCustomerID;
	}
}
