package com.pristine.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.dto.MovementDailyAggregateDTO;
import com.pristine.dto.salesanalysis.ProductGroupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class TransactionLogDAO {

	static Logger logger = Logger.getLogger("TransactionLogDAO");
	
	
	// Added categoryIdStr parameter to exclude specific categories during
	// revenue calculation
	public List<MovementDailyAggregateDTO> getTLogByTransaction(
			Connection _Conn, int storeId, int calendar_id,
			String categoryIdStr, int maxPOS) throws GeneralException {

		List<MovementDailyAggregateDTO> movementDataList = new ArrayList<MovementDailyAggregateDTO>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select NVL(TL.CUSTOMER_ID, 0) AS CUSTOMER_ID, TRX_NO, TL.ITEM_ID AS ITEM_CODE, TL.SALE_TYPE as FLAG");

		// add uom_id
		sql.append(" ,I.UOM_ID");

		// add the movement by volume - multiply the quantity * item size.
		sql.append(" ,sum(TL.QUANTITY * I.ITEM_SIZE) as VOLUME");
		sql.append(" ,sum(QUANTITY) as QUANTITY");

		// Actual weight to Margin calculation
		sql.append(" ,sum(TL.WEIGHT) as WEIGHT_ACTUAL");
		sql.append(" ,count(case when TL.WEIGHT=0  then null  else 1 end) as WEIGHT");
		sql.append(" ,sum(TL.NET_AMT) as REVENUE");

		// code added for gas revenue
		sql.append(" ,sum(TL.REGULAR_AMT) as GROSSREVENUE");
		
		sql.append(" FROM TRANSACTION_LOG_T_203 TL LEFT JOIN ITEM_LOOKUP I On TL.ITEM_ID=I.ITEM_CODE");
		
		sql.append(" where  TL.CALENDAR_ID =").append(calendar_id);
		
		sql.append(" and TL.STORE_ID=").append(storeId);

		// add the new condition pos_department
		sql.append(" and TL.POS_DEPARTMENT_ID <").append(maxPOS);

		
		// Exclude categories for revenue calculation
		if (categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" and I.CATEGORY_ID NOT IN (").append(categoryIdStr)
					.append(")");

		//sql.append(" group by MV.CUSTOMER_CARD_NO, MV.TRANSACTION_NO, MV.ITEM_CODE, MV.SALE_FLAG, I.UOM_ID");
		sql.append(" group by TL.CUSTOMER_ID, TL.TRX_NO, TL.ITEM_ID, TL.SALE_TYPE, I.UOM_ID");

		sql.append(" order by CUSTOMER_ID, TRX_NO");

		logger.debug("getTLogByTransaction SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
					"getTLogByTransaction");

			while (rst.next()) {

				MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();

				objMovementDto.setCustomerId(rst.getString("CUSTOMER_ID"));
				objMovementDto.setTransactionNo(rst.getInt("TRX_NO"));
				objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
				objMovementDto.setFlag(rst.getString("FLAG"));
				objMovementDto.setuomId(rst.getString("UOM_ID"));

				if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
					objMovementDto.set_regularQuantity(rst
							.getDouble("QUANTITY") + rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					objMovementDto.setregMovementVolume(rst.getDouble("VOLUME")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setRevenueRegular(rst.getDouble("REVENUE"));
					objMovementDto.setregGrossRevenue(rst
							.getDouble("GROSSREVENUE"));
				}
				if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
					objMovementDto.set_saleQuantity(rst.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					objMovementDto.setsaleMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					objMovementDto.setRevenueSale(rst.getDouble("REVENUE"));
					objMovementDto.setsaleGrossRevenue(rst
							.getDouble("GROSSREVENUE"));
				}
				movementDataList.add(objMovementDto);
			}

			/*
			 * logger.info(" Daily Summary Process Count " +
			 * movementDataList.size());
			 */

			rst.close();

		} catch (GeneralException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		} catch (SQLException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		}

		return movementDataList;
	}

	
	
	// Added categoryIdStr parameter to exclude specific categories during
	// revenue calculation
	public List<MovementDailyAggregateDTO> getTLogByItem(
			Connection _Conn, int storeId, int calendar_id,
			String categoryIdStr, int maxPOS) throws GeneralException {

		List<MovementDailyAggregateDTO> movementDataList = new ArrayList<MovementDailyAggregateDTO>();

		StringBuffer sql = new StringBuffer();

		sql.append(" select NVL(TL.CUSTOMER_ID, 0) AS CUSTOMER_ID, TL.ITEM_ID AS ITEM_CODE, TL.SALE_TYPE as FLAG");

		// add uom_id
		sql.append(" ,I.UOM_ID");

		// add the movement by volume - multiply the quantity * item size.
		sql.append(" ,sum(TL.QUANTITY * I.ITEM_SIZE) as VOLUME");
		sql.append(" ,sum(QUANTITY) as QUANTITY");

		// Actual weight to Margin calculation
		sql.append(" ,sum(TL.WEIGHT) as WEIGHT_ACTUAL");
		sql.append(" ,count(case when TL.WEIGHT=0  then null  else 1 end) as WEIGHT");
		sql.append(" ,sum(TL.NET_AMT) as REVENUE");

		// code added for gas revenue
		sql.append(" ,sum(TL.REGULAR_AMT) as GROSSREVENUE, ");
		
		//Visit count
		sql.append(" ,count(TL.TRX_NO) as VISIT_CNT");
		
		sql.append(" FROM TRANSACTION_LOG TL LEFT JOIN ITEM_LOOKUP I On TL.ITEM_ID=I.ITEM_CODE");
		
		sql.append(" where  TL.CALENDAR_ID IN (").append(calendar_id).append(")");
		
		sql.append(" and TL.STORE_ID=").append(storeId);

		// add the new condition pos_department
		sql.append(" and TL.POS_DEPARTMENT_ID <").append(maxPOS);

		
		// Exclude categories for revenue calculation
		if (categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" and I.CATEGORY_ID NOT IN (").append(categoryIdStr)
					.append(")");

		//sql.append(" group by MV.CUSTOMER_CARD_NO, MV.TRANSACTION_NO, MV.ITEM_CODE, MV.SALE_FLAG, I.UOM_ID");
		sql.append(" group by TL.CUSTOMER_ID, TL.ITEM_ID, TL.SALE_TYPE, I.UOM_ID");

		sql.append(" order by CUSTOMER_ID, TL.ITEM_ID");

		logger.debug("getTLogByTransaction SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
					"getTLogByTransaction");

			while (rst.next()) {

				MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();

				objMovementDto.setCustomerId(rst.getString("CUSTOMER_ID"));
				objMovementDto.setTransactionNo(rst.getInt("TRX_NO"));
				objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
				objMovementDto.setFlag(rst.getString("FLAG"));
				objMovementDto.setuomId(rst.getString("UOM_ID"));

				if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
					objMovementDto.set_regularQuantity(rst
							.getDouble("QUANTITY") + rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					objMovementDto.setregMovementVolume(rst.getDouble("VOLUME")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setRevenueRegular(rst.getDouble("REVENUE"));
					objMovementDto.setregGrossRevenue(rst
							.getDouble("GROSSREVENUE"));
				}
				if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
					objMovementDto.set_saleQuantity(rst.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst.getDouble("QUANTITY"));
					objMovementDto.setsaleMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					objMovementDto.setRevenueSale(rst.getDouble("REVENUE"));
					objMovementDto.setsaleGrossRevenue(rst
							.getDouble("GROSSREVENUE"));
				}
				movementDataList.add(objMovementDto);
			}

			/*
			 * logger.info(" Daily Summary Process Count " +
			 * movementDataList.size());
			 */

			rst.close();

		} catch (GeneralException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		} catch (SQLException exe) {
			logger.error(" Error while fetching the results.... ", exe);
			throw new GeneralException(
					" Error while fetching the results.... ", exe);
		}

		return movementDataList;
	}

	
	
	
	/*
	 * Method used to get the distinct item code for given calendar and store no
	 * Argument 1 : conn Argument 2 : Store number Argument 3 : calendar_id
	 * 
	 * @throw GeneralException
	 */

	public List<String> getDistinctItemCode(Connection _conn,
			int storeId, int calendar_id, int maxPOS) throws GeneralException {

		// Return List
		List<String> returnList = new ArrayList<String>();

		try {
			// Query
			StringBuffer sql = new StringBuffer();

			sql.append(" select distinct ITEM_ID from TRANSACTION_LOG");
			sql.append(" where CALENDAR_ID=").append(calendar_id);
			sql.append(" and STORE_ID=").append(storeId);
		
			sql.append(" and POS_DEPARTMENT_ID <").append(maxPOS);
			
			logger.debug("getDistinctItemCode SQL:" + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getDistinctItemCode");

			while (result.next()) {

				returnList.add(result.getString("ITEM_ID"));
			}

			result.close();

			logger.debug("getDistinctItemCode:" + returnList.size());
		} catch (SQLException e) {
			logger.error(
					" Error while fetching the distinct item code in movement daily ",
					e);
			throw new GeneralException(
					" Error while fetching the distinct item code in movement daily ",
					e);
		} catch (GeneralException e) {
			logger.error(
					" Error while fetching the distinct item code in movement daily ",
					e);
			throw new GeneralException(
					" Error while fetching the distinct item code in movement daily ",
					e);
		}

		return returnList;
	}
	
	
	public HashMap<Integer , List<MovementDailyAggregateDTO>> 
		getTLogForDateRangeByCustomer(Connection _Conn, int storeId, 
		String  dailyCalListStr, String categoryIdStr, 
		ArrayList<Integer> leastProductLevel, String maxPOS) 
											throws GeneralException {

		HashMap<Integer, List<MovementDailyAggregateDTO>> customerSummaryMap = 
					new HashMap<Integer, List<MovementDailyAggregateDTO>>(); 
		
		List<MovementDailyAggregateDTO> movementDataList = 
			new ArrayList<MovementDailyAggregateDTO>();
		
		StringBuffer sql = new StringBuffer();
		StringBuffer selectSB = new StringBuffer();
		StringBuffer joinSB = new StringBuffer();
		StringBuffer whereSB = new StringBuffer();
		StringBuffer groupSB = new StringBuffer();
		StringBuffer orderSB = new StringBuffer();
		
		for (int i=0; i < leastProductLevel.size(); i++ ){
		selectSB.append(" PR" + i +".PRODUCT_ID as PRODUCT_ID" + i + ", ");
		joinSB.append(" LEFT OUTER JOIN PRODUCT_GROUP_RELATION PR");
		joinSB.append(i).append(" ON MV.ITEM_ID = PR"+i+ ".CHILD_PRODUCT_ID");
		joinSB.append(" AND PR" + i +".PRODUCT_LEVEL_ID=" + leastProductLevel.get(i));
		groupSB.append("PR" + i + ".PRODUCT_ID, ");
		orderSB.append("PR" + i + ".PRODUCT_ID, ");
		}
		
		sql.append("select").append(selectSB);
		sql.append(" MV.CUSTOMER_ID, MV.ITEM_ID AS ITEM_CODE, MV.SALE_TYPE as FLAG");
		sql.append(", I.UOM_ID, MV.POS_DEPARTMENT_ID AS POS_DEPARTMENT");
		sql.append(", sum(MV.QUANTITY * I.ITEM_SIZE) as VOLUME");
		sql.append(", sum(MV.QUANTITY) as QUANTITY");
		sql.append(", sum(MV.WEIGHT) as WEIGHT_ACTUAL");
		sql.append(", count(case when MV.WEIGHT=0  then null else 1 end) as WEIGHT");
		sql.append(", sum( MV.NET_AMT) as REVENUE");
		sql.append(", sum(MV.REGULAR_AMT) as GROSSREVENUE");
		sql.append(" from TRANSACTION_LOG MV");
		sql.append(" Left Join ITEM_LOOKUP I On MV.ITEM_ID=I.ITEM_CODE");
		sql.append(joinSB);
		sql.append(" where  MV.CALENDAR_ID in (").append(dailyCalListStr); 
		sql.append(") and STORE_ID=").append(storeId);
		
		if (Integer.parseInt(maxPOS) > 0)
		sql.append(" and MV.POS_DEPARTMENT_ID <= ").append(maxPOS);
		
		sql.append(whereSB);
		
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
		sql.append(" and (I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(") OR I.CATEGORY_ID IS NULL)");
		//sql.append(" and rownum <= 100 ");
		sql.append(" group by ").append(groupSB);
		sql.append(" MV.CUSTOMER_ID, MV.ITEM_ID, MV.SALE_TYPE, I.UOM_ID, MV.POS_DEPARTMENT_ID");
		sql.append(" order by MV.CUSTOMER_ID, ").append(orderSB);
		sql.append(" MV.ITEM_ID, MV.SALE_TYPE");
			
		logger.debug("getTLogForDateRangeByCustomer SQL:" + sql.toString());
		
		try {
		CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
			"getMovementDailyData");
		
		if (rst.size() > 0 ) {

			int customerIdTemp = 0;
			int cnter = 0;
			while (rst.next()) {
				
				if (cnter > 0 && customerIdTemp != rst.getInt("CUSTOMER_ID") )
				{
					//logger.debug("Customer " + customerIdTemp + " Items " + movementDataList.size());
					customerSummaryMap.put(customerIdTemp, movementDataList);
					movementDataList = new ArrayList<MovementDailyAggregateDTO>();
				}
					
				MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();
				
				HashMap<Integer, Integer> parentProductData = new HashMap<Integer, Integer>();
				for (int i=0; i < leastProductLevel.size(); i++ ){
					parentProductData.put(leastProductLevel.get(i) , rst.getInt("PRODUCT_ID" + i));
				}
		
				objMovementDto.setProductData(parentProductData);
				
				objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
				objMovementDto.setFlag(rst.getString("FLAG"));
				objMovementDto.setuomId(rst.getString("UOM_ID"));
				
				objMovementDto.setPosDepartment(
											rst.getInt("POS_DEPARTMENT"));
		
				if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
					objMovementDto.setRegularQuantity(rst
							.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst
							.getDouble("QUANTITY"));
					objMovementDto.setregMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					
					objMovementDto.setRevenueRegular(
											rst.getDouble("REVENUE"));
		
					objMovementDto.setregGrossRevenue(rst
											.getDouble("GROSSREVENUE"));
				}
				if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {
					objMovementDto.setSaleQuantity(rst
							.getDouble("QUANTITY")
							+ rst.getDouble("WEIGHT"));
					objMovementDto.setActualWeight(rst
							.getDouble("WEIGHT_ACTUAL"));
					objMovementDto.setActualQuantity(rst
							.getDouble("QUANTITY"));
					objMovementDto.setsaleMovementVolume(rst
							.getDouble("VOLUME") + rst.getDouble("WEIGHT"));
					
					objMovementDto.setRevenueSale(
												rst.getDouble("REVENUE"));
					
					objMovementDto.setsaleGrossRevenue(rst
									.getDouble("GROSSREVENUE"));
				}
				movementDataList.add(objMovementDto);
			
				customerIdTemp = rst.getInt("CUSTOMER_ID");
				cnter++;
			}
			
			if (cnter > 0)
				customerSummaryMap.put(customerIdTemp, movementDataList);
		}
		else
		{
			logger.warn("There is no movement data");
		}
		
		/*logger.info(" Daily Summary Process Count " + movementDataList.size());*/
		
		rst.close();
				
		
		} catch (GeneralException exe) {
		logger.error(" Error while fetching the results.... ",exe);
		throw  new GeneralException(" Error while fetching the results.... ",exe);
		} catch (SQLException exe) {
		logger.error(" Error while fetching the results.... ",exe);
		throw  new GeneralException(" Error while fetching the results.... ",exe);
		}
		
		return customerSummaryMap;
	}
	
	public HashMap<Double, Double> getVisitForDateRangeByCustomer(Connection _Conn, int storeId, 
			String dailyCalListStr, String categoryIdStr, String maxPOS) throws GeneralException {
		
		double customerId;
		double storeVisit;
		
		HashMap<Double, Double> storeVisitMap = new HashMap<Double, Double>();
		
		
		try {
		StringBuffer sql = new StringBuffer();
		
		sql.append(" select M.CUSTOMER_ID, COUNT( DISTINCT (TRX_NO || CALENDAR_ID)) AS VISIT_COUNT ");
		sql.append(" FROM TRANSACTION_LOG M");

		if(categoryIdStr != null && !categoryIdStr.isEmpty())
		sql.append(" LEFT JOIN ITEM_LOOKUP I ON I.ITEM_CODE = M.ITEM_CODE");
		
		sql.append(" where  CALENDAR_ID in (").append(dailyCalListStr); 
		sql.append(") and  M.STORE_ID=").append(storeId).append("");
		
		if (Integer.parseInt(maxPOS) > 0)
		sql.append(" and M.POS_DEPARTMENT_ID <=").append(maxPOS);
		
		if(categoryIdStr != null && !categoryIdStr.isEmpty())
		sql.append(" and I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(")");
		
		//sql.append(" and rownum <= 100 ");
		
		sql.append(" GROUP BY M.CUSTOMER_ID");
		
		logger.debug("getVisitForDateRangeByCustomer SQL: " + sql.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql, 
											"getVisitForDateRangeByCustomer");
		
		while (result.next()) {
			customerId = result.getDouble("CUSTOMER_ID");
			storeVisit = result.getDouble("VISIT_COUNT");
			storeVisitMap.put(customerId, storeVisit);
		}
		
		result.close();
		
		} catch (SQLException sql) {
		logger.error("Error While Fetching Store Visitor Summary " , sql);
		throw new GeneralException("Error While Fetching VisitSummary" ,sql);
		} catch (GeneralException e) {
		logger.error("Error While Fetching VisitSummary " , e);
		throw new GeneralException("Error While Fetching VisitSummary" ,e);
		}
		
		return storeVisitMap;
	}

	public List<String> getDistinctItemCodeForWeek(Connection _conn,
			String storeNumber, String dailyCalListStr, String maxPOS) throws GeneralException {

		// Return List
		List<String> returnList = new ArrayList<String>();

		try {
			// Query
			StringBuffer sql = new StringBuffer();

			sql.append(" select distinct ITEM_ID ITEM_CODE from TRANSACTION_LOG T, COMPETITOR_STORE CS");
			sql.append(" where T.store_id = CS.comp_str_id and CALENDAR_ID in (").append(dailyCalListStr);
			sql.append(") and COMP_STR_NO='").append(storeNumber).append("'");
			
			if (Integer.parseInt(maxPOS) > 0)
				sql.append(" and POS_DEPARTMENT_ID <=").append(maxPOS);			
			
			//sql.append(" and ROWNUM <=500");
			
			logger.debug("getDistinctItemCode SQL:" + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(_conn, sql,
					"getDistinctItemCode");

			while (result.next()) {

				returnList.add(result.getString("ITEM_CODE"));
			}

			result.close();

			logger.debug("Movement Item code List size:" + returnList.size());
		} catch (SQLException e) {
			logger.error(
					" Error while fetching the distinct item code in movement daily ",
					e);
			throw new GeneralException(
					" Error while fetching the distinct item code in movement daily ",
					e);
		} catch (GeneralException e) {
			logger.error(
					" Error while fetching the distinct item code in movement daily ",
					e);
			throw new GeneralException(
					" Error while fetching the distinct item code in movement daily ",
					e);
		}

		return returnList;
	}
	


	public HashMap<Double, HashMap<String, Integer>> getItemVisit(
		Connection _Conn, int storeId, String calListStr, String maxPOS 
												) throws GeneralException {
		
		HashMap<Double, HashMap<String, Integer>> custVisitMap = 
							new HashMap<Double, HashMap<String, Integer>>();
		
		HashMap<String, Integer> visitMap = new HashMap<String, Integer>();
		
		try {		
		
			StringBuffer sb = new StringBuffer();
			
			sb.append("SELECT CUSTOMER_ID,");
			sb.append(" ITEM_ID AS PRODUCT_ID,");
			sb.append(" COUNT(DISTINCT M.TRX_NO) AS VISIT_COUNT");
			sb.append(" FROM TRANSACTION_LOG M");
			sb.append(" WHERE M.CALENDAR_ID IN (").append(calListStr).append(")");
			sb.append(" AND STORE_ID = ").append(storeId) ;
			
			if (Integer.parseInt(maxPOS) > 0)
				sb.append(" AND M.POS_DEPARTMENT_ID <= ").append(maxPOS) ;
			
			sb.append(" GROUP BY CUSTOMER_ID, ITEM_ID");
			sb.append(" ORDER BY CUSTOMER_ID, ITEM_ID "); 
			
			logger.debug("getItemVisit SQL:" + sb.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
													"getItemVisit");
			
			if (result.size() >0){
				
				double custIdTemp = 0;
				
				
				int cntTemp = 0;
				while (result.next()) {
					
					if (cntTemp > 0 && custIdTemp != result.getDouble("CUSTOMER_ID"))
					{
						//logger.debug("For customer " +  custIdTemp + " Product count " + visitMap.size());
						custVisitMap.put(custIdTemp, visitMap);
						
						visitMap = new HashMap<String, Integer>();
					}	
					
					//logger.debug(result.getDouble("CUSTOMER_ID") +", " + result.getString("PRODUCT_ID") + ", " + result.getInt("VISIT_COUNT"));
					visitMap.put(result.getString("PRODUCT_ID"), result.getInt("VISIT_COUNT"));
				
					custIdTemp = result.getDouble("CUSTOMER_ID");
					cntTemp++;
				}
				
				if (cntTemp > 0){
					logger.debug("For customer " +  custIdTemp + " Product count " + visitMap.size());
					custVisitMap.put(custIdTemp, visitMap);
				}
			}
			
			result.close();
		
		} catch (SQLException sql) {
			logger.error("Error While Fetching Product Visit Info" , sql);
			throw new GeneralException("Error While Fetching VisitSummary" ,sql);
		} catch (GeneralException e) {
			logger.error("Error While Fetching Product Visit Info" , e);
			throw new GeneralException("Error While Fetching Product Visit" ,e);
		}
		
		return custVisitMap;
	}
	
	
	
	public HashMap<Double, HashMap<String, Integer>> getProductVisit(
		Connection _Conn, int storeId, String calListStr, int productLevelId, 
		String maxPOS, ArrayList<ProductGroupDTO> childProdutLevelData 
												) throws GeneralException {
		
		HashMap<Double, HashMap<String, Integer>> custVisitMap = 
							new HashMap<Double, HashMap<String, Integer>>();
		
		HashMap<String, Integer> visitMap = new HashMap<String, Integer>();
		
		try {		
		
			StringBuffer sbjoin = new StringBuffer();
			
			int childLevel = Constants.ITEMLEVELID;
			int previousProductLevel = 0;
			
			if (childProdutLevelData !=null && childProdutLevelData.size() > 0){
			
				for(int i=0; i < childProdutLevelData.size(); i++){
					
					ProductGroupDTO childProductDTO = childProdutLevelData.get(i);
					
					//if (childProductDTO.getProductLevelId() != productLevelId){
			
						if (childProductDTO.getProductLevelId() > Constants.ITEMLEVELID) {
						
							String tableAlias0 = "P" + i;
							String tableAlias1 = "P" + (i + 1);
			
							if (childProductDTO.getChildLevelId() == Constants.ITEMLEVELID) {
								String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
								tableAlias0 + " ON M.ITEM_ID = "+ tableAlias0 + 
								".CHILD_PRODUCT_ID AND " + tableAlias0 + 
								".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
								
								//logger.debug("JOIN string:"+ joinStr);
								sbjoin.insert(0, joinStr);
							}
							else {
								String joinStr = " LEFT JOIN PRODUCT_GROUP_RELATION "+ 
								tableAlias0 + " ON "+ tableAlias1+ ".PRODUCT_ID = "+ tableAlias0 + 
								".CHILD_PRODUCT_ID AND " + tableAlias0 + 
								".PRODUCT_LEVEL_ID=" + childProductDTO.getProductLevelId();
								
								//logger.debug("JOIN string:"+ joinStr);
								sbjoin.insert(0, joinStr);
							}
							
							previousProductLevel = childProductDTO.getProductLevelId();
							
						}
				}
			}
			
			StringBuffer sb = new StringBuffer();
			sb.append("select CUSTOMER_ID, P0.PRODUCT_ID as PRODUCT_ID, COUNT(DISTINCT M.TRX_NO)");
			sb.append(" as VISIT_COUNT from TRANSACTION_LOG M");
			
			if (childProdutLevelData != null)
				sb.append(sbjoin.toString());
			else {	
				if (childLevel == Constants.ITEMLEVELID){
					sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
					sb.append(" ON M.ITEM_ID = P0.CHILD_PRODUCT_ID ");
					sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
				}
				else{
					sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION P0 ");
					sb.append(" ON P").append(previousProductLevel);
					sb.append(".PRODUCT_ID = P0.CHILD_PRODUCT_ID ");
					sb.append(" AND P0.PRODUCT_LEVEL_ID=").append(productLevelId);
				}
			}
			
			sb.append("	where  M.CALENDAR_ID IN (").append(calListStr);
			sb.append(")	and STORE_ID=").append(storeId);
			
			if (Integer.parseInt(maxPOS) > 0)
				sb.append(" and M.POS_DEPARTMENT_ID <=").append(maxPOS);	
			
			//sb.append(" and ROWNUM <=100");

			sb.append("	group by CUSTOMER_ID, P0.PRODUCT_ID order by CUSTOMER_ID, P0.PRODUCT_ID ");
			
			logger.debug("getProductVisit SQL:" + sb.toString());
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sb, 
													"GetLeastProductVisit");
			
			if (result.size() >0){
				
				double custIdTemp = 0;
				
				
				int cntTemp = 0;
				while (result.next()) {
					
					if (cntTemp > 0 && custIdTemp != result.getDouble("CUSTOMER_ID"))
					{
						//logger.debug("For customer " +  custIdTemp + " Product count " + visitMap.size());
						custVisitMap.put(custIdTemp, visitMap);
						
						visitMap = new HashMap<String, Integer>();
					}	
					
					//logger.debug(result.getDouble("CUSTOMER_ID") +", " + result.getString("PRODUCT_ID") + ", " + result.getInt("VISIT_COUNT"));
					visitMap.put(result.getString("PRODUCT_ID"), result.getInt("VISIT_COUNT"));
				
					custIdTemp = result.getDouble("CUSTOMER_ID");
					cntTemp++;
				}
				
				if (cntTemp > 0){
					logger.debug("For customer " +  custIdTemp + " Product count " + visitMap.size());
					custVisitMap.put(custIdTemp, visitMap);
				}
			}
			
			result.close();
		
		} catch (SQLException sql) {
			logger.error("Error While Fetching Product Visit Info" , sql);
			throw new GeneralException("Error While Fetching VisitSummary" ,sql);
		} catch (GeneralException e) {
			logger.error("Error While Fetching Product Visit Info" , e);
			throw new GeneralException("Error While Fetching Product Visit" ,e);
		}
		
		return custVisitMap;
	}
	

	
}
