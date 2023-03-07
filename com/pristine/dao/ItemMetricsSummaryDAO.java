package com.pristine.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import javax.sql.rowset.CachedRowSet;
import org.apache.log4j.Logger;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;
import com.pristine.dto.MovementDailyAggregateDTO;

public class ItemMetricsSummaryDAO implements IDAO {
	static Logger logger = Logger.getLogger("MovementDAO");

	public List<MovementDailyAggregateDTO> getItemWeeklySummaryByDayCalendarId(
						Connection _Conn, int storeId, int calendar_id, 
					String categoryIdStr, ArrayList<Integer> leastProductLevel, 
				HashMap<Integer, Integer> gasItems) throws GeneralException {

		List<MovementDailyAggregateDTO> movementDataList = 
				new ArrayList<MovementDailyAggregateDTO>();

		StringBuffer sql = new StringBuffer();
		StringBuffer selectSB = new StringBuffer();
		StringBuffer joinSB = new StringBuffer();
		StringBuffer whereSB = new StringBuffer();
		StringBuffer orderSB = new StringBuffer();

		for (int i=0; i < leastProductLevel.size(); i++ ){
			selectSB.append(" PR" + i +".PRODUCT_ID as PRODUCT_ID" + i + ", ");
			joinSB.append(" LEFT OUTER JOIN PRODUCT_GROUP_RELATION PR");
			joinSB.append(i).append(" ON MV.PRODUCT_ID = PR"+i+ ".CHILD_PRODUCT_ID");
			joinSB.append(" AND PR" + i +".PRODUCT_LEVEL_ID=" + leastProductLevel.get(i));
			orderSB.append("PR" + i + ".PRODUCT_ID, ");
		}
		
		sql.append("select").append(selectSB);
		sql.append(" MV.PRODUCT_ID AS ITEM_CODE, FINAL_PRICE, FINAL_COST, REG_REVENUE, ");
		sql.append(" SALE_REVENUE, REG_MOVEMENT, SALE_MOVEMENT, 0 as POS_DEPARTMENT, ");
		sql.append(" MV.SALE_FLAG AS FLAG, I.ITEM_SIZE, I.UOM_ID ");

		sql.append(" from ITEM_METRIC_SUMMARY_WEEKLY MV");
		sql.append(" Left Join ITEM_LOOKUP I On MV.PRODUCT_ID=I.ITEM_CODE");
		sql.append(joinSB);
		
		sql.append(" WHERE MV.CALENDAR_ID = ").append(calendar_id);
		sql.append(" AND LOCATION_LEVEL_ID   =").append(Constants.STORE_LEVEL_ID);
		sql.append(" AND LOCATION_ID = ").append(storeId);
		sql.append(" AND MV.PRODUCT_LEVEL_ID = ").append(Constants.ITEMLEVELID); 
		
		sql.append(whereSB);

		if(categoryIdStr != null && !categoryIdStr.isEmpty())
			sql.append(" and (I.CATEGORY_ID NOT IN (").append(categoryIdStr).append(") OR I.CATEGORY_ID IS NULL)");
		
		sql.append(" order by ").append(orderSB);
		sql.append(" MV.PRODUCT_ID, MV.SALE_FLAG");
				
		logger.debug("GetItemWeeklySummaryByDayCalendarId SQL:" + sql.toString());

		try {
			CachedRowSet rst = PristineDBUtil.executeQuery(_Conn, sql,
				"getItemWeeklySummaryByDayCalendarId");
			
			if (rst.size() > 0 ) {
				while (rst.next()) {

					MovementDailyAggregateDTO objMovementDto = new MovementDailyAggregateDTO();
					
					HashMap<Integer, Integer> parentProductData = new HashMap<Integer, Integer>();
					for (int i=0; i < leastProductLevel.size(); i++ ){
						parentProductData.put(leastProductLevel.get(i) , rst.getInt("PRODUCT_ID" + i));
					}
	
					objMovementDto.setProductData(parentProductData);
					objMovementDto.setItemCode(rst.getInt("ITEM_CODE"));
					objMovementDto.setFlag(rst.getString("FLAG"));
					objMovementDto.setPosDepartment(rst.getInt("POS_DEPARTMENT"));
					objMovementDto.setFinalCost(rst.getDouble("FINAL_COST"));
					objMovementDto.setUnitPrice(rst.getDouble("FINAL_PRICE"));
					
					if (rst.getObject("UOM_ID") != null && rst.getDouble("UOM_ID") > 0)
					{
						objMovementDto.setuomId(rst.getString("UOM_ID"));
						
						if (rst.getString("FLAG").trim().equalsIgnoreCase("N") 
										&& rst.getDouble("REG_MOVEMENT") > 0)
							
							objMovementDto.setregMovementVolume(
								rst.getDouble("ITEM_SIZE") * rst.getDouble("REG_MOVEMENT"));
						
						if (rst.getString("FLAG").trim().equalsIgnoreCase("Y") 
										&& rst.getDouble("SALE_MOVEMENT") > 0)

							objMovementDto.setsaleMovementVolume(
								rst.getDouble("ITEM_SIZE") * rst.getDouble("SALE_MOVEMENT"));
					}

					if (rst.getString("FLAG").trim().equalsIgnoreCase("N")) {
						objMovementDto.setRegularQuantity(
								rst.getDouble("REG_MOVEMENT"));
						objMovementDto.setRevenueRegular(
								rst.getDouble("REG_REVENUE"));
					}
					else if (rst.getString("FLAG").trim().equalsIgnoreCase("Y")) {

						objMovementDto.setSaleQuantity(
							rst.getDouble("SALE_MOVEMENT"));
						objMovementDto.setRevenueSale(
							rst.getDouble("SALE_REVENUE"));
					}
					
					movementDataList.add(objMovementDto);

				}
			}
			else
			{
				logger.warn("There is no movement data");
			}
			
			rst.close();

		} catch (GeneralException exe) {
			logger.error(" Error while fetching the results.... ",exe);
			throw  new GeneralException(" Error while fetching the results.... ",exe);
		} catch (SQLException exe) {
			logger.error(" Error while fetching the results.... ",exe);
			throw  new GeneralException(" Error while fetching the results.... ",exe);
		}
		
		return movementDataList;
	}
	
}
