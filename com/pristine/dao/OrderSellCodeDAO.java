package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import org.apache.log4j.Logger;

import com.pristine.dto.OrderSellCodeDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class OrderSellCodeDAO {

	static Logger logger = Logger.getLogger("OrderSellCodeDAO");

	private static final String DELETE_FROM_PR_ORDER_SELL_CODE_MAPPING = "DELETE FROM PR_ORDER_SELL_CODE_MAPPING ";

	private static final String INSERT_INTO_PR_ORDER_SELL_CODE_MAPPING = "INSERT INTO PR_ORDER_SELL_CODE_MAPPING (ORDER_SELL_CODE_MAPPING_ID, ORDER_CODE, SELL_CODE, ORDER_CODE_DESC, "
			+ "SELL_CODE_DESC, UPC, YIELD, DEPT_NO, SUB_DEPT_NO) VALUES "
			+ "(PR_REC_ORDER_CODE_ID_SEQ.NEXTVAL, ?, ?, ?, ?, ?, ?, ?, ?)";

	private static final String UPDATE_SELL_CODE_IN_ITEM_LOOKUP = "UPDATE ITEM_LOOKUP SET SELL_CODE = ? WHERE UPC =?";

	/*private static final String UPDATE_YIELD_IN_PR_ORDER_SELL_CODE_MAPPING =  "update pr_order_sell_code_mapping set yield = 0 "+ 
			   "where  sell_code not in (WITH category_info AS ( "+
			   "SELECT IL.SELL_CODE, IL.ITEM_NAME, IL.UPC, PROD.* FROM ITEM_LOOKUP IL "+ 
			   "JOIN (SELECT START_WITH_CHILD_id AS item_code, " +
			   "MIN(CASE WHEN PH.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID + " THEN PH.PRODUCT_ID END) AS CATEGORY_ID, "+ 
			   "MIN(CASE WHEN PH.PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID + " THEN PG.NAME END) AS CATEGORY_NAME "+
			   "FROM ( SELECT  PRODUCT_ID, PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID, CHILD_PRODUCT_LEVEL_ID, "+
			   "CONNECT_BY_root CHILD_PRODUCT_ID AS START_WITH_CHILD_id FROM PRODUCT_GROUP_RELATION PGr "+ 
			   "START WITH CHILD_PRODUCT_ID in ( "+
			   "SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGr "+ 
			   "START WITH PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID +
			   "CONNECT BY  PRIOR CHILD_PRODUCT_ID = PRODUCT_ID  AND  PRIOR CHILD_PRODUCT_LEVEL_ID = PRODUCT_LEVEL_ID) "+ 
			   "where CHILD_PRODUCT_LEVEL_ID =  " + Constants.ITEMLEVELID + ") " +
			   "AND CHILD_PRODUCT_LEVEL_ID = " + Constants.ITEMLEVELID +
			   "CONNECT BY PRIOR PRODUCT_ID =  CHILD_PRODUCT_ID AND PRIOR PRODUCT_LEVEL_ID =  CHILD_PRODUCT_LEVEL_ID ) PH "+
			   "LEFT JOIN PRODUCT_GROUP PG ON PH.PRODUCT_LEVEL_ID = PG.PRODUCT_LEVEL_ID AND PH.PRODUCT_ID = PG.PRODUCT_ID " +
			   "GROUP BY START_WITH_CHILD_id) PROD ON IL.ITEM_CODE = PROD.ITEM_CODE) " +
			   "select sell_code from category_info "+
			   "where category_id in (select product_id from pr_product_group_property "+
			   "where use_yield = 'Y'))";*/
	
	
	private static final String UPDATE_YIELD_IN_PR_ORDER_SELL_CODE_MAPPING =  
			"UPDATE PR_ORDER_SELL_CODE_MAPPING SET YIELD = 0 WHERE  SELL_CODE  IN( " +                
					" SELECT SCM.SELL_CODE FROM PR_ORDER_SELL_CODE_MAPPING SCM LEFT JOIN ITEM_LOOKUP IL ON IL.UPC = SCM.UPC " +
					" WHERE IL.ITEM_CODE NOT IN ((SELECT CHILD_PRODUCT_ID AS ITEM_CODE FROM(SELECT CHILD_PRODUCT_ID, " +
					" CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION PGR START WITH PRODUCT_LEVEL_ID = " + Constants.CATEGORYLEVELID +
					" AND PRODUCT_ID IN (SELECT PRODUCT_ID FROM PR_PRODUCT_GROUP_PROPERTY WHERE USE_YIELD = 'Y')" +
					" CONNECT BY PRIOR CHILD_PRODUCT_ID = PRODUCT_ID AND PRIOR CHILD_PRODUCT_LEVEL_ID    = PRODUCT_LEVEL_ID ) " +
					" WHERE CHILD_PRODUCT_LEVEL_ID = 1 )))";

	/*private static final String UPDATE_PR_ORDER_SELL_CODE_MAPPING = "update T_PR_ORDER_SELL_CODE_MAPPING "
			+ "set order_code=?, sell_code=?, order_code_desc=?, sell_code_desc=?, upc=?, yield=?, dept_no=?, sub_dept_no=? "
			+ "where order_sell_code_mapping_id=?";*/

	public int deleteOrderSellCodeMapping(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		int totalDeleteCnt = 0;
		try {
			statement = conn.prepareStatement(DELETE_FROM_PR_ORDER_SELL_CODE_MAPPING);
			totalDeleteCnt = statement.executeUpdate();
			statement.close();
		} catch (Exception e) {
			logger.error("Error while deleting records From PR_ORDER_SELL_CODE_MAPPING - " + e.toString());
			throw new GeneralException("Error in deleteFromOrderSellCodeMapping()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return totalDeleteCnt;
	}

	public int insertOrderSellCodeMapping(Connection conn, List<OrderSellCodeDTO> updatedRecordList)
			throws GeneralException {
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalInsertCnt = 0;

		try {
			statement = conn.prepareStatement(INSERT_INTO_PR_ORDER_SELL_CODE_MAPPING);

			for (OrderSellCodeDTO orderSellCodeDTO : updatedRecordList) {
				statement.setInt(1, orderSellCodeDTO.getOrderCode());
				statement.setInt(2, orderSellCodeDTO.getSellCode());
				statement.setString(3, orderSellCodeDTO.getOrderCodeDescr());
				statement.setString(4, orderSellCodeDTO.getSellCodeDescr());
				statement.setString(5, orderSellCodeDTO.getUPCtoUse());
				statement.setFloat(6, orderSellCodeDTO.getUpdatedYield());
				statement.setInt(7, orderSellCodeDTO.getDeptNum());
				statement.setInt(8, orderSellCodeDTO.getSubDeptNum());

				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					totalInsertCnt = totalInsertCnt + count.length;
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalInsertCnt = totalInsertCnt + count.length;
				statement.clearBatch();
			}
			statement.close();

		} catch (Exception e) {
			logger.error("Error while inserting records From PR_ORDER_SELL_CODE_MAPPING - " + e.toString());
			e.printStackTrace();
			throw new GeneralException("Error in insertOrderSellCodeMapping()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return totalInsertCnt;
	}

	public int updateSellCodeInItemLookup(Connection conn, List<OrderSellCodeDTO> updatedRecordList)
			throws GeneralException {
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalUpdateCnt = 0;
		try {
			statement = conn.prepareStatement(UPDATE_SELL_CODE_IN_ITEM_LOOKUP);
			for (OrderSellCodeDTO orderSellCodeDTO : updatedRecordList) {
				statement.setInt(1, orderSellCodeDTO.getSellCode());
				statement.setString(2, orderSellCodeDTO.getUPCtoUse());
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {

					int[] count = statement.executeBatch();
					totalUpdateCnt = totalUpdateCnt + count.length;
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalUpdateCnt = totalUpdateCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
			// conn.commit();
		} catch (Exception e) {
			logger.error("Exception in updateSellCodeInItemLookup() - " + e.toString());
			e.printStackTrace();
			throw new GeneralException("Exception in updateSellCodeInItemLookup()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return totalUpdateCnt;
	}

	public int updateYield(Connection conn, List<OrderSellCodeDTO> orderAndSellCodes) throws GeneralException {
		PreparedStatement statement = null;
		int totalUpdateCnt = 0;
		try {
			statement = conn.prepareStatement(UPDATE_YIELD_IN_PR_ORDER_SELL_CODE_MAPPING);
			totalUpdateCnt = statement.executeUpdate();
			statement.close();
		} catch (Exception e) {
			logger.error("Exception updateYield()  - " + e.toString());
			throw new GeneralException("Error in updateYield()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return totalUpdateCnt;
	}

	/*
	 * private static final String GET_PR_PRODUCT_LOCATION_MAPPING =
	 * "SELECT ORDER_SELL_CODE_MAPPING_ID, " +
	 * "ORDER_CODE, SELL_CODE FROM T_PR_ORDER_SELL_CODE_MAPPING";
	 * 
	 * public HashMap<OrderSellCodeMappingKey, Long> getOrderSellCodeMappingId(
	 * Connection conn) throws GeneralException {
	 * HashMap<OrderSellCodeMappingKey, Long> orderSellMappingId = new
	 * HashMap<OrderSellCodeMappingKey, Long>(); PreparedStatement statement =
	 * null; ResultSet rs = null; try { statement =
	 * conn.prepareStatement(GET_PR_PRODUCT_LOCATION_MAPPING); rs =
	 * statement.executeQuery(); while (rs.next()) { OrderSellCodeMappingKey
	 * osKey = new OrderSellCodeMappingKey( rs.getInt("ORDER_CODE"),
	 * rs.getInt("SELL_CODE")); orderSellMappingId.put(osKey,
	 * rs.getLong("ORDER_SELL_CODE_MAPPING_ID")); } } catch (Exception e) {
	 * logger.error("Error in getOrderSellCodeMappingId()  - " + e.toString());
	 * throw new GeneralException("Error in getOrderSellCodeMappingId()", e); }
	 * finally { PristineDBUtil.close(rs); PristineDBUtil.close(statement); }
	 * return orderSellMappingId; }
	 */

	public void setSellCodeToNull(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		int totalUpdateCnt = 0;
		try {
			String sql = "UPDATE ITEM_LOOKUP SET SELL_CODE = NULL";
			statement = conn.prepareStatement(sql);
			totalUpdateCnt = statement.executeUpdate();
			statement.close();
		} catch (Exception e) {
			logger.error("Exception deleteUnmatchedRecords()  - "
					+ e.toString());
			throw new GeneralException("Error in setSellCodeToNull()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	/*
	 * public void updateDeptNoAndSubDeptNo(Connection conn) throws
	 * GeneralException { PreparedStatement statement = null; int count = 0;
	 * StringBuffer sql = new StringBuffer(); try {
	 * sql.append("update T_PR_ORDER_SELL_CODE_MAPPING "); sql.append(
	 * "set dept_no = (select dept_code from department where id in (select dept_id from item_lookup where sell_code in (select sell_code from T_PR_ORDER_SELL_CODE_MAPPING))), "
	 * ); sql.append(
	 * "sub_dept_no = (select cat_code from category where id in (select category_id from item_lookup where sell_code in (select sell_code from T_PR_ORDER_SELL_CODE_MAPPING)))"
	 * ); count = PristineDBUtil.executeUpdate(conn, sql,
	 * "Updating PR_ORDER_SELL_CODE_MAPPING"); } catch (Exception e) {
	 * logger.error("Exception deleteUnmatchedRecords()  - " + e.toString());
	 * throw new GeneralException("Error in updateDeptNoAndSubDeptNo()", e); }
	 * finally { PristineDBUtil.close(statement); }
	 * 
	 * }
	 */

	public void resetSequenceOrderSellCodeMapping(Connection conn)
			throws GeneralException {
		PreparedStatement statement = null;
		StringBuffer sql = new StringBuffer();

		try {
			sql.append("ALTER SEQUENCE PR_REC_ORDER_CODE_ID_SEQ RESTART START WITH 1");

			PristineDBUtil.execute(conn, sql,
					"Reseting Sequence PR_REC_ORDER_CODE_ID_SEQ");
		} catch (Exception e) {
			logger.error("Exception resetSequenceOrderSellCodeMapping()  - "
					+ e.toString());
			throw new GeneralException(
					"Error in resetSequenceOrderSellCodeMapping()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
}

	/*
	 * public int updateOrderSellCodeMapping(Connection conn,
	 * HashMap<OrderSellCodeMappingKey, List<OrderSellCodeDTO>> finalMap,
	 * HashMap<OrderSellCodeMappingKey, Long> orderSellMappingId) throws
	 * GeneralException { logger.debug("Inside updateProductLocationMapping()");
	 * PreparedStatement statement = null; int itemNoInBatch = 0; int
	 * totalUpdateCnt = 0; try { statement =
	 * conn.prepareStatement(UPDATE_PR_ORDER_SELL_CODE_MAPPING);
	 * 
	 * for (Entry<OrderSellCodeMappingKey, List<OrderSellCodeDTO>> entry :
	 * finalMap.entrySet()) { OrderSellCodeMappingKey orderSellCodeMappingKey =
	 * entry.getKey(); //if combination is already there if
	 * (orderSellMappingId.get(orderSellCodeMappingKey) != null &&
	 * entry.getValue().size() > 0) { OrderSellCodeDTO orderSellCodeDTO =
	 * entry.getValue().get(0); statement.setInt(1,
	 * orderSellCodeDTO.getOrderCode()); statement.setInt(2,
	 * orderSellCodeDTO.getSellCode()); statement.setString(3,
	 * orderSellCodeDTO.getOrderCodeDescr()); statement.setString(4,
	 * orderSellCodeDTO.getSellCodeDescr()); statement.setString(5,
	 * orderSellCodeDTO.getUPCtoUse()); statement.setFloat(6,
	 * orderSellCodeDTO.getUpdatedYield()); statement.setInt(7,
	 * orderSellCodeDTO.getDeptNum()); statement.setInt(8,
	 * orderSellCodeDTO.getSubDeptNum()); statement.setLong(9,
	 * orderSellMappingId.get(orderSellCodeMappingKey)); statement.addBatch();
	 * itemNoInBatch++;
	 * 
	 * if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) { int[] count =
	 * statement.executeBatch(); totalUpdateCnt = totalUpdateCnt + count.length;
	 * statement.clearBatch(); itemNoInBatch = 0; } } } if (itemNoInBatch > 0) {
	 * int[] count = statement.executeBatch(); totalUpdateCnt = totalUpdateCnt +
	 * count.length; statement.clearBatch(); } statement.close(); finalMap =
	 * null; } catch (Exception e) {
	 * logger.error("Error in updateProductLocationMapping()  - " +
	 * e.toString()); throw new
	 * GeneralException("Error in updateProductLocationMapping()", e); } finally
	 * { PristineDBUtil.close(statement); }
	 * 
	 * return totalUpdateCnt;
	 * 
	 * }
	 */

}
