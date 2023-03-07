package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class ItemListDAO {

	private static Logger logger = Logger.getLogger("ItemListDAO");

	/**
	 * 
	 * @param conn
	 * @param itemType
	 * @param masterItemListId
	 * @return target item list for given master item list
	 * @throws GeneralException
	 */
	public List<PRItemDTO> getMasterItemList(Connection conn, String itemType, int masterItemListId)
			throws GeneralException {

		List<PRItemDTO> targetItems = new ArrayList<PRItemDTO>();

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT PCLI.ITEM_CODE, IL.RET_LIR_ID FROM PRICE_CHECK_LIST_ITEMS PCLI ");
		sb.append(" LEFT JOIN ITEM_LOOKUP IL ON PCLI.ITEM_CODE = IL.ITEM_CODE ");
		sb.append(" WHERE PCLI.PRICE_CHECK_LIST_ID = ? ");
		if (itemType.equals("OB")) {
			sb.append(" AND IL.PRIVATE_LABEL_IND = 'Y'");
		} else if (itemType.equals("NB")) {
			sb.append(" AND IL.PRIVATE_LABEL_IND = 'N'");
		}

		logger.debug(sb.toString());

		try {
			statement = conn.prepareStatement(sb.toString());
			statement.setInt(1, masterItemListId);

			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				int itemCode = resultSet.getInt("ITEM_CODE");
				int lirId = resultSet.getInt("RET_LIR_ID");
				PRItemDTO prItemDTO = new PRItemDTO();
				prItemDTO.setItemCode(itemCode);
				prItemDTO.setRetLirId(lirId);
				targetItems.add(prItemDTO);
			}
		} catch (SQLException ex) {
			throw new GeneralException("Error while getting items from master list", ex);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return targetItems;
	}

	
	
	
	/**
	 * 
	 * @param conn
	 * @param targetListId
	 * @throws GeneralException
	 */
	public void deleteItemsFromTargetItemList(Connection conn, int targetListId)
			throws GeneralException {
		PreparedStatement statement = null;
		try {
			String query = "DELETE FROM PRICE_CHECK_LIST_ITEMS WHERE PRICE_CHECK_LIST_ID = " + targetListId;
			statement = conn.prepareStatement(query);
			statement.executeUpdate();
		} catch (SQLException e) {
			throw new GeneralException("Error deleting items from PRICE_CHECK_LIST_ITEMS", e);
		}finally {
			PristineDBUtil.close(statement);
		}
	}
	
	
	/**
	 * 
	 * @param conn
	 * @param targetItems
	 * @param targetListId
	 * @throws GeneralException
	 */
	public void saveItemsIntoTargetItemList(Connection conn, List<PRItemDTO> targetItems, int targetListId)
			throws GeneralException {
		PreparedStatement statement = null;
		int itemNumInBatch = 0;
		try {

			String query = "INSERT INTO PRICE_CHECK_LIST_ITEMS (PRICE_CHECK_LIST_ID, ITEM_CODE) VALUES (?, ?)";

			statement = conn.prepareStatement(query);
			for (PRItemDTO dto : targetItems) {
				int count = 0;
				statement.setInt(++count, targetListId);
				statement.setInt(++count, dto.getItemCode());
				statement.addBatch();
				itemNumInBatch++;
				if (itemNumInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					statement.executeBatch();
					statement.clearBatch();
					itemNumInBatch = 0;
				}
			}

			if (itemNumInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			throw new GeneralException("Error saving items into PRICE_CHECK_LIST_ITEMS", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
}
