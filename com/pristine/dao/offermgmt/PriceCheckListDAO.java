package com.pristine.dao.offermgmt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.PriceCheckListDTO;
import com.pristine.util.PristineDBUtil;

public class PriceCheckListDAO {
	private static Logger logger = Logger.getLogger("PriceCheckListDAO");

	private static final String GET_PRICE_CHECK_LIST_INFO = " SELECT ID, NAME FROM PRICE_CHECK_LIST ";

	public HashMap<Integer, PriceCheckListDTO> getAllPriceCheckListInfo(Connection conn) {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<Integer, PriceCheckListDTO> priceCheckListMap = new HashMap<Integer, PriceCheckListDTO>();
		try {
			stmt = conn.prepareStatement(GET_PRICE_CHECK_LIST_INFO);
			rs = stmt.executeQuery();
			while (rs.next()) {
				PriceCheckListDTO pDTO = new PriceCheckListDTO();

				pDTO.setPriceCheckListId(rs.getInt("ID"));
				pDTO.setPriceCheckListName(rs.getString("NAME"));

				priceCheckListMap.put(pDTO.getPriceCheckListId(), pDTO);
			}
		} catch (SQLException e) {
			logger.error("Error in getAllPriceCheckListInfo() " + e.getMessage());
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return priceCheckListMap;
	}
}
