package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class ChainDAO implements IDAO {
	
	private static final String GET_BASE_CHAIN_ID = "SELECT COMP_CHAIN_ID FROM COMPETITOR_CHAIN WHERE PRESTO_SUBSCRIBER='Y' ";
	
	
	public String getBaseChainId(Connection connection) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		String chainId = "";
		try {
			stmt = connection.prepareStatement(GET_BASE_CHAIN_ID);
			rs = stmt.executeQuery();
			while (rs.next()) {
				chainId = rs.getString("COMP_CHAIN_ID");
			}
		} catch (Exception e) {
			throw new GeneralException("getBaseChainId()- Error when retrieving COMPETITOR_CHAIN  " + e);
		} finally {
			PristineDBUtil.close(rs, stmt, null);
		}

		return chainId;
	}
}
