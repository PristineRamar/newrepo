package com.pristine.dao.Autozone;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.CompetitiveDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

public class StoreDAO {

	private static Logger logger = Logger.getLogger("StoreDAO");

	static String CompStr = "'" + PropertyManager.getProperty("COMP_STR") + "'";

	private static final String GET_COMP_STR_NUMBER = "SELECT CS.COMP_STR_NO,CS.COMP_CHAIN_ID,C.COMP_CHAIN_NAME,C.SHRT_NAME,"
			+ " CS.ADDR_LINE1,CS.STATE,CS.CITY,CS.ZIP  FROM COMPETITOR_STORE CS"
			+ " LEFT JOIN COMPETITOR_CHAIN C ON C.COMP_CHAIN_ID=CS.COMP_CHAIN_ID WHERE CS.COMP_CHAIN_ID<>100";

	private static final String INSERT_INTO_COMPETITOR_STORE = "INSERT INTO COMPETITOR_STORE(COMP_STR_ID, COMP_STR_NO, COMP_CHAIN_ID, NAME,"
			+ "ADDR_LINE1, CITY,STATE,ZIP,TIME_ZONE, CREATE_TIMESTAMP, SOURCE_INFO) VALUES (COMP_STORE_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,SYSDATE,?)";

	private static final String GET_HAWAI_ALASKA_STORES = "SELECT CS.COMP_STR_NO,C.COMP_CHAIN_ID,C.COMP_CHAIN_NAME,"
			+ " CS.ADDR_LINE1,CS.STATE,CS.CITY,CS.ZIP  FROM COMPETITOR_STORE CS "
			+ " LEFT JOIN COMPETITOR_CHAIN C ON C.COMP_CHAIN_NAME= " + CompStr
			+ " WHERE CS.COMP_STR_NO LIKE '84-%' AND (CS.STATE = 'HI' OR CS.STATE = 'AK')";

	private static final String GET_PURTO_RICO_STORES = "SELECT CS.COMP_STR_NO,C.COMP_CHAIN_ID,C.COMP_CHAIN_NAME,"
			+ " CS.ADDR_LINE1,CS.STATE,CS.CITY,CS.ZIP  FROM COMPETITOR_STORE CS "
			+ " LEFT JOIN COMPETITOR_CHAIN C ON C.COMP_CHAIN_NAME= " + CompStr
			+ " WHERE CS.COMP_STR_NO LIKE '44-%' AND CS.STATE = 'PR'";

	/**
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public List<CompetitiveDataDTO> getCompStr(Connection conn) throws GeneralException {

		List<CompetitiveDataDTO> strList = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		String sql = GET_COMP_STR_NUMBER;
		logger.debug(sql);
		try {
			statement = conn.prepareStatement(sql);

			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				CompetitiveDataDTO competitiveDataDTO = new CompetitiveDataDTO();
				competitiveDataDTO.setCompStrNo(resultSet.getString("COMP_STR_NO"));
				competitiveDataDTO.setCompChainId(resultSet.getString("COMP_CHAIN_ID"));
				competitiveDataDTO.setCompChainName(resultSet.getString("COMP_CHAIN_NAME"));
				competitiveDataDTO.setShortName(resultSet.getString("SHRT_NAME"));
				competitiveDataDTO.setAddressLine1(resultSet.getString("ADDR_LINE1"));
				competitiveDataDTO.setState(resultSet.getString("STATE"));
				competitiveDataDTO.setCity(resultSet.getString("CITY"));
				competitiveDataDTO.setZip(resultSet.getString("ZIP"));

				strList.add(competitiveDataDTO);
			}
		} catch (SQLException e) {
			logger.error("Error while executing getCompStr", e);
			throw new GeneralException("Error while executing getCompStr", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return strList;
	}

	public void setUpStores(Connection conn, List<String> insertList) throws GeneralException, SQLException {

		PreparedStatement statement = null;
		statement = conn.prepareStatement(INSERT_INTO_COMPETITOR_STORE);

		try {
			for (String insert : insertList) {
				// statement.setString(1, "COMP_STORE_SEQ.NEXTVAL");
				statement.setString(1, insert.split(";")[0]);
				statement.setString(2, insert.split(";")[1]);
				statement.setString(3, insert.split(";")[2]);
				statement.setString(4, insert.split(";")[3]);
				statement.setString(5, insert.split(";")[4]);
				statement.setString(6, insert.split(";")[5]);
				statement.setString(7, insert.split(";")[6]);
				statement.setString(8, "EST");
				// statement.setString(10, "SYSDATE");
				statement.setString(9, "'COMP-PRICE-FILE'");

				statement.executeUpdate();
				PristineDBUtil.commitTransaction(conn, "insert Store");
			}

		} catch (Exception e) {
			logger.error("Error -- setUpStores()", e);
		} finally {

			PristineDBUtil.close(statement);
		}
	}

	/**
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, CompetitiveDataDTO> getAlaskaHawaiStores(Connection conn) throws GeneralException {

		HashMap<String, CompetitiveDataDTO> strMap = new HashMap<String, CompetitiveDataDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		String sql = GET_HAWAI_ALASKA_STORES;

		try {
			statement = conn.prepareStatement(sql);

			logger.debug(sql);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				CompetitiveDataDTO competitiveDataDTO = new CompetitiveDataDTO();
				competitiveDataDTO.setCompStrNo(resultSet.getString("COMP_STR_NO"));
				competitiveDataDTO.setCompChainId(resultSet.getString("COMP_CHAIN_ID"));
				competitiveDataDTO.setCompChainName(resultSet.getString("COMP_CHAIN_NAME"));
				competitiveDataDTO.setAddressLine1(resultSet.getString("ADDR_LINE1"));
				competitiveDataDTO.setState(resultSet.getString("STATE"));
				competitiveDataDTO.setCity(resultSet.getString("CITY"));
				competitiveDataDTO.setZip(resultSet.getString("ZIP"));

				strMap.put(competitiveDataDTO.getCompStrNo(), competitiveDataDTO);
			}
		} catch (SQLException e) {
			logger.error("getAlaskaHawaiStores()-Error while executing getAlaskaHawaiStores", e);
			throw new GeneralException("getAlaskaHawaiStores()-Error while executing getCompStr", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return strMap;
	}

	/**
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, CompetitiveDataDTO> getPurtoRicoStores(Connection conn) throws GeneralException {

		HashMap<String, CompetitiveDataDTO> strMap = new HashMap<String, CompetitiveDataDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		String sql = GET_PURTO_RICO_STORES;
		logger.debug(sql);
		try {
			statement = conn.prepareStatement(sql);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				CompetitiveDataDTO competitiveDataDTO = new CompetitiveDataDTO();
				competitiveDataDTO.setCompStrNo(resultSet.getString("COMP_STR_NO"));
				competitiveDataDTO.setCompChainId(resultSet.getString("COMP_CHAIN_ID"));
				competitiveDataDTO.setCompChainName(resultSet.getString("COMP_CHAIN_NAME"));
				competitiveDataDTO.setAddressLine1(resultSet.getString("ADDR_LINE1"));
				competitiveDataDTO.setState(resultSet.getString("STATE"));
				competitiveDataDTO.setCity(resultSet.getString("CITY"));
				competitiveDataDTO.setZip(resultSet.getString("ZIP"));

				strMap.put(competitiveDataDTO.getCompStrNo(), competitiveDataDTO);
			}
		} catch (SQLException e) {
			logger.error("getAlaskaHawaiStores()-Error while executing getAlaskaHawaiStores", e);
			throw new GeneralException("getAlaskaHawaiStores()-Error while executing getCompStr", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return strMap;
	}

}
