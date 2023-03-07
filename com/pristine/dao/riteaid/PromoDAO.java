package com.pristine.dao.riteaid;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.pristine.dto.PromoDataStandardDTO;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

@SuppressWarnings("unused")
public class PromoDAO {
	static Logger logger = Logger.getLogger("PromoDAO");

	private static final String GET_PROMO_FOR_WEEK = " SELECT "
			+ "A.PROMO_DEFINITION_ID,A.PROMO_TYPE_ID,A.PROMO_SUB_TYPE_ID,RC.START_DATE ,"
			+ "A.START_CALENDAR_ID,A.END_CALENDAR_ID,A.NAME AS PROMO_NAME, "
			+ "A.\"NUMBER\"  AS PROMO_NUMBER,A.PROMO_GROUP_ID,          "
			+ "B.ITEM_CODE,G.UPC,D.PROMO_TYPE_NAME,B.REG_PRICE,B.REG_QTY,B.REG_M_PRICE,B.SALE_PRICE,B.SALE_QTY,B.SALE_M_PRICE,C.BUY_X,E.OFFER_COUNT,"
			+ "C.MUSTBUY_QTY,C.MUSTBUY_AMT,C.STORE_COUPON,"
			+ "E.OFFER_VALUE,E.OFFER_UNIT_TYPE,F.OFFER_NAME,I.BLOCK_NUMBER,J.PAGE_NUMBER,K.AD_NAME,m.DISPLAY_NAME,K.TOTAL_PAGES, "
			+ "pl.LOCATION_ID,pl.LOCATION_LEVEL_ID,A.IS_EDLP_PROMO,  RC2.START_DATE AS ORIGINAL_START_DATE,RC3.START_DATE AS ORIGINAL_END_DATE,IT.ITEM_CODE AS OFFER_ITEM  "
			+ "FROM PM_PROMO_DEFINITION A LEFT JOIN PM_PROMO_BUY_ITEM B ON A.PROMO_DEFINITION_ID = B.PROMO_DEFINITION_ID "
			+ "LEFT JOIN PM_PROMO_BUY_REQUIREMENT C ON A.PROMO_DEFINITION_ID = C.PROMO_DEFINITION_ID LEFT JOIN PM_PROMO_TYPE_LOOKUP D "
			+ "ON A.PROMO_TYPE_ID = D.PROMO_TYPE_ID "
			+ "LEFT JOIN PM_PROMO_OFFER_DETAIL E LEFT JOIN PM_PROMO_OFFER_ITEM IT "
			+ "ON IT.PROMO_OFFER_DETAIL_ID=E.PROMO_OFFER_DETAIL_ID  "
			+ "ON C.PROMO_BUY_REQUIREMENT_ID = E.PROMO_BUY_REQUIREMENT_ID " + "LEFT JOIN PM_PROMO_OFFER_TYPE_LOOKUP F "
			+ "ON E.PROMO_OFFER_TYPE_ID = F.PROMO_OFFER_TYPE_ID " + "LEFT JOIN ITEM_LOOKUP G "
			+ "ON G.ITEM_CODE = B.ITEM_CODE " + "LEFT JOIN PM_WEEKLY_AD_PROMO H "
			+ "on H.PROMO_DEFINITION_ID = A.PROMO_DEFINITION_ID " + "LEFT JOIN PM_WEEKLY_AD_BLOCK I "
			+ "ON H.BLOCK_ID = I.BLOCK_ID " + "LEFT JOIN PM_WEEKLY_AD_PAGE J " + "on I.PAGE_ID = J.PAGE_ID "
			+ "LEFT JOIN PM_WEEKLY_AD_DEFINITION K " + "on J.WEEKLY_AD_ID = K.WEEKLY_AD_ID "
			+ "LEFT JOIN RETAIL_CALENDAR RC " + "on RC.CALENDAR_ID = a.START_CALENDAR_ID "
			+ "LEFT JOIN RETAIL_CALENDAR RC2 " + "on RC2.CALENDAR_ID = b.START_CALENDAR_ID "
			+ "LEFT JOIN RETAIL_CALENDAR RC3 " + "on RC3.CALENDAR_ID = b.END_CALENDAR_ID "
			+ "LEFT JOIN PM_PROMO_DISPLAY L " + " on L.PROMO_DEFINITION_ID = a.PROMO_DEFINITION_ID "
			+ "LEFT JOIN pm_display_type_lookup m " + " on M.DISPLAY_TYPE_ID = L.DISPLAY_TYPE_ID "
			+ "LEFT JOIN PM_PROMO_LOCATION PL on PL.PROMO_DEFINITION_ID = a.PROMO_DEFINITION_ID "
			+ " WHERE RC.START_DATE=to_date(?, 'MM/DD/YYYY') AND PL.LOCATION_LEVEL_ID <>1 ";
	//+ "WHERE \"NUMBER\"='GNSL20200712021' AND RC.START_DATE=to_date(?,'MM/DD/YYYY')";

	private static final String DELETE_WEEKLY_AD_DEF = "DELETE FROM PM_WEEKLY_AD_DEFINITION WHERE WEEKLY_AD_ID IN "
			+ " (SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID IN "
			+ "		(select PAGE_ID from PM_WEEKLY_AD_BLOCK WHERE BLOCK_ID IN "
			+ "		( SELECT BLOCK_ID FROM PM_WEEKLY_AD_PROMO WHERE PROMO_DEFINITION_ID =?) ))";

	private static final String DELETE_WEEKLY_AD_PAGE = "DELETE FROM PM_WEEKLY_AD_PAGE WHERE PAGE_ID IN "
			+ "(select PAGE_ID from PM_WEEKLY_AD_BLOCK WHERE BLOCK_ID IN "
			+ "( SELECT BLOCK_ID FROM PM_WEEKLY_AD_PROMO WHERE PROMO_DEFINITION_ID =?)) ";

	private static final String DELETE_WEEKLY_AD_BLOCK = " DELETE FROM PM_WEEKLY_AD_BLOCK WHERE BLOCK_ID IN "
			+ "	(SELECT BLOCK_ID FROM PM_WEEKLY_AD_PROMO WHERE PROMO_DEFINITION_ID  =? )";

	private static final String DELETE_WEEKLY_AD_PROMO = " DELETE FROM PM_WEEKLY_AD_PROMO WHERE PROMO_DEFINITION_ID  = ? ";

	private static final String DELETE_OFFER_ITEM = "DELETE FROM PRESTO_OM.PM_PROMO_OFFER_ITEM WHERE PROMO_OFFER_DETAIL_ID "
			+ "IN (select PROMO_OFFER_DETAIL_ID from PM_PROMO_OFFER_DETAIL where PROMO_BUY_REQUIREMENT_ID "
			+ "IN (SELECT PROMO_BUY_REQUIREMENT_ID FROM PM_PROMO_BUY_REQUIREMENT WHERE PROMO_DEFINITION_ID "
			+ " = ? )) ";
	private static final String DELETE_OFFER_DETAIL = " DELETE FROM PRESTO_OM.PM_PROMO_OFFER_DETAIL WHERE PROMO_BUY_REQUIREMENT_ID "
			+ " IN (SELECT PROMO_BUY_REQUIREMENT_ID FROM PM_PROMO_BUY_REQUIREMENT WHERE PROMO_DEFINITION_ID  =? )";

	private static final String DELETE_BUY_REQUIREMENT = " DELETE FROM PM_PROMO_BUY_REQUIREMENT WHERE PROMO_DEFINITION_ID=?";

	private static final String DELETE_BUY_ITEM = "DELETE FROM Pm_Promo_Buy_Item WHERE PROMO_DEFINITION_ID =? ";
	private static final String DELETE_PROMO_DEFINITION = "DELETE FROM PM_PROMO_DEFINITION WHERE PROMO_DEFINITION_ID  = ?";

	private static final String DELETE_LOCATIONS = "DELETE FROM PM_PROMO_LOCATION WHERE PROMO_DEFINITION_ID = ?";

	private static final String UPDATE_LOCATIONS = "UPDATE PM_PROMO_LOCATION SET LOCATION_LEVEL_ID = 1, LOCATION_ID = 52 WHERE PROMO_DEFINITION_ID = ?";

	private static final String UDATE_PROMO_BUY_ITEMS = "UPDATE PM_PROMO_BUY_ITEM SET REG_PRICE= ? ,REG_QTY= ? ,SALE_PRICE= ? , "
			+ "SALE_QTY= ?  WHERE PROMO_DEFINITION_ID =? AND ITEM_CODE=?";

	private static final String UPDATE_WEEKLY_AD = " UPDATE PM_WEEKLY_AD_DEFINITION SET LOCATION_LEVEL_ID = 1, LOCATION_ID = 52  WHERE WEEKLY_AD_ID IN "
			+ "(SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_PAGE WHERE PAGE_ID IN  "
			+ "	(select PAGE_ID from PM_WEEKLY_AD_BLOCK WHERE BLOCK_ID IN "
			+ "	( SELECT BLOCK_ID FROM PM_WEEKLY_AD_PROMO WHERE PROMO_DEFINITION_ID "
			+ "	IN (SELECT PROMO_DEFINITION_ID FROM PM_PROMO_LOCATION WHERE PROMO_DEFINITION_ID = ?))))";

	public HashMap<String, HashMap<Integer, List<PromoDataStandardDTO>>> getWeeklyPromotions(Connection conn,
			String startDate) throws GeneralException {
		HashMap<String, HashMap<Integer, List<PromoDataStandardDTO>>> promoMap = new HashMap<String, HashMap<Integer, List<PromoDataStandardDTO>>>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {

			String query = new String(GET_PROMO_FOR_WEEK);
			logger.debug("query:-" + query);
			statement = conn.prepareStatement(query);
			int counter = 0;
			statement.setString(++counter, startDate);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {

				PromoDataStandardDTO promoDataStandardDTO = new PromoDataStandardDTO();
				promoDataStandardDTO.setPromoDefId(resultSet.getInt("PROMO_DEFINITION_ID"));
				int locationId = resultSet.getInt("location_id");
				promoDataStandardDTO.setEventID(resultSet.getString("PROMO_NUMBER"));
				promoDataStandardDTO.setPromoTypeId(resultSet.getInt("PROMO_TYPE_ID"));
				promoDataStandardDTO.setPromoSubtypeId(resultSet.getInt("PROMO_SUB_TYPE_ID"));
				promoDataStandardDTO.setStartcalendarID(resultSet.getInt("START_CALENDAR_ID"));
				promoDataStandardDTO.setEndcalendarID(resultSet.getInt("END_CALENDAR_ID"));
				promoDataStandardDTO.setPromoStartDate(resultSet.getString("START_DATE"));
				promoDataStandardDTO.setPromoName(resultSet.getString("PROMO_NAME"));
				promoDataStandardDTO.setPromoGroup(resultSet.getString("PROMO_GROUP_ID"));
				promoDataStandardDTO.setItemCode(resultSet.getString("ITEM_CODE"));
				promoDataStandardDTO.setUpc(resultSet.getString("UPC"));
				promoDataStandardDTO.setPromoTypeName(resultSet.getString("PROMO_TYPE_NAME"));
				promoDataStandardDTO.setEverydayPrice(resultSet.getString("REG_PRICE"));
				promoDataStandardDTO.setEverdayQty(resultSet.getString("REG_QTY"));
				promoDataStandardDTO.setSalePrice(resultSet.getDouble("SALE_PRICE"));
				promoDataStandardDTO.setSaleQty(resultSet.getInt("SALE_QTY"));
				promoDataStandardDTO.setBuyQty(resultSet.getInt("BUY_X"));
				promoDataStandardDTO.setOfferCount(resultSet.getInt("OFFER_COUNT"));
				promoDataStandardDTO.setMustbuyPrice(resultSet.getDouble("MUSTBUY_AMT"));
				promoDataStandardDTO.setMustBuyQty(resultSet.getInt("MUSTBUY_QTY"));
				promoDataStandardDTO.setOfferValue(resultSet.getDouble("OFFER_VALUE"));
				promoDataStandardDTO.setOffer_unit_type(resultSet.getString("OFFER_UNIT_TYPE"));
				promoDataStandardDTO.setLocationId(resultSet.getInt("location_id"));
				promoDataStandardDTO.setLocationLevelId(resultSet.getInt("location_level_id"));
				promoDataStandardDTO.setOfferItem(resultSet.getInt("OFFER_ITEM"));
				promoDataStandardDTO.setBlockNumber(resultSet.getString("BLOCK_NUMBER"));
				promoDataStandardDTO.setPageNumber(resultSet.getString("PAGE_NUMBER"));
				
				String promoNum = resultSet.getString("PROMO_NUMBER") + "-" + resultSet.getString("PROMO_GROUP_ID");

				HashMap<Integer, List<PromoDataStandardDTO>> temp = new HashMap<Integer, List<PromoDataStandardDTO>>();
				List<PromoDataStandardDTO> tempList = new ArrayList<PromoDataStandardDTO>();
				if (promoMap.containsKey(promoNum)) {
					temp = promoMap.get(promoNum);

					if (temp.containsKey(locationId)) {
						tempList = temp.get(locationId);
						tempList.add(promoDataStandardDTO);
					} else {
						tempList.add(promoDataStandardDTO);
					}

					temp.put(locationId, tempList);
				} else {
					tempList.add(promoDataStandardDTO);
					temp.put(locationId, tempList);
				}
				promoMap.put(promoNum, temp);

			}
		} catch (SQLException sqlE) {
			logger.error("Error while getting promotions by date range - " + sqlE);
			throw new GeneralException("Error while getting promotions by date range", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return promoMap;
	}

	public void deletePromotions(Set<Long> promodefIdTodelete, Connection conn) throws SQLException, GeneralException {

		logger.debug("Delete started for #promodef:"+ promodefIdTodelete.size());
		/*deleteWeeklyAd(promodefIdTodelete, conn);
		deleteWeeklyAdPage(promodefIdTodelete, conn);
		deleteWeeklyAdBlock(promodefIdTodelete, conn);*/
		deleteWeeklyAdPromo(promodefIdTodelete, conn);
		deletePromoOfferItem(promodefIdTodelete, conn);
		deletePromoOfferDetail(promodefIdTodelete, conn);
		deletePromoBuyRequirement(promodefIdTodelete, conn);
		deleteBuyitems(promodefIdTodelete, conn);
		deletepromoDef(promodefIdTodelete, conn);
		deletePromoLocation(promodefIdTodelete, conn);
		

	}

	private void deletePromoLocation(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_LOCATIONS);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when deleting promotion location - " + exception);
			throw new GeneralException("Error while DELETE_LOCATIONS", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	private void deletePromoOfferItem(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_OFFER_ITEM);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when deleting DELETE_OFFER_ITEM- " + exception);
			throw new GeneralException("Error while DELETE_OFFER_ITEM", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	/**
	 * Deletes from PM_PROMO_OFFER_DETAIL table
	 * 
	 * @param promoDefnId
	 * @return
	 * @throws GeneralException
	 */
	private void deletePromoOfferDetail(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {

		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_OFFER_DETAIL);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when deleting DELETE_OFFER_DETAIL - " + exception);
			throw new GeneralException("Error while DELETE_OFFER_DETAIL", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	/**
	 * Deletes from PM_PROMO_BUY_REQUIREMENT table
	 * 
	 * @param promoDefnId
	 * @return
	 * @throws GeneralException
	 */
	private void deletePromoBuyRequirement(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_BUY_REQUIREMENT);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when DELETE_BUY_REQUIREMENT - " + exception);
			throw new GeneralException("Error while DELETE_BUY_REQUIREMENT", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	private void deleteWeeklyAd(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_DEF);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when deleting DELETE_WEEKLY_AD_DEF - " + exception);
			throw new GeneralException("Error while DELETE_WEEKLY_AD_DEF", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	private void deleteWeeklyAdPage(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_PAGE);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when DELETE_WEEKLY_AD_PAGE- " + exception);
			throw new GeneralException("Error while DELETE_WEEKLY_AD_PAGE", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	private void deleteWeeklyAdBlock(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_BLOCK);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when deleting DELETE_WEEKLY_AD_BLOCK - " + exception);
			throw new GeneralException("Error while DELETE_WEEKLY_AD_BLOCK", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}

	}

	private void deleteWeeklyAdPromo(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_PROMO);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when deleting DELETE_WEEKLY_AD_PROMO - " + exception);
			throw new GeneralException("Error while DELETE_WEEKLY_AD_PROMO", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	private void deleteBuyitems(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_BUY_ITEM);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when DELETE_BUY_ITEM - " + exception);
			throw new GeneralException("Error while DELETE_BUY_ITEM", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	private void deletepromoDef(Set<Long> promodefIdTodelete, Connection conn) throws GeneralException {
		PreparedStatement stmt = null;

		try {
			stmt = conn.prepareStatement(DELETE_PROMO_DEFINITION);
			int itemNoInBatch = 0;
			for (Long promoDefnId : promodefIdTodelete) {
				int counter = 0;
				stmt.setLong(++counter, promoDefnId);
				stmt.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = stmt.executeBatch();
					stmt.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = stmt.executeBatch();
				stmt.clearBatch();
			}

		} catch (SQLException exception) {
			logger.error("Error when DELETE_PROMO_DEFINITION- " + exception);
			throw new GeneralException("Error while DELETE_PROMO_DEFINITION", exception);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public void updatePromotions(List<PromoDataStandardDTO> updateList, Connection conn) throws GeneralException {

		Set<Long> promoDefIds = new HashSet<Long>();

		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UDATE_PROMO_BUY_ITEMS);

			int itemNoInBatch = 0;
			for (PromoDataStandardDTO promoDTO : updateList) {
				int counter = 0;
				statement.setString(++counter, promoDTO.getEverydayPrice());
				statement.setString(++counter, promoDTO.getEverdayQty());
				statement.setDouble(++counter, promoDTO.getSalePrice());
				statement.setInt(++counter, promoDTO.getSaleQty());
				statement.setLong(++counter, promoDTO.getPromoDefId());
				statement.setString(++counter, promoDTO.getItemCode());
				promoDefIds.add(promoDTO.getPromoDefId());
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing updatePromotions");
			throw new GeneralException("Error while executing updatePromotions", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		UpdateLocations(promoDefIds, conn);
		UpdateADLocations(promoDefIds, conn);

	}

	private void UpdateLocations(Set<Long> promoDefIds, Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_LOCATIONS);
			int itemNoInBatch = 0;
			for (Long promoIds : promoDefIds) {
				int counter = 0;
				statement.setLong(++counter, promoIds);
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing UpdateLocations");
			throw new GeneralException("Error while executing UpdateLocations", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	private void UpdateADLocations(Set<Long> promoDefIds, Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_WEEKLY_AD);
			int itemNoInBatch = 0;
			for (Long promoIds : promoDefIds) {
				int counter = 0;
				statement.setLong(++counter, promoIds);
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing UPDATE_WEEKLY_AD");
			throw new GeneralException("Error while executing UPDATE_WEEKLY_AD", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

}
