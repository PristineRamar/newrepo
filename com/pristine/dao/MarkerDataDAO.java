package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dto.MarketDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class MarkerDataDAO {
	static Logger logger = Logger.getLogger("MarkerDataDAO");
	private static final String DELETE_EXISITNG_MARKET_DATA = "DELETE FROM MARKET_DATA_V2 WHERE DATE_RECEIVED = TO_DATE(?,'MM/DD/YY')";
	private static final String INSERT_INTO_MARKET_DATA = "INSERT INTO MARKET_DATA_V2(DATE_RECEIVED,ITEM_CODE, UPC, LOB, GROUPS, SUB_GROUP, "
			+ "PRD_DESC,SHORT_PROD_DESC, ORG_SIZE, ITEM_SIZE, UOM_ID, BRAND_LOW, BRAND_HIGH,BRAND_ID, BRAND_OWNER_LOW, BRAND_OWNER_HIGH, PACKAGE_SHAPE, "
			+ "COMMON_NAME,COMMODITY_GROUP, PERIOD_END_DATE,LY_PERIOD_END_DATE, OUR_TA_SALES, OUR_TA_UNITS, OUR_TA_NON_PROMO_SALES,OUR_TA_NON_PROMO_UNITS, "
			+ "OUR_TA_PCT_ACV,OUR_TA_PER_MM_ACV, REM_TA_SALES,REM_TA_UNITS, REM_TA_NON_PROMO_SALES, REM_TA_NON_PROMO_UNITS, "
			+ "REM_TA_PCT_ACV, REM_TA_PER_MM_ACV, TOT_REM_TA_SALES, TOT_REM_TA_UNITS, TOT_REM_TA_NON_PROMO_SALES,TOT_REM_TA_NON_PROMO_UNITS, "
			+ "TOT_REM_TA_PCT_ACV,TOT_REM_TA_PER_MM_ACV, LY_OUR_TA_SALES, LY_OUR_TA_UNITS, LY_OUR_TA_NON_PROMO_SALES,LY_OUR_TA_NON_PROMO_UNITS, "
			+ "LY_OUR_TA_PCT_ACV,LY_OUR_TA_PER_MM_ACV, LY_REM_TA_SALES,LY_REM_TA_UNITS, LY_REM_TA_NON_PROMO_SALES, LY_REM_TA_NON_PROMO_UNITS, "
			+ "LY_REM_TA_PCT_ACV, LY_REM_TA_PER_MM_ACV, LY_TOT_REM_TA_SALES, LY_TOT_REM_TA_UNITS, LY_TOT_REM_TA_NON_PROMO_SALES, "
			+ "LY_TOT_REM_TA_NON_PROMO_UNITS, LY_TOT_REM_TA_PCT_ACV,LY_TOT_REM_TA_PER_MM_ACV ) "
			+ "VALUES(TO_DATE(?, 'MM/dd/yy'),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, TO_DATE(?, 'MM/dd/yy'), "
			+ "TO_DATE(?, 'MM/dd/yy'),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	public void deleteExistingMarketData(Connection conn, String processingDate) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(DELETE_EXISITNG_MARKET_DATA);
			statement.setString(1, processingDate);
			statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new GeneralException("Error while executing deleteExistingMarketData", e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	public void insertMarketData(Connection conn, String processDate, HashMap<String, MarketDataDTO> mdBasedOnUpc)
			throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_INTO_MARKET_DATA);
			int itemNoInBatch = 0;
			for (Map.Entry<String, MarketDataDTO> finalEntry : mdBasedOnUpc.entrySet()) {
				MarketDataDTO value = finalEntry.getValue();
				int counter = 0;
				try {
					statement.setString(++counter, processDate);
					if (value.getItemCode() > 0) {
						statement.setInt(++counter, value.getItemCode());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					statement.setString(++counter, value.getUpc());
					statement.setString(++counter, value.getLOB());
					statement.setString(++counter, value.getGroup());
					statement.setString(++counter, value.getSubGroup());
					statement.setString(++counter, value.getProductDescription());
					statement.setString(++counter, value.getShortProductDescription());
					statement.setString(++counter, value.getOrgSize());
					statement.setString(++counter, value.getSize());
					if (value.getUOMId() != null) {
						statement.setString(++counter, value.getUOMId());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					statement.setString(++counter, value.getBrandLow());
					statement.setString(++counter, value.getBrandHigh());
					if (value.getBrandId() > 0) {
						statement.setLong(++counter, value.getBrandId());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					statement.setString(++counter, value.getBrandOwnLow());
					statement.setString(++counter, value.getBrandOwnHigh());
					statement.setString(++counter, value.getPackageShape());
					statement.setString(++counter, value.getCommonName());
					statement.setString(++counter, value.getCommodityGroup());
					if (value.getPeriodEndDate() != null) {
						statement.setString(++counter, value.getPeriodEndDate());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyPeriodEndDate() != null) {
						statement.setString(++counter, value.getLyPeriodEndDate());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getOurTASales() > 0) {
						statement.setDouble(++counter, value.getOurTASales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getOurTAUnits() > 0) {
						statement.setDouble(++counter, value.getOurTAUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getOurTANonPromoSales() > 0) {
						statement.setDouble(++counter, value.getOurTANonPromoSales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getOurTANonPromoUnits() > 0) {
						statement.setDouble(++counter, value.getOurTANonPromoUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getOurTAPctACV() > 0) {
						statement.setDouble(++counter, value.getOurTAPctACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getOurTAPerMMACV() > 0) {
						statement.setDouble(++counter, value.getOurTAPerMMACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getRemTASales() > 0) {
						statement.setDouble(++counter, value.getRemTASales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getRemTAUnits() > 0) {
						statement.setDouble(++counter, value.getRemTAUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getRemTANonPromoSales() > 0) {
						statement.setDouble(++counter, value.getRemTANonPromoSales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getRemTANonPromoUnits() > 0) {
						statement.setDouble(++counter, value.getRemTANonPromoUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getRemTAPctACV() > 0) {
						statement.setDouble(++counter, value.getRemTAPctACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getRemTAPerMMACV() > 0) {
						statement.setDouble(++counter, value.getRemTAPerMMACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getTotRemTASales() > 0) {
						statement.setDouble(++counter, value.getTotRemTASales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getTotRemTAUnits() > 0) {
						statement.setDouble(++counter, value.getTotRemTAUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getTotRemTANonPromoSales() > 0) {
						statement.setDouble(++counter, value.getTotRemTANonPromoSales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getTotRemTANonPromoUnits() > 0) {
						statement.setDouble(++counter, value.getTotRemTANonPromoUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getTotRemTAPctACV() > 0) {
						statement.setDouble(++counter, value.getTotRemTAPctACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getTotRemTAPerMMACV() > 0) {
						statement.setDouble(++counter, value.getTotRemTAPerMMACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyOurTASales() > 0) {
						statement.setDouble(++counter, value.getLyOurTASales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyOurTAUnits() > 0) {
						statement.setDouble(++counter, value.getLyOurTAUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyOurTANonPromoSales() > 0) {
						statement.setDouble(++counter, value.getLyOurTANonPromoSales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyOurTANonPromoUnits() > 0) {
						statement.setDouble(++counter, value.getLyOurTANonPromoUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyOurTAPctACV() > 0) {
						statement.setDouble(++counter, value.getLyOurTAPctACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyOurTAPerMMACV() > 0) {
						statement.setDouble(++counter, value.getLyOurTAPerMMACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyRemTASales() > 0) {
						statement.setDouble(++counter, value.getLyRemTASales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyRemTAUnits() > 0) {
						statement.setDouble(++counter, value.getLyRemTAUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyRemTANonPromoSales() > 0) {
						statement.setDouble(++counter, value.getLyRemTANonPromoSales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyRemTANonPromoUnits() > 0) {
						statement.setDouble(++counter, value.getLyRemTANonPromoUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyRemTAPctACV() > 0) {
						statement.setDouble(++counter, value.getLyRemTAPctACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyRemTAPerMMACV() > 0) {
						statement.setDouble(++counter, value.getLyRemTAPerMMACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyTotRemTASales() > 0) {
						statement.setDouble(++counter, value.getLyTotRemTASales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyTotRemTAUnits() > 0) {
						statement.setDouble(++counter, value.getLyTotRemTAUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyTotRemTANonPromoSales() > 0) {
						statement.setDouble(++counter, value.getLyTotRemTANonPromoSales());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyTotRemTANonPromoUnits() > 0) {
						statement.setDouble(++counter, value.getLyTotRemTANonPromoUnits());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyTotRemTAPctACV() > 0) {
						statement.setDouble(++counter, value.getLyTotRemTAPctACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					if (value.getLyTotRemTAPerMMACV() > 0) {
						statement.setDouble(++counter, value.getLyTotRemTAPerMMACV());
					} else {
						statement.setNull(++counter, Types.NULL);
					}
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						int[] count = statement.executeBatch();
						statement.clearBatch();
						itemNoInBatch = 0;
					}

				} catch (Exception e) {
					e.printStackTrace();
					logger.error("Error while executing INSERT_RETAIL_SALE_PRICE_INFO");
					throw new GeneralException("Error while executing INSERT_RETAIL_SALE_PRICE_INFO" + e);
				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing INSERT_RETAIL_SALE_PRICE_INFO");
			throw new GeneralException("Error while executing INSERT_RETAIL_SALE_PRICE_INFO" + e);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("Error while executing INSERT_RETAIL_SALE_PRICE_INFO");
			throw new GeneralException("Error while executing INSERT_RETAIL_SALE_PRICE_INFO" + e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}
}
