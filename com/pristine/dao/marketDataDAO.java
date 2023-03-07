package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.NelsonMarketDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class marketDataDAO {

	private static Logger logger = Logger.getLogger("marketDataDAO");

	private static String INSERT_MARKETDATA = "INSERT INTO MARKET_DATA"
			+ "(MARKET_DATA_ID,MARKET_NAME,LOCATION_LEVEL_ID,LOCATION_NUMBER,LOCATION_ID,LOCATION_LEVEL,ITEM_CODE,"
			+ "UPC,UPC_DESCRIPTION,TYPE_OF_ITEM,TIME_PERIOD,ANALYSIS_START_DATE,ANALYSIS_END_DATE,"
			+ "CALENDAR_ID,CALENDAR_TYPE,RETAIL_CONDITION,ACTUAL_UNITS_PROJ,"
			+ "UNITS_PROJ_COMP_MKT,ACTUAL_SALES_PROJ,SALES_PROJ_COMP_MKT,AVERAGE_PRICE,"
			+ "AVERAGE_PRICE_COMP_MKT,ELASTICITY_EST_FCS_MKT,ELASTICITY_EST_TOTAL_MKT,"
			+ "NF2_FOCUS_MKT,NF3_FOCUS_MKT,NF4PLUS_FOCUS_MKT,EST_DISPLAY_FOCUS_MRKT,"
			+ "EST_AD_FOCUS_MKT,EST_DISAD_FOCUS_MRKT,MCP_COMP_MRKT,UNIT_PERCENT_MCP_COMP_MKT,"
			+ "FIVE_PERCENTILE_PRC,TEN_PERCENTILE_PRC,FIFTEEN_PERCENTILE_PRC,TWENTY_PERCENTILE_PRC,"
			+ "TWENTYFIVE_PERCENTILE_PRC,THIRTY_PERCENTILE_PRC,THIRTYFIVE_PERCENTILE_PRC,FOURTY_PERCENTILE_PRC,"
			+ "FOURTYFIVE_PERCENTILE_PRC,FIFTY_PERCENTILE_PRC,FIFTYFIVE_PERCENTILE_PRC,SIXTY_PERCENTILE_PRC,"
			+ "SIXTYFIVE_PERCENTILE_PRC,SEVENTY_PERCENTILE_PRC,SEVENTYFIVE_PERCENTILE_PRC,EIGHTY_PERCENTILE_PRC,"
			+ "EIGHTYFIVE_PERCENTILE_PRC,NINETY_PERCENTILE_PRC,NINETYFIVE_PERCENTILE_PRC )"
			+ " VALUES(MARKETID_SEQ.NEXTVAL ,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

	public int insertMarketDataBatch(List<NelsonMarketDataDTO> insertList, Connection conn) throws GeneralException {

		PreparedStatement stmt = null;
		int[] count = null;
		int insertCount = 0;
		try {
			if (insertList.size() > 0) {
				stmt = conn.prepareStatement(INSERT_MARKETDATA);
				for (NelsonMarketDataDTO mktData : insertList) {
					int counter = 0;

					stmt.setString(++counter, mktData.getMarketName());
					stmt.setInt(++counter, mktData.getLocationlevelID());
					stmt.setString(++counter, mktData.getLocationNo());
					stmt.setInt(++counter, mktData.getZoneId());
					stmt.setString(++counter, mktData.getLocationLevel());
					stmt.setInt(++counter, mktData.getItemCode());
					stmt.setString(++counter, mktData.getUPC());
					stmt.setString(++counter, mktData.getUPCDesc());
					stmt.setString(++counter, mktData.getIscarriedItem());
					stmt.setString(++counter, mktData.getTimePeriod());
					stmt.setString(++counter, mktData.getStartDate());
					stmt.setString(++counter, mktData.getEndDate());
					stmt.setInt(++counter, mktData.getCalendarId());
					stmt.setString(++counter, mktData.getCalendarType());
					stmt.setString(++counter, mktData.getRetailCondn());
					stmt.setInt(++counter, mktData.getActUnitsProj());
					stmt.setInt(++counter, mktData.getActUnitsProjCompMkt());
					stmt.setInt(++counter, mktData.getActSalesProj());
					stmt.setInt(++counter, mktData.getActsalesProjCompMkt());
					stmt.setDouble(++counter, mktData.getAvgPrc());
					stmt.setDouble(++counter, mktData.getAvglPrcCompMkt());
					stmt.setDouble(++counter, mktData.getElasticityEstFcsMKt());
					stmt.setDouble(++counter, mktData.getElasticityEstTotMkt());
					stmt.setDouble(++counter, mktData.getNf2FcsMkt());
					stmt.setDouble(++counter, mktData.getNf3FcsMkt());
					stmt.setDouble(++counter, mktData.getNf4FcsMkt());
					stmt.setDouble(++counter, mktData.getEstFcsDisMkt());
					stmt.setDouble(++counter, mktData.getEstAdFcsMkt());
					stmt.setDouble(++counter, mktData.getEstDisAdFcsMkt());
					stmt.setDouble(++counter, mktData.getMcpCompMkt());
					stmt.setDouble(++counter, mktData.getUnitPerMcpCompMkt());
					stmt.setDouble(++counter, mktData.getFivePercentilePrc());
					stmt.setDouble(++counter, mktData.getTenPercentilePrc());
					stmt.setDouble(++counter, mktData.getFifteenPercentilePrc());
					stmt.setDouble(++counter, mktData.getTwentyPercentilePrc());
					stmt.setDouble(++counter, mktData.getTwentyFivePercentilePrc());
					stmt.setDouble(++counter, mktData.getThirtyPercentilePrc());
					stmt.setDouble(++counter, mktData.getThirtyFivePercentilePrc());
					stmt.setDouble(++counter, mktData.getFourtyPercentilePrc());
					stmt.setDouble(++counter, mktData.getFourtyFivePercentilePrc());
					stmt.setDouble(++counter, mktData.getFiftyPercentilePrc());
					stmt.setDouble(++counter, mktData.getFiftyfivePercentilePrc());
					stmt.setDouble(++counter, mktData.getSixtyPercentilePrc());
					stmt.setDouble(++counter, mktData.getSixtyFivePercentilePrc());
					stmt.setDouble(++counter, mktData.getSeventyPercentilePrc());
					stmt.setDouble(++counter, mktData.getSeventyFivePercentilePrc());
					stmt.setDouble(++counter, mktData.getEightyPercentilePrc());
					stmt.setDouble(++counter, mktData.getEightyFivePercentilePrc());
					stmt.setDouble(++counter, mktData.getNinetyPercentilePrc());
					stmt.setDouble(++counter, mktData.getNinetyFivePercentilePrc());

					stmt.addBatch();
				}
				count = stmt.executeBatch();
			}
		} catch (SQLException e) {
			logger.error("Error when inserting marketData - " + e);
			throw new GeneralException("Error in insertmarketData", e);
		} finally {
			PristineDBUtil.close(stmt);
		}
		if (count != null)
			insertCount = count.length;
		return insertCount;
	}

}
