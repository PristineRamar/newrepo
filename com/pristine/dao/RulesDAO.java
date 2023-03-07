/*
 * Author : Naimish Start Date : Jul 22, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import javax.sql.rowset.CachedRowSet; // import org.apache.log4j.Logger;

import org.apache.log4j.Logger;

import com.pristine.dto.*;
import com.pristine.exception.GeneralException;
import com.pristine.util.GenericUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.SIApplicationInterface;
import com.sun.rowset.CachedRowSetImpl;

public class RulesDAO
{
	private static Logger	logger	= Logger.getLogger("RulesDAO");
	
	private Connection			connection		= null;
	private ConfigSettingDTO	configSettings	= null;

	public RulesDAO (Connection conn, ConfigSettingDTO configSettings) throws GeneralException
	{
		this.connection		= conn;
		this.configSettings = configSettings;
	}

	public int getOtherStoreCount(int chainId, int excludeSchId, int itemCode, 
								  java.sql.Date date, int noOfDays, boolean isSale)
		throws GeneralException
	{
		int noOfStores = 0;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append(" SELECT Count(*) AS COUNT FROM competitive_data_view cd ");
			sqlBuffer.append(" WHERE cd.comp_chain_id = ? "); 
			sqlBuffer.append(" AND   (cd.status_chg_date>(to_date(?)-?) AND cd.status_chg_date<=?) ");
			sqlBuffer.append(" AND cd.schedule_id<>? ");
			sqlBuffer.append(" AND Item_Code=? "); 
			
			if ( isSale){
				sqlBuffer.append(" AND unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack)>0 ");
			}else {
				sqlBuffer.append(" AND unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack)>0 ");
			}
			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, chainId);
			preparedStatement.setDate(2, date);
			preparedStatement.setInt(3, noOfDays);
			preparedStatement.setDate(4, date);
			preparedStatement.setInt(5, excludeSchId);
			preparedStatement.setInt(6, itemCode);

			GenericUtil.logMessage("getOtherStoreCount query " + sqlBuffer);

			resultSet = preparedStatement.executeQuery();

			if (resultSet.next())
			{
				noOfStores = resultSet.getInt("COUNT");
				GenericUtil.logMessage("Number of other stores " + noOfStores + 
									  ((isSale) ? " - Sale": " - Reg"));
			}
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return noOfStores;
		
	}
	

	public HashMap<Integer, Integer> getRegularPriceEnding(int storeId, java.sql.Date date) throws GeneralException
	{
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		HashMap<Integer, Integer>	regularPricePointsMap	= new HashMap<Integer, Integer>();

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT  Mod(reg_price*100, 100) AS Ending_Digits, count(*) AS COUNT FROM ");
			sqlBuffer.append(" (SELECT cd.reg_price, row_number() OVER (ORDER BY check_data_id desc) nrows FROM competitive_data cd ");
			sqlBuffer.append(" INNER JOIN schedule sd ON (cd.schedule_id=sd.schedule_id)");
			sqlBuffer.append(" WHERE sd.status_chg_date<? AND cd.reg_price>0 AND cd.item_not_found_flg='N' and cd.price_not_found_flg='N' ");
			sqlBuffer.append(" AND sd.comp_str_id=?) WHERE nrows<? ");
			sqlBuffer.append(" GROUP BY Mod(reg_price*100, 100) ORDER BY COUNT DESC ");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setDate(1, date);
			preparedStatement.setInt(2, storeId);
			preparedStatement.setInt(3, (getConfigSettings().getPriceEndingObservations() * getConfigSettings().getMaxItemsPerPriceCheck()));


			resultSet = preparedStatement.executeQuery();
			
			while (resultSet.next())
			{
				regularPricePointsMap.put(resultSet.getInt("Ending_Digits"), resultSet.getInt("COUNT"));
			}
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return regularPricePointsMap;
	}

	public HashMap<Integer, Integer> getSalePriceEnding(int storeId, java.sql.Date date) throws GeneralException
	{
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		HashMap<Integer, Integer>	 salePricePointsMap	= new HashMap<Integer, Integer>();

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT  Mod(sale_price*100, 100) AS Ending_Digits, count(*) AS COUNT FROM ");
			sqlBuffer.append(" (SELECT cd.sale_price, row_number() OVER (ORDER BY check_data_id desc) nrows FROM competitive_data cd ");
			sqlBuffer.append(" INNER JOIN schedule sd ON (cd.schedule_id=sd.schedule_id)  ");
			sqlBuffer.append(" WHERE sd.status_chg_date<? AND cd.sale_price>0 AND cd.item_not_found_flg='N' ");
			sqlBuffer.append(" AND sd.comp_str_id=?) WHERE nrows<? ");
			sqlBuffer.append(" GROUP BY Mod(sale_price*100, 100) ORDER BY COUNT DESC ");


			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setDate(1, date);
			preparedStatement.setInt(2, storeId);
			preparedStatement.setInt(3, (getConfigSettings().getPriceEndingObservations() * getConfigSettings().getMaxItemsPerPriceCheck()));

	
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				salePricePointsMap.put(resultSet.getInt("Ending_Digits"), resultSet.getInt("COUNT"));
			}
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return salePricePointsMap;
	}

	public CachedRowSet getsaleToRegularTransition(CompetitiveDataDTO competitiveDataDTO, java.sql.Date date) throws GeneralException
	{
		CachedRowSet cachedRowSet = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			cachedRowSet = new CachedRowSetImpl();

			StringBuffer sqlBuffer = new StringBuffer();

			// As per Raj's mail (17 Aug 2009), added item not found flag and
			// price not found flag conditions - also added 3 weeks condition
			// according to discussion with Raj
			sqlBuffer.append("SELECT Unitprice(A.REG_PRICE, A.REG_M_PRICE, A.Reg_M_PACK ) AS reg_unit_price, ");
			sqlBuffer.append("Unitprice(a.sale_PRICE, a.sale_M_PRICE, a.sale_M_PACK) AS sale_unit_price, a.item_not_found_flg, a.price_not_found_flg ");
			sqlBuffer.append("FROM competitive_data_view_for_si a WHERE a.item_not_found_flg='N' and  a.price_not_found_flg='N' and a.check_data_id=(select max(check_data_id) from ");
			sqlBuffer.append("competitive_data_view_for_si C where (C.status_chg_date>(to_date(?) - ?) AND C.status_chg_date<=?) AND C.item_not_found_flg='N' and C.price_not_found_flg='N' and C.item_code = ? and C.Comp_Str_id = ? and c.check_data_id < ?) ");

	
			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setDate(1, date);
			// Apply this rule only if last observed prices are within 3 weeks
			preparedStatement.setInt(2, getConfigSettings().getLastObservedPriceWithinWeeks() * 7);
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, competitiveDataDTO.getItemCode());
			preparedStatement.setInt(5, competitiveDataDTO.getCompStrID());
			preparedStatement.setInt(6, competitiveDataDTO.getCheckDataID());

			resultSet = preparedStatement.executeQuery();

			if (resultSet.isBeforeFirst())
			{
				cachedRowSet.populate(resultSet);
			}
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return cachedRowSet;
	}

	public CachedRowSet getLastPriceChecks(int compStrId, int itemCode, int excludeSchId, java.sql.Date date, int noOfMonths, boolean showFlavors) throws GeneralException
	{
		CachedRowSet priceDateMap;

		// HashMap<Date, Float> priceDateMap = new HashMap<Date, Float>();
		// ArrayList<Float> arrayPriceList = new ArrayList<Float>();
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			priceDateMap = new CachedRowSetImpl();

			StringBuffer sqlBuffer = new StringBuffer();
			

			sqlBuffer.append("SELECT unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack) AS reg_price, ");
			sqlBuffer.append(" unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack) AS sale_price, "); 
			sqlBuffer.append(" sd.status_chg_date, sd.start_date ");
			if( showFlavors)
				sqlBuffer.append(" FROM competitive_data cd ");
			else
				sqlBuffer.append(" FROM comp_data_view_no_flavor  cd ");
			
			sqlBuffer.append("INNER JOIN schedule sd ON (cd.schedule_id=sd.schedule_id) AND sd.comp_str_id=? AND cd.schedule_id<>? ");
			sqlBuffer.append(" WHERE (item_code=? AND unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack)>0) and cd.item_not_found_flg='N' ");
			sqlBuffer.append("AND (sd.status_chg_date>(Add_Months(to_date(?), ?)) AND sd.status_chg_date<=?) ");
			sqlBuffer.append("ORDER BY cd.check_datetime DESC ");
			
			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, compStrId);
			preparedStatement.setInt(2, excludeSchId);
			preparedStatement.setInt(3, itemCode);
			preparedStatement.setDate(4, date);
			if ( getConfigSettings() != null)
				preparedStatement.setInt(5, getConfigSettings().getUniquePPMonths());
			else
				preparedStatement.setInt(5, noOfMonths);
			preparedStatement.setDate(6, date);
			
			resultSet = preparedStatement.executeQuery();

			if (resultSet.isBeforeFirst())
			{
				priceDateMap.populate(resultSet);
			}
		}
		catch (SQLException sqlex)
		{
			throw new GeneralException(sqlex.getMessage(), sqlex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}
		return priceDateMap;

	}


	public HashMap<Integer, Integer> getChangeDirectionOccurences(CompetitiveDataDTO competitiveDataDTO, java.sql.Date date, int chainId) 
			throws GeneralException
	{
		HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT cd1.change_direction, Count(cd1.change_direction) as count FROM competitive_data_view_for_si cd1 ");
			sqlBuffer.append("WHERE cd1.schedule_id <> ? AND cd1.check_data_id IN (SELECT Max(cd2.check_data_id) FROM competitive_data_view_for_si cd2 ");
			sqlBuffer.append("WHERE cd2.item_code=? AND unitprice(cd2.reg_price, cd2.reg_m_price, cd2.reg_m_pack) > 0 AND cd2.item_not_found_flg='N' AND cd2.price_not_found_flg='N' ");
			sqlBuffer.append("AND cd2.status_chg_date>(to_date(?) - ?) AND cd2.status_chg_date <= ?  ");
			sqlBuffer.append("AND cd2.current_status IN (2, 4) AND cd2.comp_str_id IN ");
			sqlBuffer.append("(SELECT comp_str_id FROM competitor_store cs WHERE cs.comp_chain_id = ? ))");
			sqlBuffer.append(" GROUP BY cd1.change_direction ");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, competitiveDataDTO.getScheduleID());
			preparedStatement.setInt(2, competitiveDataDTO.getItemCode());
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, getConfigSettings().getUniqueStoresDays());
			preparedStatement.setDate(5, date);
			preparedStatement.setInt(6, chainId);
			
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				hashMap.put(resultSet.getInt("change_direction"), resultSet.getInt("count"));
			}
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return hashMap;
	}


	public HashMap<Integer, Float> getWeekPrice(SIApplicationInterface appInterface, CompetitiveDataDTO competitiveDataDTO, Date date, int chainId) throws GeneralException
	//public HashMap<Integer, Float> getWeekPrice(CompetitiveDataDTO competitiveDataDTO, Date date, int chainId) throws GeneralException
	{
		HashMap<Integer, Float> hashMap = new HashMap<Integer, Float>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT sd.comp_str_id,unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack) AS reg_price FROM competitive_data cd ");
			sqlBuffer.append("INNER JOIN schedule sd ON cd.schedule_id=sd.schedule_id  ");
			sqlBuffer.append("WHERE item_code=? AND unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack)>0 and cd.item_not_found_flg='N' and cd.price_not_found_flg='N' ");
			sqlBuffer.append("AND sd.status_chg_date>(to_date(?) - ?) AND sd.status_chg_date<=to_date(?) ");
			sqlBuffer.append("AND sd.current_status IN (2, 4)AND sd.comp_str_id IN ");
			sqlBuffer.append("(SELECT comp_str_id FROM competitor_store cs ");
			sqlBuffer.append("WHERE cs.comp_chain_id = ?) ");
			//sqlBuffer.append("ORDER BY sd.status_chg_date DESC");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			//logger.info("ChainId:" + chainId + ", item code:" + competitiveDataDTO.getItemCode() + ", date:" + date.toString() + ", days:" + getConfigSettings().getUniqueStoresDays());
			
			// preparedStatement.setInt(1, competitiveDataDTO.getScheduleID());
			preparedStatement.setInt(1, competitiveDataDTO.getItemCode());
			preparedStatement.setDate(2, date);
			preparedStatement.setInt(3, appInterface.getNumberOfRegularDays());
			preparedStatement.setDate(4, date);
			preparedStatement.setInt(5, chainId);

			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				hashMap.put(resultSet.getInt("comp_str_id"), resultSet.getFloat("reg_price"));
			}
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return hashMap;
	}

	public HashMap<Integer, Float> getSalePrices(SIApplicationInterface appInterface, CompetitiveDataDTO competitiveDataDTO, Date date, int weekIndex, int chainId) throws GeneralException
	//public HashMap<Integer, Float> getSalePrices(CompetitiveDataDTO competitiveDataDTO, Date date, int weekIndex, int chainId) throws GeneralException
	{
		HashMap<Integer, Float> salePriceMap = new HashMap<Integer, Float>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT comp_str_id, unitprice(sale_price, sale_m_price, sale_m_pack) AS sale_price FROM competitive_data_view_for_si ");
			sqlBuffer.append("WHERE check_data_id IN (SELECT Max(check_data_id) FROM competitive_data_view_for_si ");
			sqlBuffer.append("WHERE item_code=? AND unitprice(reg_price, reg_m_price, reg_m_pack) > 0 AND item_not_found_flg='N'  ");
			sqlBuffer.append("AND status_chg_date <= (To_Date(?) - ?) AND status_chg_date >= (To_Date(?) - ?) AND current_status IN (2, 4) AND comp_str_id IN ");
			sqlBuffer.append("(SELECT comp_str_id FROM competitor_store cs WHERE cs.comp_chain_id = ?)) ");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, competitiveDataDTO.getItemCode());
			preparedStatement.setDate(2, date);
			preparedStatement.setInt(3, appInterface.getNumberOfSaleDays() * weekIndex);
			preparedStatement.setDate(4, date);
			preparedStatement.setInt(5, appInterface.getNumberOfSaleDays() * (1+ weekIndex));
			preparedStatement.setInt(6, chainId);


			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				salePriceMap.put(resultSet.getInt("comp_str_id"), resultSet.getFloat("sale_price"));
			}
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return salePriceMap;
	}

	public ArrayList<Float> getSaleRegularPriceDifference(CompetitiveDataDTO competitiveDataDTO, Date date, int chainId) throws GeneralException
	{
		ArrayList<Float> saleRegularPriceList = new ArrayList<Float>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT cd1.comp_str_id, round((((unitprice(cd1.reg_price, cd1.reg_m_price, cd1.reg_m_pack) - ");
			sqlBuffer.append("unitprice(cd1.sale_price, cd1.sale_m_price, cd1.sale_m_pack)) / unitprice(cd1.reg_price, cd1.reg_m_price, cd1.reg_m_pack) )* 100), 1) ");
			sqlBuffer.append("AS percentdifference FROM competitive_data_view_for_si cd1 ");
			sqlBuffer.append("WHERE cd1.schedule_id <> ? AND cd1.check_data_id IN (SELECT Max(cd2.check_data_id) FROM competitive_data_view_for_si cd2 ");
			sqlBuffer.append("WHERE cd2.item_code=? AND unitprice(cd2.reg_price, cd2.reg_m_price, cd2.reg_m_pack) > 0 AND unitprice(cd2.sale_price, cd2.sale_m_price, cd2.sale_m_pack)>0 AND cd2.item_not_found_flg='N' AND cd2.price_not_found_flg='N' ");
			sqlBuffer.append("AND (cd2.status_chg_date>(to_date(?) - ?) AND cd2.status_chg_date <= ? ) ");
			sqlBuffer.append("AND cd2.current_status IN (2, 4) AND cd2.comp_str_id IN ");
			sqlBuffer.append("(SELECT comp_str_id FROM competitor_store cs WHERE cs.comp_chain_id = ?))");
			//sqlBuffer.append("GROUP BY cd2.comp_str_id) ORDER BY comp_str_id ");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, competitiveDataDTO.getScheduleID());
			preparedStatement.setInt(2, competitiveDataDTO.getItemCode());
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, getConfigSettings().getUniqueStoresDaysSale());
			preparedStatement.setDate(5, date);
			preparedStatement.setInt(6, chainId);

			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				// saleRegularPriceMap.put(resultSet.getInt("comp_str_id"),
				// resultSet.getFloat("percentdifference"));
				saleRegularPriceList.add(resultSet.getFloat("percentdifference"));
			}
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return saleRegularPriceList;
	}

	public ArrayList<Float> getOnSalesElseWhere(CompetitiveDataDTO competitiveDataDTO, Date date, int chainId) throws GeneralException
	{
		ArrayList<Float> salePriceList = new ArrayList<Float>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT unitprice(cd1.sale_price, cd1.sale_m_price, cd1.sale_m_pack) AS sale_price FROM competitive_data_view_for_si cd1 ");
			sqlBuffer.append("WHERE cd1.schedule_id <> ? AND cd1.check_data_id IN (SELECT Max(cd2.check_data_id) FROM competitive_data_view_for_si cd2 ");
			sqlBuffer.append("WHERE cd2.item_code=? AND cd2.item_not_found_flg='N'  ");
			sqlBuffer.append("AND (cd2.status_chg_date>(to_date(?) - ?) AND cd2.status_chg_date <= ? ) ");
			sqlBuffer.append("AND cd2.current_status IN (2, 4) AND cd2.comp_str_id IN ");
			sqlBuffer.append("(SELECT comp_str_id FROM competitor_store cs WHERE cs.comp_chain_id = ?) ");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, competitiveDataDTO.getScheduleID());
			preparedStatement.setInt(2, competitiveDataDTO.getItemCode());
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, getConfigSettings().getUniqueStoresDaysSale());
			preparedStatement.setDate(5, date);
			preparedStatement.setInt(6, competitiveDataDTO.getCompStrID());
			preparedStatement.setInt(7, chainId);
			
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				salePriceList.add(resultSet.getFloat("sale_price"));
			}
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return salePriceList;
	}

	public ArrayList<Integer> groupItemPriceChangedOrNotChanged(CompetitiveDataDTO competitiveDataDTO) throws GeneralException
	{
		ArrayList<Integer> itemChangedDirectionList = new ArrayList<Integer>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT change_direction FROM competitive_data cd ");
			sqlBuffer.append("INNER JOIN schedule sd ON (cd.schedule_id=sd.schedule_id) ");
			sqlBuffer.append("WHERE cd.schedule_id = ? AND cd.item_code <> ? AND unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack)>0 and cd.item_not_found_flg='N' and cd.price_not_found_flg='N' ");
			sqlBuffer.append("AND sd.current_status IN (2, 4) AND cd.item_code IN ");
			sqlBuffer.append("(SELECT a.item_code FROM item_lookup a INNER JOIN item_lookup b ON (b.item_code = ?) ");
			sqlBuffer.append("WHERE a.dept_id = b.dept_id AND a.category_id = b.category_id AND SubStr(a.upc, 1, 5)=SubStr(b.upc, 1, 5)) ");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, competitiveDataDTO.getScheduleID());
			preparedStatement.setInt(2, competitiveDataDTO.getItemCode());
			preparedStatement.setInt(3, competitiveDataDTO.getItemCode());

			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				itemChangedDirectionList.add(resultSet.getInt("change_direction"));
			}
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return itemChangedDirectionList;
	}

	public HashMap<String, Integer> itemNotFoundSummaryInOtherStores(CompetitiveDataDTO competitiveDataDTO, Date date, int chainId) throws GeneralException
	{
		HashMap<String, Integer> itemFoundMap = new HashMap<String, Integer>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT item_not_found_flg, Count(item_not_found_flg) as count_flag FROM competitive_data cd ");
			sqlBuffer.append("INNER JOIN schedule sd ON (cd.schedule_id = sd.schedule_id) ");
			sqlBuffer.append("WHERE cd.item_code = ?  AND sd.comp_str_id <> ? AND (sd.status_chg_date > (? - ?) ");
			sqlBuffer.append("AND sd.status_chg_date <= ?) AND sd.current_status IN (2, 4) AND sd.comp_str_id IN ");
			sqlBuffer.append("(SELECT comp_str_id FROM competitor_store cs ");
			sqlBuffer.append("WHERE cs.comp_chain_id = ?) ");
			sqlBuffer.append("GROUP BY item_not_found_flg");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, competitiveDataDTO.getItemCode());
			preparedStatement.setInt(2, competitiveDataDTO.getCompStrID());
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, getConfigSettings().getUniqueStoresDays());
			preparedStatement.setDate(5, date);
			preparedStatement.setInt(6, chainId);

			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				itemFoundMap.put(resultSet.getString("item_not_found_flg").toUpperCase(), resultSet.getInt("count_flag"));
			}
		}
		catch (Exception ex)
		{
			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return itemFoundMap;
	}

	public CachedRowSet getCurrAndPrevPricesFromAllStores(int chainId, SuspectItemDTO suspectItemDTO, Date date)
	{
		CachedRowSet cachedRowSet = null;

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			cachedRowSet = new CachedRowSetImpl();

			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT cd.check_data_id, cd.comp_str_id, cd.status_chg_date, Unitprice(cd.REG_PRICE, cd.REG_M_PRICE, cd.Reg_M_PACK) AS reg_unit_price, ");
			sqlBuffer.append("Unitprice(cd.SALE_PRICE, cd.SALE_M_PRICE, cd.SALE_M_PACK) AS sale_unit_price, cd.item_not_found_flg, cd.price_not_found_flg, ");
			sqlBuffer.append("cdl.check_data_id, cdl.comp_str_id AS comp_str_id2, cdl.status_chg_date AS status_chg_date2, Unitprice(cdl.REG_PRICE, cdl.REG_M_PRICE, cdl.Reg_M_PACK) AS reg_unit_price2, ");
			sqlBuffer.append("Unitprice(cdl.SALE_PRICE, cdl.SALE_M_PRICE, cdl.SALE_M_PACK) AS sale_unit_price2, cdl.item_not_found_flg, cdl.price_not_found_flg, cd.change_direction, cdl.change_direction as change_direction2 ");
			sqlBuffer.append("FROM competitive_data_view_for_si cd, competitive_data_view_for_si cdl WHERE cd.item_code=cdl.item_code ");
			sqlBuffer.append("AND cd.item_code=? AND cd.status_chg_date>(To_Date(?) - ?) AND cd.status_chg_date <= ? ");
			sqlBuffer.append("AND cd.comp_str_id IN ( SELECT comp_str_id FROM competitor_store cs WHERE cs.comp_chain_id = ?)");
			sqlBuffer.append("AND (cdl.status_chg_date <= (To_Date(?) - ?)) ");
			sqlBuffer.append("AND cdl.check_data_id IN (SELECT Max(check_data_id) FROM competitive_data_view_for_si C WHERE C.item_code = cd.item_code ");
			sqlBuffer.append("AND C.Comp_Str_id = cd.comp_str_id AND c.check_data_id < cd.check_data_id) ");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, suspectItemDTO.getItem_ID());
			preparedStatement.setDate(2, date);
			preparedStatement.setInt(3, getConfigSettings().getRemovalUniqueStoreDays());
			preparedStatement.setDate(4, date);
			preparedStatement.setInt(5, chainId);
			preparedStatement.setDate(6, date);
			preparedStatement.setInt(7, getConfigSettings().getRemovalUniqueStoreDays());

			resultSet = preparedStatement.executeQuery();

			if (resultSet.isBeforeFirst())
			{
				cachedRowSet.populate(resultSet);
			}
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			GenericUtil.logError(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return cachedRowSet;
	}

	public HashMap<Float, Integer> getRegularPriceOccurences(int chainId, SuspectItemDTO suspectItemDTO, java.sql.Date date) throws GeneralException
	{
		HashMap<Float, Integer> hashMap = new HashMap<Float, Integer>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack) as reg_unit_price, Count(unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack)) as count FROM competitive_data cd ");
			sqlBuffer.append("INNER JOIN schedule sd ON cd.schedule_id=sd.schedule_id AND sd.schedule_id <> ? ");
			sqlBuffer.append("WHERE item_code = ? AND unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack)>0 ");
			sqlBuffer.append("AND sd.status_chg_date>(to_date(?) - ?) AND sd.status_chg_date <= ?  ");
			sqlBuffer.append(" AND sd.comp_str_id IN ");
			sqlBuffer.append("(SELECT comp_str_id FROM competitor_store cs WHERE cs.comp_chain_id = ?)");
			sqlBuffer.append("GROUP BY unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack)");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, suspectItemDTO.getSchedule_ID());
			preparedStatement.setInt(2, suspectItemDTO.getItem_ID());
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, getConfigSettings().getRemovalUniqueStoreDays());
			preparedStatement.setDate(5, date);
			preparedStatement.setInt(6, chainId);

			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				hashMap.put(resultSet.getFloat("reg_unit_price"), resultSet.getInt("count"));
			}
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return hashMap;
	}

	public HashMap<Float, Integer> getSalePriceOccurences(int chainId, SuspectItemDTO suspectItemDTO, java.sql.Date date) throws GeneralException
	{
		HashMap<Float, Integer> hashMap = new HashMap<Float, Integer>();

		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append("SELECT unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack) as sale_unit_price, Count(unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack)) as count FROM competitive_data cd ");
			sqlBuffer.append("INNER JOIN schedule sd ON cd.schedule_id=sd.schedule_id AND sd.schedule_id <> ? ");
			sqlBuffer.append("WHERE item_code = ? AND unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack)>0 ");
			sqlBuffer.append("AND sd.status_chg_date>(to_date(?) - ?) AND sd.status_chg_date <= ? ");
			sqlBuffer.append(" AND sd.comp_str_id IN ");
			sqlBuffer.append("(SELECT comp_str_id FROM competitor_store cs WHERE cs.comp_chain_id = ?)");
			sqlBuffer.append("GROUP BY unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack)");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, suspectItemDTO.getSchedule_ID());
			preparedStatement.setInt(2, suspectItemDTO.getItem_ID());
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, getConfigSettings().getRemovalUniqueStoreDays());
			preparedStatement.setDate(5, date);
			preparedStatement.setInt(6, chainId);

			resultSet = preparedStatement.executeQuery();

			while (resultSet.next())
			{
				hashMap.put(resultSet.getFloat("sale_unit_price"), resultSet.getInt("count"));
			}
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			throw new GeneralException(ex.getMessage(), ex);
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return hashMap;
	}

	
	public int getRemovalUniqueStoresByRegularPrice(SuspectItemDTO suspectItemDataDTO, java.sql.Date date)
	{
		int noOfStores = 0;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append(" SELECT Count(*) AS COUNT FROM competitive_data cd ");
			sqlBuffer.append(" INNER JOIN schedule sd ON (cd.schedule_id=sd.schedule_id) AND cd.schedule_id<>? ");
			sqlBuffer.append(" WHERE Item_Code=? AND unitprice(cd.reg_price, cd.reg_m_price, cd.reg_m_pack)>0 and cd.item_not_found_flg='N' and cd.price_not_found_flg='N' ");
			sqlBuffer.append(" AND   (sd.STATUS_CHG_DATE>(to_date(?)-?) AND sd.STATUS_CHG_DATE<=?) AND sd.comp_str_id IN ");
			sqlBuffer.append(" ( SELECT comp_str_id FROM competitor_store cs ");
			sqlBuffer.append(" WHERE cs.comp_chain_id = (SELECT comp_chain_id FROM competitor_store WHERE comp_str_id=?) )");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, suspectItemDataDTO.getSchedule_ID());
			preparedStatement.setInt(2, suspectItemDataDTO.getItem_ID());
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, getConfigSettings().getRemovalUniqueStoreDays());
			preparedStatement.setDate(5, date);
			preparedStatement.setInt(6, suspectItemDataDTO.getComp_Str_ID());

			resultSet = preparedStatement.executeQuery();

			if (resultSet.next())
				noOfStores = resultSet.getInt("COUNT");
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			GenericUtil.logMessageSuspect(suspectItemDataDTO, ex.getMessage());
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return noOfStores;
	}

	public int getRemovalUniqueStoresBySalePrice(SuspectItemDTO suspectItemDataDTO, java.sql.Date date)
	{
		int noOfStores = 0;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;

		try
		{
			StringBuffer sqlBuffer = new StringBuffer();

			sqlBuffer.append(" SELECT Count(*) AS COUNT FROM competitive_data cd ");
			sqlBuffer.append(" INNER JOIN schedule sd ON (cd.schedule_id=sd.schedule_id) AND cd.schedule_id<>? ");
			sqlBuffer.append(" WHERE Item_Code=? AND unitprice(cd.sale_price, cd.sale_m_price, cd.sale_m_pack)>0 and cd.item_not_found_flg='N' ");
			sqlBuffer.append(" AND   (sd.STATUS_CHG_DATE>(to_date(?)-?) AND sd.STATUS_CHG_DATE<=?) AND sd.comp_str_id IN ");
			sqlBuffer.append(" ( SELECT comp_str_id FROM competitor_store cs ");
			sqlBuffer.append(" WHERE cs.comp_chain_id = (SELECT comp_chain_id FROM competitor_store WHERE comp_str_id=?) )");

			preparedStatement = connection.prepareStatement(sqlBuffer.toString());

			preparedStatement.setInt(1, suspectItemDataDTO.getSchedule_ID());
			preparedStatement.setInt(2, suspectItemDataDTO.getItem_ID());
			preparedStatement.setDate(3, date);
			preparedStatement.setInt(4, getConfigSettings().getRemovalUniqueStoreDaysSale());
			preparedStatement.setDate(5, date);
			preparedStatement.setInt(6, suspectItemDataDTO.getComp_Str_ID());

			resultSet = preparedStatement.executeQuery();

			if (resultSet.next())
				noOfStores = resultSet.getInt("COUNT");
		}
		catch (Exception ex)
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);

			GenericUtil.logMessageSuspect(suspectItemDataDTO, ex.getMessage());
		}
		finally
		{
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return noOfStores;
	}
	
	public void clearSuspectData(int schID) throws GeneralException {
		
		StringBuffer sb = new StringBuffer();
		sb.append("delete from suspect_item_reasons where check_data_id in ( select check_data_id from competitive_data where schedule_id= ");
		sb.append(schID).append(')');
		PristineDBUtil.execute(connection, sb, " suspect Item Cleanup");
		sb = new StringBuffer();
		sb.append("delete from suspect_item where check_data_id in ( select check_data_id from competitive_data where schedule_id= ");
		sb.append(schID).append(')');
		PristineDBUtil.execute(connection, sb, " suspect Item Cleanup");
		
	}
	
	private ConfigSettingDTO getConfigSettings()
	{
		return configSettings;
	}
}
