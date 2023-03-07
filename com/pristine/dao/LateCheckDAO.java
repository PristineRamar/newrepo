/*
 * Author : vaibhavkumar Start Date : Aug 4, 2009
 * 
 * Change Description Changed By Date
 * --------------------------------------------------------------
 */

package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.log4j.Logger;
import com.pristine.dto.LateCheckDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class LateCheckDAO {
	private static Logger logger = Logger.getLogger(LateCheckDAO.class);
	private Connection connection = null;
	private static HashMap<String, String> configSettingsMap = null;

	public static HashMap<String, String> getConfigSettingsMap() {
		return configSettingsMap;
	}

	public LateCheckDAO() throws GeneralException {
		try {
			logger.debug("Opening Connection for LateCheckDAO");

			connection = DBManager.getOracleConnection();

			configSettingsMap = GenericDAO.getConfigSettings(connection);

		} catch (Exception ex) {
			PristineDBUtil.close(connection);

			throw new GeneralException("Connection to database failed.", ex);
		}
	}

	/**
	 * This method returns list of schedules group by to price checkers where price
	 * check not done for the week corresponding to the date passed. schedule
	 * 
	 * @param date
	 * @return
	 * @throws GeneralException
	 */

	public HashMap<String, ArrayList<LateCheckDTO>> getLateCheckForCheckers(String date) throws GeneralException {
		logger.debug("getLateCheckForCheckers-->Date Received>>" + date);

		ArrayList<LateCheckDTO> lateCheckList = null;
		PreparedStatement preparedStatement = null;
		ResultSet resultSet = null;
		StringBuffer sbf = new StringBuffer();
		String startDate = null;
//		String endDate = null;
		HashMap<String, ArrayList<LateCheckDTO>> hashMap = null;

		try {
			if (!date.trim().equals("sysdate")) {
				logger.debug("getLateCheckForCheckers-->Inside Date not null");

				sbf.append(" select TRUNC(To_Date(?,'YYYY-MM-DD') , 'D') startofweek,");
				sbf.append(" TRUNC(To_Date(?,'YYYY-MM-DD') , 'D') + 6 endofweek");
				sbf.append(" FROM DUAL");

			} else {
				sbf.append(" select TRUNC(sysdate , 'D') startofweek,");
				sbf.append(" TRUNC(sysdate , 'D') + 6 endofweek");
				sbf.append(" FROM DUAL");
			}

			preparedStatement = connection.prepareStatement(sbf.toString());

			logger.debug("getLateCheckForCheckers-->weeks days query>>" + sbf.toString());

			if (!date.trim().equals("sysdate")) {
				preparedStatement.setString(1, date);
				preparedStatement.setString(2, date);
			}

			resultSet = preparedStatement.executeQuery();

			if (resultSet.next()) {
				startDate = resultSet.getString("startofweek");
//				endDate = resultSet.getString("endofweek");
			}

			sbf.delete(0, sbf.length());

			resultSet.close();
			resultSet = null;
			preparedStatement.close();
			preparedStatement = null;

			sbf.append(" SELECT sc.schedule_Id,to_char(nvl(sc.check_date,sc.end_date),'DY-MM/DD') checkDate,");
			sbf.append(" cc.comp_chain_name, cs.city , ud.first_name , ");
			sbf.append(" ud.last_name  , ud.e_mail,ud.user_id, ud.supervisor_id,pcl.name ");
			sbf.append(" FROM schedule sc");
			sbf.append(" INNER JOIN user_details ud ON (sc.price_checker_id=ud.user_id)");
			sbf.append(" INNER JOIN competitor_store cs ON (sc.comp_str_id = cs.comp_str_id)");
			sbf.append(" INNER JOIN competitor_chain cc ON (cs.comp_chain_id = cc.comp_chain_id)");
			sbf.append(" INNER JOIN price_check_list pcl ON (sc.price_check_list_id =pcl.id)");
			sbf.append(" WHERE ");
			sbf.append(" sc.start_date >= To_Date('" + startDate.substring(0, 10) + "','YYYY-MM-DD') ");
			if (!date.trim().equals("sysdate")) {
				sbf.append(" AND NVL(sc.check_date,sc.end_date) <= to_date('" + date + "','yyyy-MM-dd')");
			} else {
				sbf.append(" AND NVL(sc.check_date,sc.end_date) <= to_date(" + date + ")");
			}
			sbf.append(" AND  sc.current_status in(0,1) ");
			sbf.append(" ORDER BY ud.user_id, nvl(sc.check_date, sc.end_date) desc");

			preparedStatement = connection.prepareStatement(sbf.toString(), ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);

			logger.debug("getLateCheckForCheckers-->query >>" + sbf.toString());

			resultSet = preparedStatement.executeQuery();

			lateCheckList = new ArrayList<LateCheckDTO>();

			hashMap = new HashMap<String, ArrayList<LateCheckDTO>>();

			boolean isFirst = true;
			String prevKey = "";

			// Iteration logic is implemented such that all the results are grouped by price
			// checkers.

			while (resultSet.next()) {
				String userIdKey = resultSet.getString("user_id");

				if (isFirst) {
					hashMap.put(userIdKey, lateCheckList);

					LateCheckDTO lateDto = new LateCheckDTO();

					lateDto.setScheduleId(resultSet.getInt("schedule_Id"));
					lateDto.setCheckDate(resultSet.getString("checkDate"));
					lateDto.setChainName(resultSet.getString("comp_chain_name"));
					lateDto.setCity(resultSet.getString("city"));
					lateDto.setFirstName(resultSet.getString("first_name"));
					lateDto.setLastName(resultSet.getString("last_name"));
					lateDto.setEmail(resultSet.getString("e_mail"));
					lateDto.setUserId(resultSet.getString("user_id"));
					lateDto.setSupervisorId(resultSet.getString("supervisor_id"));
					lateDto.setCheckListName(resultSet.getString("name"));
					lateCheckList.add(lateDto);

					prevKey = userIdKey;

					isFirst = false;

				} else if (prevKey.equalsIgnoreCase(userIdKey) && hashMap.containsKey(prevKey)) {
					LateCheckDTO lateDto = new LateCheckDTO();

					lateDto.setScheduleId(resultSet.getInt("schedule_Id"));
					lateDto.setCheckDate(resultSet.getString("checkDate"));
					lateDto.setChainName(resultSet.getString("comp_chain_name"));
					lateDto.setCity(resultSet.getString("city"));
					lateDto.setFirstName(resultSet.getString("first_name"));
					lateDto.setLastName(resultSet.getString("last_name"));
					lateDto.setEmail(resultSet.getString("e_mail"));
					lateDto.setUserId(resultSet.getString("user_id"));
					lateDto.setSupervisorId(resultSet.getString("supervisor_id"));
					lateDto.setCheckListName(resultSet.getString("name"));
					lateCheckList.add(lateDto);

				} else {
					lateCheckList = new ArrayList<LateCheckDTO>();

					hashMap.put(userIdKey, lateCheckList);

					LateCheckDTO lateDto = new LateCheckDTO();

					lateDto.setScheduleId(resultSet.getInt("schedule_Id"));
					lateDto.setCheckDate(resultSet.getString("checkDate"));
					lateDto.setChainName(resultSet.getString("comp_chain_name"));
					lateDto.setCity(resultSet.getString("city"));
					lateDto.setFirstName(resultSet.getString("first_name"));
					lateDto.setLastName(resultSet.getString("last_name"));
					lateDto.setEmail(resultSet.getString("e_mail"));
					lateDto.setUserId(resultSet.getString("user_id"));
					lateDto.setSupervisorId(resultSet.getString("supervisor_id"));
					lateDto.setCheckListName(resultSet.getString("name"));
					lateCheckList.add(lateDto);

					prevKey = userIdKey;

				}

			}

			logger.debug("getLateCheckForCheckers-->Size of late Checks>>" + lateCheckList.size());

		} catch (Exception ex) {
			logger.debug("getLateCheckForCheckers-->EXCEPTIONS IS >>" + ex.getMessage());
			throw new GeneralException("getLateCheckForCheckers method failed.", ex);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return hashMap;

	}

	/**
	 * * This method returns list of schedules to be send to Supervisor where price
	 * check not done for the week corresponding to the date passed. schedule
	 * 
	 * @param date
	 * @return
	 * @throws GeneralException
	 */

	public ArrayList<LateCheckDTO> getLateCheckForSupervisors(String date) throws GeneralException {
		logger.debug("getLateCheckForSupervisors-->Date Received>>" + date + "VALUE");

		ArrayList<LateCheckDTO> lateCheckList = null;

		PreparedStatement preparedStatement = null;

		ResultSet resultSet = null;
		StringBuffer sbf = new StringBuffer();
		String startDate = null;
//		String endDate = null;

		try {
			if (!date.trim().equals("sysdate")) {
				logger.debug("getLateCheckForSupervisors-->Inside Date not null");

				sbf.append(" select TRUNC(To_Date(?,'YYYY-MM-DD') , 'D') startofweek,");
				sbf.append(" TRUNC(To_Date(?,'YYYY-MM-DD') , 'D') + 6 endofweek");
				sbf.append(" FROM DUAL");
			} else {
				sbf.append(" select TRUNC(sysdate , 'D') startofweek,");
				sbf.append(" TRUNC(sysdate , 'D') + 6 endofweek");
				sbf.append(" FROM DUAL");
			}

			preparedStatement = connection.prepareStatement(sbf.toString());

			logger.debug("getLateCheckForSupervisors-->weeks days query>>" + sbf.toString());

			if (!date.trim().equals("sysdate")) {
				preparedStatement.setString(1, date);
				preparedStatement.setString(2, date);

			}
			resultSet = preparedStatement.executeQuery();

			if (resultSet.next()) {
				startDate = resultSet.getString("startofweek");
			}

			sbf.delete(0, sbf.length());

			resultSet.close();
			resultSet = null;
			preparedStatement.close();
			preparedStatement = null;

			sbf.append(" SELECT sc.schedule_Id,to_char(nvl(sc.check_date,sc.end_date),'DY-MM/DD') checkDate,");
			sbf.append(" cc.comp_chain_name, cs.city , ud.first_name , ");
			sbf.append(" ud.last_name  , ud.e_mail,ud.user_id, ud.supervisor_id,pcl.name ");
			sbf.append(" FROM schedule sc");
			sbf.append(" INNER JOIN user_details ud ON (sc.price_checker_id=ud.user_id)");
			sbf.append(" INNER JOIN competitor_store cs ON (sc.comp_str_id = cs.comp_str_id)");
			sbf.append(" INNER JOIN competitor_chain cc ON (cs.comp_chain_id = cc.comp_chain_id)");
			sbf.append(" INNER JOIN price_check_list pcl ON (sc.price_check_list_id =pcl.id)");
			sbf.append(" WHERE ");
			sbf.append(" sc.start_date >= To_Date('" + startDate.substring(0, 10) + "','yyyy-MM-dd') ");
			if (!date.trim().equals("sysdate")) {
				sbf.append(" AND NVL(sc.check_date,sc.end_date) <= to_date('" + date + "','yyyy-MM-dd')");
			} else {
				sbf.append(" AND NVL(sc.check_date,sc.end_date) <= to_date(" + date + ")");
			}
			sbf.append(" AND  sc.current_status in(0,1) ");
			sbf.append(" ORDER BY nvl(sc.check_date, sc.end_date) desc");

			preparedStatement = connection.prepareStatement(sbf.toString());

			logger.debug("getLateCheckForSupervisors-->query >>" + sbf.toString());

			resultSet = preparedStatement.executeQuery();

			lateCheckList = new ArrayList<LateCheckDTO>();

			while (resultSet.next()) {

				LateCheckDTO lateDto = new LateCheckDTO();

				lateDto.setScheduleId(resultSet.getInt("schedule_Id"));
				lateDto.setCheckDate(resultSet.getString("checkDate"));
				lateDto.setChainName(resultSet.getString("comp_chain_name"));
				lateDto.setCity(resultSet.getString("city"));
				lateDto.setFirstName(resultSet.getString("first_name"));
				lateDto.setLastName(resultSet.getString("last_name"));
				lateDto.setEmail(resultSet.getString("e_mail"));
				lateDto.setUserId(resultSet.getString("user_id"));
				lateDto.setSupervisorId(resultSet.getString("supervisor_id"));
				lateDto.setCheckListName(resultSet.getString("name"));
				lateCheckList.add(lateDto);
			}

			logger.debug("getLateCheckForSupervisors-->Size of late Checks>>" + lateCheckList.size());

		} catch (Exception ex) {
			logger.debug("getLateCheckForSupervisors-->EXCEPTIONS IS >>" + ex.getMessage());
			throw new GeneralException("getLateCheckForSupervisors method failed.", ex);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(preparedStatement);
		}

		return lateCheckList;

	}

	/**
	 * This method inserts/updates LATE_FLAG='Y' in Performance_Stats table
	 * 
	 * @param lateChkDtoList
	 */

	public void insertUpdatePerformanceStats(ArrayList<LateCheckDTO> lateChkDtoList) throws GeneralException {

		ResultSet rs = null;
		PreparedStatement psmt = null;
		StringBuffer sbf = new StringBuffer();
		boolean isPresent = false;

		try {

			connection.setAutoCommit(false);

			for (LateCheckDTO lateChkDTO : lateChkDtoList) {

				sbf.delete(0, sbf.length());

				sbf.append(" select Schedule_Id from performance_stat ");
				sbf.append(" where schedule_id = ?");

				psmt = connection.prepareStatement(sbf.toString());

				psmt.setInt(1, lateChkDTO.getScheduleId());

				logger.debug("insertUpdatePerformanceStats-->sel qry-->" + sbf.toString());

				rs = psmt.executeQuery();

				if (rs.next()) {

					isPresent = true;

				} else {

					isPresent = false;
				}

				sbf.delete(0, sbf.length());
				
				psmt.close();

				if (isPresent) {

					sbf.append(" update performance_stat set LATE_FLAG='Y',");
					sbf.append(" NOT_DONE_FLAG ='Y'");
					sbf.append(" where schedule_id = ?");

					psmt = connection.prepareStatement(sbf.toString());

					psmt.setInt(1, lateChkDTO.getScheduleId());

					logger.debug("insertUpdatePerformanceStats-->Qry>>" + sbf.toString());

					int j = psmt.executeUpdate();

					if (j > 0) {

						connection.commit();
						connection.setAutoCommit(true);

						logger.debug(" insertUpdatePerformanceStats-->Update success for ScheduleId>>"
								+ lateChkDTO.getScheduleId());

					} else {

						connection.rollback();

						logger.debug(" insertUpdatePerformanceStats-->Update FAILED for ScheduleId>>"
								+ lateChkDTO.getScheduleId());
					}

				} else {

					sbf.append(" insert into performance_stat(schedule_id,late_flag,not_done_flag,item_ct,");
					sbf.append(" items_checked_ct,item_not_found_ct,price_not_found_ct,");
					sbf.append(" out_of_range_ct,out_of_reasonability_ct,duration,on_sale_ct,");
					sbf.append(" went_up_ct,not_found_x_times_ct)");
					sbf.append(" values(?,'Y','Y',0,0,0,0,0,0,0,0,0,0)");

					psmt = connection.prepareStatement(sbf.toString());

					psmt.setInt(1, lateChkDTO.getScheduleId());

					logger.debug("insertUpdatePerformanceStats-->Qry>>" + sbf.toString());

					int j = psmt.executeUpdate();

					if (j > 0) {

						connection.commit();
						connection.setAutoCommit(true);

						logger.debug(" insertUpdatePerformanceStats-->INSERT success for ScheduleId>>"
								+ lateChkDTO.getScheduleId());

					} else {

						connection.rollback();

						logger.info(" insertUpdatePerformanceStats-->INSERT FAILED for ScheduleId>>"
								+ lateChkDTO.getScheduleId());
					}

				}

			}

		} catch (Exception ex) {
			logger.debug("EXCEPTIONS IS >>" + ex.getMessage());
			throw new GeneralException("getLateCheckDetails method failed.", ex);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(psmt);
		}
	}
}
