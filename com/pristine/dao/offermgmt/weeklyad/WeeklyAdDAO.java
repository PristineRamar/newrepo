package com.pristine.dao.offermgmt.weeklyad;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.pristine.dao.offermgmt.promotion.PromotionDAO;
import com.pristine.dto.AdKey;
import com.pristine.dto.offermgmt.promotion.PromoDefinition;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAd;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdBlock;
import com.pristine.dto.offermgmt.weeklyad.WeeklyAdPage;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.PristineDBUtil;

public class WeeklyAdDAO {
	static Logger logger = Logger.getLogger("WeeklyAdDAO");
	
	private Connection conn = null;
	
	private static final Integer STATUS = 3;
	
	private static final String SELECT_WEEKLY_AD = "SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_DEFINITION WHERE CALENDAR_ID = ? "
			+ "AND LOCATION_LEVEL_ID = ? AND LOCATION_ID = ?";
	
	private static final String GET_WEEKLY_AD_ID = "SELECT PM_WEEKLY_AD_DEFINITION_SEQ.NEXTVAL WEEKLY_AD_ID FROM DUAL";
	
	private static final String INSERT_WEEKLY_AD = "INSERT INTO PM_WEEKLY_AD_DEFINITION (WEEKLY_AD_ID, AD_NAME, LOCATION_LEVEL_ID, "
			+ " LOCATION_ID, CALENDAR_ID, TOTAL_PAGES, STATUS, CREATED_BY, CREATED, APPROVED_BY, APPROVED) VALUES "
			+ " (?, ?, ?, ? ,? ,?, ?, ?, SYSDATE, ?, SYSDATE)";
	
	private static final String UPDATE_WEEKLY_AD = "UPDATE PM_WEEKLY_AD_DEFINITION SET AD_NAME = ?, TOTAL_PAGES = ?, STATUS = ?, "
			+ "MODIFIED_BY = ?, MODIFIED = SYSDATE, APPROVED_BY = ?, APPROVED = SYSDATE WHERE WEEKLY_AD_ID = ?";
	
	private static final String SELECT_WEEKLY_AD_PAGE = "SELECT PAGE_ID FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID = ? AND PAGE_NUMBER = ?";
	
	private static final String GET_PAGE_ID = "SELECT PM_WEEKLY_AD_PAGE_SEQ.NEXTVAL PAGE_ID FROM DUAL";
	
	private static final String INSERT_WEEKLY_AD_PAGE = "INSERT INTO PM_WEEKLY_AD_PAGE (PAGE_ID, WEEKLY_AD_ID, PAGE_NUMBER, "
			+ "TOTAL_BLOCKS, STATUS) VALUES (?, ?, ?, ?, ?)";
	
	private static final String UPDATE_WEEKLY_AD_PAGE = "UPDATE PM_WEEKLY_AD_PAGE SET TOTAL_BLOCKS = ?, STATUS = ? WHERE PAGE_ID = ?";
	
	private static final String SELECT_WEEKLY_AD_BLOCK = "SELECT BLOCK_ID FROM PM_WEEKLY_AD_BLOCK WHERE PAGE_ID = ? AND BLOCK_NUMBER = ?";
	
	private static final String GET_BLOCK_ID = "SELECT PM_WEEKLY_AD_BLOCK_SEQ.NEXTVAL BLOCK_ID FROM DUAL";
	
	private static final String INSERT_WEEKLY_AD_BLOCK = "INSERT INTO PM_WEEKLY_AD_BLOCK (BLOCK_ID, PAGE_ID, BLOCK_NUMBER, "
			+ "TOTAL_PROMOTIONS, STATUS, ADJUSTED_UNITS, ACTUAL_TOTAL_ITEMS) VALUES (?, ?, ?, ?, ?, ?, ?)";
	
	private static final String UPDATE_WEEKLY_AD_BLOCK = "UPDATE PM_WEEKLY_AD_BLOCK SET TOTAL_PROMOTIONS = ?, STATUS = ?, "
			+ "ADJUSTED_UNITS = ? WHERE BLOCK_ID = ?";
	
	private static final String GET_WEEKLY_AD_PROMO = "SELECT COUNT(1) AS COUNT FROM PM_WEEKLY_AD_PROMO WHERE BLOCK_ID = ? "
			+ "AND PROMO_DEFINITION_ID = ?";
	
	private static final String INSERT_WEEKLY_AD_PROMO = "INSERT INTO PM_WEEKLY_AD_PROMO (BLOCK_ID, PROMO_DEFINITION_ID, TOTAL_ITEMS, STATUS, "
			+ "CREATED_BY, CREATED, APPROVED_BY, APPROVED) VALUES (?, ?, ?, ?, ?, SYSDATE, ?, SYSDATE)";
	
	private static final String UPDATE_WEEKLY_AD_PROMO = "UPDATE PM_WEEKLY_AD_PROMO SET TOTAL_ITEMS = ?, STATUS = ?, CREATED_BY = ?, "
			+ "CREATED = SYSDATE, APPROVED_BY = ?, APPROVED = SYSDATE WHERE BLOCK_ID = ? AND PROMO_DEFINITION_ID = ?";
	
//	private static final String GET_ALL_BLOCKS = " SELECT WAP.PAGE_NUMBER, WAB.BLOCK_NUMBER, ACTUAL_TOTAL_ITEMS FROM PM_WEEKLY_AD_BLOCK WAB "
//			+ " LEFT JOIN PM_WEEKLY_AD_PAGE WAP ON WAB.PAGE_ID = WAP.PAGE_ID WHERE WAP.PAGE_ID IN "
//			+ " (SELECT PAGE_ID FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID IN (SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_DEFINITION "
//			+ " WHERE LOCATION_LEVEL_ID= ? AND LOCATION_ID=? AND CALENDAR_ID = ?))";
	
	private static final String GET_ALL_BLOCKS = " SELECT PAGE_NUMBER, BLOCK_NUMBER, "
			+ " COUNT(DISTINCT(RETAILER_ITEM_CODE)) AS ACTUAL_TOTAL_ITEMS, "
			+ " MIN(ACTUAL_TOTAL_ITEMS) AS AD_PLEX_TOTAL_ITEMS FROM PM_PROMO_BUY_ITEM PBI "
			+ " LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PBI.ITEM_CODE "
			+ " LEFT JOIN PM_WEEKLY_AD_PROMO WAP ON WAP.PROMO_DEFINITION_ID = PBI.PROMO_DEFINITION_ID "
			+ " LEFT JOIN PM_WEEKLY_AD_BLOCK WAB ON WAB.BLOCK_ID = WAP.BLOCK_ID "
			+ " LEFT JOIN PM_WEEKLY_AD_PAGE WAP ON WAB.PAGE_ID = WAP.PAGE_ID WHERE WAP.PAGE_ID IN "
			+ " (SELECT PAGE_ID FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID IN (SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_DEFINITION "
			+ " WHERE LOCATION_LEVEL_ID= ? AND LOCATION_ID=? AND CALENDAR_ID = ?)) GROUP BY PAGE_NUMBER, BLOCK_NUMBER" ;
	
	private static final String GET_BLOCK_ID_LIST = "SELECT P.PAGE_NUMBER, B.BLOCK_NUMBER, B.BLOCK_ID FROM PM_WEEKLY_AD_BLOCK B LEFT JOIN "
			+ "PM_WEEKLY_AD_PAGE P ON B.PAGE_ID = P.PAGE_ID LEFT JOIN PM_WEEKLY_AD_DEFINITION AD "
			+ "ON AD.WEEKLY_AD_ID = P.WEEKLY_AD_ID WHERE AD.LOCATION_LEVEL_ID= ? AND AD.LOCATION_ID=? AND AD.CALENDAR_ID = ? ";
	
	private static final String INSERT_INTO_AD_PLEX_ITEMS = "INSERT INTO PM_AD_PLEX_ITEMS(BLOCK_ID, RETAILER_ITEM_CODE) VALUES(?, ?) ";
	
	private static final String GET_ACTUAL_ITEMS = "SELECT DISTINCT(RETAILER_ITEM_CODE) AS RETAILER_ITEM_CODE, PAGE_NUMBER, "
			+ " BLOCK_NUMBER FROM PM_WEEKLY_AD_PAGE WAP LEFT JOIN PM_WEEKLY_AD_BLOCK WAB ON WAB.PAGE_ID = WAP.PAGE_ID"
			+ " LEFT JOIN PM_WEEKLY_AD_PROMO WP ON WP.BLOCK_ID = WAB.BLOCK_ID"
			+ " LEFT JOIN PM_PROMO_BUY_ITEM PBI ON PBI.PROMO_DEFINITION_ID = WP.PROMO_DEFINITION_ID"
			+ " LEFT JOIN ITEM_LOOKUP IL ON IL.ITEM_CODE = PBI.ITEM_CODE"
			+ " WHERE WAP.WEEKLY_AD_ID IN (SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_DEFINITION WHERE CALENDAR_ID=?"
			+ " AND LOCATION_LEVEL_ID = ? AND LOCATION_ID=?)";
	
	private static final String GET_AD_PLEX_ITEMS ="SELECT DISTINCT(RETAILER_ITEM_CODE) AS RETAILER_ITEM_CODE, WAP.PAGE_NUMBER, WAB.BLOCK_NUMBER "
			+ " FROM PM_AD_PLEX_ITEMS API LEFT JOIN PM_WEEKLY_AD_BLOCK WAB ON WAB.BLOCK_ID = API.BLOCK_ID"
			+ " LEFT JOIN PM_WEEKLY_AD_PAGE WAP ON WAB.PAGE_ID = WAP.PAGE_ID WHERE WAP.WEEKLY_AD_ID IN "
			+ " (SELECT WEEKLY_AD_ID FROM PM_WEEKLY_AD_DEFINITION WHERE CALENDAR_ID=? "
			+ " AND LOCATION_LEVEL_ID = ? AND LOCATION_ID =?)";
	
	public WeeklyAdDAO(Connection conn){
		this.conn = conn;
	}
	
	public void saveWeeklyAd(WeeklyAd weeklyAd) {
		
		long weeklyAdId = -1;
		weeklyAdId = getWeeklyAd(weeklyAd);
		// If weekly ad already exists
		if (weeklyAdId > 0) {
			weeklyAd.setAdId(weeklyAdId);
			if (weeklyAd.getTotalPages() == 0) {
				//If there is no page, delete the entire weekly ad
				deletePageBlockAndPromo(weeklyAdId);
				deleteWeeklyAdDefinition(weeklyAdId);				
			} else {
				//other wise update weekly ad
				updateWeeklyAd(weeklyAd);
				//delete rest of the tables
				deletePageBlockAndPromo(weeklyAdId);
				//Insert weekly ad
				insertPageBlockPromo(weeklyAd);
			}
		} else {
			boolean dbStatus = false;
			// if not exists insert weekly ad
			if (weeklyAd.getTotalPages() > 0) {
				weeklyAdId = getWeeklyAdId();
				weeklyAd.setAdId(weeklyAdId);
				dbStatus = insertWeeklyAd(weeklyAd);
				if(dbStatus) {
					insertPageBlockPromo(weeklyAd);
				} else {
					logger.error("Insert/Update of Weekly Ad failed");
					return;
				}
			}			
		}
	}
	
	/**
	 * Insert Page, Block and Promo to a weekly ad
	 * @param weeklyAd
	 */
	private void insertPageBlockPromo(WeeklyAd weeklyAd) {
		for (WeeklyAdPage adPage : weeklyAd.getAdPages().values()) {
			adPage.setAdId(weeklyAd.getAdId());
			saveWeeklyAdPage(adPage);
		}
	}
	/**
	 * Saves weekly ad info
	 * @param weeklyAd
	 */
//	public void saveWeeklyAd(WeeklyAd weeklyAd){
//		if(weeklyAd.getAdPages() != null && weeklyAd.getAdPages().size() > 0){
//			int totalPages = weeklyAd.getAdPages().lastKey();
//			if(totalPages % 2 == 0)
//				weeklyAd.setTotalPages(totalPages);
//			else
//				weeklyAd.setTotalPages(totalPages + 1);
//		}else{
//			logger.error("No Ad Page in Weekly Ad");
//			return;
//		}
//		
//		boolean dbStatus = false;
//		long weeklyAdId = -1;
//		weeklyAdId = getWeeklyAd(weeklyAd);
//		if(weeklyAdId > 0){
//			//logger.info("Updating weekly ad " + weeklyAdId);
//			weeklyAd.setAdId(weeklyAdId);
//			dbStatus = updateWeeklyAd(weeklyAd);
//		}else{
//			weeklyAdId = getWeeklyAdId();
//			//logger.info("Inserting weekly ad " + weeklyAdId);
//			weeklyAd.setAdId(weeklyAdId);
//			dbStatus = insertWeeklyAd(weeklyAd);
//		}
//		
//		if(dbStatus){
//			for(WeeklyAdPage adPage : weeklyAd.getAdPages().values()){
//				adPage.setAdId(weeklyAdId);
//				saveWeeklyAdPage(adPage);
//			}
//		}else{
//			logger.error("Insert/Update of Weekly Ad failed");
//			return;
//		}
//	}
	
	//Delete page, block, promo and AdPlexItems of the weekly ad
	private void deletePageBlockAndPromo(long weeklyAdId){
		String DELETE_WEEKLY_AD_PAGE = "DELETE FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID = " + weeklyAdId;
		String DELETE_WEEKLY_AD_BLOCK = "DELETE FROM PM_WEEKLY_AD_BLOCK WHERE PAGE_ID IN (SELECT PAGE_ID FROM " +
				"PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID = " + weeklyAdId + ")";
		String DELETE_WEEKLY_AD_PROMO = "DELETE FROM PM_WEEKLY_AD_PROMO WHERE BLOCK_ID IN " +
				"(SELECT BLOCK_ID FROM PM_WEEKLY_AD_BLOCK WHERE PAGE_ID IN " +
				"(SELECT PAGE_ID FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID = " + weeklyAdId + "))";
		String DELETE_VALUES_FROM_AD_PLEX_ITEMS = "DELETE FROM PM_AD_PLEX_ITEMS WHERE BLOCK_ID IN "+
				"(SELECT BLOCK_ID FROM PM_WEEKLY_AD_BLOCK WHERE PAGE_ID IN " +
				"(SELECT PAGE_ID FROM PM_WEEKLY_AD_PAGE WHERE WEEKLY_AD_ID = " + weeklyAdId + "))";
		PreparedStatement stmt = null;		
		try {
			stmt = conn.prepareStatement(DELETE_VALUES_FROM_AD_PLEX_ITEMS);			 
			stmt.executeUpdate();
			PristineDBUtil.close(stmt);
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_PROMO);						 
			stmt.executeUpdate();
			PristineDBUtil.close(stmt);
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_BLOCK);			 
			stmt.executeUpdate();
			PristineDBUtil.close(stmt);
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_PAGE);			 
			stmt.executeUpdate();
			
		} catch (SQLException exception) {
			logger.error("Error when deleting Weekly Ad Definition - " + exception);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	//Delete only the weekly ad definition record
	private void deleteWeeklyAdDefinition(long weeklyAdId) {
		String DELETE_WEEKLY_AD_DEF = "DELETE FROM PM_WEEKLY_AD_DEFINITION WHERE WEEKLY_AD_ID = " + weeklyAdId;
		PreparedStatement stmt = null;		
		try {
			stmt = conn.prepareStatement(DELETE_WEEKLY_AD_DEF);			 
			stmt.executeUpdate();
		} catch (SQLException exception) {
			logger.error("Error when deleting Weekly Ad Definition - " + exception);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}
	
	/**
	 * Saves ad page info
	 * @param adPage
	 */
	public void saveWeeklyAdPage(WeeklyAdPage adPage){
		if(adPage.getAdBlocks() != null){
			adPage.setTotalBlocks(adPage.getAdBlocks().lastKey());
		}else{
			logger.error("No Ad Block in Ad Page");
			return;
		}
		
		boolean dbStatus = false;
		long pageId = -1;
		pageId = getWeeklyAdPage(adPage);
		if(pageId > 0){
			//logger.info("Updating weekly ad page " + pageId);
			adPage.setPageId(pageId);
			dbStatus = updateWeeklyAdPage(adPage);
		}else{
			pageId = getWeeklyAdPageId();
			//logger.info("Inserting weekly ad page " + pageId);
			adPage.setPageId(pageId);
			dbStatus = insertWeeklyAdPage(adPage);
		}
		
		if(dbStatus){
			for(WeeklyAdBlock adBlock : adPage.getAdBlocks().values()){
				adBlock.setPageId(pageId);
				saveWeeklyAdBlock(adBlock);
			}
		}else{
			logger.error("Insert/Update of Weekly Ad Page failed");
			return;
		}
	}
	
	/**
	 * Saves ad block info
	 * @param adBlock
	 */
	public void saveWeeklyAdBlock(WeeklyAdBlock adBlock){
		if(adBlock.getPromotions() != null){
			adBlock.setTotalPromotions(adBlock.getPromotions().size());
		}else{
			logger.error("No Promotion in Ad Block");
			return;
		}
		
		boolean dbStatus = false;
		long blockId = -1;
		blockId = getWeeklyAdBlock(adBlock);
		if(blockId > 0){
			//logger.info("Updating weekly ad block " + blockId);
			adBlock.setBlockId(blockId);
			dbStatus = updateWeeklyAdBlock(adBlock);
		}else{
			blockId = getWeeklyAdBlockId();
			//logger.info("Inserting weekly ad block " + blockId);
			adBlock.setBlockId(blockId);
			dbStatus = insertWeeklyAdBlock(adBlock);
		}
		
		if(dbStatus){
			PromotionDAO promoDAO = new PromotionDAO(conn);
			for(PromoDefinition promotion : adBlock.getPromotions()){
				//promoDAO.savePromotion(promotion);
				saveWeeklyAdPromo(adBlock, promotion);
			}
		}else{
			logger.error("Insert/Update of Weekly Ad Block failed");
			return;
		}
	}
	
	/**
	 * Saves weekly ad promo info
	 * @param adBlock
	 * @param promotion
	 */
	private void saveWeeklyAdPromo(WeeklyAdBlock adBlock, PromoDefinition promotion) {
		int count = getWeeklyAdPromo(adBlock.getBlockId(), promotion.getPromoDefnId());
		if(count > 0)
			updateWeeklyAdPromo(adBlock, promotion);
		else
			insertWeeklyAdPromo(adBlock, promotion);
	}

	/**
	 * Get Weekly Ad for given calendar id, location level id and location id
	 * @return
	 */
	private long getWeeklyAd(WeeklyAd weeklyAd){
		long weeklyAdId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(SELECT_WEEKLY_AD);
			int counter = 0;
			stmt.setInt(++counter, weeklyAd.getCalendarId());
			stmt.setInt(++counter, weeklyAd.getLocationLevelId());
			stmt.setInt(++counter, weeklyAd.getLocationId());
			rs = stmt.executeQuery();
			if(rs.next()){
				weeklyAdId = rs.getInt("WEEKLY_AD_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving weekly ad id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return weeklyAdId;
	}
	
	/**
	 * Returns weekly ad id
	 * @return
	 */
	private long getWeeklyAdId(){
		long weeklyAdId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_WEEKLY_AD_ID);
			rs = stmt.executeQuery();
			if(rs.next()){
				weeklyAdId = rs.getInt("WEEKLY_AD_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving weekly ad id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return weeklyAdId;
	}

	/**
	 * Inserts into PM_WEEKLY_AD_DEFINITION
	 * @param weeklyAd
	 * @return
	 */
	private boolean insertWeeklyAd(WeeklyAd weeklyAd){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(INSERT_WEEKLY_AD);
			int counter = 0;
			stmt.setLong(++counter, weeklyAd.getAdId());
			stmt.setString(++counter, weeklyAd.getAdName());
			stmt.setInt(++counter, weeklyAd.getLocationLevelId());
			stmt.setInt(++counter, weeklyAd.getLocationId());
			stmt.setInt(++counter, weeklyAd.getCalendarId());
			stmt.setInt(++counter, weeklyAd.getTotalPages());
			stmt.setInt(++counter, STATUS);
			stmt.setString(++counter, weeklyAd.getCreatedBy());
//			stmt.setString(++counter, weeklyAd.getWeekStartDate());
			stmt.setString(++counter, weeklyAd.getApprovedBy());
//			stmt.setString(++counter, weeklyAd.getWeekStartDate());
			//logger.info(weeklyAd.getAdId() + "\t" + weeklyAd.getLocationLevelId() + "\t" + weeklyAd.getLocationId() + "\t" + weeklyAd.getCalendarId() + "\t" + weeklyAd.getTotalPages());
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when inserting weekly ad - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}
	
	/**
	 * Updates PM_WEEKLY_AD_DEFINITION
	 * @param weeklyAd
	 * @return
	 */
	private boolean updateWeeklyAd(WeeklyAd weeklyAd){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(UPDATE_WEEKLY_AD);
			int counter = 0;
			stmt.setString(++counter, weeklyAd.getAdName());
			stmt.setInt(++counter, weeklyAd.getTotalPages());
			stmt.setInt(++counter, STATUS);
			stmt.setString(++counter, weeklyAd.getModifiedBy());
//			stmt.setString(++counter, weeklyAd.getWeekStartDate());
			stmt.setString(++counter, weeklyAd.getApprovedBy());
//			stmt.setString(++counter, weeklyAd.getWeekStartDate());
			stmt.setLong(++counter, weeklyAd.getAdId());
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when updating weekly ad - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}
	
	/**
	 * Get Weekly Ad Page for given weekly ad id and page number
	 * @return
	 */
	private long getWeeklyAdPage(WeeklyAdPage adPage){
		long weeklyAdId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(SELECT_WEEKLY_AD_PAGE);
			int counter = 0;
			stmt.setLong(++counter, adPage.getAdId());
			stmt.setInt(++counter, adPage.getPageNumber());
			rs = stmt.executeQuery();
			if(rs.next()){
				weeklyAdId = rs.getInt("PAGE_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving weekly ad page id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return weeklyAdId;
	}
	
	/**
	 * Returns weekly ad page id
	 * @return
	 */
	private long getWeeklyAdPageId(){
		long pageId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_PAGE_ID);
			rs = stmt.executeQuery();
			if(rs.next()){
				pageId = rs.getInt("PAGE_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving weekly ad page id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return pageId;
	}

	/**
	 * Inserts into PM_WEEKLY_AD_PAGE
	 * @param adPage
	 * @return
	 */
	private boolean insertWeeklyAdPage(WeeklyAdPage adPage){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(INSERT_WEEKLY_AD_PAGE);
			int counter = 0;
			stmt.setLong(++counter, adPage.getPageId());
			stmt.setLong(++counter, adPage.getAdId());
			stmt.setInt(++counter, adPage.getPageNumber());
			stmt.setInt(++counter, adPage.getTotalBlocks());
			stmt.setInt(++counter, STATUS);
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when inserting weekly ad page - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}
	
	/**
	 * Updates PM_WEEKLY_AD_PAGE
	 * @param adPage
	 * @return
	 */
	private boolean updateWeeklyAdPage(WeeklyAdPage adPage){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(UPDATE_WEEKLY_AD_PAGE);
			int counter = 0;
			stmt.setInt(++counter, adPage.getTotalBlocks());
			stmt.setInt(++counter, STATUS);
			stmt.setLong(++counter, adPage.getPageId());
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when updating weekly ad page - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}

	/**
	 * Get Weekly Ad Block for given weekly ad page id and block number
	 * @return
	 */
	private long getWeeklyAdBlock(WeeklyAdBlock adBlock){
		long weeklyAdId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(SELECT_WEEKLY_AD_BLOCK);
			int counter = 0;
			if(adBlock.getPageId() == 24 && adBlock.getBlockNumber() == 13){
				logger.info("Block number - " + adBlock.getBlockNumber());
			}
			stmt.setLong(++counter, adBlock.getPageId());
			stmt.setInt(++counter, adBlock.getBlockNumber());
			rs = stmt.executeQuery();
			if(rs.next()){
				weeklyAdId = rs.getInt("BLOCK_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving weekly ad block id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return weeklyAdId;
	}
	
	/**
	 * Returns weekly ad block id
	 * @return
	 */
	private long getWeeklyAdBlockId(){
		long pageId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_BLOCK_ID);
			rs = stmt.executeQuery();
			if(rs.next()){
				pageId = rs.getInt("BLOCK_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving weekly ad block id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return pageId;
	}

	/**
	 * Inserts into PM_WEEKLY_AD_BLOCK
	 * @param adBlock
	 * @return
	 */
	private boolean insertWeeklyAdBlock(WeeklyAdBlock adBlock){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(INSERT_WEEKLY_AD_BLOCK);
			int counter = 0;
			stmt.setLong(++counter, adBlock.getBlockId());
			stmt.setLong(++counter, adBlock.getPageId());
			stmt.setInt(++counter, adBlock.getBlockNumber());
			stmt.setInt(++counter, adBlock.getTotalPromotions());
			stmt.setInt(++counter, STATUS);
			if(adBlock.getAdjustedUnits() > 0)
				stmt.setLong(++counter, adBlock.getAdjustedUnits());
			else
				stmt.setNull(++counter, Types.NULL);
			
			stmt.setInt(++counter, adBlock.getActualTotalItems());
			logger.debug(adBlock.getBlockId() + ", " + adBlock.getPageId() + ", " + adBlock.getBlockNumber() + ", " + adBlock.getTotalPromotions());
			
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when inserting weekly ad block - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}
	
	/**
	 * Updates PM_WEEKLY_AD_BLOCK
	 * @param adBlock
	 * @return
	 */
	private boolean updateWeeklyAdBlock(WeeklyAdBlock adBlock){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(UPDATE_WEEKLY_AD_BLOCK);
			int counter = 0;
			stmt.setInt(++counter, adBlock.getTotalPromotions());
			stmt.setInt(++counter, STATUS);
			if(adBlock.getAdjustedUnits() > 0)
				stmt.setLong(++counter, adBlock.getAdjustedUnits());
			else
				stmt.setNull(++counter, Types.NULL);
			stmt.setLong(++counter, adBlock.getBlockId());
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when updating weekly ad block - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}
	
	/**
	 * Get Weekly Ad Promo for given block and promo id
	 * @return
	 */
	private int getWeeklyAdPromo(long blockId, long promoDefnId){
		int count = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(GET_WEEKLY_AD_PROMO);
			int counter = 0;
			stmt.setLong(++counter, blockId);
			stmt.setLong(++counter, promoDefnId);
			rs = stmt.executeQuery();
			if(rs.next()){
				count = rs.getInt("COUNT");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving weekly ad promo - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return count;
	}
	
	/**
	 * Inserts into PM_WEEKLY_AD_PROMO
	 * @param adBlock
	 * @param promotion
	 * @return
	 */
	private boolean insertWeeklyAdPromo(WeeklyAdBlock adBlock, PromoDefinition promotion){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(INSERT_WEEKLY_AD_PROMO);
			int counter = 0;
			stmt.setLong(++counter, adBlock.getBlockId());
			stmt.setLong(++counter, promotion.getPromoDefnId());
			stmt.setInt(++counter, promotion.getTotalItems());
			stmt.setInt(++counter, STATUS);
			stmt.setString(++counter, promotion.getCreatedBy());
//			stmt.setString(++counter, promotion.getWeekStartDate());
			stmt.setString(++counter, promotion.getApprovedBy());
//			stmt.setString(++counter, promotion.getWeekStartDate());
			int count = stmt.executeUpdate();
			
			
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when inserting weekly ad promo - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}
	
	/**
	 * Updates PM_WEEKLY_AD_PROMO
	 * @param adBlock
	 * @param promotion
	 * @return
	 */
	private boolean updateWeeklyAdPromo(WeeklyAdBlock adBlock, PromoDefinition promotion){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(UPDATE_WEEKLY_AD_PROMO);
			int counter = 0;
			stmt.setInt(++counter, promotion.getTotalItems());
			stmt.setInt(++counter, STATUS);
			stmt.setString(++counter, promotion.getCreatedBy());
//			stmt.setString(++counter, promotion.getWeekStartDate());
			stmt.setString(++counter, promotion.getApprovedBy());
//			stmt.setString(++counter, promotion.getWeekStartDate());
			stmt.setLong(++counter, adBlock.getBlockId());
			stmt.setLong(++counter, promotion.getPromoDefnId());
			int count = stmt.executeUpdate();
			
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error when updating weekly ad block - " + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		
		return status;
	}

	public HashMap<AdKey, WeeklyAdBlock> getAllBlocks(int locationLevelId, int locationId, int weekCalendarId) {
		HashMap<AdKey, WeeklyAdBlock> adBlocks = new HashMap<AdKey, WeeklyAdBlock>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		WeeklyAdBlock weeklyAdBlock = null;
		AdKey adKey = null;
		try {
			stmt = conn.prepareStatement(GET_ALL_BLOCKS);
			int counter = 0;
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, locationId);
			stmt.setInt(++counter, weekCalendarId);
			rs = stmt.executeQuery();
			while (rs.next()) {
				weeklyAdBlock = new WeeklyAdBlock();

				weeklyAdBlock.setBlockNumber(rs.getInt("BLOCK_NUMBER"));
				weeklyAdBlock.setActualTotalItems(rs.getInt("ACTUAL_TOTAL_ITEMS"));
				weeklyAdBlock.setAdPlexTotalItems(rs.getInt("AD_PLEX_TOTAL_ITEMS"));
				adKey = new AdKey(rs.getInt("PAGE_NUMBER"), weeklyAdBlock.getBlockNumber());
				//logger.debug("AdKey:" + adKey.toString() + ",ActualTotalItems:" + weeklyAdBlock.getActualTotalItems());
				adBlocks.put(adKey, weeklyAdBlock);
			}
		} catch (SQLException exception) {
			logger.error("Error in getAllBlocks() - " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return adBlocks;
	}
	/**
	 * Get block id with respect to Page number and Block number.
	 * @param calendarId
	 * @return
	 */
	public HashMap<AdKey, Long> getBlockIdMap(WeeklyAd weeklyAd){
		HashMap<AdKey, Long> blockIdMap = new HashMap<AdKey, Long>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_BLOCK_ID_LIST);
			int counter = 0;
			stmt.setInt(++counter, weeklyAd.getLocationLevelId());
			stmt.setInt(++counter, weeklyAd.getLocationId());
			stmt.setInt(++counter, weeklyAd.getCalendarId());
			rs = stmt.executeQuery();
			while(rs.next()) {
				AdKey key = new AdKey(rs.getInt("PAGE_NUMBER"), rs.getInt("BLOCK_NUMBER"));
//				String key = rs.getInt("PAGE_NUMBER")+"-"+rs.getInt("BLOCK_NUMBER");
				blockIdMap.put(key, rs.getLong("BLOCK_ID"));
			}
		} catch (SQLException exception) {
			logger.error("Error in getBlockIdMap() - " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return blockIdMap;
		
	}
	
	/**
	 * To insert values into Ad_plex_item table..
	 * @param blockIdandRetailerItemCode
	 * @return
	 * @throws GeneralException
	 */
	public int insertintoAdPlexItems(HashMap<Long, List<String>> blockIdandRetailerItemCode) throws GeneralException {

		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalInsertCnt = 0;
		try {
			statement = conn.prepareStatement(INSERT_INTO_AD_PLEX_ITEMS);

			for(Map.Entry<Long, List<String>> entry: blockIdandRetailerItemCode.entrySet()) {
				Long blockId = entry.getKey();
				List<String> itemCodeList = entry.getValue();
				//Iterate each block id and set the item code values..
				for(String itemCode : itemCodeList){
					int counter = 0;
					statement.setLong(++counter, blockId);
					statement.setString(++counter, itemCode);
					statement.addBatch();
					itemNoInBatch++;

					if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						int[] count = statement.executeBatch();
						totalInsertCnt = totalInsertCnt + count.length;
						statement.clearBatch();
						itemNoInBatch = 0;
					}
				}
			}
			
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalInsertCnt = totalInsertCnt + count.length;
				statement.clearBatch();
			}
			statement.close();
		} catch (Exception e) {
			logger.error("Error in insertintoAdPlexItems()  - " + e.toString());
			throw new GeneralException("Error in insertintoAdPlexItems()", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return totalInsertCnt;
	}
	
	public HashMap<AdKey, List<String>> getActualItems(int locationLevelId, int locationId, int weekCalendarId) throws GeneralException {
		HashMap<AdKey, List<String>> actualItemsMap = new HashMap<AdKey, List<String>>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		AdKey adKey = null;
		try {
			stmt = conn.prepareStatement(GET_ACTUAL_ITEMS);
			int counter = 0;
			stmt.setInt(++counter, weekCalendarId);
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, locationId);
			
			rs = stmt.executeQuery();
			while (rs.next()) {
				List<String> retailerItemCodeList= new ArrayList<String>();
				String retailerItemCode;
				adKey = new AdKey(rs.getInt("PAGE_NUMBER"), rs.getInt("BLOCK_NUMBER"));
				if(actualItemsMap.containsKey(adKey)){
					retailerItemCodeList = actualItemsMap.get(adKey);
				}
				retailerItemCode= rs.getString("RETAILER_ITEM_CODE");
				retailerItemCodeList.add(retailerItemCode);
				actualItemsMap.put(adKey, retailerItemCodeList);
			}
		} catch (SQLException exception) {
			logger.error("Error in getActualItems() - " + exception);
			throw new GeneralException("Error in getActualItems()", exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return actualItemsMap;
	}
	
	public HashMap<AdKey, List<String>> getAdplexItems(int locationLevelId, int locationId, int weekCalendarId) throws GeneralException {
		HashMap<AdKey, List<String>> adPlexItemsMap = new HashMap<AdKey, List<String>>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		AdKey adKey = null;
		try {
			stmt = conn.prepareStatement(GET_AD_PLEX_ITEMS);
			int counter = 0;
			stmt.setInt(++counter, weekCalendarId);
			stmt.setInt(++counter, locationLevelId);
			stmt.setInt(++counter, locationId);
			
			rs = stmt.executeQuery();
			while (rs.next()) {
				List<String> retailerItemCodeList= new ArrayList<String>();
				String retailerItemCode;
				adKey = new AdKey(rs.getInt("PAGE_NUMBER"), rs.getInt("BLOCK_NUMBER"));
				if(adPlexItemsMap.containsKey(adKey)){
					retailerItemCodeList = adPlexItemsMap.get(adKey);
				}
				retailerItemCode = rs.getString("RETAILER_ITEM_CODE");
				retailerItemCodeList.add(retailerItemCode);
				adPlexItemsMap.put(adKey, retailerItemCodeList);
			}
		} catch (SQLException exception) {
			logger.error("Error in getAdplexItems() - " + exception);
			throw new GeneralException("Error in getAdplexItems()", exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return adPlexItemsMap;
	}
}
