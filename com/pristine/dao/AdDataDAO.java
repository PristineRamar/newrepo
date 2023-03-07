/*
 * Title: Ad Data DAO
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	05/23/2013	Ganapathy		New Creation
 *******************************************************************************
 */
package com.pristine.dao;

import java.sql.Connection;
//import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.HashMap;
import java.util.List;

//import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.AdDataDTO;
//import com.pristine.dto.ShippingInfoDTO;
//import com.pristine.dto.SummaryGoalDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class AdDataDAO implements IDAO {
	

	private static Logger logger = Logger
			.getLogger("AdDataDAO");
	
	
	/*
	 *****************************************************************
	 * Method to delete shipping info
	 * Argument 1 : DB connection
	 * Argument 2 : Shipping info DTO
	 * Return	  : void
	 * @throws Exception
	 * ****************************************************************
	 */	
	public void deleteAdData(Connection conn, int calendarId) 
													throws GeneralException {
		StringBuffer sbA = new StringBuffer("delete from ad_info_item_list where ad_info_id in (select ad_info_id from ad_info where calendar_id = "+calendarId+" )");
		StringBuffer sbB = new StringBuffer("delete from ad_info where calendar_id = "+calendarId);
		try {
			PristineDBUtil.execute(conn, sbA, "deleteAdInfo");
			PristineDBUtil.execute(conn, sbB, "deleteAdInfo");
			//logger.debug("DELETE");	
		} 
		catch (GeneralException e) {
			logger.error(e);
		}
	}
	

	public void expandAdDataToLIG(Connection conn, int calendarId) 
			throws GeneralException {
		StringBuffer sbA = new StringBuffer("insert into ad_info_item_list select ad_info_id, item_code from " +
				"(select tabAD.ad_info_id,tabAD.presto_item_code, tabAD.ret_lir_id, tabAD.category_id, il.item_code, il.item_name , tabAD.item_name from " +
				"(select w.ad_info_id, w.presto_item_code, i.ret_lir_id, i.category_id, i.item_name " +
				"from ad_info w, item_lookup i where w.presto_item_code = i.item_code and ret_lir_id is not null and  calendar_id = "+calendarId+") tabAD, item_lookup il " +
				"where tabAD.ret_lir_id = il.ret_lir_id and tabAD.category_id = il.category_id " +
				"and tabAD.presto_item_code <> il.item_code and il.item_name like concat(substr(tabAD.item_name,1,instr(tabAD.item_name,' ',1,1)),'%'))");
		
		StringBuffer sbB = new StringBuffer("delete from ad_info_item_list where item_code in (select item_code from " +
				"(select item_code, count(ad_info_id) cc from ad_info_item_list where ad_info_id " +
				"in (select ad_info_id from ad_info where calendar_id = "+calendarId+") group by item_code) where cc > 1)");

		StringBuffer sbC = new StringBuffer("delete from ad_info_item_list where item_code in (select presto_item_code from ad_info where calendar_id = "+calendarId+")");

		try {
			PristineDBUtil.execute(conn, sbA, "expandAdInfo");
			PristineDBUtil.execute(conn, sbB, "expandAdInfo");
			PristineDBUtil.execute(conn, sbC, "expandAdInfo");
		} 
		catch (GeneralException e) {
			logger.error("Error expanding LIG for Ad Info:"+e.getMessage());
			e.printStackTrace();
			deleteAdData(conn, calendarId);
		}
}

	
	public void insertAdData(Connection conn, List<AdDataDTO> aDL) 
													throws GeneralException {

		PreparedStatement pstmt= null;
		
		StringBuffer sb = new StringBuffer();

		
		try {


			// INSERT
			sb = new StringBuffer();
			sb.append(" INSERT INTO AD_INFO (AD_INFO_ID," +
					"PAGE,BLOCK,RETAILER_ITEM_CODE,ITEM_DESCRIPTION,STORE_LOCATION,CATEGORY,AD_LOCATION,CASE_PACK,CASE_COST," +
					"UNIT_COST,REG_PRICE,MARGIN_PCT,ON_TPR,OFF_INVOICE_COST,BILLBACK,SCAN,NET_UNIT_COST,ORG_UNIT_AD_PRICE,ORG_AD_RETAIL,ORIGINAL_NET_SALES,ORIGINAL_GP," +
					"ORIGINAL_MARGIN,ADJ_UNIT_AD_PRICE,ADJ_AD_RETAIL,ADJ_NET_SALES,ADJ_GP,ADJ_MARGIN,ORG_UNITS,ADJ_UNITS,NO_OF_CASES,DA,DISPLAY_TYPE,LOCATION_LEVEL_ID," +
					"LOCATION_ID,CALENDAR_ID,PRESTO_ITEM_CODE) " +
					"VALUES (AD_INFO_SEQ.NEXTVAL,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
//			conn.setAutoCommit(false);
			pstmt=conn.prepareStatement(sb.toString());
			for(AdDataDTO aD: aDL){
				int counter=0;	
				pstmt.setString(++counter,aD.getPage());
				pstmt.setString(++counter,aD.getBlock());
				pstmt.setString(++counter,aD.getRetailerItemCode());
				pstmt.setString(++counter,aD.getItemDescription());
				pstmt.setString(++counter,aD.getStoreLocation());
				pstmt.setString(++counter,aD.getCategory());
				pstmt.setString(++counter,aD.getAdLocation());
				pstmt.setInt(++counter,aD.getCasePack());
				pstmt.setDouble(++counter,aD.getCaseCost());
				pstmt.setDouble(++counter,aD.getUnitCost());
				pstmt.setDouble(++counter,aD.getRegPrice());
				pstmt.setDouble(++counter,aD.getMarginPct());
				pstmt.setString(++counter,aD.getOnTpr());
				pstmt.setDouble(++counter,aD.getOffInvoiceCost());
				pstmt.setDouble(++counter,aD.getBillback());
				pstmt.setDouble(++counter,aD.getScan());
				pstmt.setDouble(++counter,aD.getNetUnitCost());
				pstmt.setDouble(++counter,aD.getOrgUnitAdPrice());
				pstmt.setString(++counter,aD.getOrgAdRetail());
				pstmt.setDouble(++counter,aD.getOriginalNetSales());
				pstmt.setDouble(++counter,aD.getOriginalGP());
				pstmt.setDouble(++counter,aD.getOriginalMargin());
				pstmt.setDouble(++counter,aD.getAdjUnitAdPrice());
				pstmt.setString(++counter,aD.getAdjAdRetail());
				pstmt.setDouble(++counter,aD.getAdjNetSales());
				pstmt.setDouble(++counter,aD.getAdjGP());
				pstmt.setDouble(++counter,aD.getAdjMargin());
				pstmt.setInt(++counter,aD.getOrgUnits());
				pstmt.setInt(++counter,aD.getAdjUnits());
				pstmt.setDouble(++counter,aD.getNoOfCases());
				pstmt.setDouble(++counter,aD.getDa());
				pstmt.setString(++counter,aD.getDisplayType());
				pstmt.setInt(++counter,aD.getLocationLevelId());
				pstmt.setInt(++counter,aD.getLocationId());
				pstmt.setInt(++counter,aD.getCalendarId());
				pstmt.setInt(++counter,aD.getPrestoItemCode());

				pstmt.addBatch();

			}
			
			logger.debug("Begin Execute insert into db");
	    	int[] c = pstmt.executeBatch();
	    	PristineDBUtil.commitTransaction(conn, "Ad Data Insert");
	    	pstmt.clearBatch();
	    	pstmt.close();
			logger.debug("End Execute insert into db");
			if(c!=null && c.length>0){
			logger.debug("The number of records inserted: "+c[0]);
			}else{
				logger.debug("The number of records inserted: 0");

			}
			
		} 
		catch (Exception e) {
			logger.error(e);
			try {
				conn.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		finally{
	    	try {
				pstmt.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
}
