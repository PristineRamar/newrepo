/*
 * Title: Shipping Info DAO
 *
 *******************************************************************************
 * Modification History
 *------------------------------------------------------------------------------
 * Version		Date		Name			Remarks
 *------------------------------------------------------------------------------
 * Version 0.1	04/09/2013	Ganapathy		New Creation
 *******************************************************************************
 */
package com.pristine.dao;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.ShippingInfoDTO;
import com.pristine.dto.SummaryGoalDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class ShippingInfoDAO implements IDAO {
	

	private static Logger logger = Logger
			.getLogger("ShippingInfoDAO");
	
	
	/*
	 *****************************************************************
	 * Method to delete shipping info
	 * Argument 1 : DB connection
	 * Argument 2 : Shipping info DTO
	 * Return	  : void
	 * @throws Exception
	 * ****************************************************************
	 */	
	public void deleteShippingInfo(Connection conn, ShippingInfoDTO shippingInfoDTO) 
													throws GeneralException {
		StringBuffer sb = new StringBuffer();
		//logger.debug(sb.toString());
		try {
			PristineDBUtil.execute(conn, sb, "deleteShippingInfo");
			//logger.debug("DELETE");	
		} 
		catch (GeneralException e) {
			logger.error(e);
		}
	}
	
	
	public void insertShippingInfoWrap(Connection conn, List<ShippingInfoDTO> sIL) 
			throws GeneralException {
		try {
			
			Collections.sort(sIL);
			List<ShippingInfoDTO> newSIL = new ArrayList<ShippingInfoDTO>();
			java.util.Date shippingDate = null;
			for(ShippingInfoDTO sI: sIL){
				if (shippingDate == null){
					newSIL.add(sI);
					shippingDate = sI.getShipmentDate();
					continue;
				}
				if(shippingDate.equals(sI.getShipmentDate())){
					newSIL.add(sI);
					continue;
				}
				insertShippingInfo(conn,newSIL);
				newSIL = new ArrayList<ShippingInfoDTO>();
				newSIL.add(sI);
				shippingDate = sI.getShipmentDate();
			}
			insertShippingInfo(conn,newSIL);
			
			
			
		}		catch (Exception e) {
			logger.error(e);
			throw new GeneralException(e.getMessage());
		}

	}
	
	/*
	 *****************************************************************
	 * Method to insert shipping info
	 * Argument 1 : DB connection
	 * Argument 2 : Shipping Info DTO
	 * Return	  : void
	 * @throws Exception
	 * ****************************************************************
	 */	
	public void insertShippingInfo(Connection conn, List<ShippingInfoDTO> sIL) 
													throws GeneralException {

		PreparedStatement pstmt= null;
		
		StringBuffer sb = new StringBuffer();

		
		try {

			// DELETE
			sb = new StringBuffer();

			Collections.sort(sIL);
			
			sb.append(" DELETE FROM SHIPPING_INFO WHERE INVOICE_DATE = ? AND SHIPMENT_DATE = ? AND ITEM_CODE = ? AND STORE_ID = ?");

			pstmt=conn.prepareStatement(sb.toString());
			for(ShippingInfoDTO sI: sIL){
				int counter=0;	
				pstmt.setDate(++counter, new java.sql.Date(sI.getInvoiceDate().getTime()));	
				pstmt.setDate(++counter,new java.sql.Date(sI.getShipmentDate().getTime()));
				pstmt.setInt(++counter,sI.getItemCode());	
				pstmt.setInt(++counter,sI.getStoreID());	

				pstmt.addBatch();

			}
			
			logger.debug("Begin Execute delete from db");
	    	int[] c = pstmt.executeBatch();
//	    	PristineDBUtil.commitTransaction(conn, "Shipping Info Delete Old Records");
	    	pstmt.clearBatch();
	    	pstmt.close();
			logger.debug("End Execute Delete from db");
			if(c!=null && c.length>0){
			logger.debug("The number of records deleted: "+c[0]);
			}else{
				logger.debug("The number of records deleted: 0");

			}


			// UPDATE
			sb = new StringBuffer();

			sb.append(" UPDATE SHIPPING_INFO SET LATEST_IND = 'N'  WHERE ITEM_CODE = ? AND STORE_ID = ? AND SHIPMENT_DATE <= ?");

			pstmt=conn.prepareStatement(sb.toString());
			for(ShippingInfoDTO sI: sIL){
				int counter=0;	
				pstmt.setInt(++counter,sI.getItemCode());	
				pstmt.setInt(++counter,sI.getStoreID());	
				pstmt.setDate(++counter,new java.sql.Date(sI.getShipmentDate().getTime()));
				if(sI.getItemCode()==19471){
						logger.info(sI.getShipmentDate() +":"+sI.getShipmentDate().getTime());
				}
				pstmt.addBatch();

			}
			
			logger.debug("Begin Execute update db");
	    	c = pstmt.executeBatch();
	    	pstmt.clearBatch();
	    	pstmt.close();
			logger.debug("End Execute update from db");
			if(c!=null && c.length>0){
			logger.debug("The number of records updated : "+c[0]);
			}else{
				logger.debug("The number of records updated: 0");

			}


			// INSERT
			sb = new StringBuffer();

			sb.append(" INSERT INTO SHIPPING_INFO (INVOICE_DATE, SHIPMENT_DATE, ITEM_CODE, ITEM_DESC, STORE_ID" +
					",RETAILER_ITEM_CODE,UPC,STORE_PACK,CASES,QUANTITY,LATEST_IND");
			sb.append("  ) VALUES(?,?,?,?,?,?,?,?,?,?,?)");

			pstmt=conn.prepareStatement(sb.toString());
			for(ShippingInfoDTO sI: sIL){
				int counter=0;	
				java.sql.Date sDate = new java.sql.Date(sI.getInvoiceDate().getTime());
				pstmt.setDate(++counter, new java.sql.Date(sI.getInvoiceDate().getTime()));	
				pstmt.setDate(++counter,new java.sql.Date(sI.getShipmentDate().getTime()));
				pstmt.setInt(++counter,sI.getItemCode());	
				pstmt.setString(++counter,sI.getItemDesc());
				pstmt.setInt(++counter,sI.getStoreID());	
				pstmt.setString(++counter,sI.getRetailerCode());
				pstmt.setString(++counter,sI.getUPC());
				pstmt.setInt(++counter,sI.getStorePack());	
				pstmt.setInt(++counter,sI.getCasesShipped());	
				pstmt.setInt(++counter,sI.getQuantity());	
				pstmt.setString(++counter,"Y");

				pstmt.addBatch();

			}
			
			logger.debug("Begin Execute insert into db");
			logger.debug("For Date:"+sIL.get(0).getShipmentDate());
	    	c = pstmt.executeBatch();
	    	PristineDBUtil.commitTransaction(conn, "Shipping Info Insert");
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
		}
	}
}
