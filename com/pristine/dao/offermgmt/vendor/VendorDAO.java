package com.pristine.dao.offermgmt.vendor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.VendorFileDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.Constants;
import com.pristine.util.DateUtil;
import com.pristine.util.PristineDBUtil;

public class VendorDAO {
	static Logger logger = Logger.getLogger("VendorDAO");
	
	private Connection conn = null;
	
	private static final String DSD_FLAG = "D";
	private static final int STORE_LEVEL_TYPE_ID = 2;
	
	private static final String SELECT_VENDOR_ID = "select vendor_id from vendor_lookup where ltrim(vendor_number,'0') like ltrim(?,'0')";
	private static final String GET_VENDOR_IDS = "select vendor_id, vendor_number from vendor_lookup where vendor_number in (%s)";
	private static final String INSERT_VENDOR = "insert into vendor_lookup(vendor_id,vendor_name,vendor_number,dist_flag) values (?,?,?,?)";
	private static final String SELECT_COMP_STORE_ID = "select comp_str_id from competitor_store where comp_str_no like ltrim(?,'0') and comp_chain_id = ?";
	private static final String SELECT_ITEM_CODE = "select item_code from item_lookup where ltrim(retailer_item_code,'0') like ltrim(?,'0') and ltrim(upc,'0') like ltrim(?,'0')";
	private static final String UPDATE_STORE_ITEM_MAP = "update store_item_map " +
			"set dist_flag = '" + DSD_FLAG + "' , update_timestamp = sysdate, vendor_id = ? " +
			"where level_type_id = " + STORE_LEVEL_TYPE_ID + " and level_id = ? and item_code = ?";
	
	
	public VendorDAO(Connection conn){
		this.conn = conn;
	}
	
		
	public long getVendorId(String vendorNumber){
		long vendorId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(SELECT_VENDOR_ID);
			int counter = 0;
			stmt.setString(++counter, vendorNumber);
			rs = stmt.executeQuery();
			if(rs.next()){
				vendorId = rs.getInt("VENDOR_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving vendor id - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return vendorId;
	}
	
	public long getCompStoreId(String compStoreNumber, int compChainId){
		long compStoreId = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(SELECT_COMP_STORE_ID);
			int counter = 0;
			stmt.setString(++counter, compStoreNumber);
			stmt.setInt(++counter, compChainId);
			rs = stmt.executeQuery();
			if(rs.next()){
				compStoreId = rs.getInt("COMP_STR_ID");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving COMP_STR_ID - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return compStoreId;
	}
	
	public long getItemCode(String retailerItemCode, String upc ){
		long itemCode = 0;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try{
			stmt = conn.prepareStatement(SELECT_ITEM_CODE);
			int counter = 0;
			stmt.setString(++counter, retailerItemCode);
			stmt.setString(++counter, upc);
			rs = stmt.executeQuery();
			if(rs.next()){
				itemCode = rs.getInt("ITEM_CODE");
			}
		}catch(SQLException exception){
			logger.error("Error when retrieving Item code - " + exception);
		}finally{
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemCode;
	}
	
	
	public boolean insertVendor(VendorFileDTO vendor){
		boolean status = false;
		PreparedStatement stmt = null;
		try{
			StringBuffer sbb = new StringBuffer("SELECT VENDOR_ID_SEQ.NEXTVAL FROM DUAL");	
			CachedRowSet resultcard = PristineDBUtil.executeQuery(conn, sbb, "getMovementsByTransactions fetching card");
			if(resultcard.next()){
				long vendorId = resultcard.getLong("NEXTVAL");
				vendor.setVendorId(vendorId);
			}
			stmt = conn.prepareStatement(INSERT_VENDOR);
			int counter = 0;
			stmt.setLong(++counter, vendor.getVendorId());
			stmt.setString(++counter, vendor.getVendorName());
			stmt.setString(++counter, vendor.getVendorCode());
			stmt.setString(++counter, DSD_FLAG);
			logger.debug("Inserting Vendor  - Code:" + vendor.getVendorCode() + "Vendor Name:" + vendor.getVendorName());
			int count = stmt.executeUpdate();
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error inserting Vendor - Vendor Code:" + vendor.getVendorCode() + "Vendor Name:" + vendor.getVendorName() + exception);
		} catch (GeneralException e) {
			logger.error("Error inserting Vendor - Vendor Code:" + vendor.getVendorCode() + "Vendor Name:" + vendor.getVendorName() + e);
		}finally{
			PristineDBUtil.close(stmt);
		}
		return status;
	}
	
	public HashMap<String, Long> insertVendor(List<VendorFileDTO> vendorList){
		HashMap<String, Long> vendorIdMap = new HashMap<String, Long>();
		PreparedStatement stmt = null;
		int recordCount = 0;
		long vendorId = 0;
		String currentVendor = "";
		String currentVendorName = "";
		try{
		stmt = conn.prepareStatement(INSERT_VENDOR);
		for(VendorFileDTO vendor:vendorList){
			
					recordCount++;
					StringBuffer sbb = new StringBuffer("SELECT VENDOR_ID_SEQ.NEXTVAL FROM DUAL");	
					CachedRowSet resultcard = PristineDBUtil.executeQuery(conn, sbb, "getMovementsByTransactions fetching card");
					if(resultcard.next()){
						vendorId = resultcard.getLong("NEXTVAL");
					}
					
					int counter = 0;
					stmt.setLong(++counter, vendorId);
					stmt.setString(++counter, vendor.getVendorName());
					stmt.setString(++counter, vendor.getVendorCode());
					stmt.setString(++counter, DSD_FLAG);
					logger.debug("Inserting Vendor  - Code:" + vendor.getVendorCode() + "Vendor Name:" + vendor.getVendorName());
					stmt.addBatch();
					if(vendorId > 0)
					vendorIdMap.put(vendor.getVendorCode(), vendorId);
					currentVendor = vendor.getVendorCode();
					currentVendorName = vendor.getVendorName();
					if(recordCount % Constants.LIMIT_COUNT == 0){
						stmt.executeBatch();
						stmt.clearBatch();
						recordCount = 0;
						}
					}
		
				if(recordCount > 0){
					stmt.executeBatch();
					stmt.clearBatch();
					recordCount = 0;
				}
		}catch(SQLException exception){
			logger.error("Error inserting Vendor - Vendor Code:" + currentVendor + "Vendor Name:" + currentVendorName + exception);
		} 
		catch (GeneralException e) {
			logger.error("Error inserting Vendor - Vendor Code:" + currentVendor + "Vendor Name:" + currentVendorName + e);
		}
			finally{
				PristineDBUtil.close(stmt);
			}
		return vendorIdMap;
	}
	
	public HashMap<String, Long> getVendorIdMap(Set<String> vendorNumberSet){
		int limitcount = 0;
		List<String> vendorCodeList = new ArrayList<String>();
		HashMap<String, Long> vendorIdMap = new HashMap<String, Long>();
		try{
			for(String vendorCode: vendorNumberSet){
					vendorCodeList.add(vendorCode);
					limitcount++;
					if( limitcount > 0 && (limitcount% Constants.LIMIT_COUNT== 0)){
						Object[] values = vendorCodeList.toArray();
						retrieveVendorMapping(conn, vendorIdMap, values);
						vendorCodeList.clear();
		            }
				}
				if(vendorCodeList.size() > 0){
					Object[] values = vendorCodeList.toArray();
					retrieveVendorMapping(conn, vendorIdMap, values);
					vendorCodeList.clear();
				}
		}
		catch(GeneralException e){
			logger.error("Error getting vendor map - " + e);
		}
		return vendorIdMap;
	}
	
	public void retrieveVendorMapping(Connection conn, HashMap<String, Long> vendorIdMap, Object[] values) throws GeneralException{
		logger.debug("Inside retrieveItemStoreMapping() of CostDAO");
		PreparedStatement statement = null;
	    ResultSet resultSet = null;
	    try{
			statement = conn.prepareStatement(String.format(GET_VENDOR_IDS, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
	     	resultSet = statement.executeQuery();
	     	String vendorCode = null;
	     	long vendorId = 0;
	     	 while(resultSet.next()){
	     		 vendorId = resultSet.getLong("VENDOR_ID");
	     		 vendorCode = resultSet.getString("VENDOR_NUMBER");
	     		if(!vendorIdMap.containsKey(vendorCode)){
	     			vendorIdMap.put(vendorCode, vendorId);
	        	}
	         }
		}
	    catch(SQLException e){
	    	logger.error("Error getting vendor id mapping - " + e);
	    }
	    finally{
	    	PristineDBUtil.close(resultSet);
	    	PristineDBUtil.close(statement);
	    }
	}
	
	public boolean updateStoreItemMap(long vendorId, long compStoreId, long itemCode){
		boolean status = false;
		PreparedStatement stmt = null;
		
		try{
			stmt = conn.prepareStatement(UPDATE_STORE_ITEM_MAP);
			int counter = 0;
			stmt.setLong(++counter, vendorId);
			stmt.setLong(++counter, compStoreId);
			stmt.setLong(++counter, itemCode);
			logger.info("Updating Store Item Map - CompStoreId:" + compStoreId + ", Item Code:" + itemCode + ", VendorId:" + vendorId );
			int count = stmt.executeUpdate();
			if(count > 0)
				status = true;
		}catch(SQLException exception){
			logger.error("Error updating Store Item Map - CompStoreId:" + compStoreId + ", Item Code:" + itemCode + ", VendorId:" + vendorId + exception);
		}finally{
			PristineDBUtil.close(stmt);
		}
		return status;
	}
	
}
