package com.pristine.dao;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.ItemGroupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class ItemGroupDAO {

	private static Logger logger = Logger.getLogger("ItemGroupDAO");

	private static String UNKNOWN_NAME = "UNKNOWN - ";
	HashMap<String, Integer> deptIdandNameMap;
	HashMap<String, Integer> deptIdandCodeMap;
	HashMap<String, Integer> categoryIdandNameMap;
	HashMap<String, Integer> categoryIdandCodeMap;
	HashMap<String, Integer> subCategoryIdandNameMap;
	HashMap<String, Integer> subCategoryIdandCodeMap;
	HashMap<String, Integer> segmentIdandCodeMap;

	public void init(Connection conn) throws GeneralException {
		boolean convertToUppercase = true;
		boolean dontConvertToUppercase = false;
		ItemLoaderDAO itemLoaderDAO = new ItemLoaderDAO();
		deptIdandNameMap = itemLoaderDAO.getTwoColumnsCache(conn, "ID", "NAME", "DEPARTMENT", convertToUppercase);
		deptIdandCodeMap = itemLoaderDAO.getTwoColumnsCache(conn, "ID", "DEPT_CODE", "DEPARTMENT",
				dontConvertToUppercase);
		categoryIdandNameMap = itemLoaderDAO.getThreeColumnsCache(conn, "ID", "DEPT_ID", "NAME", "CATEGORY");
		categoryIdandCodeMap = itemLoaderDAO.getThreeColumnsCache(conn, "ID", "DEPT_ID","CAT_CODE", "CATEGORY");
		subCategoryIdandNameMap = itemLoaderDAO.getThreeColumnsCache(conn, "ID", "CATEGORY_ID", "NAME", "SUB_CATEGORY");
		subCategoryIdandCodeMap = itemLoaderDAO.getThreeColumnsCache(conn, "ID", "CATEGORY_ID", "SUB_CAT_CODE", "SUB_CATEGORY");
		segmentIdandCodeMap = itemLoaderDAO.getTwoColumnsCache(conn, "ID", "SEG_NUM", "ITEM_SEGMENT",
				dontConvertToUppercase);
	}

	public int populateDept(Connection conn, ItemGroupDTO itemGrp) throws GeneralException {
		long ustartTime = System.currentTimeMillis();
		StringBuffer sb;
		String deptIDStr = null;
		sb = new StringBuffer();
		String deptName = itemGrp.deptName;
		String deptCode = itemGrp.deptCode;

		sb.append(" SELECT ID FROM DEPARTMENT WHERE ");
		
		if(itemGrp.deptCode == "" && deptName == "")
		{
			itemGrp.deptCode = "UNK_CODE";
		}
		
		if (itemGrp.deptCode != null && !itemGrp.deptCode.equals("")) {
			sb.append(" DEPT_CODE = '" + itemGrp.deptCode + "'");
		}
		else if (deptName != null && !deptName.equals("")) {
			sb.append(" UPPER(NAME) = '" + deptName.trim().toUpperCase() + "'");
		}
		// To avoid db call using hash map values by passing key values.
		if ((deptIdandCodeMap != null || deptIdandNameMap != null)) {

			if (itemGrp.deptCode != null && !itemGrp.deptCode.equals("")) {
				if (deptIdandCodeMap.get(deptCode) != null) {
					deptIDStr = String.valueOf(deptIdandCodeMap.get(deptCode));
				}
			}
			else if (deptName != null && !deptName.equals("")) {
					if (deptIdandNameMap.get(deptName.trim().toUpperCase()) != null) {
						deptIDStr = String.valueOf(deptIdandNameMap.get(deptName.trim().toUpperCase()));
					}
			
			}
		}
		// If hash map values are null then existing code will be used
		else if (deptCode != null && !deptCode.equals("") || deptName != null && !deptName.equals("")) {
			
			deptIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item - getDeptID");
		} else {
			itemGrp.isEmptyDepartment = true;
			return -1;
		}

		if (deptIDStr == null) {
			if (deptName == null || deptName.equals(""))
				itemGrp.deptName = UNKNOWN_NAME + itemGrp.deptCode;

			// Call Insert Dept here
			StringBuffer deptInsert = new StringBuffer();
			deptInsert.append("INSERT INTO DEPARTMENT( id, name, analyze_flag, dept_code) values (deptcode_seq.nextval,");
			deptInsert.append("'").append(itemGrp.deptName.trim().toUpperCase()).append("'");
			deptInsert.append(", 'Y',");
			if (itemGrp.deptCode != null && !itemGrp.deptCode.equals(""))
				deptInsert.append("'").append(itemGrp.deptCode).append("'");
			else
				deptInsert.append("NULL");
			deptInsert.append(")");
			// logger.debug(deptInsert.toString());
			PristineDBUtil.execute(conn, deptInsert, "ItemGrpDAO - Dept Insert");
			deptIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert dept - populateDept");
			if (deptIDStr != null) {
				if (deptIdandCodeMap != null && (itemGrp.deptCode != null && !itemGrp.deptCode.equals(""))) {
					deptIdandCodeMap.put(itemGrp.deptCode, Integer.parseInt(deptIDStr));
				} 
				if (deptIdandNameMap != null && deptName != null && deptIDStr != null) {
					deptIdandNameMap.put(deptName.trim().toUpperCase(), Integer.parseInt(deptIDStr));
				}
			}
		}
		if (deptIDStr == null) {
			logger.debug("values is null populateDept");
			throw new GeneralException("Invalid Department");
		}
		long uendTime = System.currentTimeMillis();
		logger.debug("Time taken to complete department : " + (uendTime - ustartTime));
		return Integer.parseInt(deptIDStr);

	}

	public int populateCategory(Connection conn, ItemGroupDTO itemGrp) throws GeneralException {
		
		long ustartTime = System.currentTimeMillis();
		StringBuffer sb = new StringBuffer();	
		String catIDStr = null;
		sb.append(" SELECT ID FROM CATEGORY WHERE ");
		String categoryName = itemGrp.catName;
	/*	if( categoryName !=null && !categoryName.equals("")){
			if( itemGrp.useCatCode)
				sb.append(" CAT_CODE = '" + itemGrp.catCode+  "'");
			else 
				sb.append(" UPPER(NAME) = '" + categoryName.trim().toUpperCase()+  "'");
			
		}*/
		if(itemGrp.catCode != null &&  !itemGrp.catCode.equals(""))
			sb.append(" CAT_CODE = '" + itemGrp.catCode+  "'");
		else if( categoryName !=null && !categoryName.equals(""))
			sb.append(" UPPER(NAME) = '" + categoryName.trim().toUpperCase()+  "'");
		else{
			// Supress exception
			//throw new GeneralException("Invalid Category");
			itemGrp.isEmptyCategory = true;
			return -1;
		}

		sb.append(" AND DEPT_ID = ").append(itemGrp.deptId);
		if(categoryIdandCodeMap !=null || categoryIdandNameMap != null){
			logger.debug("Inside the cache block in category");
			if(itemGrp.catCode != null &&  !itemGrp.catCode.equals("")){
				String keyvalue = itemGrp.deptId+"-"+itemGrp.catCode;
				if(categoryIdandCodeMap.get(keyvalue) != null){
					catIDStr = String.valueOf(categoryIdandCodeMap.get(keyvalue));
				}
			}
			else if(categoryName !=null && !categoryName.equals("")){
					String keyvalue = itemGrp.deptId+"-"+categoryName;
					if(categoryIdandNameMap.get(keyvalue)!= null){
						catIDStr = String.valueOf(categoryIdandNameMap.get(keyvalue));
					}
				}
			}
			
		else{
			logger.debug("Inside the normal function");
			catIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item - getCategoryID");
		}
		if( catIDStr == null){
			if (categoryName == null || categoryName.equals("") )
				itemGrp.catName = UNKNOWN_NAME + itemGrp.catCode;
				
			//Create new Category
			StringBuffer catgInsert = new StringBuffer();
			catgInsert.append("INSERT INTO CATEGORY( id, name, cat_code, dept_id) values (category_seq.nextval,");
			catgInsert.append("'").append(itemGrp.catName.trim().toUpperCase()).append("',");
			if(itemGrp.catCode != null &&  !itemGrp.catCode.equals(""))
				catgInsert.append("'").append(itemGrp.catCode).append("', ");
			else
				catgInsert.append("NULL, ");
			catgInsert.append(itemGrp.deptId).append(")");
			//logger.debug(catgInsert.toString());
			PristineDBUtil.execute(conn, catgInsert, "ItemGrpDAO - Category Insert");
			catIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Category - populate Category");
			if(catIDStr != null){
				if(categoryIdandCodeMap != null){
					String keyvalue = itemGrp.deptId+"-"+itemGrp.catCode;
					categoryIdandCodeMap.put(keyvalue, Integer.parseInt(catIDStr));
				}
				if(categoryIdandNameMap != null){
					String keyvalue = itemGrp.deptId+"-"+categoryName.trim().toUpperCase();
					categoryIdandCodeMap.put(keyvalue, Integer.parseInt(catIDStr));
				}
			}
		}
		if(  catIDStr == null){
			throw new GeneralException("Invalid Category");
		}
		long uendTime = System.currentTimeMillis();
		logger.debug("Time taken to complete category : " + (uendTime - ustartTime));
		return Integer.parseInt(catIDStr);		
	}

	public int populateSubCategory(Connection conn, ItemGroupDTO itemGrp)
			throws GeneralException{
		long ustartTime = System.currentTimeMillis();
		String subCatIDStr = null;
		StringBuffer sb = new StringBuffer();
		String subCategoryName = itemGrp.subCatName;
		sb.append(" SELECT ID FROM SUB_CATEGORY WHERE ");
		if (itemGrp.subCatCode != null && !itemGrp.subCatCode.equals(""))
			sb.append(" SUB_CAT_CODE = '" + itemGrp.subCatCode + "'");
		else if (subCategoryName != null && !subCategoryName.equals(""))
			sb.append(" UPPER(NAME) = '" + subCategoryName.trim().toUpperCase() + "'");
		else {
			itemGrp.isEmptySubCategory = true;
			return -1;
		}
		sb.append(" AND CATEGORY_ID = ").append(itemGrp.catId);

		// Using Hash map values to avoid db call if values existed
		if ((subCategoryIdandCodeMap != null || subCategoryIdandNameMap != null)
				&& (itemGrp.subCatCode != null && !itemGrp.subCatCode.equals("")
						|| subCategoryName != null && !subCategoryName.equals(""))) {

			if (itemGrp.subCatCode != null && !itemGrp.subCatCode.equals("")) {
				String keyValue = itemGrp.catId + "-" + itemGrp.subCatCode;
				if (subCategoryIdandCodeMap.get(keyValue) != null) {
					subCatIDStr = String.valueOf(subCategoryIdandCodeMap.get(keyValue));
				}
			} else if (subCategoryName != null && !subCategoryName.equals("")) {
				String keyValue = itemGrp.catId + "-" + subCategoryName.trim().toUpperCase();
				if (subCategoryIdandNameMap.get(keyValue) != null) {
					subCatIDStr = String.valueOf(subCategoryIdandNameMap.get(keyValue));
				}

			}

		}
		// If hash map values are null then existing work flow will continue to generate values
		else{
			subCatIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item Grp - getSubCategoryID");
		}

		if (subCatIDStr == null) {
			if (subCategoryName == null || subCategoryName.equals(""))
				itemGrp.subCatName = UNKNOWN_NAME + itemGrp.subCatCode;
			// Create new Category
			StringBuffer subCatgInsert = new StringBuffer();
			subCatgInsert.append("INSERT INTO SUB_CATEGORY( id, name, sub_cat_code, category_id) values (sub_category_seq.nextval,");
			subCatgInsert.append("'").append(itemGrp.subCatName.trim().toUpperCase()).append("',");
			if (itemGrp.subCatCode != null && !itemGrp.subCatCode.equals(""))
				subCatgInsert.append("'").append(itemGrp.subCatCode).append("', ");
			else
				subCatgInsert.append("NULL, ");
			subCatgInsert.append(itemGrp.catId).append(")");
			PristineDBUtil.execute(conn, subCatgInsert, "ItemGrpDAO - Sub Category Insert");
			subCatIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert sub Category - populate Sub cat");
			if (subCatIDStr != null) {
				if (subCategoryIdandCodeMap != null && (itemGrp.subCatCode != null && !itemGrp.subCatCode.equals(""))) {
					String keyValue = itemGrp.catId + "-" + itemGrp.subCatCode;
					subCategoryIdandCodeMap.put(keyValue, Integer.parseInt(subCatIDStr));
				} 
				if (subCategoryIdandNameMap != null
						&& (subCategoryName != null && !subCategoryName.equals(""))) {
					String keyValue = itemGrp.catId + "-" + subCategoryName.trim().toUpperCase();
					subCategoryIdandNameMap.put(keyValue, Integer.parseInt(subCatIDStr));

				}
			}
		}
		if (subCatIDStr == null) {
			throw new GeneralException("Invalid Sub Category");
		}
		long uendTime = System.currentTimeMillis();
		logger.debug("Time taken to to complete Sub_category : " + (uendTime - ustartTime));
		return Integer.parseInt(subCatIDStr);
	}

	public int populateSegment(Connection conn, ItemGroupDTO itemGrp) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		String segmentIDStr = null;

		sb.append(" SELECT ID FROM ITEM_SEGMENT WHERE SEG_NUM = ");
		sb.append("'" + itemGrp.segmentCode.trim().toUpperCase() + "' ");
		if (segmentIdandCodeMap != null) {
			if (segmentIdandCodeMap.get(itemGrp.segmentCode) != null) {
				segmentIDStr = String.valueOf(segmentIdandCodeMap.get(itemGrp.segmentCode));
			}
		} else {
			segmentIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item Grp - getSegmentID");
		}
		if (segmentIDStr == null) {
			// Create new Category
			StringBuffer segInsert = new StringBuffer();
			segInsert.append("INSERT INTO ITEM_SEGMENT( id, name, seg_num, dept_id, category_id, sub_category_id ) ");
			segInsert.append("values (item_segment_seq.nextval, ");
			segInsert.append("'").append(itemGrp.segmentName.trim().toUpperCase()).append("',");
			segInsert.append("'").append(itemGrp.segmentCode).append("', ");
			segInsert.append(itemGrp.deptId).append(", ");
			segInsert.append(itemGrp.catId).append(", ");
			segInsert.append(itemGrp.subCatId).append(" ) ");
			// logger.debug(segInsert.toString());
			PristineDBUtil.execute(conn, segInsert, "ItemGrpDAO - Segment Insert");
			segmentIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert segement - populateSegment");
			if (segmentIDStr != null && segmentIdandCodeMap != null) {
				segmentIdandCodeMap.put(itemGrp.segmentCode.trim().toUpperCase(), Integer.parseInt(segmentIDStr));
			}
		}
		if (segmentIDStr == null) {
			throw new GeneralException("Invalid Segment");
		}
		return Integer.parseInt(segmentIDStr);
	}

	public boolean fillIDValues(Connection conn, ItemGroupDTO itemGrp) throws GeneralException {

		boolean recordPresent = false;
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ID, DEPT_ID, CATEGORY_ID, SUB_CATEGORY_ID FROM ITEM_SEGMENT WHERE SEG_NUM = ");
		sb.append("'" + itemGrp.segmentCode.trim().toUpperCase() + "' ");

		CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "ITEMDAO - getItemListFromSchedule");
		try {
			if (crs.next()) {
				itemGrp.segId = crs.getInt("ID");
				itemGrp.deptId = crs.getInt("DEPT_ID");
				itemGrp.catId = crs.getInt("CATEGORY_ID");
				itemGrp.subCatId = crs.getInt("SUB_CATEGORY_ID");
				recordPresent = true;
			}
		} catch (SQLException sqlce) {
			throw new GeneralException("Error in accessing CachedRowset", sqlce);
		}
		return recordPresent;

	}

	public int getDepartmentId(Connection conn, String deptCode) throws GeneralException {
		StringBuffer sb;
		sb = new StringBuffer();
		sb.append(" SELECT ID FROM DEPARTMENT WHERE DEPT_CODE = ");
		sb.append("'" + deptCode + "'");
		String deptIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item - getDeptID");
		int retVal = -1;
		if (deptIDStr != null) {
			retVal = Integer.parseInt(deptIDStr);
		}
		return retVal;
	}

	public int getCategoryId(Connection conn, int deptId, String categoryCode) throws GeneralException {
		StringBuffer sb;
		sb = new StringBuffer();
		sb.append(" SELECT ID FROM CATEGORY WHERE CAT_CODE = ");
		sb.append("'" + categoryCode + "'");
		sb.append(" AND DEPT_ID = " + deptId);
		String catIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item - getDeptID");
		int retVal = -1;
		if (catIDStr != null) {
			retVal = Integer.parseInt(catIDStr);
		}
		return retVal;
	}

	public void updateCategory(Connection conn, int deptId, int categoryId, String catName, String catCode)
			throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append(" UPDATE CATEGORY SET ");
		sb.append("CAT_CODE = '").append(catCode).append("',");
		sb.append("dept_id = ").append(deptId).append(",");
		sb.append("NAME = '").append(catName.trim()).append("'");
		;
		sb.append(" WHERE ID = ").append(categoryId);
		PristineDBUtil.executeUpdate(conn, sb, "Category-Update");
	}

	public void updateDepartment(Connection conn, int deptId, String deptName, String deptCode)
			throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" UPDATE DEPARTMENT SET ");
		sb.append("DEPT_CODE = '").append(deptCode).append("',");
		sb.append("NAME = '").append(deptName.trim()).append("'");
		;
		sb.append(" WHERE ID = ").append(deptId);

		PristineDBUtil.executeUpdate(conn, sb, "Dept-Update");

	}

}
