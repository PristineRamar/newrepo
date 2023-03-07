package com.pristine.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;

import com.pristine.dto.ItemDTO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.ItemGroupDTO;
import com.pristine.dto.ItemMappingDTO;
import com.pristine.dto.PriceGroupAndCategoryKey;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.RetailerLikeItemGroupDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.ProductService;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.util.offermgmt.PRCommonUtil;
//import com.pristine.util.offermgmt.PRConstants;
import com.pristine.dto.SizeGroupRange;
import com.pristine.dto.UOMDataDTO;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleItemDTO;
import com.pristine.dto.riteaid.RAItemInfoDTO;
import com.pristine.dto.salesanalysis.ProductDTO;
import com.sun.rowset.CachedRowSetImpl;
@SuppressWarnings({ "unused", "static-access", "resource" })
public class ItemDAO implements IDAO {

	static Logger logger = Logger.getLogger("com.pristine.dao.ItemDAO");
	ItemLoaderDAO itemLoaderDAO = new ItemLoaderDAO();
	HashMap<String, Integer> retailerIdandNameMap;
	HashMap<String, Integer> retailerIdandCodeMap;
	HashMap<String, Integer> brandIDandNameMap;
	HashMap<String, Integer> manufacturerIDandNameMap;
	List<ItemDTO> updateItemLookupList;
	HashMap<String, Integer> itemDetailsForAllItemsWithCheckRetItemCodeMap;
	HashMap<String, Integer> itemCodeandUPCMap;
	HashMap<String, Integer> itemCodeandStandardUPCMap;
	HashMap<String, Integer> retLirIDwithUPCandRetItemCodeMap;
	HashMap<String, Integer> retLirIDwithUPC;
	ItemGroupDAO itemGrpDAO = new ItemGroupDAO();

	HashMap<String, Integer> _productIdMap = new HashMap<String, Integer>();
	private ArrayList<Integer> retLirIdList = new ArrayList<Integer>(); // Changes
																		// to
																		// fill
																		// gaps
																		// in
																		// RET_LIR_SEQ
	private ArrayList<Integer> missingItemCodeList = new ArrayList<Integer>(); // Changes
																				// to
																				// fill
																				// gaps
																				// in
																				// ITEM_CODE_SEQ
	private int missingItemCounter = 0; // Changes to fill gaps in ITEM_CODE_SEQ
	private int retLirIdStart = 0;

	public boolean ignoreDeptShortName = false;

	private static final int commitCount = Constants.LIMIT_COUNT;
	private static final String GET_UPC = "SELECT RETAILER_ITEM_CODE, ITEM_CODE FROM RETAILER_ITEM_CODE_MAP WHERE RETAILER_ITEM_CODE IN (%s) AND ACTIVE_INDICATOR = 'Y'";
	private static final String GET_UPC_FOR_REATILER_ITEM_CODE = "SELECT RETAILER_ITEM_CODE, UPC FROM ITEM_LOOKUP WHERE RETAILER_ITEM_CODE IN (%s)";
	private static final String GET_ITEM_CODE = "SELECT RETAILER_ITEM_CODE, ITEM_CODE FROM RETAILER_ITEM_CODE_MAP WHERE RETAILER_ITEM_CODE IN (%s)";
	private static final String GET_ITEM_CODE_CACHE = "SELECT RETAILER_ITEM_CODE, ITEM_CODE FROM RETAILER_ITEM_CODE_MAP";
	private static final String GET_RETAILER_ITEM_CODE_CACHE = "SELECT RETAILER_ITEM_CODE, ITEM_CODE FROM ITEM_LOOKUP WHERE LIR_IND = 'N'";
	private static final String GET_ITEMS = "SELECT UPC, DEPT_ID FROM ITEM_LOOKUP";
	private static final String GET_LIR_INFO = "SELECT RET_LIR_ID, RET_LIR_CODE, RET_LIR_NAME, RET_LIR_ITEM_CODE FROM RETAILER_LIKE_ITEM_GROUP WHERE RET_LIR_ID IN (%s)";

	private static final String GET_BRAND_ID = "SELECT BRAND_ID FROM BRAND_LOOKUP WHERE UPPER(BRAND_NAME) = UPPER(?)";
	private static final String INSERT_BRAND = "INSERT INTO BRAND_LOOKUP (BRAND_ID, BRAND_NAME, BRAND_CODE) VALUES (BRAND_ID_SEQ.NEXTVAL, ?, ?)";
	private static final String GET_MANUFACTURER_ID = "SELECT ID FROM MANUFACTURER WHERE UPPER(NAME) = UPPER(?)";
	private static final String INSERT_MANUFACTURER = "INSERT INTO MANUFACTURER (ID, NAME, DESCRIPTION) VALUES (MANUFACTURER_ID_SEQ.NEXTVAL, ?, ?)";
	private static final String GET_LIG_NAME_FOR_UPC = "SELECT RET.RET_LIR_NAME FROM ITEM_LOOKUP IL, RETAILER_LIKE_ITEM_GROUP RET "
			+ "WHERE RET.RET_LIR_ID = IL.RET_LIR_ID AND IL.UPC = '?'";
	private static final String GET_MISSING_ITEM_CODES = "SELECT MIN_ITEM_CODE - 1 + LEVEL MISSING_ITEM_CODE "
			+ "FROM ( SELECT MIN(ITEM_CODE) MIN_ITEM_CODE " + ", MAX(ITEM_CODE) MAX_ITEM_CODE " + "FROM ITEM_LOOKUP WHERE ITEM_CODE < ? " + ") "
			+ "CONNECT BY LEVEL <= MAX_ITEM_CODE - MIN_ITEM_CODE + 1 " + "MINUS " + "SELECT ITEM_CODE " + "FROM ITEM_LOOKUP WHERE ITEM_CODE < ?";
	private static final String GET_ITEM_CODE_FOR_UPC_RETAILER_ITEM_CODE = "SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE UPC = ? AND RETAILER_ITEM_CODE = ?";
	private static final String GET_ITEM_CODE_FOR_STORE_UPC = "SELECT ITEM_CODE FROM( "
			+ "SELECT IL.RETAILER_ITEM_CODE, IL.ITEM_CODE, RANK() OVER (PARTITION BY SIM.LEVEL_ID ORDER BY IL.RETAILER_ITEM_CODE DESC) RANK "
			+ "FROM STORE_ITEM_MAP SIM LEFT JOIN ITEM_LOOKUP IL ON SIM.ITEM_CODE = IL.ITEM_CODE " + "WHERE IL.UPC = ? "
			+ "AND SIM.LEVEL_TYPE_ID = 2 AND SIM.LEVEL_ID = ? " + "AND SIM.IS_AUTHORIZED = 'Y' " + ")WHERE RANK = 1";
	private static final String GET_ITEM_CODES_FOR_UPC = "SELECT ITEM_CODE, RETAILER_ITEM_CODE, UPC FROM ITEM_LOOKUP WHERE UPC IN (%s)";
	private static final String GET_AUTHORIZED_ITEMS_FOR_STORE = "SELECT LEVEL_ID, UPC, RETAILER_ITEM_CODE, ACTIVE_INDICATOR, ITEM_CODE FROM( "
			+ "SELECT SIM.LEVEL_ID, ITEMS.UPC, ITEMS.RETAILER_ITEM_CODE, ITEMS.ITEM_CODE, ITEMS.ACTIVE_INDICATOR, "
			+ "RANK() OVER (PARTITION BY SIM.LEVEL_ID, ITEMS.UPC ORDER BY RETAILER_ITEM_CODE) RANK FROM( "
			+ "SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE,ACTIVE_INDICATOR FROM( "
			+ "SELECT DISTINCT IL1.UPC, IL1.RETAILER_ITEM_CODE, IL1.ITEM_CODE,IL1.ACTIVE_INDICATOR FROM ITEM_LOOKUP IL1, ITEM_LOOKUP IL2 WHERE "
			+ "IL1.LIR_IND = 'N' AND IL2.LIR_IND = 'N' " + "AND IL1.UPC = IL2.UPC "
			+ "AND IL1.ITEM_CODE != IL2.ITEM_CODE " + "AND IL1.UPC != '000000000000' "
			+ "AND IL2.UPC != '000000000000' " + ")) ITEMS LEFT JOIN STORE_ITEM_MAP SIM "
			+ "ON ITEMS.ITEM_CODE = SIM.ITEM_CODE " + "WHERE SIM.IS_AUTHORIZED = 'Y' " + ") WHERE RANK = 1";
	private static final String GET_ALL_ITEMS = "SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR = 'Y' AND LIR_IND = 'N'";
	private static final String UPDATE_LIKE_ITEM_ID = "UPDATE ITEM_LOOKUP SET RET_LIR_ID = ? WHERE ITEM_CODE = ?";

	// private static final String GET_ALL_LIR_ID_WITH_SOURCE_VENDOR_AND_ITEM =
	// "SELECT NVL(IL.RET_LIR_ID, -1) RET_LIR_ID, "
	// + " RM.RETAILER_ITEM_CODE FROM ITEM_LOOKUP IL, "
	// + " RETAILER_ITEM_CODE_MAP RM WHERE RM.ITEM_CODE = IL.ITEM_CODE";

	private static final String GET_ALL_LIR_ID_WITH_SOURCE_VENDOR_AND_ITEM = "SELECT NVL(IL.RET_LIR_ID, -1) RET_LIR_ID, "
			+ " RM.RETAILER_ITEM_CODE, LIG.RET_LIR_NAME FROM ITEM_LOOKUP IL "
			+ " LEFT JOIN RETAILER_ITEM_CODE_MAP RM ON RM.ITEM_CODE = IL.ITEM_CODE "
			+ " LEFT JOIN RETAILER_LIKE_ITEM_GROUP LIG ON IL.RET_LIR_ID = LIG.RET_LIR_ID "
			+ " WHERE  NVL(IL.RET_LIR_ID, -1) > 0";
	// only for debugging
	// + " AND IL.RET_LIR_ID = 17298 ";
	// + " AND IL.ITEM_CODE IN
	// (933611,934703,761303,883376,883379,761653,761312,761350,792737,761551,761657,439733,106008)";
	private static final String GET_ITEM_CODE_MAP_WITH_LIR_INFO = "SELECT  RETAILER_ITEM_CODE, ITEM_CODE, RET_LIR_ID FROM "
			+ " ITEM_LOOKUP  ";

	private static final String GET_RETAILER_ITEM_CODE_MAP = "SELECT RETAILER_ITEM_CODE, ITEM_CODE FROM RETAILER_ITEM_CODE_MAP ";
	// only for debugging
	// + " WHERE ITEM_CODE IN (SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE
	// RET_LIR_ID=17298) ";
	// + " WHERE ITEM_CODE IN
	// (933611,934703,761303,883376,883379,761653,761312,761350,792737,761551,761657,439733,106008)";

	private static final String GET_ALL_LIR_ITEMS = "SELECT RET_LIR_ID, ITEM_CODE FROM ITEM_LOOKUP "
			+ " WHERE LIR_IND = 'N' AND RET_LIR_ID IS NOT NULL";

	private static final String UPDATE_USER_ATTRIBUTES = "UPDATE ITEM_LOOKUP SET USER_ATTR_3 = ?, USER_ATTR_4 = ?, "
			+ "USER_ATTR_5 = ?  WHERE UPC = ? AND RETAILER_ITEM_CODE = ? ";
	
	private static final String UPDATE_USER_ATTRIBUTES_TIER = "UPDATE ITEM_LOOKUP SET USER_ATTR_3 = ?, USER_ATTR_4 = ?, "
			+ "USER_ATTR_5 = ?,  TIER = ? WHERE UPC = ? AND RETAILER_ITEM_CODE = ? ";

	private static final String GET_RETAILER_NAMES_FOR_ITEMS = "SELECT ITEM_CODE, RETAILER_NAME FROM SOURCE_RETAILER_MAP WHERE ITEM_CODE IN (%s)";

	private static final String INSERT_RETAILER_NAMES_FOR_ITEMS = "INSERT INTO SOURCE_RETAILER_MAP (ITEM_CODE, RETAILER_NAME) VALUES(?, ?) ";

	private static final String EXPORT_ITEM_LOOKUP = "SELECT A.DEPT_NAME DEPT_SHRT_NAME , A.DEPT_CODE, B.DESCRIPTION DEPT_NAME, "
			+ " A.CAT_NAME, A.CATEGORY_CODE, A.RETAILER_ITEM_CODE, A.ITEM_NAME, "
			+ " A.ITEM_SIZE, A.UOM_NAME, A.PACK, A.PRIVATE_LABEL_CODE, A.RET_LIR_NAME, "
			+ " A.RET_LIR_CODE, A.RETAILER_UPC, SRM.RETAILER_NAME, BL.BRAND_NAME FROM "
			+ " ITEM_LOOKUP_VIEW A LEFT JOIN DEPARTMENT B ON A.DEPT_ID = B.ID LEFT JOIN SOURCE_RETAILER_MAP "
			+ " SRM ON SRM.ITEM_CODE = A.ITEM_CODE " + " LEFT JOIN BRAND_LOOKUP BL ON BL.BRAND_ID = A.BRAND_ID "
			+ " WHERE A.ACTIVE_INDICATOR = 'Y'";

	private static final String GET_DEPTID_AND_CATID_FROM_ITEMLOOKUP = "SELECT ITEM_CODE, DEPT_ID, CATEGORY_ID FROM ITEM_LOOKUP ";
	private static final String UPDATE_ITEM_WITH_TIMESTAMP = "UPDATE ITEM_LOOKUP SET ITEM_NAME = ?, RETAILER_ITEM_CODE = ?, "
			+ "STANDARD_UPC = ?, UPC = ?,  ACTIVE_INDICATOR = 'Y', DEPT_ID = ?, CATEGORY_ID = ?,"
			+ " SUB_CATEGORY_ID = ?, SEGMENT_ID = ?, UOM_ID = ?, ITEM_SIZE = ?, PRIVATE_LABEL_CODE = ?, PRIVATE_LABEL_IND = ?, ITEM_DESC = ?,"
			+ " RET_LIR_ID = ?, BRAND_ID = ?, MANUFACTURER_ID = ?, PREPRICED_IND = ?,  UPDATE_TIMESTAMP = SYSDATE WHERE ITEM_CODE = ?";

	public static final String INSERT_LIR_ITEM_INTO_ITEM_LOOKUP = "insert into ITEM_LOOKUP ( item_code, item_name, item_desc, item_Size, Pack, uom_id,"
			+ " UPC, DEPT_ID, MANUFACTURER_ID,CATEGORY_ID, RETAILER_ITEM_CODE, STANDARD_UPC, "
			+ "ACTIVE_INDICATOR, SUB_CATEGORY_ID, RET_LIR_ID, SEGMENT_ID, PI_ANALYZE_FLAG, "
			+ "DISCONT_FLAG, UPDATE_TIMESTAMP, CREATE_TIMESTAMP, PRIVATE_LABEL_IND, LIR_IND) "
			+ " select ?, item_name, item_desc, item_Size, Pack, uom_id, "
			+ "'L'||UPC, DEPT_ID, MANUFACTURER_ID,CATEGORY_ID, 'L'||RETAILER_ITEM_CODE, 'L'|| STANDARD_UPC, "
			+ "ACTIVE_INDICATOR, SUB_CATEGORY_ID, RET_LIR_ID, SEGMENT_ID, PI_ANALYZE_FLAG, "
			+ "DISCONT_FLAG, SYSDATE, SYSDATE, PRIVATE_LABEL_IND, 'Y' "
			+ "FROM ITEM_LOOKUP WHERE ITEM_CODE =  ?";

	private static final String GET_UNUSED_LIR_ITEM_SEQ_FROM_ITEM_LOOKUP = "SELECT MIN_ITEM_CODE - 1 + LEVEL MISSING_ITEM_CODE FROM( SELECT MIN(ITEM_CODE) "
			+ "MIN_ITEM_CODE, MAX(ITEM_CODE) MAX_ITEM_CODE FROM ITEM_LOOKUP WHERE "
			+ "ITEM_CODE < ? AND ITEM_CODE > ?)CONNECT BY LEVEL <= MAX_ITEM_CODE - MIN_ITEM_CODE + 1 "
			+ "MINUS SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE ITEM_CODE < ? AND ITEM_CODE > ?";

	private static final String GET_LIR_ITEM_CODE_SEQ = "SELECT LIR_ITEM_CODE_SEQ.nextval AS LIR_ITEM_CODE FROM DUAL";
	private static final String GET_USER_ATTR_LOOKUP_DETAILS_MAP = "SELECT ATTR_ID,ATTR_DISPLAY_NAME FROM USER_ATTR_LOOKUP";
	private static final String GET_USER_ATTR_LOOKUP_DETAILS = "SELECT ATTR_ID,ATTR_DISPLAY_NAME FROM USER_ATTR_LOOKUP WHERE ATTR_DISPLAY_NAME=?";
	private static final String INSERT_INTO_USER_ATTR_LOOKUP = "INSERT INTO USER_ATTR_LOOKUP (ATTR_ID,ATTR_DISPLAY_NAME) VALUES(USER_ATTR_ID_SEQ.NEXTVAL,?)";
	private static final String UPDATE_USER_ATTR_DETAIL_IN_ITEM_LOOKUP = "UPDATE ITEM_LOOKUP SET USER_ATTR_ID2 = ? WHERE RETAILER_ITEM_CODE = ?";
	private static final String GET_BASE_ITEM_DETAILS = "SELECT IL.ITEM_CODE,IL.FRIENDLY_NAME,IL.RETAILER_ITEM_CODE,IL.UPC,PG.NAME AS SUB_CATEGORY_NAME,"
			+ " PG1.NAME AS CATEGORY_NAME,PG2.NAME AS DEPARTMENT_NAME,IL.ITEM_NAME,IL.ITEM_SIZE,UNITPRICE(RPI.REG_PRICE,RPI.REG_M_PRICE,RPI.REG_QTY)REG_PRICE from (SELECT distinct ITEM_CODE,"
			+ " P_L_ID_1,P_ID_1 AS SUB_CATEGORY,P_L_ID_2 ,P_ID_2 AS category,P_L_ID_3 ,P_ID_3 as Department FROM (SELECT A1.PRODUCT_LEVEL_ID P_L_ID_1,"
			+ " A1.PRODUCT_ID P_ID_1,A1.CHILD_PRODUCT_ID ITEM_CODE ,A2.PRODUCT_LEVEL_ID P_L_ID_2,A2.PRODUCT_ID P_ID_2,A2.CHILD_PRODUCT_ID CHILD_P_ID_2,"
			+ " A3.PRODUCT_LEVEL_ID P_L_ID_3,A3.PRODUCT_ID P_ID_3,A3.CHILD_PRODUCT_ID CHILD_P_ID_3 FROM  ( ( PRODUCT_GROUP_RELATION A1"
			+ " LEFT JOIN PRODUCT_GROUP_RELATION A2 ON A1.PRODUCT_ID = A2.CHILD_PRODUCT_ID AND A1.PRODUCT_LEVEL_ID = A2.CHILD_PRODUCT_LEVEL_ID)"
			+ " LEFT JOIN PRODUCT_GROUP_RELATION A3 ON A2.PRODUCT_ID = A3.CHILD_PRODUCT_ID AND A2.PRODUCT_LEVEL_ID = A3.CHILD_PRODUCT_LEVEL_ID )"
			+ " WHERE A1.CHILD_PRODUCT_LEVEL_ID = 1) WHERE P_L_ID_3 = 5) PGR JOIN PRODUCT_GROUP PG ON PGR.SUB_CATEGORY = PG.PRODUCT_ID"
			+ " JOIN RETAIL_PRICE_INFO RPI ON RPI.ITEM_CODE = PGR.ITEM_CODE LEFT JOIN PRODUCT_GROUP PG1 ON PGR.CATEGORY = PG1.PRODUCT_ID"
			+ " LEFT JOIN PRODUCT_GROUP PG2 ON PGR.DEPARTMENT = PG2.PRODUCT_ID LEFT JOIN ITEM_LOOKUP IL ON PGR.ITEM_CODE = IL.ITEM_CODE "
			+ " AND IL.PRIVATE_LABEL_IND='Y' AND IL.ACTIVE_INDICATOR='Y' WHERE PG1.PRODUCT_ID IN (SELECT PRODUCT_ID FROM PRODUCT_GROUP"
			+ " WHERE PRODUCT_LEVEL_ID=4) AND RPI.CALENDAR_ID IN (?) AND RPI.LEVEL_ID='53' AND PG1.PRODUCT_ID in (SELECT PRODUCT_ID FROM PRODUCT_GROUP WHERE PRODUCT_ID IN "
			+ " (2137,6119,2115,6108,4870,2624,2112,6254,2953,2917,5697,2598,2189,6379,2306,6433,2745,3199) "
			+ " OR PRODUCT_ID IN (SELECT CHILD_PRODUCT_ID FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_ID IN (2160, 2231)) "
			+ " ) AND IL.ITEM_CODE IS NOT NULL";
	
	// Dairy, Frozen and HBC cat: SELECT PRODUCT_ID FROM PRODUCT_GROUP WHERE PRODUCT_ID IN (2137,6119,2115, 6108,4870,2624,2112,6254,2953,2917,5697,2598,2189,6379,2306,6433,2745,3199)
	 //OR PRODUCT_ID IN (SELECT CHILD_PRODUCT_ID FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_ID IN (2160, 2231))
	// Meal Solution = 2170, other 10 category = 2487,5386,2385,2354,2065,2631,2045,2388,2151,6108
	private static final String GET_ITEM_CODE_BASED_ON_UPC = "SELECT ITEM_CODE,UPC FROM ITEM_LOOKUP WHERE LIR_IND='N'";
	private static final String GET_PL_COMP_CATEGORY_MAPPING = "SELECT BASE_CATEGORY,COMP_CATEGORY FROM PL_COMP_CATEGORY_MAPPING WHERE COMP_NAME=?";
	private static final String GET_ITEM_LOOKUP_ACTIVE_ITEMS = "SELECT RETAILER_ITEM_CODE, ITEM_CODE, USER_ATTR_3 FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR='Y' AND LIR_IND='N'";
//			+ " and retailer_item_code in ('494161008','1200133898','1200907271','1200965104','1202073880','1202073881','1202053682','1201546986','14402348','135400328','148858586','230286477','241860676','254435958','274925559','286724486','355276000','355288000','355360000','355384000','355420000','403480000','428653676','654307949','696131840','847936000','858760000','858853666','920502199','932271373','1200008834','1200073222','1200124933','1200444099','1200485078','1200702158','1200704934','1200704935','1200736908','1200911905','1200913801','1200931900','200978161','1200978168','1200978170','1200978830','1200978831','1201039363','1201040975','1201083048','1201083049','1201083050','1201083051','1201083052','1201083053','1201083054','1201086160','1201128593','1201321193','1201321194','1201332355','1201358834','1201358847','1201730630','1201730631','1201730632','1201774711','1201774715','1201923781','1202042082','1202042083','1202118265')";
	// PG1.PRODUCT_ID IN (SELECT PRODUCT_ID FROM PRODUCT_GROUP"'PIZZA','CEREALS','CHEESE',
	// + " WHERE PRODUCT_LEVEL_ID=4)
	
	private static final String GET_ALL_LIR = "SELECT RET_LIR_ID, RET_LIR_CODE, RET_LIR_NAME, RET_LIR_ITEM_CODE FROM RETAILER_LIKE_ITEM_GROUP "
			+ "WHERE RET_LIR_ITEM_CODE IS NOT NULL";
			 
	public CachedRowSet getProductGroupForAnalysis(Connection conn) throws GeneralException {

		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		sb.append("SELECT ID DEPT_ID, NAME, DESCRIPTION, ANALYZE_FLAG FROM DEPARTMENT ");
		sb.append(" WHERE ANALYZE_FLAG = 'Y'");
		// logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "ITEMDAO - getStoresCheckedinLastXDays");
		return crs;
	}

	/**
	 * Initializing all the core values from database
	 * 
	 * @param connection
	 * @throws GeneralException
	 */
	public void init(Connection connection) throws GeneralException {
		boolean convertToUppercase = true;
		boolean dontConvertToUppercase = false;

		// retailerIdandNameMap = itemLoaderDAO.getTwoColumnsCache(connection, "RET_LIR_ID", "RET_LIR_NAME",
		// "RETAILER_LIKE_ITEM_GROUP", convertToUppercase);
		// retailerIdandCodeMap = itemLoaderDAO.getTwoColumnsCache(connection, "RET_LIR_ID", "RET_LIR_CODE",
		// "RETAILER_LIKE_ITEM_GROUP", dontConvertToUppercase);
		brandIDandNameMap = itemLoaderDAO.getTwoColumnsCache(connection, "BRAND_ID", "BRAND_NAME", "BRAND_LOOKUP",
				convertToUppercase);
		manufacturerIDandNameMap = itemLoaderDAO.getTwoColumnsCache(connection, "ID", "NAME", "MANUFACTURER",
				convertToUppercase);
		itemGrpDAO.init(connection);
		boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		retLirIDwithUPCandRetItemCodeMap = itemLoaderDAO.getThreeColumnsCache(connection, "RET_LIR_ID",
				"UPC", "RETAILER_ITEM_CODE", "ITEM_LOOKUP");
		retLirIDwithUPC = itemLoaderDAO.getTwoColumnsCache(connection, "RET_LIR_ID", "UPC", "ITEM_LOOKUP", dontConvertToUppercase);
		if (checkRetailerItemCode) {
			itemDetailsForAllItemsWithCheckRetItemCodeMap = itemLoaderDAO.getThreeColumnsCache(connection, "ITEM_CODE",
					"UPC", "RETAILER_ITEM_CODE", "ITEM_LOOKUP");
		} else {
			itemCodeandUPCMap = itemLoaderDAO.getTwoColumnsCache(connection, "ITEM_CODE", "UPC", "ITEM_LOOKUP", dontConvertToUppercase);
			itemCodeandStandardUPCMap = itemLoaderDAO.getTwoColumnsCache(connection, "ITEM_CODE", "STANDARD_UPC", "ITEM_LOOKUP",
					dontConvertToUppercase);
		}
	}

	public CachedRowSet getLIRItems(Connection conn, boolean onlyNull) throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		// Changed query to get RET_LIR_ID based on Active items from item_lookup and to avoid unused ret_lir_id processing
		// By Dinesh(05/24/17)
		sb.append("SELECT RET_LIR_ID, RET_LIR_CODE, RET_LIR_NAME, RET_LIR_ITEM_CODE, NVL(RET_LIR_ITEM_CODE_PREV,-1) RET_LIR_ITEM_CODE_PREV "
				+ "FROM RETAILER_LIKE_ITEM_GROUP WHERE RET_LIR_ID IN (SELECT DISTINCT RET_LIR_ID FROM ITEM_LOOKUP WHERE "
				+ "LIR_IND='N' AND ACTIVE_INDICATOR='Y')");
		if (onlyNull) {
			sb.append(" AND RET_LIR_ITEM_CODE is null");
		}
		//sb.append(" WHERE RET_LIR_ID = 20759 "); //for testing purposes.
		logger.debug("getLIRItems() query- " + sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "ITEMDAO - getLineItemGroup");
		return crs;
	}

	public CachedRowSet getLIRInfo(Connection conn, int lirId) throws GeneralException {

		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		sb.append("SELECT RET_LIR_ID, RET_LIR_CODE, RET_LIR_NAME, RET_LIR_ITEM_CODE FROM RETAILER_LIKE_ITEM_GROUP ");
		if (lirId > 0)
			sb.append(" WHERE RET_LIR_ID = ").append(lirId);
		crs = PristineDBUtil.executeQuery(conn, sb, "ITEMDAO - getLineItemGroup");
		return crs;
	}

	public int getLirMinItemCode(Connection conn, int retLirId, int lirMinItemCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		if (!checkRetailerItemCode) {
			sb.append("SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE UPC = ");
			sb.append("(SELECT MIN(UPC) FROM ITEM_LOOKUP WHERE RET_LIR_ID = ");
			sb.append(retLirId);
			sb.append(" AND ACTIVE_INDICATOR = 'Y'  ");
			if (lirMinItemCode > 0)
				sb.append(" AND ITEM_CODE > ").append(lirMinItemCode);
			sb.append(" ) ");
		} else {
			sb = new StringBuffer();
			sb.append("SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE (UPC, RETAILER_ITEM_CODE) IN ");
			sb.append("((SELECT UPC, RETAILER_ITEM_CODE FROM (");
			sb.append(
					"SELECT UPC, RETAILER_ITEM_CODE, RANK() OVER (PARTITION BY RET_LIR_ID ORDER BY UPC ASC) RANK FROM ITEM_LOOKUP WHERE RET_LIR_ID = ");
			sb.append(retLirId);
			if (lirMinItemCode > 0)
				sb.append(" AND ITEM_CODE > ").append(lirMinItemCode);
			sb.append(" AND ACTIVE_INDICATOR = 'Y'  ) WHERE RANK = 1))");
		}
		logger.debug("getLirMinItemCode() query - "+sb.toString());
		String retVal = PristineDBUtil.getSingleColumnVal(conn, sb, "getLirMinItemCode");

		int itemCode = 0;
		if (retVal != null) {
			itemCode = Integer.parseInt(retVal);
		}
		return itemCode;

	}

	public String getRetailerItemCode(Connection conn, int itemCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		sb.append("SELECT RETAILER_ITEM_CODE FROM ITEM_LOOKUP WHERE ITEM_CODE = ");
		sb.append(itemCode);
		String retVal = PristineDBUtil.getSingleColumnVal(conn, sb, "getLirMinItemCode");

		String retItemcode = "";
		if (retVal != null) {
			retItemcode = retVal;
		}
		return retItemcode;

	}

	public String getUpcForSingleRetailerItemCode(Connection conn, String itemCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		sb.append("SELECT UPC FROM ITEM_LOOKUP WHERE RETAILER_ITEM_CODE = '");
		sb.append(itemCode + "'");
		String retVal = PristineDBUtil.getSingleColumnVal(conn, sb, "getLirMinItemCode");

		String upc = "";
		if (retVal != null) {
			upc = retVal;
		}
		return upc;

	}

	public boolean setupLirItemCode(Connection conn, int retLirId, int lirItemCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE RETAILER_LIKE_ITEM_GROUP SET ");
		sb.append(" RET_LIR_ITEM_CODE = ").append(lirItemCode);
		sb.append(" WHERE RET_LIR_ID = ").append(retLirId);

		int count = PristineDBUtil.executeUpdate(conn, sb, "Update LIR Item");
		boolean retVal = (count >= 1) ? true : false;
		return retVal;
	}

	public boolean insertItem(Connection conn, ItemDTO item, boolean populateGrpInfo, boolean isPriceCheck) {
		boolean noItemCodeSequence = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.NO_ITEM_CODE_SEQ", "FALSE"));
		try {
			// Changes to fill gaps in ITEM_CODE_SEQ
			if (noItemCodeSequence && missingItemCodeList.size() <= 0) {
				setMissingItemList(conn);
				// logger.info("Size of Missing Item list" + missingItemCodeList.size());
			}
			// Changes to fill gaps in ITEM_CODE_SEQ - Ends

			StringBuffer sb = new StringBuffer();
			// Changes to check retailer item code along with upc to identify an item for Ahold
			boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
			String itemCodeStr = null;
			if (checkRetailerItemCode) {
				String keyValue = item.upc + "-" + item.retailerItemCode;
				if (itemDetailsForAllItemsWithCheckRetItemCodeMap != null
						&& itemDetailsForAllItemsWithCheckRetItemCodeMap.get(keyValue) != null) {
					itemCodeStr = String.valueOf(itemDetailsForAllItemsWithCheckRetItemCodeMap.get(keyValue));
				} else {
					itemCodeStr = getItemCodeForUpcRetailerItemCode(conn, item.upc, item.retailerItemCode);
				}
				if (itemCodeStr != null) {
					item.itemCode = Integer.parseInt(itemCodeStr);
					return false;
				}
			}
			// Changes to check retailer item code along with upc to identify an item for Ahold
			else {
				// Check if retailer item code exists
				if (itemCodeandUPCMap != null && itemCodeandUPCMap.get(item.upc) != null) {
					itemCodeStr = String.valueOf(itemCodeandUPCMap.get(item.upc));
				} else {
					sb.append(" SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE UPC = ");
					sb.append("'" + item.upc + "'");
					itemCodeStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item - getItemCode");
				}
				if (itemCodeStr != null) {
					// logger.info( item.upc+ "Already exists");
					// Loading Product Group Tables - Price Index Portfolio Support
					item.itemCode = Integer.parseInt(itemCodeStr);
					return false;
				}
				if (itemCodeandStandardUPCMap != null && itemCodeandStandardUPCMap.get(item.standardUPC) != null) {
					itemCodeStr = String.valueOf(itemCodeandStandardUPCMap.get(item.standardUPC));
				} else {
					sb = new StringBuffer();
					// Check if retailer item code exists
					sb.append(" SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE STANDARD_UPC = ");
					sb.append("'" + item.standardUPC + "'");
					itemCodeStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item - getItemCode");
				}
				if (itemCodeStr != null) {
					// logger.info( item.upc + "Already exists");
					// Loading Product Group Tables - Price Index Portfolio Support
					item.itemCode = Integer.parseInt(itemCodeStr);
					return false;
				}
			}
			
			int chainId = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID"));

			// logger.info("Insert Process For " + item.upc);
			// Check if Dept exists
			if (populateGrpInfo) {
				boolean result = populateDeptCat(conn, item);
				if (!result && !isPriceCheck)
					return false;
			}
			sb = new StringBuffer();
			sb.append("insert into item_lookup(");
			sb.append("item_code, item_name, ACTIVE_INDICATOR, ");
			sb.append(" upc, dept_id, category_id,  RETAILER_ITEM_CODE, STANDARD_UPC, sub_category_id, SEGMENT_ID, ");
			if(chainId == 52)
			{
				sb.append(" uom_id, item_size, pack,  ITEM_DESC, RET_LIR_ID, CREATE_TIMESTAMP, UPDATE_TIMESTAMP, PRIVATE_LABEL_IND, PRIVATE_LABEL_CODE, BRAND_ID, MANUFACTURER_ID, USER_ATTR_VAL_1, USER_ATTR_VAL_2, USER_ATTR_VAL_3, USER_ATTR_VAL_4, PREPRICED_IND) values (");
			}
			else
			{
				sb.append(" uom_id, item_size, pack,  ITEM_DESC, RET_LIR_ID, CREATE_TIMESTAMP, UPDATE_TIMESTAMP, PRIVATE_LABEL_IND, PRIVATE_LABEL_CODE, BRAND_ID, MANUFACTURER_ID, PREPRICED_IND) values (");
			}
			

			// Changes to fill gaps in ITEM_CODE_SEQ
			if (noItemCodeSequence && missingItemCounter <= (missingItemCodeList.size() - 1)) {
				sb.append(missingItemCodeList.get(missingItemCounter));
				sb.append(",");
				missingItemCounter++;
			} else
				sb.append("ITEM_CODE_SEQ.NEXTVAL" + ",");
			// Changes to fill gaps in ITEM_CODE_SEQ - Ends

			sb.append("'" + item.itemName + "',");
			sb.append("'Y',");
			sb.append("'" + item.upc + "',");
			sb.append("'" + item.deptID + "',");
			sb.append("'" + item.catID + "',");
			sb.append("'" + item.retailerItemCode + "', ");
			sb.append("'" + item.standardUPC + "', ");

			if (item.subCatID > 0)
				sb.append("'" + item.subCatID + "', ");
			else
				sb.append("NULL, ");

			if (item.segmentID > 0)
				sb.append("'" + item.segmentID + "', ");
			else
				sb.append("NULL, ");

			if (item.uomId != null && !item.uomId.equals(""))
				sb.append("'" + item.uomId + "', ");
			else
				sb.append("NULL, ");

			if (item.size != null && !item.size.equals(""))
				sb.append("'" + item.size + "', ");
			else
				sb.append("NULL, ");

			if (item.pack != null && !item.pack.equals(""))
				sb.append("'" + item.pack + "', ");
			else
				sb.append("NULL, ");

			if (item.itemRank != null && !item.itemRank.equals(""))
				sb.append("'" + item.itemRank + "', ");
			else
				sb.append("NULL, ");

			if (item.likeItemId > 0)
				sb.append(item.likeItemId + ", ");
			else
				sb.append("NULL, ");

			sb.append("SYSDATE, SYSDATE,");

			if (item.privateLabelFlag != null && !item.privateLabelFlag.equals(""))
				sb.append("'" + item.privateLabelFlag + "', ");
			else
				sb.append("NULL, ");

			if (item.privateLabelCode != null && !item.privateLabelCode.equals(""))
				sb.append("'" + item.privateLabelCode + "', ");
			else
				sb.append("NULL, ");

			if (item.brandId > 0)
				sb.append(item.brandId + ", ");
			else
				sb.append("NULL, ");

			if (item.manufactId > 0)
				sb.append(item.manufactId + ", ");
			else
				sb.append("NULL, ");
			
			if(chainId == 52)
			{
				if (item.UserAttrVal1 != null && !item.UserAttrVal1.equals(""))
					sb.append("'" + item.UserAttrVal1 + "', ");
				else
					sb.append("NULL, ");
			
				if (item.UserAttrVal2 != null && !item.UserAttrVal2.equals(""))
					sb.append("'" + item.UserAttrVal2 + "', ");
				else
					sb.append("NULL, ");
			
				if (item.UserAttrVal3 != null && !item.UserAttrVal3.equals(""))
					sb.append("'" + item.UserAttrVal3 + "', ");
				else
					sb.append("NULL, ");
				
				if (item.UserAttrVal4 != null && !item.UserAttrVal4.equals(""))
					sb.append("'" + item.UserAttrVal4 + "', ");
				else
					sb.append("NULL, ");
			}

			// Changes for incorporating Pre Price Ind
			if (item.prePriceInd != null && !item.prePriceInd.equals(""))
				sb.append("'" + item.prePriceInd + "') ");
			else
				sb.append("NULL) ");
			// Changes for incorporating Pre Price Ind - Ends

			logger.debug(sb.toString());
			PristineDBUtil.execute(conn, sb, "ItemDAO - Insert");

			// Loading Product Group Tables - Price Index Portfolio Support
			// Changes to check retailer item code along with upc to identify an item for Ahold
			if (checkRetailerItemCode) {
				itemCodeStr = getItemCodeForUpcRetailerItemCode(conn, item.upc, item.retailerItemCode);
				if (itemCodeStr != null) {
					String keyValue = item.upc + "-" + item.retailerItemCode;
					if (itemDetailsForAllItemsWithCheckRetItemCodeMap != null && retLirIDwithUPCandRetItemCodeMap != null) {
						itemDetailsForAllItemsWithCheckRetItemCodeMap.put(keyValue, Integer.parseInt(itemCodeStr));
						retLirIDwithUPCandRetItemCodeMap.put(keyValue, item.likeItemId);
					}
					item.itemCode = Integer.parseInt(itemCodeStr);
			}
			else {
					sb = new StringBuffer();
					sb.append(" SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE STANDARD_UPC = ");
					sb.append("'" + item.standardUPC + "'");
					itemCodeStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item - getItemCode");
					if (itemCodeStr != null) {
						String keyValue = item.upc + "-" + item.retailerItemCode;
						if (itemDetailsForAllItemsWithCheckRetItemCodeMap != null && retLirIDwithUPCandRetItemCodeMap != null) {
							itemDetailsForAllItemsWithCheckRetItemCodeMap.put(keyValue, Integer.parseInt(itemCodeStr));
							retLirIDwithUPCandRetItemCodeMap.put(keyValue, item.likeItemId);
						}
						item.itemCode = Integer.parseInt(itemCodeStr);
					}
				}
			}
			// Changes to check retailer item code along with upc to identify an item for Ahold - Ends
			else {
				sb = new StringBuffer();
				sb.append(" SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE STANDARD_UPC = ");
				sb.append("'" + item.standardUPC + "'");
				itemCodeStr = PristineDBUtil.getSingleColumnVal(conn, sb, "Insert Item - getItemCode");
				if (itemCodeStr != null) {
					if (itemCodeandUPCMap != null && retLirIDwithUPC != null && itemCodeandStandardUPCMap != null) {
						itemCodeandUPCMap.put(item.upc, Integer.parseInt(itemCodeStr));
						retLirIDwithUPC.put(item.upc, item.likeItemId);
						itemCodeandStandardUPCMap.put(item.standardUPC, Integer.parseInt(itemCodeStr));
						// Add newly inserted values into Hash Map with respect to UPC and Retailer_item_code..
					}
					item.itemCode = Integer.parseInt(itemCodeStr);
				}
			}
			// Loading Product Group Tables - Price Index Portfolio Support - Ends
		} catch (GeneralException e) {
			logger.error("Error while inserting new item.", e);
			logger.error("Error while inserting new item." + e.getMessage());
			return false;
		}
		return true;

	}

	private boolean populateDeptCat(Connection conn, ItemDTO item)
			throws GeneralException{

		ItemGroupDTO itemGrpDTO = new ItemGroupDTO();

		// logger.debug("Insert Department");
		itemGrpDTO.deptName = item.deptName;
		if (item.deptShortName != null && !item.deptShortName.equals("")) {
			// Use department full name if ignoreDeptShortName is true
			if (!ignoreDeptShortName)
				itemGrpDTO.deptName = item.deptShortName;
		}
		itemGrpDTO.deptCode = item.deptCode;
		itemGrpDTO.catName = item.catName;
		itemGrpDTO.catCode = item.catCode;
		itemGrpDTO.subCatName = item.subCatName;
		itemGrpDTO.subCatCode = item.subCatCode;
		itemGrpDTO.segmentName = item.segmentName;
		itemGrpDTO.segmentCode = item.segmentCode;

		itemGrpDTO.deptId = itemGrpDAO.populateDept(conn, itemGrpDTO);
		item.deptID = itemGrpDTO.deptId;
		if (itemGrpDTO.isEmptyDepartment) {
			item.isEmptyDepartment = itemGrpDTO.isEmptyDepartment;
			return false;
		}
		if (item.deptName == null || item.deptName.equals(""))
			item.deptName = itemGrpDTO.deptName;

		// logger.debug("Insert Category");
		itemGrpDTO.catId = itemGrpDAO.populateCategory(conn, itemGrpDTO);
		item.catID = itemGrpDTO.catId;
		if (itemGrpDTO.isEmptyCategory) {
			item.isEmptyCategory = itemGrpDTO.isEmptyCategory;
			return false;
		}
		if (item.catName == null || item.catName.equals(""))
			item.catName = itemGrpDTO.catName;

		/*
		 * Commented to make logic similar to department and category if (
		 * itemGrpDTO.subCatName.equals("")) itemGrpDTO.subCatId = -1; else
		 */
		itemGrpDTO.subCatId = itemGrpDAO.populateSubCategory(conn, itemGrpDTO);
		if (itemGrpDTO.isEmptySubCategory) {
			item.isEmptySubCategory = itemGrpDTO.isEmptySubCategory;
			return false;
		}
		if (item.subCatName == null || item.subCatName.equals(""))
			item.subCatName = itemGrpDTO.subCatName;

		if (itemGrpDTO.segmentCode == null || itemGrpDTO.segmentCode.equals(""))
			itemGrpDTO.segId = -1;
		else {
			itemGrpDTO.segId = itemGrpDAO.populateSegment(conn, itemGrpDTO);
		}
		// logger.debug("Insert Ends");

		item.subCatID = itemGrpDTO.subCatId;
		item.segmentID = itemGrpDTO.segId;
		return true;
	}

	public void updateItem(Connection conn, ItemDTO item, boolean populateGrpInfo, boolean isPriceCheck)
			throws GeneralException {
		long ustartTime = System.currentTimeMillis();
		boolean checkRetailerItemCode = Boolean
				.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		if (populateGrpInfo) {
			
			boolean result = populateDeptCat(conn, item);
			if (!result && !isPriceCheck)
				return;
		}
		
		int chainId = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID"));
		
		//If updateAsBatchStatement is true then needs to be added in list 
		if (updateItemLookupList != null){
			//Add item object into the list to update as batch
			ItemDTO itemDTO;
			try {
				itemDTO = (ItemDTO)item.clone();
			} catch (CloneNotSupportedException e) {
				throw new GeneralException("Clone Exception", e);
			}
			updateItemLookupList.add(itemDTO);

		}else {
			StringBuffer sb = new StringBuffer();
			sb.append("UPDATE ITEM_LOOKUP SET ");
			sb.append(" item_name = '").append(item.itemName).append("',");
			sb.append(" RETAILER_ITEM_CODE = '").append(item.retailerItemCode).append("',");
			sb.append(" STANDARD_UPC = '").append(item.standardUPC).append("',");
			sb.append(" UPC = '").append(item.upc).append("',");
			sb.append(" ACTIVE_INDICATOR = 'Y',");
			sb.append(" dept_id = ").append(item.deptID).append(",");
			
			//Added by RB
			if(chainId == 52)
			{
				sb.append(" USER_ATTR_VAL_1 = '").append(item.UserAttrVal1).append("',");
				sb.append(" USER_ATTR_VAL_2 = '").append(item.UserAttrVal2).append("',");
				sb.append(" USER_ATTR_VAL_3 = '").append(item.UserAttrVal3).append("',");
				sb.append(" USER_ATTR_VAL_4 = '").append(item.UserAttrVal4).append("',");
			}
						
			sb.append(" category_id = ").append(item.catID).append(" ");

			if (item.subCatID > 0)
				sb.append(", sub_category_id = ").append(item.subCatID).append(" ");
			if (item.segmentID > 0)
				sb.append(", segment_id = ").append(item.segmentID).append(" ");
			if (item.uomId != null && !item.uomId.equals(""))
				sb.append(", UOM_ID = '").append(item.uomId).append("' ");
			if (!item.size.equals(""))
				sb.append(", ITEM_SIZE= '").append(item.size).append("' ");
			if (item.privateLabelCode != null && !item.privateLabelCode.equals(""))
				sb.append(", PRIVATE_LABEL_CODE = '").append(item.privateLabelCode).append("' ");
			if (item.privateLabelFlag != null && !item.privateLabelFlag.equals(""))
				sb.append(", PRIVATE_LABEL_IND = '").append(item.privateLabelFlag).append("' ");
			if (item.itemRank != null && !item.itemRank.equals(""))
				sb.append(", ITEM_DESC = '").append(item.itemRank).append("' ");
			else
				sb.append(", ITEM_DESC = NULL ");
			if (item.likeItemId > 0)
				sb.append(", RET_LIR_ID = ").append(item.likeItemId);
			else { // Change to update RET_LIR_ID as null when item is no more
					// part
					// of any LIG
				sb.append(", RET_LIR_ID = NULL");
			}

			ItemDTO existingItem = getItemDetails(conn, item);
			if ((existingItem.deptID != item.deptID) || (existingItem.catID != item.catID))
				item.updateTimeStamp = true;
			else
				item.updateTimeStamp = false;

			// Changes to incorporate Brand
			if (item.brandId > 0)
				sb.append(", BRAND_ID = ").append(item.brandId);
			else
				sb.append(", BRAND_ID = NULL");

			if (item.manufactId > 0)
				sb.append(", MANUFACTURER_ID = ").append(item.manufactId);
			else
				sb.append(", MANUFACTURER_ID = NULL");
			// Changes to incorporate Brand - Ends

			// Changes for incorporating Pre Price Ind
			if (item.prePriceInd != null && !item.prePriceInd.equals(""))
				sb.append(", PREPRICED_IND = '").append(item.prePriceInd).append("' ");
			else
				sb.append(", PREPRICED_IND = NULL ");
			// Changes for incorporating Pre Price Ind - Ends

			if (item.updateTimeStamp)
				sb.append(", UPDATE_TIMESTAMP = SYSDATE ");
			if (!checkRetailerItemCode) {
				sb.append(" WHERE UPC= '").append(item.upc).append("'");
				// logger.debug(sb.toString());
				int count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");

				// Try appending Standard UPC and see if that works.
				if (count == 0) {
					sb.append(" OR STANDARD_UPC= '").append(item.standardUPC).append("'");
					count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");

				}

				// Check update count and > 1, return error
			} else {
				// Changes to check retailer item code along with upc to
				// identify an
				// item for Ahold
				sb.append(" WHERE UPC= '").append(item.upc).append("' AND RETAILER_ITEM_CODE= '")
						.append(item.retailerItemCode).append("'");
				int count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");
			}
		}
		long uendTime = System.currentTimeMillis();
		logger.debug("Time taken to to complete UpdateItem : " + (uendTime - ustartTime));
	}
	/**
	 * Batch update for update items using list of items
	 * @param conn
	 * @throws GeneralException
	 */
	public void updateItemFromList(Connection conn) throws GeneralException {
		if(updateItemLookupList != null && !updateItemLookupList.isEmpty()){
			PreparedStatement stmt = null;
			// Get deptid and catid from item_lookup table
//			HashMap<Integer, ItemDTO> deptIdandCatIdFromItemLookupMap = getDeptIdandCatIdFromItemLookup(conn);
			long UILstartTime = System.currentTimeMillis();
			try {
				int noOfItem = 0;
				stmt = conn.prepareStatement(UPDATE_ITEM_WITH_TIMESTAMP);
				for (ItemDTO itemDTO : updateItemLookupList) {

					int counter = 0;
					stmt.setString(++counter, itemDTO.getItemName());
					stmt.setString(++counter, itemDTO.getRetailerItemCode());
					stmt.setString(++counter, itemDTO.getStandardUPC());
					stmt.setString(++counter, itemDTO.getUpc());
					stmt.setInt(++counter, itemDTO.getDeptID());
					stmt.setInt(++counter, itemDTO.getCatID());

					if (itemDTO.getSubCatID() > 0) {
						stmt.setInt(++counter, itemDTO.getSubCatID());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					if (itemDTO.getSegmentID() > 0) {
						stmt.setInt(++counter, itemDTO.getSegmentID());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					if (itemDTO.getUomId() != null && !itemDTO.getUomId().equals("")) {
						stmt.setString(++counter, itemDTO.getUomId());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					if (!itemDTO.getSize().equals("")) {
						stmt.setString(++counter, itemDTO.getSize());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					if (itemDTO.getPrivateLabelCode() != null && !itemDTO.getPrivateLabelCode().equals("")) {
						stmt.setString(++counter, itemDTO.getPrivateLabelCode());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					if (itemDTO.getPrivateLabelFlag() != null && !itemDTO.getPrivateLabelFlag().equals("")) {
						stmt.setString(++counter, itemDTO.getPrivateLabelFlag());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					if (itemDTO.getItemRank() != null && !itemDTO.getItemRank().equals("")) {
						stmt.setString(++counter, itemDTO.getItemRank());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					if (itemDTO.getLikeItemId() > 0) {
						stmt.setInt(++counter, itemDTO.getLikeItemId());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					// Changes to incorporate Brand
					if (itemDTO.getBrandId() > 0) {
						stmt.setInt(++counter, itemDTO.getBrandId());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					if (itemDTO.manufactId > 0) {
						stmt.setInt(++counter, itemDTO.manufactId);
					} else {
						stmt.setNull(++counter, Types.NULL);
					}

					// Changes for incorporating Pre Price Ind
					if (itemDTO.getPrePriceInd() != null && !itemDTO.getPrePriceInd().equals("")) {
						stmt.setString(++counter, itemDTO.getPrePriceInd());
					} else {
						stmt.setNull(++counter, Types.NULL);
					}
					stmt.setInt(++counter, itemDTO.getItemCode());
					stmt.addBatch();
					noOfItem++;
					if (noOfItem % Constants.BATCH_UPDATE_COUNT == 0) {
						stmt.executeBatch();
						stmt.clearBatch();
						noOfItem = 0;
					}
				}
				if (noOfItem > 0) {
					stmt.executeBatch();
					stmt.clearBatch();
				}
				updateItemLookupList.clear();
				
			} catch (SQLException e) {
				logger.error("Error while executing updateItemFromList() " + e);
				throw new GeneralException("Error while executing updateItemFromList() " + e);
			} finally {
				PristineDBUtil.close(stmt);
			}
		
			long UILendTime = System.currentTimeMillis();
			logger.debug("Time taken to to complete UpdateItemList : " + (UILendTime - UILstartTime));
		}
		
	}

	/**
	 * Get deptid, categoryId and itemCode values from item_lookup table
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, ItemDTO> getDeptIdandCatIdFromItemLookup(Connection conn) throws GeneralException {
		HashMap<Integer, ItemDTO> deptIdandCatIdFromItemLookup = new HashMap<Integer, ItemDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String query = new String(GET_DEPTID_AND_CATID_FROM_ITEMLOOKUP);
			statement = conn.prepareStatement(query);
			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				ItemDTO itemDTO = new ItemDTO();
				itemDTO.itemCode = resultSet.getInt("ITEM_CODE");
				itemDTO.deptID = resultSet.getInt("DEPT_ID");
				itemDTO.catID = resultSet.getInt("CATEGORY_ID");
				deptIdandCatIdFromItemLookup.put(itemDTO.itemCode, itemDTO);
			}

		} catch (SQLException e) {
			logger.error("Error while executing getDeptIdandCatIdFromItemLookup() " + e);
			throw new GeneralException("Error while executing getDeptIdandCatIdFromItemLookup() " + e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return deptIdandCatIdFromItemLookup;

	}

	public CachedRowSet getItemListFromSchedule(Connection conn, int[] schArr, boolean showFlavors) throws GeneralException {

		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		sb.append(" SELECT A.ITEM_CODE, A.ITEM_NAME, A.STANDARD_UPC, A.DEPT_NAME, A.CAT_NAME CATEGORY_NAME, ");
		sb.append(" A.DEPT_ID, A.CATEGORY_ID, A.RETAILER_ITEM_CODE, A.RET_LIR_NAME  LIKE_ITEM_GRP, A.ALT_ITEM_NAME, ");
		sb.append(" A.ITEM_SIZE, A.UOM_NAME UOM, A.SUB_CAT_NAME SUB_CATEGORY_NAME, ");
		sb.append(" A.MANUFACT_CODE, A.RETAILER_ITEM_CODE ");
		sb.append(" FROM ITEM_LOOKUP_VIEW A ");
		sb.append(" WHERE A.ITEM_CODE IN ");
		sb.append(" (Select distinct item_code from ");
		if (showFlavors)
			sb.append(" competitive_data ");
		else
			sb.append(" comp_data_view_no_flavor ");
		sb.append(" where schedule_id in ( ");
		for (int i = 0; i < schArr.length; i++) {
			sb.append(schArr[i]);
			if (i < schArr.length - 1)
				sb.append(",");
		}
		sb.append(" ) AND ITEM_NOT_FOUND_FLG='N' )");
		// sb.append(" AND ROWNUM < 100");
		sb.append(" ORDER BY DEPT_NAME, CAT_NAME, SUB_CAT_NAME, ITEM_NAME");
		logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "ITEMDAO - getItemListFromSchedule");
		logger.debug("Item list size - " + crs.size());
		return crs;

	}

	public int updateLikeItemGrp(Connection conn, ItemDTO item) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		int count = 0;
		if (item.likeItemGrp != null && !item.likeItemGrp.equals("")) {
			// logger.debug("Updating item group - " + item.likeItemGrp);
			sb.append("UPDATE ITEM_LOOKUP SET ");
			sb.append(" RET_LIR_ID = ").append(item.likeItemId).append(" ");
			sb.append(" WHERE STANDARD_UPC= '").append(item.standardUPC).append("'");
			count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");
			if (count != 1)
				count = -1;
		}
		return count;

	}

	public int updateRDSDesc(Connection conn, ItemDTO item) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		int count = 0;
		if (item.rdsItemName != null && !item.rdsItemName.equals("")) {
			// logger.debug("Updating item group - " + item.likeItemGrp);
			sb.append("UPDATE ITEM_LOOKUP SET ");
			sb.append(" ALT_ITEM_NAME = '").append(item.rdsItemName).append("' ");
			sb.append(" WHERE UPC= '").append(item.upc).append("'");
			count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");
			if (count != 1)
				count = -1;
		}
		return count;

	}

	public String populateUOM(Connection conn, String uom) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ID FROM UOM_LOOKUP WHERE NAME = ");
		sb.append("'" + uom.trim().toUpperCase() + "' ");

		String uomIDStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getUOM");
		/*
		 * if( uomIDStr == null){ //Create new Category StringBuffer uomInsert = new StringBuffer(); uomInsert.append(
		 * "INSERT INTO UOM_LOOKUP( id, name ) "); uomInsert.append( "values (uom_seq.nextval, ");
		 * uomInsert.append("'").append(uom.trim().toUpperCase()).append("')"); logger.debug(uomInsert.toString());
		 * PristineDBUtil.execute(conn, uomInsert, "UOM Insert"); uomIDStr = PristineDBUtil.getSingleColumnVal(conn, sb,
		 * "Insert segement - populateSegment"); } if( uomIDStr == null){ throw new GeneralException("Invalid Segment"); }
		 */
		return uomIDStr;
	}

	public String getLirNameforUPC(Connection conn, String upc) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT RET.RET_LIR_NAME FROM ITEM_LOOKUP IL, RETAILER_LIKE_ITEM_GROUP RET "
				+ "WHERE RET.RET_LIR_ID = IL.RET_LIR_ID AND IL.UPC = '" + upc + "'");
		String lirName = PristineDBUtil.getSingleColumnVal(conn, sb, "getLirName for UPC");
		return lirName;
	}

	public HashMap<String, String> getUOMList(Connection conn, String methodOfCalling) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT ID, NAME FROM UOM_LOOKUP ");
		HashMap<String, String> uomMap = new HashMap<String, String>();
		logger.debug("getUOMList SQL:" + sb.toString());

		try {

			CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "getUOMList");

			while (crs.next()) {
				String uomid = crs.getString("ID");
				String uom = crs.getString("NAME");
				if (methodOfCalling.equalsIgnoreCase("SALES_AGGR_DAILY"))
					uomMap.put(uomid, uom);
				else
					uomMap.put(uom, uomid);

			}
		} catch (GeneralException ge) {
			logger.error("Error while fetching UOM data..." + ge.getMessage());
			throw new GeneralException("getUOMList", ge);
		} catch (SQLException se) {
			logger.error("Error while fetching UOM data..." + se.getMessage());
			throw new GeneralException("getUOMList", se);
		}

		return uomMap;
	}

	public CachedRowSet getRelatedItemSet(Connection conn, int itemCode, boolean showFlavor) throws GeneralException {
		CachedRowSet crs = null;
		StringBuffer sb = new StringBuffer();
		// Fill in the query
		sb.append("select B.item_code RELATED_ITEM_CODE, B.Item_name from item_lookup_view A, item_lookup_view B where");
		sb.append(" a.item_code = ").append(itemCode).append(" and ");
		sb.append(" b.item_code <> a.item_code and");
		sb.append(" b.dept_id = a.dept_id and");
		sb.append(" b.category_id = a.category_id and");
		// sb.append(" b.sub_category_id = a.sub_category_id and");
		// sb.append(" a.segment_id = b.segment_id and");
		// sb.append(" b.sub_category_id is not null and");
		// sb.append(" b.segment_id is not null and");
		sb.append(" b.manufact_code = a.manufact_code and");
		sb.append(" b.uom_id = a.uom_id ");
		// sb.append(" and b.item_size <> a.item_size ");

		// logger.debug(sb);
		// execute the statement
		crs = PristineDBUtil.executeQuery(conn, sb, "ITEMDAO - getRelatedItemSet");
		// logger.debug( "Related Item list size - "+ crs.size());
		return crs;

	}

	public int setupRetailerLikeItem(Connection conn, String likeItemCode, String likeItemName) throws GeneralException {
		// Changes to fill gaps in RET_LIR_SEQ
		if (retLirIdList.size() <= 0) {
			setRetLirIdList(conn);
			Collections.sort(retLirIdList);
			logger.info("Size of LIR Item list" + retLirIdList.size());
		}
		// Changes to fill gaps in RET_LIR_SEQ - Ends

		String likeItemStr = null;
		StringBuffer sb = new StringBuffer();
		/*
		 * sb.append(" SELECT RET_LIR_ID FROM RETAILER_LIKE_ITEM_GROUP "); if( likeItemCode != null && !likeItemCode.equals(""))
		 * sb.append("WHERE RET_LIR_CODE = '" + likeItemCode+ "' "); else sb.append("WHERE RET_LIR_NAME = '" + likeItemName+ "' "
		 * );
		 */

		// Changed on 29th Apr 2014, to avoid duplicating of LIR NAME with same
		// name but with diff LIR CODE
		// Giving preference to RET_LIR_NAME than RET_LIR_CODE
		// Check if any record already exists with RET_LIR_NAME, if so don't
		// insert
		if (likeItemName != null && !likeItemName.equals("")) {
			if (retailerIdandNameMap != null) {
				logger.debug("Inside the hash map function in setupRetailerLikeItem");
				if (retailerIdandNameMap.get(likeItemName.trim().toUpperCase()) != null) {
					likeItemStr = String.valueOf(retailerIdandNameMap.get(likeItemName.trim().toUpperCase()));
					logger.debug("setupRetailerLikeItem value obtain in hash map:" + likeItemStr);
				}
			}
			// If hash map is null then it executes existing code to get values
			else {
				sb.append(" SELECT RET_LIR_ID FROM RETAILER_LIKE_ITEM_GROUP ");
				sb.append(" WHERE RET_LIR_NAME = '" + likeItemName + "' ");

				likeItemStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getRetailerLikeItem");
			}
			// Update RET_LIR_CODE
			if (likeItemStr != null && likeItemCode != null && !likeItemCode.equals("")) {
				sb = new StringBuffer();
				sb.append(" UPDATE RETAILER_LIKE_ITEM_GROUP SET RET_LIR_CODE = '");
				sb.append(likeItemCode);
				sb.append("' WHERE RET_LIR_NAME = '" + likeItemName + "' ");
				PristineDBUtil.executeUpdate(conn, sb, "Update RET_LIR_CODE");
				if (retailerIdandCodeMap != null) {
					retailerIdandCodeMap.put(likeItemCode, Integer.parseInt(likeItemStr));
				}
			}
		}

		// Added on 27th Oct 2014, to ignore checking of lir code for Ahold
		Boolean isIgnoreLirCode = false;
		isIgnoreLirCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEMLOAD.IGNORE_LIR_CODE", "false"));

		if (!isIgnoreLirCode) {
			// LIR_CODE doesn't need to be checked for ahold, as LIR_CODE
			// doesn't have clear meaning here
			if (likeItemStr == null) {
				// Check if any record already exists with RET_LIR_CODE, if so
				// don't insert otherwise insert
				if (likeItemCode != null && !likeItemCode.equals("")) {
					if (retailerIdandCodeMap != null) {
						if (retailerIdandCodeMap.get(likeItemCode) != null) {
							likeItemStr = String.valueOf(retailerIdandCodeMap.get(likeItemCode));
						}
					} else {
						sb = new StringBuffer();
						sb.append(" SELECT RET_LIR_ID FROM RETAILER_LIKE_ITEM_GROUP ");
						sb.append(" WHERE RET_LIR_CODE = '" + likeItemCode + "' ");
						likeItemStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getRetailerLikeItem");
					}
				}
			}
		}

		if (likeItemStr == null) {

			sb = new StringBuffer();
			sb.append(" INSERT INTO RETAILER_LIKE_ITEM_GROUP ");
			sb.append("(RET_LIR_ID, RET_LIR_CODE, RET_LIR_NAME)");
			sb.append(" VALUES ( ");

			// Changes to fill gaps in RET_LIR_SEQ
			boolean isRetLirIdSet = false;
			if (retLirIdList != null && retLirIdList.size() > 0)
				for (int i = retLirIdStart + 1; i <= retLirIdList.get(retLirIdList.size() - 1); i++) {
					if (retLirIdList.contains(i))
						continue;
					else {
						// sb.append("RET_LIR_SEQ.NEXTVAL,");
						sb.append(i);
						sb.append(",");
						retLirIdStart = i;
						isRetLirIdSet = true;
						break;
					}
				}
			if (!isRetLirIdSet) {
				sb.append("RET_LIR_SEQ.NEXTVAL,");
			}
			// Changes to fill gaps in RET_LIR_SEQ - Ends

			if (likeItemCode != null && !likeItemCode.equals(""))
				sb.append("'").append(likeItemCode).append("',");
			else
				sb.append("NULL,");
			sb.append("'").append(likeItemName).append("')");
			try {
				PristineDBUtil.execute(conn, sb, "ItemDAO - Insert Like Item");
			} catch (GeneralException e) {
				logger.info("Error in Like Item Insert- " + sb.toString());
				throw e;
			}
			// Get the Like item code
			sb = new StringBuffer();
			sb.append(" SELECT RET_LIR_ID FROM RETAILER_LIKE_ITEM_GROUP WHERE RET_LIR_NAME = ");
			sb.append("'" + likeItemName + "' ");
			likeItemStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getRetailerLikeItem");
			if (likeItemStr != null) {
				if (retailerIdandNameMap != null) {
					retailerIdandNameMap.put(likeItemName.trim().toUpperCase(), Integer.parseInt(likeItemStr));
				}
				if (retailerIdandCodeMap != null) {
					retailerIdandCodeMap.put(likeItemCode, Integer.parseInt(likeItemStr));
				}
			}
		}
		return Integer.parseInt(likeItemStr);

	}

	public int setupBrand(Connection conn, String brandName, String brandCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" SELECT BRAND_ID FROM BRAND_LOOKUP WHERE BRAND_NAME = ");
		sb.append("'" + brandName + "' ");

		String brandIdStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getBrandIdFromName");

		if (brandIdStr == null) {

			sb = new StringBuffer();
			sb.append(" INSERT INTO BRAND_LOOKUP ");
			sb.append("(BRAND_ID, BRAND_CODE, BRAND_NAME)");
			sb.append(" VALUES ( ");
			sb.append("BRAND_ID_SEQ.NEXTVAL,");
			if (brandCode != null && !brandCode.equals(""))
				sb.append("'").append(brandCode).append("',");
			else
				sb.append("NULL,");
			sb.append("'").append(brandName).append("')");
			PristineDBUtil.execute(conn, sb, "ItemDAO - Insert brand");
			// Get the Like item code
			sb = new StringBuffer();
			sb.append("  SELECT BRAND_ID FROM BRAND_LOOKUP WHERE BRAND_NAME  = ");
			sb.append("'" + brandName + "' ");
			brandIdStr = PristineDBUtil.getSingleColumnVal(conn, sb, "getBrandIdFromName");
		}
		return Integer.parseInt(brandIdStr);
	}

	public int updateBrandInfo(Connection conn, ItemDTO item) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		int count = 0;
		if (item.brandId > 0) {
			sb.append("UPDATE ITEM_LOOKUP SET ");
			sb.append(" BRAND_ID = ").append(item.brandId).append(" ");
			sb.append(" WHERE STANDARD_UPC= '").append(item.standardUPC).append("'");
			count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");
			if (count != 1)
				count = -1;
		}
		return count;

	}

	public void clearItemRank(Connection conn) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" update item_lookup set ITEM_DESC = NULL");
		int count = PristineDBUtil.executeUpdate(conn, sb, "Update Item Rank");

	}

	public void updatePriceIndexFlag(Connection conn, int schId) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" update item_lookup set pi_analyze_flag ='Y'  ");
		sb.append(" where item_code In   ");
		sb.append(" (select item_code from competitive_data_view ");
		sb.append(" where schedule_id = ").append(schId).append(" and ret_lir_id is null) ");
		int count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");

	}

	public void updateActiveIndicatorFlag(Connection conn, int itemCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" update item_lookup set ACTIVE_INDICATOR ='N', UPDATE_TIMESTAMP = SYSDATE  ");
		if (itemCode > 0) {
			sb.append(" WHERE ITEM_CODE = ").append(itemCode);
		}
		int count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");
		logger.info("No of Items updated " + count);
	}

	public void updateActiveIndicatorFlagV2(Connection conn, int itemCode, int ligItemCodeStart) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" update item_lookup set ACTIVE_INDICATOR ='N' ");
		sb.append(" WHERE ITEM_CODE < ").append(ligItemCodeStart);
		if (itemCode > 0) {
			sb.append(" WHERE ITEM_CODE = ").append(itemCode);
		}
		int count = PristineDBUtil.executeUpdate(conn, sb, "Update Item");
		logger.info("No of Items updated " + count);
	}

	public int updateItemFlags(Connection conn, ArrayList<ItemDTO> updateItemList) throws GeneralException {

		int updateCount = 0;
		PreparedStatement statement = null;
		String UPDATE_ITEM_FLAGS = "UPDATE ITEM_LOOKUP SET ACTIVE_INDICATOR = 'Y', PREPRICED_IND = ? WHERE UPC = ?";
		try {
			statement = conn.prepareStatement(UPDATE_ITEM_FLAGS);

			int itemsInBatch = 0;
			for (ItemDTO itemDTO : updateItemList) {
				int counter = 0;
				statement.setString(++counter, itemDTO.getPrePriceInd());
				statement.setString(++counter, itemDTO.getUpc());
				statement.addBatch();
				itemsInBatch++;
				if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					statement.clearBatch();
					itemsInBatch = 0;
					updateCount += PristineDBUtil.getUpdateCount(count);
				}

			}
			if (itemsInBatch > 0) {
				int[] count = statement.executeBatch();
				statement.clearBatch();
				updateCount += PristineDBUtil.getUpdateCount(count);
			}

		} catch (SQLException e) {
			logger.error("Error while executing UPDATE_ITEM_FLAGS" + e.getMessage());
			throw new GeneralException("Error while executing UPDATE_ITEM_FLAGS", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return updateCount;
	}

	public int deactivateItemByUPC(Connection conn, String upc, String retailerItemCode, boolean isLigRepItem) throws GeneralException {
		boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		if (isLigRepItem) {
			upc = 'L' + upc;
			retailerItemCode = 'L' + retailerItemCode;
		}
		StringBuffer sb = new StringBuffer();
		sb.append(" update item_lookup set ACTIVE_INDICATOR ='N', UPDATE_TIMESTAMP = SYSDATE  ");
		if (!upc.isEmpty()) {
			sb.append(" WHERE UPC = '").append(upc).append("'");
			if (checkRetailerItemCode)
				sb.append(" AND RETAILER_ITEM_CODE = '").append(retailerItemCode).append("'");
		}
		// logger.debug("DEACTIVE SQL " + sb.toString());
		int count = PristineDBUtil.executeUpdate(conn, sb, "Deactivate Item");
		return count;
		// logger.debug("Update count......" + count);
	}

	public ItemDTO getItemDetails(Connection conn, ItemDTO item) throws GeneralException {
		boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT item_code, item_name, ");
		sb.append("upc, dept_id, category_id,  RETAILER_ITEM_CODE, STANDARD_UPC, sub_category_id, SEGMENT_ID,RET_LIR_ID ");
		sb.append("from ITEM_LOOKUP WHERE ");
		if (!checkRetailerItemCode) {
			if (item.upc != null && !item.upc.equals(""))
				sb.append(" UPC = '" + item.upc + "'");
			else if (item.retailerItemCode != null && !item.retailerItemCode.equals(""))
				sb.append(" RETAILER_ITEM_CODE = '" + item.retailerItemCode + "'");
			else
				return null;
		} else {
			sb.append(" UPC = '" + item.upc + "' AND ");
			sb.append(" RETAILER_ITEM_CODE = '" + item.retailerItemCode + "'");
		}

		ItemDTO retItem = null;
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");

		// Trying looking up using Standard UPC - 1/24/12
		if (!checkRetailerItemCode) {
			if (result.size() == 0) {
				if (item.standardUPC != null && !item.standardUPC.equals(""))
					sb.append(" OR STANDARD_UPC = '" + item.standardUPC + "'");
				result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");

			}
		}

		try {
			if (result.next()) {
				retItem = new ItemDTO();
				retItem.itemCode = result.getInt("ITEM_CODE");
				retItem.itemName = result.getString("ITEM_NAME");
				retItem.upc = result.getString("UPC");
				retItem.deptID = result.getInt("DEPT_ID");
				retItem.catID = result.getInt("CATEGORY_ID");
				retItem.retailerItemCode = result.getString("RETAILER_ITEM_CODE");
				retItem.standardUPC = result.getString("STANDARD_UPC");
				retItem.subCatID = result.getInt("sub_category_id");
				retItem.segmentID = result.getInt("SEGMENT_ID");
				retItem.likeItemId = result.getInt("RET_LIR_ID");
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return retItem;
	}

	/**
	 * 
	 * @param conn
	 * @param item
	 * @return map of all items
	 * @throws GeneralException
	 */
	public HashMap<ItemDetailKey, ItemDTO> getItemDetailsForAllItemsWithCheckRetItemCode(Connection conn) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT ITEM_CODE, ITEM_NAME, ");
		sb.append("UPC, DEPT_ID, CATEGORY_ID,  RETAILER_ITEM_CODE, STANDARD_UPC, SUB_CATEGORY_ID, SEGMENT_ID ");
		sb.append("FROM ITEM_LOOKUP ");
		HashMap<ItemDetailKey, ItemDTO> allItemsLookup = new HashMap<>();
		ResultSet result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");
		try {
			while (result.next()) {
				ItemDTO retItem = new ItemDTO();
				retItem.itemCode = result.getInt("ITEM_CODE");
				retItem.itemName = result.getString("ITEM_NAME");
				retItem.upc = result.getString("UPC");
				retItem.deptID = result.getInt("DEPT_ID");
				retItem.catID = result.getInt("CATEGORY_ID");
				retItem.retailerItemCode = result.getString("RETAILER_ITEM_CODE");
				retItem.standardUPC = result.getString("STANDARD_UPC");
				retItem.subCatID = result.getInt("SUB_CATEGORY_ID");
				retItem.segmentID = result.getInt("SEGMENT_ID");
				ItemDetailKey itemDetailKey = new ItemDetailKey(retItem.upc, retItem.retailerItemCode);
				allItemsLookup.put(itemDetailKey, retItem);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return allItemsLookup;
	}

	/**
	 * 
	 * @param conn
	 * @param item
	 * @return map of all items
	 * @throws GeneralException
	 */
	public HashMap<String, ItemDTO> getItemDetailsForAllItems(Connection conn) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT item_code, item_name, ");
		sb.append("upc, dept_id, category_id,  RETAILER_ITEM_CODE, STANDARD_UPC, sub_category_id, SEGMENT_ID ");
		sb.append("from ITEM_LOOKUP ");
		HashMap<String, ItemDTO> allItemsLookup = new HashMap<>();
		ResultSet result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");
		try {
			while (result.next()) {
				ItemDTO retItem = new ItemDTO();
				retItem.itemCode = result.getInt("ITEM_CODE");
				retItem.itemName = result.getString("ITEM_NAME");
				retItem.upc = result.getString("UPC");
				retItem.deptID = result.getInt("DEPT_ID");
				retItem.catID = result.getInt("CATEGORY_ID");
				retItem.retailerItemCode = result.getString("RETAILER_ITEM_CODE");
				retItem.standardUPC = result.getString("STANDARD_UPC");
				retItem.subCatID = result.getInt("sub_category_id");
				retItem.segmentID = result.getInt("SEGMENT_ID");
				allItemsLookup.put(retItem.upc, retItem);
			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return allItemsLookup;
	}

	public ItemDTO getItemDetailsFuzzyLookup(Connection conn, ItemDTO item) throws GeneralException {
		StringBuffer sb = new StringBuffer();

		String bletter = "";
		if (item.getItemName().length() == 0) {
			return null;
		} else {
			bletter = item.getItemName().toLowerCase().substring(0, 1);
		}

		sb.append("SELECT I.item_code, I.item_name, ");
		sb.append("I.upc, I.dept_id, I.category_id,  RM.RETAILER_ITEM_CODE, I.STANDARD_UPC, I.sub_category_id, I.SEGMENT_ID, I.ret_lir_id ");
		sb.append("from RETAILER_ITEM_CODE_MAP RM, ITEM_LOOKUP I WHERE ");
		if (item.retailerItemCode != null && !item.retailerItemCode.equals("")) {
			String rIC = item.retailerItemCode.trim();
			int pos = item.retailerItemCode.indexOf("-");
			if (pos != -1) {
				rIC = rIC.substring(pos + 1);
			}
			while (rIC.length() < 6) {
				rIC = "0" + rIC;
			}
			sb.append(" RM.RETAILER_ITEM_CODE like '%" + rIC
					+ "'  and RM.ITEM_CODE = I.ITEM_CODE and I.active_indicator = 'Y' and lower(I.item_name) like '" + bletter + "%'");
		} else {
			return null;
		}

		ItemDTO retItem = null;
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");

		// Trying looking up using Standard UPC - 1/24/12
		if (result.size() == 0) {
			sb = new StringBuffer();
			sb.append("SELECT I.item_code, I.item_name, ");
			sb.append("I.upc, I.dept_id, I.category_id,  RM.RETAILER_ITEM_CODE, I.STANDARD_UPC, I.sub_category_id, I.SEGMENT_ID, ret_lir_id ");
			sb.append("from RETAILER_ITEM_CODE_MAP RM, ITEM_LOOKUP I WHERE ");
			if (item.retailerItemCode != null && !item.retailerItemCode.equals("")) {
				String rIC = item.retailerItemCode.trim();
				int pos = item.retailerItemCode.indexOf("-");
				if (pos != -1) {
					rIC = rIC.substring(pos + 1);
				}
				if (rIC.length() < 6) {
					rIC = "0" + rIC;
				}
				sb.append(" RM.RETAILER_ITEM_CODE like '%" + rIC + "'  and RM.ITEM_CODE = I.ITEM_CODE and lower(item_name) like '" + bletter + "%'");
			} else {
				return null;
			}
			result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");

		}
		boolean multiple = false;
		if (result.size() > 1) {
			multiple = true;
		}

		try {
			int qPos = item.getItemName().indexOf("'");
			int sPos = item.getItemName().indexOf(" ");
			int d = 0;
			int p = -1;
			if (qPos == -1) {
				p = sPos;
			} else if (sPos == -1) {
				p = qPos;
			} else {
				p = Math.min(qPos, sPos);
			}
			if (p == -1) {
				p = item.getItemName().length();
			}
			String match = item.getItemName().substring(0, p).toLowerCase();
			while (result.next()) {
				retItem = new ItemDTO();
				retItem.itemCode = result.getInt("ITEM_CODE");
				retItem.itemName = result.getString("ITEM_NAME");
				retItem.upc = result.getString("UPC");
				retItem.deptID = result.getInt("DEPT_ID");
				retItem.catID = result.getInt("CATEGORY_ID");
				retItem.retailerItemCode = result.getString("RETAILER_ITEM_CODE");
				retItem.standardUPC = result.getString("STANDARD_UPC");
				retItem.subCatID = result.getInt("sub_category_id");
				retItem.segmentID = result.getInt("SEGMENT_ID");
				retItem.likeItemId = result.getInt("RET_LIR_ID");
				if (retItem.itemName.toLowerCase().startsWith(match)) {
					return retItem;
				}
			}
			if (multiple) {
				logger.debug("multiple match: " + item.retailerItemCode);
				return null;
			}
			if (!multiple && retItem != null) {
				if (!retItem.itemName.toLowerCase().startsWith(bletter)) {
					return null;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return retItem;
	}

	public List<ItemDTO> getItemDetailsFuzzyLookupV2(Connection conn, ItemDTO item) throws GeneralException {
		List<ItemDTO> itemList = new ArrayList<ItemDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT I.item_code, I.item_name, ");
		sb.append("I.upc, I.dept_id, I.category_id,  RM.RETAILER_ITEM_CODE, I.STANDARD_UPC, I.sub_category_id, I.SEGMENT_ID, I.ret_lir_id ");
		sb.append("from RETAILER_ITEM_CODE_MAP RM, ITEM_LOOKUP I WHERE ");
		if (item.retailerItemCode != null && !item.retailerItemCode.equals("")) {
			String rIC = item.retailerItemCode.trim();
			int pos = item.retailerItemCode.indexOf("-");
			if (pos != -1) {
				rIC = rIC.substring(pos + 1);
			}
			while (rIC.length() < 6) {
				rIC = "0" + rIC;
			}
			sb.append(" RM.RETAILER_ITEM_CODE like '%" + rIC + "'  and RM.ITEM_CODE = I.ITEM_CODE and I.active_indicator = 'Y'");// and
																																	// lower(I.item_name)
																																	// like
																																	// '"+bletter+"%'
		} else {
			return itemList;
		}

		ItemDTO retItem = null;
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");

		// Trying looking up using Standard UPC - 1/24/12
		if (result.size() == 0) {
			sb = new StringBuffer();
			sb.append("SELECT I.item_code, I.item_name, ");
			sb.append("I.upc, I.dept_id, I.category_id,  RM.RETAILER_ITEM_CODE, I.STANDARD_UPC, I.sub_category_id, I.SEGMENT_ID, ret_lir_id ");
			sb.append("from RETAILER_ITEM_CODE_MAP RM, ITEM_LOOKUP I WHERE ");
			if (item.retailerItemCode != null && !item.retailerItemCode.equals("")) {
				String rIC = item.retailerItemCode.trim();
				int pos = item.retailerItemCode.indexOf("-");
				if (pos != -1) {
					rIC = rIC.substring(pos + 1);
				}
				if (rIC.length() < 6) {
					rIC = "0" + rIC;
				}
				sb.append(" RM.RETAILER_ITEM_CODE like '%" + rIC + "'  and RM.ITEM_CODE = I.ITEM_CODE"); // and
																											// lower(item_name)
																											// like
																											// '"+bletter+"%'
			} else {
				return itemList;
			}
			result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");

		}
		try {
			while (result.next()) {
				retItem = new ItemDTO();
				retItem.itemCode = result.getInt("ITEM_CODE");
				retItem.itemName = result.getString("ITEM_NAME");
				retItem.upc = result.getString("UPC");
				retItem.deptID = result.getInt("DEPT_ID");
				retItem.catID = result.getInt("CATEGORY_ID");
				retItem.retailerItemCode = result.getString("RETAILER_ITEM_CODE");
				retItem.standardUPC = result.getString("STANDARD_UPC");
				retItem.subCatID = result.getInt("sub_category_id");
				retItem.segmentID = result.getInt("SEGMENT_ID");
				retItem.likeItemId = result.getInt("RET_LIR_ID");
				itemList.add(retItem);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new GeneralException("Cached Rowset Access Exception", e);
		} finally {
			PristineDBUtil.close(result);
		}
		return itemList;
	}

	public boolean isRetailerItemCodeExist(Connection conn, ItemDTO item) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT COUNT(*) FROM RETAILER_ITEM_CODE_MAP WHERE ");
		sb.append(" RETAILER_ITEM_CODE = ?");
		sb.append(" AND UPC = ?");

		PreparedStatement stmt = null;
		ResultSet rs = null;
		int count = 0;
		try {
			stmt = conn.prepareStatement(sb.toString());
			stmt.setString(1, item.getRetailerItemCode());
			stmt.setString(2, item.getUpc());
			rs = stmt.executeQuery();
			if (rs.next()) {
				count = rs.getInt(1);
			}
		} catch (SQLException sqlException) {
			logger.error("Error while executing isRetailerItemCodeExist" + sqlException);
			throw new GeneralException("Error while executing isRetailerItemCodeExist" + sqlException);
		} finally {
			PristineDBUtil.close(stmt);
		}
		boolean value = true;
		if (count != 1)
			value = false;
		return value;

	}

	public void addRetailerItemCode(Connection conn, ItemDTO item) throws GeneralException {

		// Delete and Insert

		/*
		 * StringBuffer sb = new StringBuffer(); sb.append("DELETE FROM RETAILER_ITEM_CODE_MAP WHERE "); sb.append(
		 * " RETAILER_ITEM_CODE = '" + item.retailerItemCode+ "'"); sb.append(" AND UPC = '" + item.upc+ "'");
		 * 
		 * PristineDBUtil.execute(conn, sb, "delete from retailer_Item_code_map");
		 */

		// logger.debug("retailer Item Code/ UPC " + item.retailerItemCode + " / " + item.upc);

		StringBuffer sb = new StringBuffer();
		sb.append("INSERT INTO RETAILER_ITEM_CODE_MAP(RETAILER_ITEM_CODE, UPC, ITEM_CODE, ACTIVE_INDICATOR, UPDATE_TIMESTAMP) VALUES (");
		sb.append(" ?, ?, ?, ?, SYSDATE) ");
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sb.toString());
			stmt.setString(1, item.retailerItemCode);
			stmt.setString(2, item.upc);
			stmt.setInt(3, item.itemCode);
			stmt.setString(4, String.valueOf(Constants.YES));
			int count = stmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("Error while executing addRetailerItemCode " + e);
			throw new GeneralException("Error while executing addRetailerItemCode " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public ArrayList<String> getItemFromRetailItemCodeMap(Connection conn, ItemDTO item) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT RETAILER_ITEM_CODE, UPC, ITEM_CODE ");
		sb.append("from RETAILER_ITEM_CODE_MAP WHERE ");
		if (item.upc != null && !item.upc.equals(""))
			sb.append(" UPC = '" + item.upc + "'");
		else if (item.retailerItemCode != null && !item.retailerItemCode.equals(""))
			sb.append(" RETAILER_ITEM_CODE = '" + item.retailerItemCode + "'");
		else
			return null;

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");

		ArrayList<String> upcList = null;
		if (result.size() > 0)
			upcList = new ArrayList<String>();

		try {
			while (result.next()) {
				String upc = result.getString("UPC");
				upcList.add(PrestoUtil.castUPC(upc, true));

			}
		} catch (Exception e) {
			throw new GeneralException("Cached Rowset Access Exception", e);
		}
		return upcList;
	}

	public boolean isUpcActiveInRetailerItemCodeMap(Connection conn, String upc, String retailerItemCode) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT DISTINCT(RM.ACTIVE_INDICATOR) AS ACTIVE_INDICATOR FROM RETAILER_ITEM_CODE_MAP RM, ITEM_LOOKUP IL WHERE ");
		sb.append(" RM.ITEM_CODE = IL.ITEM_CODE ");
		sb.append(" AND RM.UPC = ?");
		sb.append(" AND IL.RETAILER_ITEM_CODE = ?");

		PreparedStatement stmt = null;
		ResultSet rs = null;
		boolean isActive = false;
		try {
			stmt = conn.prepareStatement(sb.toString());
			stmt.setString(1, PrestoUtil.castUPC(upc, false));
			stmt.setString(2, retailerItemCode);
			rs = stmt.executeQuery();
			while (rs.next()) {
				if (String.valueOf(Constants.YES).equalsIgnoreCase(rs.getString("ACTIVE_INDICATOR"))) {
					isActive = true;
				}
			}
		} catch (SQLException sqlException) {
			logger.error("Error while executing isUpcActiveInRetailerItemCodeMap" + sqlException);
			throw new GeneralException("Error while executing isUpcActiveInRetailerItemCodeMap" + sqlException);
		} finally {
			PristineDBUtil.close(stmt);
		}

		return isActive;

	}

	public boolean updateNewLirItemCode(Connection conn, int lirItemCodePrev, int lirItemCodeNew) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" UPDATE COMPETITIVE_DATA_LIG SET ");
		sb.append(" ITEM_CODE = ").append(lirItemCodeNew);
		sb.append(" WHERE ITEM_CODE = ").append(lirItemCodePrev);

		int count = PristineDBUtil.executeUpdate(conn, sb, "Update LIR Item");

		sb = new StringBuffer();
		sb.append(" UPDATE MOVEMENT_WEEKLY_LIG SET");
		sb.append(" ITEM_CODE = ").append(lirItemCodeNew);
		sb.append(" WHERE ITEM_CODE = ").append(lirItemCodePrev);

		count = PristineDBUtil.executeUpdate(conn, sb, "Update LIR Item");

		return true;
	}

	// Added changes to take unused seq for Lir item code..By Dinesh(10/13/2016)
	public void setLirItem(Connection conn, int retLirId, int modelItemCode) throws GeneralException {
		String lirItemCode = null;
		PreparedStatement statement = null;
		boolean useUnusedSeq = false;
		// TO use unused sequence in Lir item code configuration must be TRUE..
		if (PropertyManager.getProperty("USE_UNUSED_LIR_ITEM_CODE_SEQUENCE") != null) {
			useUnusedSeq = Boolean.parseBoolean(PropertyManager.getProperty("USE_UNUSED_LIR_ITEM_CODE_SEQUENCE"));
		}
		if (useUnusedSeq) {
			lirItemCode = getUnusedLirSeqFromItemLookup(conn);
		} else if (lirItemCode == null || lirItemCode == "") {
			lirItemCode = getLirItemCode(conn);
		}

		try {
			statement = conn.prepareStatement(INSERT_LIR_ITEM_INTO_ITEM_LOOKUP);
			int counter = 0;
			statement.setString(++counter, lirItemCode);
			statement.setInt(++counter, modelItemCode);
			statement.executeUpdate();

		} catch (SQLException SqlE) {
			logger.error("setLirItem() - Error while Insert Like Item. Details of LIR item code: " + lirItemCode + " " + "and Lig member Item code: "
					+ modelItemCode);
			throw new GeneralException("setLirItem() - Error while Insert Like Item", SqlE);
		} finally {
			PristineDBUtil.close(statement);
		}

	}

	public void setLirItemInProductGroup(Connection conn, int itemCode) throws GeneralException {
		int lirItemCode = -1;
		ProductGroupDAO productGroupDAO = new ProductGroupDAO();

		lirItemCode = itemCode;
		if (lirItemCode > 0) {
			ProductDTO productDTO = new ProductDTO();
			productDTO.setChildProductId(lirItemCode);
			productDTO.setChildProductLevelId(Constants.ITEMLEVELID);
			int parentLevelId = productGroupDAO.getParentOfItem(conn);
			productDTO.setProductLevelId(parentLevelId);
			int parentId = productGroupDAO.getParentProductIdFromProductRelation(conn, productDTO);
			// logger.debug(lirItemCode + "\t" + parentId + "\t" +
			// parentLevelId);
			if (parentId > 0) {
				productGroupDAO.updateProductGroupRelation(conn, lirItemCode, parentLevelId);
			} else {
				productGroupDAO.insertProductGroupRelation(conn, lirItemCode, parentLevelId);
			}
		}
	}

	/*
	 * This method used to get the product group id Argument 1 : Product Level Id Argument 2 : Product Id Argument 3 : Product
	 * Name
	 * 
	 * @throws GeneralException, SqlException
	 */
	public int getGroupId(Connection conn, int levelId, String code, String name) throws GeneralException, SQLException {

		int retVal = -1;
		StringBuffer sql = new StringBuffer();
		if (code == null || code.equals("")) {
			// Lookup by name
			sql.append(" SELECT PRODUCT_ID FROM PRODUCT_GROUP_T WHERE NAME = '" + name + "' and PRODUCT_LEVEL_ID =" + levelId + "");
		} else {
			// Lookup by code
			sql.append(" SELECT PRODUCT_ID FROM PRODUCT_GROUP_T WHERE CODE = '" + code + "' and PRODUCT_LEVEL_ID = " + levelId + "");
		}
		// logger.debug("Sql" +sql.toString());
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "Get Group Id Method");
		if (result.next()) {
			retVal = result.getInt("PRODUCT_ID");
		}
		// logger.debug("Product Id : " + retVal);
		return retVal;

	}

	public int deleteProductGroupRelation(Connection conn, int productId, int productLevelId, int childProductId, int childProductLevelId)
			throws GeneralException {

		int retVal = 0;

		try {
			StringBuffer sql = new StringBuffer();

			sql.append(" delete from PRODUCT_GROUP_RELATION_T ");
			sql.append(" where  PRODUCT_ID=").append(productId);
			sql.append(" and  CHILD_PRODUCT_ID=").append(childProductId);
			sql.append(" and PRODUCT_LEVEL_ID=").append(productLevelId);
			sql.append(" and CHILD_PRODUCT_LEVEL_ID=").append(childProductLevelId);

			retVal = PristineDBUtil.executeUpdate(conn, sql, "Delete Product Group Relation.....");
		} catch (GeneralException e) {
			logger.error(" Error in delete Processing....", e);
			throw new GeneralException(" Error In delete Processing....", e);
		}

		return retVal;
	}

	/*
	 * Method used to Check the Product Group relation avilability in product_group_relation Argument 1 : Connection Argument 2 :
	 * ProductId Argument 3 : Child Product Id Argument 4 : Product Level Id
	 * 
	 * @throws GeneralException,SQLException
	 */
	public int getProductChildDetails(Connection conn, String productName, int productLevelId) throws GeneralException {

		int productId = 0;

		StringBuffer sql = new StringBuffer();
		sql.append(" select PRODUCT_ID  from PRODUCT_GROUP_T");
		sql.append(" where NAME='").append(productName).append("'");
		sql.append(" and PRODUCT_LEVEL_ID=").append(productLevelId);

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "checkProductGroupRealtion");

			if (result.next()) {
				productId = result.getInt("PRODUCT_ID");

			}
		} catch (SQLException e) {
			logger.error(e);
			throw new GeneralException("getProductChildDetails", e);
		}

		return productId;
	}

	public HashMap<String, Integer> getUPCAndItem(Connection conn) throws GeneralException {

		HashMap<String, Integer> itemHashMap = new HashMap<String, Integer>();

		CachedRowSet itemRS = null;

		StringBuffer sb = new StringBuffer();
		sb.append("select UPC, ITEM_CODE from item_lookup");
		logger.debug(sb);

		// execute the statement
		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getUPCAndItem");
			while (itemRS.next()) {
				itemHashMap.put(itemRS.getString("UPC"), itemRS.getInt("ITEM_CODE"));

				int size = itemHashMap.size();
				if ((size % 25000) == 0) {
					logger.debug("getUPCAndItem: processed " + String.valueOf(size));
				}
			}

		} catch (SQLException e) {

			e.printStackTrace();
		}

		return itemHashMap;
	}

	public HashMap<String, Integer> getUOMAndName(Connection conn) throws GeneralException {

		HashMap<String, Integer> itemHashMap = new HashMap<String, Integer>();

		CachedRowSet itemRS = null;

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT ID, NAME FROM UOM_LOOKUP WHERE ID IS NOT NULL AND NAME IS NOT NULL");
		logger.debug(" getUOMAndName SQL: " + sb);

		// execute the statement
		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getUOMAndName");
			while (itemRS.next()) {
				if (!itemHashMap.containsKey(itemRS.getString("NAME")))
					itemHashMap.put(itemRS.getString("NAME"), itemRS.getInt("ID"));

				int size = itemHashMap.size();
				if ((size % 25000) == 0) {
					logger.debug("getUOMAndName: processed " + String.valueOf(size));
				}
			}
		} catch (SQLException e) {
			logger.error("getUOMAndName: " + e.getMessage());
			e.printStackTrace();
		}

		return itemHashMap;
	}

	/**
	 * returns item code from ITEM_LOOKUP table if the given UPC is available. It will check the given upc in both UPC and
	 * STANDARD_UPC columns.
	 * 
	 * @param conn
	 *            Connection
	 * @param inputUPC
	 *            String
	 * @throws GeneralException
	 */
	public int getItemCodeForUPC(Connection conn, String inputUPC) throws GeneralException {

		int itemCode = -1;

		CachedRowSet itemRS = null;

		StringBuffer sb = new StringBuffer();
		sb.append("select ITEM_CODE from item_lookup where UPC ='").append(inputUPC).append("'");
		StringBuffer sb1 = new StringBuffer();
		sb1.append("select ITEM_CODE from item_lookup where STANDARD_UPC ='").append(inputUPC).append("'");
		// logger.debug(sb);

		// execute the statement
		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getItemCodeForUPC");
			while (itemRS.next()) {
				itemCode = itemRS.getInt("ITEM_CODE");
			}
			// added to check STANDARD_UPC column also.
			if (itemCode == -1) {
				itemRS = PristineDBUtil.executeQuery(conn, sb1, "ItemDAO - getItemCodeForStdUPC");
				while (itemRS.next()) {
					itemCode = itemRS.getInt("ITEM_CODE");
				}
			}

		} catch (SQLException e) {
			logger.error(" Error in GetitemCode For Upc.... ", e);
			throw new GeneralException("  Error in GetitemCode For Upc.... ", e);
		}

		return itemCode;
	}

	public boolean excuteBatch(PreparedStatement psmt, String modeOfprocess) throws SQLException {
		boolean insertFlag = false;
		try {
			int[] count = psmt.executeBatch();
			insertFlag = true;
			logger.debug(modeOfprocess + "..." + count.length);

		} catch (Exception exe) {
			logger.error(exe);
			throw new SQLException(exe);
		}
		return insertFlag;
	}

	public int getcategoryId(Connection _Conn, String categoryCode) throws GeneralException {

		StringBuffer sql = new StringBuffer();
		int categoryId = 0;
		sql = new StringBuffer(" select ID from CATEGORY ");
		sql.append(" where CAT_CODE=").append(categoryCode);

		// logger.debug("Cate Code SQl" + sql.toString());
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(_Conn, sql, "Get category Id");
			if (result.next()) {
				categoryId = result.getInt("ID");

			}
		} catch (Exception e) {
			logger.error(e);
			throw new GeneralException("getcategoryCode", e);
		}

		return categoryId;

	}

	/**
	 * Returns the UPC and RET_LIR_ID of the item if it represents a LIG. Returns 0, if any catch occurs / RET_LIR_ID not found
	 * 
	 * @param conn
	 * @param itemCode
	 * @return
	 */
	public String getUpcAndRetLirIdOfLigItem(Connection conn, int itemCode) {

		int retLirId = 0;
		String upc = "";
		String upcAndRetLirId = upc + "," + retLirId;
		StringBuffer sql = new StringBuffer();

		sql = new StringBuffer(" SELECT UPC,RET_LIR_ID FROM ITEM_LOOKUP WHERE UPC = ");
		sql.append(" (SELECT 'L'||UPC FROM ITEM_LOOKUP WHERE ITEM_CODE = ").append(itemCode).append(")");

		logger.debug("getUpcAndRetLirIdOfLigItem() query - "+sql.toString());
		
		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getRetLirIdOfLigItem");
			while (result.next()) {
				
				upc = result.getString("UPC");				
				retLirId = result.getInt("RET_LIR_ID");
								
				upcAndRetLirId = upc + "," + retLirId;
			}
		} catch (GeneralException e) {
			retLirId = 0;
			upc = "";
		} catch (SQLException e) {
			retLirId = 0;
			upc = "";
		}

		return upcAndRetLirId;
	}

	/**
	 * Returns the UPC and RET_LIR_ID of the item if it represents a LIG. Returns 0, if any catch occurs / RET_LIR_ID not found
	 * 
	 * @param conn
	 * @param itemCode
	 * @return
	 * @throws GeneralException 
	 * @throws SQLException 
	 */
	public String getUpcRetItemCodeAndRetLirIdOfLigItem(Connection conn, int itemCode)
			{

		int retLirId = 0;
		String upc = "";
		String retItemCode = "";
		String upcAndRetLirId = upc + "," + retItemCode + "," + retLirId;
		StringBuffer sql = new StringBuffer();

		sql = new StringBuffer(
				" SELECT UPC,RETAILER_ITEM_CODE,RET_LIR_ID FROM ITEM_LOOKUP WHERE (UPC,RETAILER_ITEM_CODE) IN ");
		sql.append(" ((SELECT 'L'||UPC, 'L'||RETAILER_ITEM_CODE FROM ITEM_LOOKUP WHERE ITEM_CODE = ").append(itemCode)
				.append("))");

		logger.debug("getUpcRetItemCodeAndRetLirIdOfLigItem() query - " + sql.toString());
		try {
		CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getRetLirIdOfLigItem");
		while (result.next()) {

			upc = result.getString("UPC");
			retItemCode = result.getString("RETAILER_ITEM_CODE");
			retLirId = result.getInt("RET_LIR_ID");

			upcAndRetLirId = upc + "," + retItemCode + "," + retLirId;
		}

		} catch (GeneralException e) {
			e.printStackTrace();
			logger.debug("GeneralException: " + e);
			retLirId = 0;
			upc = "";
			retItemCode = "";
		} catch (SQLException e) {
			e.printStackTrace();
			logger.debug("SQLException: " + e);
			retLirId = 0;
			upc = "";
			retItemCode = "";
		}

		return upcAndRetLirId;
	}

	public boolean updateLirItemCode(Connection conn, int retLirId) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE RETAILER_LIKE_ITEM_GROUP SET ");
		sb.append(" RET_LIR_ITEM_CODE = null");
		sb.append(" WHERE RET_LIR_ID = ").append(retLirId);

		int count = PristineDBUtil.executeUpdate(conn, sb, "updateLirItemCode");
		boolean retVal = (count >= 1) ? true : false;
		return retVal;
	}

	public boolean updateLirItem(Connection conn, int retLirId, int prevRetLirId, int itemCode, String upc) throws GeneralException {
		StringBuffer sb = new StringBuffer();

		sb.append(" UPDATE ITEM_LOOKUP ");
		sb.append(
				" SET (ITEM_NAME,ITEM_DESC,FRIENDLY_NAME,ITEM_SIZE,PACK,UOM_ID,UPC,DEPT_ID,MANUFACTURER_ID,CATEGORY_ID,RETAILER_ITEM_CODE,ACTIVE_INDICATOR,");
		sb.append(
				"SUB_CATEGORY_ID,RET_LIR_ID,PREV_RET_LIR_ID,ALT_ITEM_NAME,SEGMENT_ID,PRIVATE_LABEL_CODE,STANDARD_UPC,PI_ANALYZE_FLAG,DISCONT_FLAG,UPDATE_TIMESTAMP");
		sb.append(",CREATE_TIMESTAMP,BRAND_ID,PRIVATE_LABEL_IND, LIR_IND)=(SELECT ITEM_NAME,ITEM_DESC,FRIENDLY_NAME,ITEM_SIZE,PACK,UOM_ID,");
		sb.append("'L'||UPC,DEPT_ID,MANUFACTURER_ID,CATEGORY_ID,'L'||RETAILER_ITEM_CODE,ACTIVE_INDICATOR,SUB_CATEGORY_ID,");
		sb.append(retLirId).append(",").append(prevRetLirId);
		sb.append(",ALT_ITEM_NAME,SEGMENT_ID,PRIVATE_LABEL_CODE,'L'||STANDARD_UPC,PI_ANALYZE_FLAG,DISCONT_FLAG,SYSDATE,SYSDATE,BRAND_ID");
		sb.append(",PRIVATE_LABEL_IND,'Y' FROM ITEM_LOOKUP WHERE ITEM_CODE = ").append(itemCode).append(")");
		sb.append(" WHERE UPC = '").append(upc).append("'");

		logger.debug("updateLirItem() query - " + sb.toString());
		int count = PristineDBUtil.executeUpdate(conn, sb, "updateLirItem");
		boolean retVal = (count >= 1) ? true : false;
		return retVal;
	}

	public boolean updateLirItem(Connection conn, int retLirId, int prevRetLirId, int itemCode, String upc, String retItemCode) throws GeneralException {
		StringBuffer sb = new StringBuffer();

		sb.append(" UPDATE ITEM_LOOKUP ");
		sb.append(
				" SET (ITEM_NAME,ITEM_DESC,FRIENDLY_NAME,ITEM_SIZE,PACK,UOM_ID,UPC,DEPT_ID,MANUFACTURER_ID,CATEGORY_ID,RETAILER_ITEM_CODE,ACTIVE_INDICATOR,");
		sb.append(
				"SUB_CATEGORY_ID,RET_LIR_ID,PREV_RET_LIR_ID,ALT_ITEM_NAME,SEGMENT_ID,PRIVATE_LABEL_CODE,STANDARD_UPC,PI_ANALYZE_FLAG,DISCONT_FLAG,UPDATE_TIMESTAMP");
		sb.append(",CREATE_TIMESTAMP,BRAND_ID,PRIVATE_LABEL_IND, LIR_IND)=(SELECT ITEM_NAME,ITEM_DESC,FRIENDLY_NAME,ITEM_SIZE,PACK,UOM_ID,");
		sb.append("'L'||UPC,DEPT_ID,MANUFACTURER_ID,CATEGORY_ID,'L'||RETAILER_ITEM_CODE,ACTIVE_INDICATOR,SUB_CATEGORY_ID,");
		sb.append(retLirId).append(",").append(prevRetLirId);
		sb.append(",ALT_ITEM_NAME,SEGMENT_ID,PRIVATE_LABEL_CODE,'L'||STANDARD_UPC,PI_ANALYZE_FLAG,DISCONT_FLAG,SYSDATE,SYSDATE,BRAND_ID");
		sb.append(",PRIVATE_LABEL_IND,'Y' FROM ITEM_LOOKUP WHERE ITEM_CODE = ").append(itemCode).append(")");
		sb.append(" WHERE UPC = '").append(upc).append("' AND RETAILER_ITEM_CODE = '" + retItemCode + "'");

		int count = PristineDBUtil.executeUpdate(conn, sb, "updateLirItem");
		boolean retVal = (count >= 1) ? true : false;
		return retVal;
	}

	public int getRetLirId(Connection conn, String upc, String retailerItemCode) {
		boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		StringBuffer sql = new StringBuffer();
		int retLirId = 0;
		// Storing in hashMap values..If values are null then db process needs to be done..
		String keyValue = upc + "-" + retailerItemCode;
		if (checkRetailerItemCode && retLirIDwithUPCandRetItemCodeMap != null && retLirIDwithUPCandRetItemCodeMap.get(keyValue) != null) {
			retLirId = retLirIDwithUPCandRetItemCodeMap.get(keyValue);
		} else if (!checkRetailerItemCode && retLirIDwithUPC != null && retLirIDwithUPC.get(upc) != null) {
			retLirId = retLirIDwithUPC.get(upc);
		} else {
			sql = new StringBuffer(" SELECT RET_LIR_ID FROM ITEM_LOOKUP WHERE UPC = ");
			sql.append("'").append(upc).append("'");
			if (checkRetailerItemCode)
				sql.append(" AND RETAILER_ITEM_CODE = '").append(retailerItemCode).append("'");

			try {
				CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getRetLirId");
				while (result.next()) {
					retLirId = result.getInt("RET_LIR_ID");
				}
			} catch (GeneralException e) {
				retLirId = 0;
			} catch (SQLException e) {
				retLirId = 0;
			}
		}

		return retLirId;
	}

	public int getPreRetLirId(Connection conn, String upc, String retailerItemCode) {
		boolean checkRetailerItemCode = Boolean.parseBoolean(PropertyManager.getProperty("ITEM_LOAD.CHECK_RETAILER_ITEM_CODE", "FALSE"));
		StringBuffer sql = new StringBuffer();
		int retLirId = 0;

		sql = new StringBuffer(" SELECT PREV_RET_LIR_ID FROM ITEM_LOOKUP WHERE UPC = ");
		sql.append("'").append(upc).append("'");
		if (checkRetailerItemCode)
			sql.append(" AND RETAILER_ITEM_CODE = '").append(retailerItemCode).append("'");

		try {
			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "getpreRetLirId");
			while (result.next()) {
				retLirId = result.getInt("PREV_RET_LIR_ID");
			}
		} catch (GeneralException e) {
			retLirId = 0;
		} catch (SQLException e) {
			retLirId = 0;
		}

		return retLirId;
	}

	public int processProductGroup(Connection conn, String productName, String code, String addOnType, int productLevelId) throws GeneralException {
		int status = -1;

		try {
			StringBuffer sql = new StringBuffer();
			sql.append("SELECT NAME, PRODUCT_ID FROM PRODUCT_GROUP_T");
			sql.append(" where NAME = '").append(productName).append("'");
			sql.append(" and product_level_id = '").append(productLevelId).append("'");

			// logger.debug(" processProductGroup Sql..." + sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "processProductGroup");

			if (result.next()) {
				status = result.getInt("PRODUCT_ID");
			} else {

				StringBuffer seqSql = new StringBuffer();
				seqSql.append("select PRODUCT_GROUP_SEQ.NEXTVAL as PRODUCTID from dual");
				String seqresult = PristineDBUtil.getSingleColumnVal(conn, seqSql, "processProductGroup");

				sql = new StringBuffer();
				sql.append(" insert into PRODUCT_GROUP_T");
				sql.append(" (PRODUCT_ID, PRODUCT_LEVEL_ID, NAME, CODE, ADD_ON_TYPE)");
				sql.append("values (").append(seqresult).append(", ");
				sql.append(productLevelId).append(", '").append(productName);
				sql.append("', '").append(code).append("', '").append(addOnType).append("')");
				int recCount = PristineDBUtil.executeUpdate(conn, sql, "processProductGroup");

				if (recCount != -1) {
					status = Integer.parseInt(seqresult);
				}

			}

		} catch (Exception e) {
			logger.error("SQL Exception ", e);
			throw new GeneralException("SQL Exception ", e);
		}

		return status;
	}

	public int processProductGroupChild(Connection conn, int productId, int childProductId, int productLevelId, int ChildProductLevelId)
			throws GeneralException {

		int status = -1;

		try {
			StringBuffer sql = new StringBuffer();
			sql.append(" select PRODUCT_ID from PRODUCT_GROUP_RELATION_T");
			sql.append(" where PRODUCT_LEVEL_ID=").append(productLevelId);
			sql.append(" and CHILD_PRODUCT_LEVEL_ID=").append(ChildProductLevelId);
			sql.append(" and CHILD_PRODUCT_ID=").append(childProductId);
			// logger.debug(" processProductGroupChild Sql..." +
			// sql.toString());

			CachedRowSet result = PristineDBUtil.executeQuery(conn, sql, "processProductGroupChild");

			if (result.next()) {

				sql = new StringBuffer();
				sql.append(" update  PRODUCT_GROUP_RELATION_T set PRODUCT_ID=").append(productId);
				sql.append("  where PRODUCT_LEVEL_ID=").append(productLevelId);
				sql.append("  and CHILD_PRODUCT_ID=").append(childProductId);
				sql.append("  and CHILD_PRODUCT_LEVEL_ID=").append(ChildProductLevelId);

				int recCount = PristineDBUtil.executeUpdate(conn, sql, "processProductGroup");

				if (recCount != -1) {
					status = productId;
				}
			}

			else {

				sql = new StringBuffer();
				sql.append(" insert into PRODUCT_GROUP_RELATION_T");
				sql.append(" (PRODUCT_ID,CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID,PRODUCT_LEVEL_ID)");
				sql.append(" values (").append(productId).append(",").append(childProductId);
				sql.append(",").append(ChildProductLevelId).append(",").append(productLevelId);
				sql.append(")");

				int recCount = PristineDBUtil.executeUpdate(conn, sql, "processProductGroupChild");

				if (recCount != -1) {
					status = productId;
				}

			}

		} catch (Exception e) {
			logger.error("SQL Exception ", e);
			throw new GeneralException("SQL Exception ", e);
		}

		return status;

	}

	//
	// Brand updates
	//
	public CachedRowSet getItemsWithoutBrandId(Connection conn) {
		StringBuffer sb = new StringBuffer("SELECT item_code, item_name FROM item_lookup WHERE brand_id IS NULL ORDER BY item_name");
		logger.debug("getItemsWithoutBrandId SQL: " + sb.toString());
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getItemsWithoutBrandId");
		} catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
		}
		return result;
	}

	public void updateBrand(Connection conn, Map<String, List<Integer>> brands) {
		Set<String> brandKeys = brands.keySet();
		Iterator<String> brandIter = brandKeys.iterator();
		int nItemsUpdated = 0;
		int nBrands = 0;
		while (brandIter.hasNext()) {
			String brand = brandIter.next();
			try {
				int brandId = setupBrand(conn, brand, null);
				nBrands++;

				List<Integer> itemCodes = brands.get(brand);
				if (itemCodes == null)
					continue;

				for (int itemCode : itemCodes) {
					StringBuffer sb = new StringBuffer("UPDATE item_lookup SET brand_id = ").append(brandId);
					sb.append(" WHERE item_code = ").append(itemCode);
					logger.debug("updateBrand SQL2: " + sb.toString());
					try {
						int count = PristineDBUtil.executeUpdate(conn, sb, "ItemDAO - updateBrand2");
						nItemsUpdated++;
					} catch (GeneralException gex) {
						logger.error("updateBrand SQL2: " + sb.toString(), gex);
					}
				}

				if (nItemsUpdated % 1000 == 0)
					logger.info("updateBrand: updated " + String.valueOf(nItemsUpdated) + " items");
			} catch (GeneralException gex) {
				logger.error("updateBrand: ", gex);
			}
		}

		logger.info("updateBrand: added " + String.valueOf(nBrands) + " brands");
		logger.info("updateBrand: updated brand id of " + String.valueOf(nItemsUpdated) + " items");
	}

	/**
	 * This method is used to retrieve ITEM_CODE for the Retailer Item Code passed in the Set
	 * 
	 * @param conn
	 *            Connection
	 * @param upcSet
	 *            Contains set of Retailer Item Code for which ITEM_CODE needs to be retrieved
	 * @return HashMap Contains Retailer Item Code as key and ITEM_CODE as value
	 * @throws GeneralException
	 */
	public HashMap<String, List<String>> getItemCodeList(Connection conn, Set<String> retailerItemCodeSet) throws GeneralException {
		logger.debug("Inside getUPCList of ItemDAO");

		int limitcount = 0;
		List<String> retItemList = new ArrayList<String>();

		HashMap<String, List<String>> itemCodeMap = new HashMap<String, List<String>>();
		for (String retItemCode : retailerItemCodeSet) {
			retItemList.add(retItemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % this.commitCount == 0)) {
				Object[] values = retItemList.toArray();
				retrieveItemCodeList(conn, itemCodeMap, values);
				retItemList.clear();
			}
		}
		if (retItemList.size() > 0) {
			Object[] values = retItemList.toArray();
			retrieveItemCodeList(conn, itemCodeMap, values);
			retItemList.clear();
		}

		logger.info("No of Retailer Item Code passed as input - " + retailerItemCodeSet.size());
		logger.info("No of Retailer Item Code for which UPC was fetched - " + itemCodeMap.size());
		return itemCodeMap;
	}

	public HashMap<String, List<String>> getUPCListForRetItemcodes(Connection conn, Set<String> retailerItemCodeSet) throws GeneralException {
		// logger.debug("Inside getUPCListForRetItemcodes of ItemDAO");

		int limitcount = 0;
		List<String> retItemList = new ArrayList<String>();

		HashMap<String, List<String>> upcMap = new HashMap<String, List<String>>();
		for (String retItemCode : retailerItemCodeSet) {
			retItemList.add(retItemCode);
			limitcount++;
			if (limitcount > 0 && (limitcount % 1000 == 0)) {
				Object[] values = retItemList.toArray();
				retrieveUPCListForRetItemcodes(conn, upcMap, values);
				retItemList.clear();
			}
		}
		if (retItemList.size() > 0) {
			Object[] values = retItemList.toArray();
			retrieveUPCListForRetItemcodes(conn, upcMap, values);
			retItemList.clear();
		}

		// logger.info("No of Retailer Item Code passed as input - " +
		// retailerItemCodeSet.size());
		// logger.info("No of Retailer Item Code for which UPC was fetched - " +
		// upcMap.size());
		return upcMap;
	}

	/**
	 * This method queries the database for UPC for every set of Retailer Item Code
	 * 
	 * @param conn
	 *            Connection
	 * @param itemCodeMap
	 *            Map that will contain the result of the database retrieval
	 * @param values
	 *            Array of Retailer Item Code that will be passed as input to the query
	 * @throws GeneralException
	 */
	private void retrieveItemCodeList(Connection conn, HashMap<String, List<String>> itemMap, Object... values) throws GeneralException {
		logger.debug("Inside retrieveUPCList() of ItemDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String kk = String.format(GET_UPC, PristineDBUtil.preparePlaceHolders(values.length));
			statement = conn.prepareStatement(String.format(GET_UPC, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
				if (itemMap.get(retailerItemCode) != null) {
					List<String> itemCodeList = itemMap.get(retailerItemCode);
					String itemCode = resultSet.getString("ITEM_CODE");
					itemCodeList.add(itemCode);
					itemMap.put(retailerItemCode, itemCodeList);
				} else {
					List<String> itemCodeList = new ArrayList<String>();
					String itemCode = resultSet.getString("ITEM_CODE");
					itemCodeList.add(itemCode);
					itemMap.put(retailerItemCode, itemCodeList);
				}
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * This method queries the database for UPC for every set of Retailer Item Code
	 * 
	 * @param conn
	 *            Connection
	 * @param itemCodeMap
	 *            Map that will contain the result of the database retrieval
	 * @param values
	 *            Array of Retailer Item Code that will be passed as input to the query
	 * @throws GeneralException
	 */
	private void retrieveUPCListForRetItemcodes(Connection conn, HashMap<String, List<String>> upcMap, Object... values) throws GeneralException {
		// logger.debug("Inside retrieveUPCList() of ItemDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String kk = String.format(GET_UPC, PristineDBUtil.preparePlaceHolders(values.length));
			statement = conn.prepareStatement(String.format(GET_UPC_FOR_REATILER_ITEM_CODE, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
				if (upcMap.get(retailerItemCode) != null) {
					List<String> upcList = upcMap.get(retailerItemCode);
					String upc = resultSet.getString("UPC");
					upcList.add(PrestoUtil.castUPC(upc, true));
					upcMap.put(retailerItemCode, upcList);
				} else {
					List<String> upcList = new ArrayList<String>();
					String upc = resultSet.getString("UPC");
					upcList.add(PrestoUtil.castUPC(upc, true));
					upcMap.put(retailerItemCode, upcList);
				}
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		} finally {

			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	public HashMap<String, List<String>> getItemCodeForUPC(Connection conn, HashMap<String, List<String>> upcMap) throws GeneralException {
		logger.debug("Inside getItemCodeForUPC() of ItemDAO");
		HashMap<String, List<String>> itemCodeMap = new HashMap<String, List<String>>();
		RetailPriceDAO retailPriceDAO = new RetailPriceDAO();
		for (Map.Entry<String, List<String>> entry : upcMap.entrySet()) {
			Set<String> upcSet = new HashSet<String>(entry.getValue());
			HashMap<String, String> itemUpcMap = retailPriceDAO.getItemCode(conn, upcSet);
			List<String> itemCodeList = new ArrayList<String>(itemUpcMap.values());
			itemCodeMap.put(entry.getKey(), itemCodeList);
		}
		return itemCodeMap;
	}

	//
	// item size
	//
	public CachedRowSet getSizeBrandSegment(Connection conn, boolean brand) throws GeneralException {
		StringBuffer sb = new StringBuffer("SELECT distinct u.id AS uom, i.segment_id, i.item_size, i.brand_id");
		sb.append(" FROM item_lookup i, uom_lookup u WHERE i.uom_id = u.id AND i.item_size IS NOT NULL");
		sb.append(" AND i.segment_id NOT IN (3550, 3551) AND i.category_id NOT IN (532,533,550)");
		sb.append(" ORDER BY uom");
		if (brand)
			sb.append(", i.brand_id");
		sb.append(", i.segment_id, i.item_size");

		logger.debug("getSizeBrandSegment SQL: " + sb.toString());
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getSizeBrandSegment");
		} catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
			throw ex;
		}
		return result;
	}

	public int updateSizeGroup(Connection conn, String uom, int brand, int segment, String sizeGroup, String sizeList) {
		StringBuffer sb = new StringBuffer("INSERT INTO size_group (uom_id, segment_id, sizes");
		if (brand != -1)
			sb.append(", brand_id");
		if (sizeGroup != null)
			sb.append(", size_group");
		sb.append(") VALUES (");
		sb.append("'").append(uom).append("', ").append(segment).append(", '").append(sizeList).append("'");
		if (brand != -1)
			sb.append(", ").append(brand);
		if (sizeGroup != null)
			sb.append(", '").append(sizeGroup).append("'");
		sb.append(')');

		// logger.debug ("updateSizeGroup SQL: " + sb.toString());
		int count;
		try {
			count = PristineDBUtil.executeUpdate(conn, sb, "ItemDAO - updateSizeGroup");

		} catch (GeneralException gex) {
			logger.error("updateSizeGroup SQL: " + sb.toString(), gex);
			count = 0;
		}

		return count;
	}

	public CachedRowSet getItemSizeBrandSegment(Connection conn, boolean brand) throws GeneralException {
		StringBuffer sb = new StringBuffer("SELECT distinct i.item_code, u.id AS uom, i.segment_id, i.item_size, i.brand_id");
		sb.append(" FROM item_lookup i, uom_lookup u WHERE i.uom_id = u.id AND i.item_size IS NOT NULL");
		sb.append(" AND i.segment_id NOT IN (3550, 3551) AND i.category_id NOT IN (532,533,550)");
		// sb.append(" AND i.segment_id IN (1982)"); // for debugging
		sb.append(" ORDER BY i.item_code, uom");
		if (brand)
			sb.append(", i.brand_id");
		sb.append(", i.segment_id, i.item_size");

		logger.debug("getItemSizeBrandSegment SQL: " + sb.toString());
		CachedRowSet result = null;
		try {
			result = PristineDBUtil.executeQuery(conn, sb, "getItemSizeBrandSegment");
		} catch (GeneralException ex) {
			logger.error("SQL: " + sb.toString(), ex);
			throw ex;
		}
		return result;
	}

	public int updateItemSizeGroup(Connection conn, int itemCode, String uom, int brand, int segment, SizeGroupRange sizeGroup) {
		StringBuffer sb = new StringBuffer("INSERT INTO item_size_group (item_code, uom_id, segment_id, size_min, size_max");
		if (brand != -1)
			sb.append(", brand_id");
		if (sizeGroup._SizeGroup != null)
			sb.append(", size_group");
		sb.append(") VALUES (");
		sb.append(itemCode).append(", '").append(uom).append("', ").append(segment);
		sb.append(", ").append(sizeGroup._Min).append(", ").append(sizeGroup._Max);
		if (brand != -1)
			sb.append(", ").append(brand);
		if (sizeGroup._SizeGroup != null)
			sb.append(", '").append(sizeGroup._SizeGroup).append("'");
		sb.append(')');

		// logger.debug ("updateItemSizeGroup SQL: " + sb.toString());
		int count;
		try {
			count = PristineDBUtil.executeUpdate(conn, sb, "ItemDAO - updateItemSizeGroup");

		} catch (GeneralException gex) {
			logger.error("updateItemSizeGroup SQL: " + sb.toString(), gex);
			count = 0;
		}

		return count;
	}

	/**
	 * Retrieves UPC and DeptId mapping from Item Lookup
	 * 
	 * @param conn
	 *            Connection
	 * @return Map containing UPC as key and DeptId as value
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getItems(Connection conn) throws GeneralException {
		HashMap<String, Integer> upcDeptMap = new HashMap<String, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_ITEMS);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				upcDeptMap.put(resultSet.getString("UPC"), resultSet.getInt("DEPT_ID"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_UPCS_IN_DEPARTMENT");
			throw new GeneralException("Error while executing GET_UPCS_IN_DEPARTMENT", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return upcDeptMap;
	}

	/**
	 * Returns a map containing lir id and lir item code
	 * 
	 * @param conn
	 *            Connection
	 * @param lirIdSet
	 *            Items for which lir item code needs to be retrieved
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, Integer> getLIRInfo(Connection conn, Set<Integer> lirIdSet) throws GeneralException {

		int limitcount = 0;
		List<Integer> lirIdList = new ArrayList<Integer>();

		HashMap<Integer, Integer> lirItemCodeMap = new HashMap<Integer, Integer>();
		for (Integer lirId : lirIdSet) {
			lirIdList.add(lirId);
			limitcount++;
			if (limitcount > 0 && (limitcount % this.commitCount == 0)) {
				Object[] values = lirIdList.toArray();
				retrieveLIRItemCode(conn, lirItemCodeMap, values);
				lirIdList.clear();
			}
		}
		if (lirIdList.size() > 0) {
			Object[] values = lirIdList.toArray();
			retrieveLIRItemCode(conn, lirItemCodeMap, values);
			lirIdList.clear();
		}

		return lirItemCodeMap;
	}

	/**
	 * Populates the input hash map with lir id key and its value as lir item code
	 * 
	 * @param conn
	 *            Connection
	 * @param lirItemCodeMap
	 *            Map containing lir id as kay and lir item code as its value
	 * @param values
	 */
	private void retrieveLIRItemCode(Connection conn, HashMap<Integer, Integer> lirItemCodeMap, Object[] values) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		String lirWithNoItemCode = "";
		try {
			statement = conn.prepareStatement(String.format(GET_LIR_INFO, PristineDBUtil.preparePlaceHolders(values.length)));
			logger.debug("retrieveLIRItemCode" + statement);
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				// Added on 1st May 2014, to ignore LIR's which doesn't have
				// RET_LIR_ITEM_CODE
				// Ignoring non-active items while creating lig
				if (resultSet.getInt("RET_LIR_ITEM_CODE") > 0)
					lirItemCodeMap.put(resultSet.getInt("RET_LIR_ID"), resultSet.getInt("RET_LIR_ITEM_CODE"));
				else
					lirWithNoItemCode = lirWithNoItemCode + resultSet.getInt("RET_LIR_ID") + ",";
			}
			if (lirWithNoItemCode != "")
				logger.info("RET_LIR_ID with no RET_LIR_ITEM_CODE : " + lirWithNoItemCode);
		} catch (SQLException e) {
			logger.error("Error while executing GET_LIR_INFO " + e);
		} catch (NullPointerException ex) {
			logger.error("Error while executing GET_LIR_INFO " + ex);
		}

		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * Returns list of items from Item Lookup
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public List<ItemDTO> getItemLookup(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		List<ItemDTO> itemList = new ArrayList<ItemDTO>();
		String sql = "SELECT IL.ITEM_CODE, IL.SEGMENT_ID, IL.SUB_CATEGORY_ID, IL.CATEGORY_ID, IL.DEPT_ID, "
				+ "ITS.NAME AS SEG_NAME, ITS.SEG_NUM, SC.NAME AS SUB_CAT_NAME, SC.SUB_CAT_CODE , C.NAME AS CAT_NAME, "
				+ "C.CAT_CODE, D.NAME AS DEPT_NAME, D.DEPT_CODE FROM ITEM_LOOKUP IL, "
				+ "DEPARTMENT D, CATEGORY C, SUB_CATEGORY SC, ITEM_SEGMENT ITS WHERE "
				+ "IL.DEPT_ID = D.ID AND IL.CATEGORY_ID = C.ID AND IL.SUB_CATEGORY_ID = SC.ID AND IL.SEGMENT_ID = ITS.ID ";
		try {
			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			while (rs.next()) {
				ItemDTO itemDTO = new ItemDTO();
				itemDTO.segmentName = rs.getString("SEG_NAME");
				itemDTO.segmentCode = rs.getString("SEG_NUM");
				itemDTO.subCatName = rs.getString("SUB_CAT_NAME");
				itemDTO.subCatCode = rs.getString("SUB_CAT_CODE");
				itemDTO.catName = rs.getString("CAT_NAME");
				itemDTO.catCode = rs.getString("CAT_CODE");
				itemDTO.deptName = rs.getString("DEPT_NAME");
				itemDTO.deptCode = rs.getString("DEPT_CODE");
				itemDTO.itemCode = rs.getInt("ITEM_CODE");
				itemDTO.segmentID = rs.getInt("SEGMENT_ID");
				itemDTO.subCatID = rs.getInt("SUB_CATEGORY_ID");
				itemDTO.catID = rs.getInt("CATEGORY_ID");
				itemDTO.deptID = rs.getInt("DEPT_ID");
				itemList.add(itemDTO);
			}
		} catch (SQLException exception) {
			logger.error("Error when retrieving items from Item Lookup - " + exception.toString());
			throw new GeneralException("Error when retrieving items from Item Lookup - " + exception.toString());
		}

		return itemList;
	}

	private void setRetLirIdList(Connection conn) {
		String query = "SELECT DISTINCT(RET_LIR_ID) FROM RETAILER_LIKE_ITEM_GROUP";
		ArrayList<Integer> retLirIdList = new ArrayList<Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(query);
			// stmt.setFetchSize(50000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				retLirIdList.add(rs.getInt("RET_LIR_ID"));
			}
		} catch (SQLException ex) {
			logger.error("Error when retrieving RET_LIR_ID list - " + ex.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		this.retLirIdList = retLirIdList;
	}

	public void populateBrand(Connection conn, ItemDTO itemDTO) throws GeneralException {
		long ustartTime = System.currentTimeMillis();
		int brandId = 0;
		String brandName = itemDTO.brandName;

		if (brandName != null && !brandName.equals("")) {
			PreparedStatement stmt = null;
			ResultSet rs = null;

			try {
				// Added set of new code to avoid db call instead using cache in hash map to retrieve and store values.
				if (brandIDandNameMap != null) {
					logger.debug("Inside cache populatedbrand");
					if (brandIDandNameMap.get(brandName.trim().toUpperCase()) != null) {
						brandId = brandIDandNameMap.get(brandName.trim().toUpperCase());
					}
				} else {
					stmt = conn.prepareStatement(GET_BRAND_ID);
					stmt.setString(1, brandName);
					rs = stmt.executeQuery();
					if (rs.next()) {
						brandId = rs.getInt("BRAND_ID");
					}

					PristineDBUtil.close(rs);
					PristineDBUtil.close(stmt);
				}

				if (brandId == 0) {
					logger.debug("Inside the insert process in populateBrand");
					stmt = conn.prepareStatement(INSERT_BRAND);
					stmt.setString(1, brandName);
					stmt.setString(2, itemDTO.brandCode);
					stmt.executeUpdate();

					PristineDBUtil.close(stmt);

					stmt = conn.prepareStatement(GET_BRAND_ID);
					stmt.setString(1, brandName);
					rs = stmt.executeQuery();
					if (rs.next()) {
						brandId = rs.getInt("BRAND_ID");
					}
					if (brandId > 0 && brandIDandNameMap != null) {
						brandIDandNameMap.put(brandName.trim().toUpperCase(), brandId);
					}
				}
			} catch (SQLException exception) {
				logger.warn("Error in Setting up Brand - " + exception.toString());
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}
		long uendTime = System.currentTimeMillis();
		logger.debug("Time taken to complete Populate brand : " + (uendTime - ustartTime));
		itemDTO.brandId = brandId;
	}

	public void populateManufacturer(Connection conn, ItemDTO itemDTO) throws GeneralException {
		long ustartTime = System.currentTimeMillis();
		int manufactId = 0;
		String manufactName = itemDTO.manufactName;

		if (manufactName != null && !manufactName.equals("")) {
			PreparedStatement stmt = null;
			ResultSet rs = null;

			try {
				// Using hash map values to avoid db process
				if (manufacturerIDandNameMap != null) {
					if (manufacturerIDandNameMap.get(manufactName.trim().toUpperCase()) != null) {
						manufactId = manufacturerIDandNameMap.get(manufactName.trim().toUpperCase());
					}
				}
				// If hash map is null then it will execute the existing flow
				// using db process
				else {
					stmt = conn.prepareStatement(GET_MANUFACTURER_ID);
					stmt.setString(1, manufactName);
					rs = stmt.executeQuery();
					if (rs.next()) {
						manufactId = rs.getInt("ID");
					}

					PristineDBUtil.close(rs);
					PristineDBUtil.close(stmt);
				}
				if (manufactId == 0) {
					stmt = conn.prepareStatement(INSERT_MANUFACTURER);
					stmt.setString(1, manufactName);
					stmt.setString(2, itemDTO.manufactCode);
					stmt.executeUpdate();

					PristineDBUtil.close(stmt);

					stmt = conn.prepareStatement(GET_MANUFACTURER_ID);
					stmt.setString(1, manufactName);
					rs = stmt.executeQuery();
					if (rs.next()) {
						manufactId = rs.getInt("ID");

					}
					if (manufactId > 0 && manufacturerIDandNameMap != null) {
						manufacturerIDandNameMap.put(manufactName.trim().toUpperCase(), manufactId);
					}
				}
			} catch (SQLException exception) {
				logger.warn("Error in Setting up Manufacturer - " + exception.toString());
			} finally {
				PristineDBUtil.close(rs);
				PristineDBUtil.close(stmt);
			}
		}
		long uendTime = System.currentTimeMillis();
		logger.debug("Time taken to complete Manu : " + (uendTime - ustartTime));
		itemDTO.manufactId = manufactId;
	}

	public void updateRetailerItemCode(Connection conn, ItemDTO item, boolean active) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append("UPDATE RETAILER_ITEM_CODE_MAP SET ACTIVE_INDICATOR = ?, UPDATE_TIMESTAMP = SYSDATE WHERE RETAILER_ITEM_CODE = ? AND UPC = ?");
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement(sb.toString());
			int counter = 0;
			if (active)
				stmt.setString(++counter, String.valueOf(Constants.YES));
			else
				stmt.setString(++counter, String.valueOf(Constants.NO));
			stmt.setString(++counter, item.retailerItemCode);
			stmt.setString(++counter, item.upc);
			int count = stmt.executeUpdate();
		} catch (SQLException e) {
			logger.error("Error while executing updateRetailerItemCode " + e);
			throw new GeneralException("Error while executing updateRetailerItemCode " + e);
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	public void updateActiveIndicatorFlagInItemCodeMap(Connection conn) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		sb.append(" update retailer_item_code_map set ACTIVE_INDICATOR ='N', UPDATE_TIMESTAMP = SYSDATE  ");
		int count = PristineDBUtil.executeUpdate(conn, sb, "Update Item In Retailer Item Code Map");
		logger.info("No of Items updated " + count);
	}

	public ArrayList<Integer> getItemsInGroup(Connection conn, int likeItemId) throws GeneralException {
		ArrayList<Integer> itemList = new ArrayList<Integer>();
		StringBuffer sb = new StringBuffer();
		sb.append("select item_code from item_lookup where ret_lir_id = ? and active_indicator = 'Y' and lir_ind <> 'Y'");
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(sb.toString());
			stmt.setInt(1, likeItemId);
			rs = stmt.executeQuery();
			while (rs.next()) {
				itemList.add(rs.getInt("ITEM_CODE"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing getItemsInGroup " + e);
			throw new GeneralException("Error while executing getItemsInGroup " + e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return itemList;
	}

	public HashMap<String, String> getItemCodeForRetailerItemCode(Connection conn, Set<String> retailerItemCodeSet) throws GeneralException {
		logger.debug("Inside getItemCodeForRetailerItemCode of ItemDAO");

		int limitcount = 0;
		List<String> retItemList = new ArrayList<String>();

		HashMap<String, String> itemCodeMap = new HashMap<String, String>();
		for (String retItemCode : retailerItemCodeSet) {
			retItemList.add(PrestoUtil.castUPC(retItemCode, false));
			limitcount++;
			if (limitcount > 0 && (limitcount % this.commitCount == 0)) {
				Object[] values = retItemList.toArray();
				retrieveItemCodeForRetailerItemCode(conn, itemCodeMap, values);
				retItemList.clear();
			}
		}
		if (retItemList.size() > 0) {
			Object[] values = retItemList.toArray();
			retrieveItemCodeForRetailerItemCode(conn, itemCodeMap, values);
			retItemList.clear();
		}

		logger.debug("No of Retailer Item Code passed as input - " + retailerItemCodeSet.size());
		logger.debug("No of Retailer Item Code for which Item Code was fetched - " + itemCodeMap.size());
		return itemCodeMap;
	}

	private void retrieveItemCodeForRetailerItemCode(Connection conn, HashMap<String, String> itemCodeMap, Object... values) throws GeneralException {
		logger.debug("Inside retrieveItemCodeForRetailerItemCode() of ItemDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(String.format(GET_ITEM_CODE, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				itemCodeMap.put(resultSet.getString("RETAILER_ITEM_CODE"), resultSet.getString("ITEM_CODE"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	public HashMap<String, String> getRetailerItemCodeMapping(Connection conn) throws GeneralException {
		logger.debug("Inside retrieveItemCodeForRetailerItemCode() of ItemDAO");
		HashMap<String, String> itemCodeMap = new HashMap<String, String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_ITEM_CODE_CACHE);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				itemCodeMap.put(resultSet.getString("RETAILER_ITEM_CODE"), resultSet.getString("ITEM_CODE"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODE_CACHE");
			throw new GeneralException("Error while executing GET_ITEM_CODE_CACHE", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return itemCodeMap;
	}

	public HashMap<String, Integer> getItemCodeAndUpc(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		HashMap<String, Integer> map = new HashMap<String, Integer>();

		try {
			String sql = "select Item_code, UPC from ITEM_LOOKUP ";
			// sql = sql + " where ROWNUM <= 2000 UNION select Item_code, UPC
			// from ITEM_LOOKUP where item_code = 41940";

			stmt = conn.prepareStatement(sql);
			rs = stmt.executeQuery();
			while (rs.next()) {
				map.put(rs.getString("UPC"), rs.getInt("Item_code"));
			}
			logger.info("# of item/upc mapping " + map.size());

		} catch (Exception e) {
			throw new GeneralException("Exception in Getting Item/UPC map", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return map;
	}

	private final static String UPDATE_SECONDARY_ITEM_INFO = " UPDATE  ITEM_LOOKUP SET ALT_RETAILER_ITEM_CODE = ? WHERE ITEM_CODE = ? ";

	public int updateSecondaryItemInfo(Connection conn, List<RAItemInfoDTO> updateItemList) throws GeneralException {

		int updateRecordCount = 0;
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(UPDATE_SECONDARY_ITEM_INFO);
			int itemNoInBatch = 0;

			for (RAItemInfoDTO raItemInfo : updateItemList) {
				int counter = 0;
				statement.setString(++counter, raItemInfo.getOriginalitemno());
				statement.setInt(++counter, raItemInfo.getPrestoItemcode());
				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					updateRecordCount += PristineDBUtil.getUpdateCount(count);
					statement.clearBatch();
					itemNoInBatch = 0;

				}
			}
			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				updateRecordCount += PristineDBUtil.getUpdateCount(count);
				statement.clearBatch();
			}
		} catch (SQLException e) {
			logger.error("Error while executing UPDATE_RETAIL_PRICE_INFO");
			throw new GeneralException("Error while executing UPDATE_RETAIL_PRICE_INFO", e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return updateRecordCount;
	}

	/**
	 * Sets list containing missing item codes in ITEM_LOOKUP
	 * 
	 * @param conn
	 */
	public void setMissingItemList(Connection conn) {
		int minLirItemCode = Integer.parseInt(PropertyManager.getProperty("ITEMLOAD.MIN_LIR_ITEM_CODE", "970000"));
		ArrayList<Integer> missingItemCodeList = new ArrayList<Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_MISSING_ITEM_CODES);
			stmt.setInt(1, minLirItemCode);
			stmt.setInt(2, minLirItemCode);
			stmt.setFetchSize(10000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				missingItemCodeList.add(rs.getInt("MISSING_ITEM_CODE"));
			}
		} catch (SQLException ex) {
			logger.error("Error when retrieving missing item code list - " + ex.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		this.missingItemCodeList = missingItemCodeList;
	}

	public ArrayList<Integer> getMissingItemList() {
		return this.missingItemCodeList;
	}

	/**
	 * Returns item code for upc and retailer item code passed in input
	 * 
	 * @param conn
	 * @param upc
	 * @param retailerItemCode
	 * @return
	 */
	public String getItemCodeForUpcRetailerItemCode(Connection conn, String upc, String retailerItemCode) {
		String itemCodeStr = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_ITEM_CODE_FOR_UPC_RETAILER_ITEM_CODE);
			stmt.setString(1, upc);
			stmt.setString(2, retailerItemCode);
			rs = stmt.executeQuery();
			if (rs.next()) {
				itemCodeStr = rs.getString("ITEM_CODE");
			}
		} catch (SQLException ex) {
			logger.error("Error when retrieving item code for upc and retailer item code - " + ex.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		return itemCodeStr;
	}

	/**
	 * Returns all items from ITEM_LOOKUP with ITEM_CODE, UPC, RETAILER_ITEM_CODE.
	 * 
	 * @param conn
	 * @throws GeneralException
	 */

	public HashMap<ItemDetailKey, String> getAllItemsFromItemLookup(Connection conn) throws GeneralException {
		HashMap<ItemDetailKey, String> itemLookupMap = new HashMap<ItemDetailKey, String>();
		CachedRowSet itemRS = null;

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE FROM ITEM_LOOKUP WHERE LIR_IND='N'");
		logger.debug(sb);

		// execute the statement
		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getUPCAndItem");
			while (itemRS.next()) {
				String upc = itemRS.getString("UPC");
				String retailerItemCode = itemRS.getString("RETAILER_ITEM_CODE");
				String itemCode = itemRS.getString("ITEM_CODE");
				ItemDetailKey itemDetailKey = new ItemDetailKey(PrestoUtil.castUPC(upc, false), retailerItemCode);
				itemLookupMap.put(itemDetailKey, itemCode);
				int size = itemLookupMap.size();
				if ((size % 25000) == 0) {
					logger.debug("getUPCAndItem: processed " + String.valueOf(size));
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return itemLookupMap;
	}

	/**
	 * Returns all items from ITEM_LOOKUP with ITEM_CODE, UPC, RETAILER_ITEM_CODE.
	 * 
	 * @param conn
	 * @throws GeneralException
	 */

	public HashMap<ItemDetailKey, String> getAllItemsFromItemLookupV2(Connection conn) throws GeneralException {
		HashMap<ItemDetailKey, String> itemLookupMap = new HashMap<ItemDetailKey, String>();
		CachedRowSet itemRS = null;

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT  IL.ITEM_CODE, RM.RETAILER_ITEM_CODE ALT_ITEM_CODE, IL.UPC FROM ITEM_LOOKUP IL, "
				+ " RETAILER_ITEM_CODE_MAP RM WHERE IL.ITEM_CODE = RM.ITEM_CODE and il.lir_ind = 'N'");
		logger.debug(sb);
		// and RM.UPc in(07078402445,07078402449,07078402460)
		// execute the statement
		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getUPCAndItem");
			while (itemRS.next()) {
				String upc = itemRS.getString("UPC");
				String retailerItemCode = itemRS.getString("ALT_ITEM_CODE");
				String itemCode = itemRS.getString("ITEM_CODE");
				ItemDetailKey itemDetailKey = new ItemDetailKey(PrestoUtil.castUPC(upc, false), retailerItemCode);
				itemLookupMap.put(itemDetailKey, itemCode);
				int size = itemLookupMap.size();
				if ((size % 25000) == 0) {
					logger.debug("getAllItemsFromItemLookupV2: processed " + String.valueOf(size));
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		return itemLookupMap;
	}

	/**
	 * Returns item code for upc and store passed in the input
	 * 
	 * @param conn
	 * @param inputUPC
	 * @return
	 * @throws GeneralException
	 */
	public int getItemCodeForStoreUPC(Connection conn, String upc, int compStrId) throws GeneralException {
		int itemCode = -1;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_ITEM_CODE_FOR_STORE_UPC);
			stmt.setString(1, upc);
			stmt.setInt(2, compStrId);
			rs = stmt.executeQuery();
			if (rs.next()) {
				itemCode = rs.getInt("ITEM_CODE");
			}
		} catch (SQLException ex) {
			logger.error("Error when retrieving item code for store and upc - " + ex.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
		return itemCode;
	}

	/**
	 * Returns list of authorized items for zone/store
	 * 
	 * @param conn
	 * @param priceZoneStores
	 * @return
	 * @throws OfferManagementException
	 */
	public List<Integer> getAuthorizedItemsOfZoneAndStore(Connection conn, List<Integer> priceZoneStores) throws GeneralException {
		List<Integer> itemList = new ArrayList<Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			String storeIds = PRCommonUtil.getCommaSeperatedStringFromIntArray(priceZoneStores);
			String sql = queryToGetAuthorizedItems(storeIds);
			stmt = conn.prepareStatement(sql);
			logger.debug("Authorized items Query: " + sql);
			stmt.setFetchSize(100000);
			rs = stmt.executeQuery();
			while (rs.next()) {
				itemList.add(rs.getInt("ITEM_CODE"));
			}
		} catch (Exception ex) {
			logger.error("Exception in getAuthorizedItemsOfZoneAndStore() " + ex);
			throw new GeneralException("Exception in getAuthorizedItemsOfZoneAndStore() " + ex);
		}

		return itemList;
	}

	/**
	 * Returns query to get authorized items for zone/store
	 * 
	 * @param priceZoneStoreIds
	 * @return
	 */
	public String queryToGetAuthorizedItems(String priceZoneStoreIds) {
		StringBuffer subQuery = new StringBuffer("");
		subQuery.append(" SELECT ITEM_CODE FROM STORE_ITEM_MAP ");
		subQuery.append(" WHERE LEVEL_TYPE_ID = 2 AND LEVEL_ID IN ");
		subQuery.append(" ( ");
		subQuery.append(priceZoneStoreIds);
		subQuery.append(" ) ");
		subQuery.append(" AND IS_AUTHORIZED = 'Y' ");
		return subQuery.toString();
	}

	/**
	 * This method is used to retrieve RETAILER_ITEM_CODE, ITEM_CODE for the UPCs passed in the Set
	 * 
	 * @param conn
	 *            Connection
	 * @param upcSet
	 *            Contains set of UPCs for which ITEM_CODE needs to be retrieved
	 * @return HashMap Contains UPC as key and ITEM_CODE as value
	 * @throws GeneralException
	 */
	public HashMap<String, HashMap<String, Integer>> getItemCode(Connection conn, Set<String> upcSet) throws GeneralException {
		logger.debug("Inside getItemCode of RetailPriceDAO");

		int limitcount = 0;
		List<String> upcList = new ArrayList<String>();

		HashMap<String, HashMap<String, Integer>> itemCodeMap = new HashMap<String, HashMap<String, Integer>>();
		for (String upc : upcSet) {
			upcList.add(PrestoUtil.castUPC(upc, false));
			limitcount++;
			if (limitcount > 0 && (limitcount % this.commitCount == 0)) {
				Object[] values = upcList.toArray();
				retrieveItemCode(conn, itemCodeMap, values);
				upcList.clear();
			}
		}
		if (upcList.size() > 0) {
			Object[] values = upcList.toArray();
			retrieveItemCode(conn, itemCodeMap, values);
			upcList.clear();
		}

		logger.debug("No of UPCs passed as input - " + upcSet.size());
		logger.debug("No of UPCs for which ITEM_CODE was fetched - " + itemCodeMap.size());
		return itemCodeMap;
	}

	/**
	 * This method queries the database for REATILER_ITEM_CODE, ITEM_CODE for every set of UPCs
	 * 
	 * @param conn
	 *            Connection
	 * @param itemCodeMap
	 *            Map that will contain the result of the database retrieval
	 * @param values
	 *            Array of UPCs that will be passed as input to the query
	 * @throws GeneralException
	 */
	private void retrieveItemCode(Connection conn, HashMap<String, HashMap<String, Integer>> itemCodeMap, Object... values) throws GeneralException {
		logger.debug("Inside retrieveItemCode() of RetailPriceDAO");
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(String.format(GET_ITEM_CODES_FOR_UPC, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			resultSet = statement.executeQuery();
			CachedRowSet crs = new CachedRowSetImpl();
			crs.populate(resultSet);
			resultSet.close();
			while (crs.next()) {
				String upc = crs.getString("UPC");
				int itemCode = crs.getInt("ITEM_CODE");
				String retailerItemCode = crs.getString("RETAILER_ITEM_CODE");
				HashMap<String, Integer> tMap = null;
				if (itemCodeMap.get(upc) != null)
					tMap = itemCodeMap.get(upc);
				else
					tMap = new HashMap<String, Integer>();
				tMap.put(retailerItemCode, itemCode);
				itemCodeMap.put(upc, tMap);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ITEM_CODE");
			throw new GeneralException("Error while executing GET_ITEM_CODE", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * Returns authorized items for all stores only for items with multiple records for same UPC
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<Integer, HashMap<String, Integer>> getAuthorizedItems(Connection conn) throws GeneralException {
		HashMap<Integer, HashMap<String, Integer>> authorizedItemMap = new HashMap<Integer, HashMap<String, Integer>>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<String, String> tActiveIndicator = null;
		try {
			statement = conn.prepareStatement(GET_AUTHORIZED_ITEMS_FOR_STORE);
			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String upc = resultSet.getString("UPC");
				int itemCode = resultSet.getInt("ITEM_CODE");
				int compStrId = resultSet.getInt("LEVEL_ID");
				String activeIndicator = resultSet.getString("ACTIVE_INDICATOR");
				HashMap<String, Integer> tMap = null;
				//HashMap<String, String> tActiveIndicator = null;
				if (authorizedItemMap.get(compStrId) != null) {
					tMap = authorizedItemMap.get(compStrId);
				} else {
					tActiveIndicator = new HashMap<String, String>();
					tMap = new HashMap<String, Integer>();
				}
				
				if(tActiveIndicator.containsKey(upc))
				{
					String active = tActiveIndicator.get(upc);
					if(active.equals("N") && activeIndicator.equals("Y")){
						tMap.put(upc, itemCode);		
					}
				}else{
					tActiveIndicator.put(upc, activeIndicator);
					tMap.put(upc, itemCode);
				}
				authorizedItemMap.put(compStrId, tMap);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_AUTHORIZED_ITEMS_FOR_STORE");
			throw new GeneralException("Error while executing GET_AUTHORIZED_ITEMS_FOR_STORE", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return authorizedItemMap;
	}

	public HashMap<String, Integer> getAllItems(Connection conn) throws GeneralException {
		HashMap<String, Integer> itemMap = new HashMap<String, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_ALL_ITEMS);
			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String upc = resultSet.getString("UPC");
				String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
				Integer itemCode = resultSet.getInt("ITEM_CODE");
				itemMap.put(upc + "-" + retailerItemCode, itemCode);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ALL_ITEMS");
			throw new GeneralException("Error while executing GET_ALL_ITEMS", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemMap;
	}

	/**
	 * Updates RET_LIR_ID for all items in the list
	 * 
	 * @param conn
	 * @param itemList
	 * @throws GeneralException
	 */
	public void updateLikeItemId(Connection conn, List<ItemDTO> itemList) throws GeneralException {
		PreparedStatement statement = null;
		try {
			String sql = UPDATE_LIKE_ITEM_ID;
			statement = conn.prepareStatement(sql);
			for (ItemDTO item : itemList) {
				int counter = 0;
				if (item.likeItemId > 0)
					statement.setInt(++counter, item.likeItemId);
				else
					statement.setNull(++counter, Types.INTEGER);
				statement.setInt(++counter, item.itemCode);
				statement.addBatch();
			}
			int[] count = statement.executeBatch();
			statement.clearBatch();
		} catch (SQLException e) {
			logger.error("Error while executing UPDATE_LIKE_ITEM_ID");
			throw new GeneralException("Error while executing UPDATE_LIKE_ITEM_ID " + e);
		} finally {
			PristineDBUtil.close(statement);
		}
	}

	/**
	 * Returns item_code for every upc, retailer_item_code in item_looup
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, Integer> getUPCAndRetItemCode(Connection conn) throws GeneralException {
		HashMap<String, Integer> itemHashMap = new HashMap<String, Integer>();
		CachedRowSet itemRS = null;
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE FROM ITEM_LOOKUP");

		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getUPCAndRetItemCode");
			while (itemRS.next()) {
				itemHashMap.put(itemRS.getString("UPC") + "-" + itemRS.getString("RETAILER_ITEM_CODE"), itemRS.getInt("ITEM_CODE"));
				int size = itemHashMap.size();
				if ((size % 25000) == 0) {
					logger.debug("getUPCAndItem: processed " + String.valueOf(size));
				}
			}
		} catch (SQLException e) {
			logger.error("Error while retrieving upc and retailer item code");
			throw new GeneralException("Error while retrieving upc and retailer item code - " + e);
		} finally {
			PristineDBUtil.close(itemRS);
		}

		return itemHashMap;
	}

	/***
	 * Get retailer item code and its item's
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, List<ItemDTO>> getRetItemCodeAndItem(Connection conn) throws GeneralException {
		HashMap<String, List<ItemDTO>> itemHashMap = new HashMap<String, List<ItemDTO>>();
		CachedRowSet itemRS = null;
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE, LIR_IND FROM ITEM_LOOKUP ");
		sb.append(" WHERE ACTIVE_INDICATOR = 'Y' ");
		// sb.append(" WHERE RETAILER_ITEM_CODE LIKE '%885933' "); //For debug
		// sb.append(" AND RETAILER_ITEM_CODE IN
		// ('870170','870171','042194','849620','618420','618425','907797','735962','735963','735964')
		// "); //For debug
		List<ItemDTO> itemList = null;
		ItemDTO itemDTO = null;
		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getUPCAndRetItemCode");
			while (itemRS.next()) {
				if (itemHashMap.get(itemRS.getString("RETAILER_ITEM_CODE")) != null) {
					itemList = itemHashMap.get(itemRS.getString("RETAILER_ITEM_CODE"));
				} else {
					itemList = new ArrayList<ItemDTO>();
				}
				itemDTO = new ItemDTO();
				itemDTO.setUpc(itemRS.getString("UPC"));
				itemDTO.setItemCode(itemRS.getInt("ITEM_CODE"));
				String lirInd = itemRS.getString("LIR_IND");
				if (String.valueOf(Constants.YES).equalsIgnoreCase(lirInd))
					itemDTO.lirInd = true;
				itemList.add(itemDTO);
				itemHashMap.put(itemRS.getString("RETAILER_ITEM_CODE"), itemList);
				int size = itemHashMap.size();
				if ((size % 25000) == 0) {
					logger.debug("getUPCAndItem: processed " + String.valueOf(size));
				}
			}
		} catch (SQLException e) {
			logger.error("Error while retrieving retailer item code");
			throw new GeneralException("Error while retrieving retailer item code - " + e);
		} finally {
			PristineDBUtil.close(itemRS);
		}

		return itemHashMap;
	}

	/**
	 * Updates LIR representations in batch
	 * 
	 * @param conn
	 * @param lirItemList
	 * @throws GeneralException
	 */
	public int updateLirItem(Connection conn, List<ItemDTO> lirItemList) throws GeneralException {
		StringBuffer sb = new StringBuffer();
		int updateRecCount = 0;
		sb.append(" UPDATE ITEM_LOOKUP ");
		sb.append(
				" SET (ITEM_NAME,ITEM_DESC,FRIENDLY_NAME,ITEM_SIZE,PACK,UOM_ID,UPC,DEPT_ID,MANUFACTURER_ID,CATEGORY_ID,RETAILER_ITEM_CODE,ACTIVE_INDICATOR,");
		sb.append(
				"SUB_CATEGORY_ID,RET_LIR_ID,ALT_ITEM_NAME,SEGMENT_ID,PRIVATE_LABEL_CODE,STANDARD_UPC,PI_ANALYZE_FLAG,DISCONT_FLAG,UPDATE_TIMESTAMP");
		sb.append(",CREATE_TIMESTAMP,BRAND_ID,PRIVATE_LABEL_IND, LIR_IND)=(SELECT ITEM_NAME,ITEM_DESC,FRIENDLY_NAME,ITEM_SIZE,PACK,UOM_ID,");
		sb.append("'L'||UPC,DEPT_ID,MANUFACTURER_ID,CATEGORY_ID,'L'||RETAILER_ITEM_CODE,ACTIVE_INDICATOR,SUB_CATEGORY_ID,");
		sb.append("?");
		sb.append(",ALT_ITEM_NAME,SEGMENT_ID,PRIVATE_LABEL_CODE,'L'||STANDARD_UPC,PI_ANALYZE_FLAG,DISCONT_FLAG,SYSDATE,SYSDATE,BRAND_ID");
		sb.append(",PRIVATE_LABEL_IND,'Y' FROM ITEM_LOOKUP WHERE ITEM_CODE = ?)");
		sb.append(" WHERE ITEM_CODE = ?");
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(sb.toString());
			for (ItemDTO item : lirItemList) {
				int counter = 0;
				if (item.likeItemId > 0)
					statement.setInt(++counter, item.likeItemId);
				else
					statement.setNull(++counter, Types.INTEGER);
				statement.setInt(++counter, item.itemCode);
				statement.setInt(++counter, item.ligRepItemCode);
				statement.addBatch();
			}
			int count[] = statement.executeBatch();
			updateRecCount = count.length;
			statement.clearBatch();
		} catch (SQLException e) {
			logger.error("Error while updating LIR items");
			throw new GeneralException("Error while updating LIR items " + e);
		} finally {
			PristineDBUtil.close(statement);
		}

		return updateRecCount;
	}

	/***
	 * Copy RET_LIR_ITEM_CODE to RET_LIR_ITEM_CODE_PREV in RETAILER_LIKE_ITEM_GROUP table
	 * 
	 * @param conn
	 * @throws GeneralException
	 */
	public void copyRetLirItemCode(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("UPDATE RETAILER_LIKE_ITEM_GROUP SET RET_LIR_ITEM_CODE_PREV = RET_LIR_ITEM_CODE");
			int count = stmt.executeUpdate();
			logger.info("No Of Records Updated: " + count);
		} catch (SQLException exception) {
			logger.error("Error when copying ret_lir_item_code - " + exception.toString());
			throw new GeneralException("Error when copying ret_lir_item_code  - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	/***
	 * Copy RET_LIR_ID to PREV_RET_LIR_ID in ITEM_LOOKUP table
	 * 
	 * @param conn
	 * @throws GeneralException
	 */
	public void copyRetLirId(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("UPDATE ITEM_LOOKUP SET PREV_RET_LIR_ID = RET_LIR_ID");
			int count = stmt.executeUpdate();
			logger.info("No Of Records Updated: " + count);
		} catch (SQLException exception) {
			logger.error("Error when copying ret_lir_id - " + exception.toString());
			throw new GeneralException("Error when copying ret_lir_id  - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	/***
	 * RESET ALL RET_LIR_ID IN ITEM_LOOKUP
	 * 
	 * @param conn
	 * @throws GeneralException
	 */
	public void resetRetLirId(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("UPDATE ITEM_LOOKUP SET RET_LIR_ID = NULL");
			int count = stmt.executeUpdate();
			logger.info("No Of Records Updated: " + count);
		} catch (SQLException exception) {
			logger.error("Error when copying ret_lir_id - " + exception.toString());
			throw new GeneralException("Error when copying ret_lir_id  - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	/***
	 * Unlink representing item to all unused RET_LIR_ID
	 * 
	 * @param conn
	 * @throws GeneralException
	 */
	public void unlinkRetLirId(Connection conn) throws GeneralException {
		PreparedStatement stmt = null;
		try {
			stmt = conn.prepareStatement("UPDATE RETAILER_LIKE_ITEM_GROUP SET RET_LIR_ITEM_CODE = NULL"
					+ " WHERE RET_LIR_ID NOT IN (SELECT DISTINCT(RET_LIR_ID) FROM ITEM_LOOKUP WHERE RET_LIR_ID IS NOT NULL) "
					+ " AND RET_LIR_ITEM_CODE IS NOT NULL");
			int count = stmt.executeUpdate();
			logger.info("No Of Records Updated: " + count);
		} catch (SQLException exception) {
			logger.error("Error when copying ret_lir_id - " + exception.toString());
			throw new GeneralException("Error when copying ret_lir_id  - " + exception.toString());
		} finally {
			PristineDBUtil.close(stmt);
		}
	}

	private static final String GET_PRICE_CHECK_TYPE_ID_KVI_MAPPING = "SELECT PRICE_CHECK_LIST_TYPE_ID, " + "CODE FROM PRICE_CHECK_LIST_TYPE_LOOKUP";

	public void getPriceCheckListTypeId(Connection conn, List<ItemDTO> kviCodeRecords) throws GeneralException {
		logger.debug("Inside getPriceCheckTypeId()");
		PreparedStatement statement = null;
		ResultSet rs = null;
		try {
			statement = conn.prepareStatement(GET_PRICE_CHECK_TYPE_ID_KVI_MAPPING);
			rs = statement.executeQuery();
			while (rs.next()) {
				for (ItemDTO itemDTO : kviCodeRecords) {
					if (itemDTO.kviCode.equals(rs.getString("CODE").trim())) {
						itemDTO.priceCheckTypeId = rs.getInt("PRICE_CHECK_LIST_TYPE_ID");
					}
				}
			}
		} catch (Exception e) {
			logger.error("Error in getPriceCheckTypeId()  - " + e.toString());
			throw new GeneralException("Error in getPriceCheckTypeId()", e);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(statement);
		}

	}

	private final static String DELETE_FROM_PRICE_CHECK_LIST_ITEMS = "DELETE FROM PRICE_CHECK_LIST_ITEMS "
			+ "WHERE PRICE_CHECK_LIST_ID IN (%PRICE_CHECK_LIST_ID%)";

	public int deletePriceCheckListRecords(Connection conn, int priceCheckListIdKvi, int priceCheckListIdK2) throws GeneralException {
		logger.debug("Inside deletePriceCheckListRecords()");
		PreparedStatement statement = null;
		String priceCheckListIds = priceCheckListIdKvi + "," + priceCheckListIdK2;
		int totalDeleteCnt = 0;
		try {
			String query = new String(DELETE_FROM_PRICE_CHECK_LIST_ITEMS);
			query = query.replaceAll("%PRICE_CHECK_LIST_ID%", priceCheckListIds);
			statement = conn.prepareStatement(query);
			totalDeleteCnt = statement.executeUpdate();
			statement.close();
		} catch (Exception e) {
			logger.error("Exception deletePriceCheckListRecords()  - " + e.toString());
			throw new GeneralException("Error in deletePriceCheckListRecords()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return totalDeleteCnt;
	}

	private final static String INSERT_INTO_PRICE_CHECK_LIST_ITEMS = "INSERT INTO PRICE_CHECK_LIST_ITEMS "
			+ " (PRICE_CHECK_LIST_ID,ITEM_CODE) VALUES(?,?) ";

	public int loadKviInfoInPriceCheckListItems(Connection conn, HashMap<Integer, ItemDTO> kviCodeRecords) throws GeneralException {
		PreparedStatement statement = null;
		int itemNoInBatch = 0;
		int totalInsertCnt = 0;

		try {
			String query = new String(INSERT_INTO_PRICE_CHECK_LIST_ITEMS);
			statement = conn.prepareStatement(query);

			for (ItemDTO itemDTO : kviCodeRecords.values()) {
				statement.setInt(1, itemDTO.priceCheckTypeId);
				statement.setInt(2, itemDTO.itemCode);

				statement.addBatch();
				itemNoInBatch++;

				if (itemNoInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int[] count = statement.executeBatch();
					totalInsertCnt = totalInsertCnt + count.length;
					statement.clearBatch();
					itemNoInBatch = 0;
				}
			}

			if (itemNoInBatch > 0) {
				int[] count = statement.executeBatch();
				totalInsertCnt = totalInsertCnt + count.length;
				statement.clearBatch();
			}
			statement.close();

		} catch (Exception e) {
			logger.error("Error while inserting records into Price_Check_List_Items - " + e.toString());
			e.printStackTrace();
			throw new GeneralException("Error in loadKviInfoInPriceCheckListItems()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return totalInsertCnt;
	}

	private final static String GET_PRICE_CHECK_LIST_ID = "SELECT PCLTL.CODE,PCL.ID "
			+ "FROM PRICE_CHECK_LIST PCL JOIN PRICE_CHECK_LIST_TYPE_LOOKUP PCLTL "
			+ "ON PCLTL.PRICE_CHECK_LIST_TYPE_ID= PCL.PRICE_CHECK_LIST_TYPE_ID " + "WHERE PCL.ID IN (%PRICE_CHECK_LIST_ID%)";

	public HashMap<String, Integer> getKviCodeForPriceCheckListId(Connection conn, int priceCheckListIdKvi, int priceCheckListIdK2)
			throws GeneralException {
		HashMap<String, Integer> priceCheckListTypeMap = new HashMap<String, Integer>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		String priceCheckListId = priceCheckListIdKvi + "," + priceCheckListIdK2;
		try {
			String query = new String(GET_PRICE_CHECK_LIST_ID);

			query = query.replaceAll("%PRICE_CHECK_LIST_ID%", priceCheckListId);
			statement = conn.prepareStatement(query);

			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				priceCheckListTypeMap.put(resultSet.getString("CODE").trim(), resultSet.getInt("ID"));
			}
		} catch (SQLException e) {
			logger.error("Error while executing PRICE_CHECK_LIST_ID");
			throw new GeneralException("Error while executing PRICE_CHECK_LIST_ID", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return priceCheckListTypeMap;
	}

	public HashMap<String, List<String>> getItemCodesForRetailerItemcode(Connection conn) throws GeneralException {
		HashMap<String, List<String>> retailerItemCodeMap = new HashMap<String, List<String>>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_RETAILER_ITEM_CODE_CACHE);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
				String itemCode = resultSet.getString("ITEM_CODE");
				if (retailerItemCodeMap.get(retailerItemCode) != null) {
					List<String> tempList = retailerItemCodeMap.get(retailerItemCode);
					tempList.add(itemCode);
					retailerItemCodeMap.put(retailerItemCode, tempList);
				} else {
					List<String> tempList = new ArrayList<String>();
					tempList.add(itemCode);
					retailerItemCodeMap.put(retailerItemCode, tempList);
				}
			}
		} catch (SQLException e) {
			logger.error("Error while getting retailer item code and item code mapping");
			throw new GeneralException("Error while getting retailer item code and item code mapping", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailerItemCodeMap;

	}

	public HashMap<String, RetailerLikeItemGroupDTO> getAllRetLirIdWithAltItemCode(Connection conn) throws GeneralException {
		HashMap<String, RetailerLikeItemGroupDTO> retLirIdMap = new HashMap<String, RetailerLikeItemGroupDTO>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_ALL_LIR_ID_WITH_SOURCE_VENDOR_AND_ITEM);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				RetailerLikeItemGroupDTO ligDTO = new RetailerLikeItemGroupDTO();
				ligDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				ligDTO.setRetLirName(resultSet.getString("RET_LIR_NAME"));
				retLirIdMap.put(resultSet.getString("RETAILER_ITEM_CODE"), ligDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error while getting retailer item code and ret lir id mapping" + sqlE);
			throw new GeneralException("Error while getting retailer item code and ret lir id mapping", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return retLirIdMap;
	}

	public HashMap<String, List<Integer>> getRetailerItemCodeMap(Connection conn) throws GeneralException {
		HashMap<String, List<Integer>> retailerItemCodeMap = new HashMap<String, List<Integer>>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<Integer> tempList = null;
		try {
			statement = conn.prepareStatement(GET_RETAILER_ITEM_CODE_MAP);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
				Integer itemCode = resultSet.getInt("ITEM_CODE");
				if (retailerItemCodeMap.get(retailerItemCode) != null)
					tempList = retailerItemCodeMap.get(retailerItemCode);
				else
					tempList = new ArrayList<Integer>();

				tempList.add(itemCode);
				retailerItemCodeMap.put(retailerItemCode, tempList);
			}
		} catch (SQLException e) {
			logger.error("Error while getting retailer item code and item code mapping");
			throw new GeneralException("Error while getting retailer item code and item code mapping", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailerItemCodeMap;

	}

	/* TWO APIS added by Suresh 7/30/2015 */

	/* Get items belonging to Category */

	public List<ItemDTO> getItemDetailsInACategory(Connection conn, int productLevelId, int productId) throws GeneralException {

		List<ItemDTO> itemList = new ArrayList<ItemDTO>();
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT IL.item_code, IL.item_name, IL.RET_LIR_ID, RLIG.RET_LIR_NAME, ");
		sb.append("IL.UPC, IL.retailer_item_code, IL.ITEM_SIZE, UL.NAME AS UOM_NAME, BL.BRAND_NAME FROM ITEM_LOOKUP IL ");
		sb.append("LEFT JOIN UOM_LOOKUP UL on IL.UOM_ID = UL.ID ");
		sb.append("LEFT JOIN RETAILER_LIKE_ITEM_GROUP RLIG on IL.RET_LIR_ID = RLIG.RET_LIR_ID ");
		sb.append("LEFT JOIN BRAND_LOOKUP BL on IL.BRAND_ID = BL.BRAND_ID ");
		sb.append("WHERE IL.ITEM_CODE IN ( ");

		sb.append(" SELECT CHILD_PRODUCT_ID FROM (SELECT CHILD_PRODUCT_ID,CHILD_PRODUCT_LEVEL_ID FROM PRODUCT_GROUP_RELATION_REC PGR ");
		sb.append(" start with product_level_id = ").append(productLevelId);
		sb.append(" and product_id = ").append(productId);
		sb.append(" connect by  prior child_product_id = product_id  and  prior child_product_level_id = product_level_id ");
		sb.append(" ) WHERE CHILD_PRODUCT_LEVEL_ID = 1 ");
		sb.append(") AND IL.ACTIVE_INDICATOR ='Y' ");

		CachedRowSet result = PristineDBUtil.executeQuery(conn, sb, "getItemDetails");
		ItemDTO retItem = null;

		try {
			while (result.next()) {
				retItem = new ItemDTO();
				retItem.itemCode = result.getInt("ITEM_CODE");
				retItem.itemName = result.getString("ITEM_NAME");
				retItem.upc = result.getString("UPC");
				retItem.retailerItemCode = result.getString("RETAILER_ITEM_CODE");
				retItem.likeItemId = result.getInt("RET_LIR_ID");
				retItem.size = result.getString("ITEM_SIZE");
				retItem.uom = result.getString("UOM_NAME");
				retItem.brandName = result.getString("BRAND_NAME");
				retItem.likeItemGrp = result.getString("RET_LIR_NAME");
				itemList.add(retItem);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new GeneralException("Cached Rowset Access Exception", e);
		}

		return itemList;
	}

	/*
	 * select NAME, CODE FROM product_group where product_level_id = 4 and product_id = 1479
	 */

	public String getProductGroupName(Connection conn, int productLevelId, int productId) throws GeneralException {

		StringBuffer sb = new StringBuffer();
		sb.append("select NAME FROM product_group where ");
		sb.append(" product_level_id = ").append(productLevelId);
		sb.append(" and product_id = ").append(productId);

		return (PristineDBUtil.getSingleColumnVal(conn, sb, "GetProductGroupName"));
	}

	// public void updateUOMData(Connection _Conn, HashMap<String, Integer>
	// uomOutMap) throws GeneralException {
	public void updateUOMData(Connection _Conn, HashMap<String, UOMDataDTO> uomOutMap) throws GeneralException {
		try {

			PreparedStatement psmt = _Conn.prepareStatement(UpdateUOMSql());

			// for (Map.Entry<String, Integer> entry : uomOutMap.entrySet()) {
			// addUOMData(entry.getKey(), entry.getValue(), psmt);
			// }

			// Modified by RB
			for (Map.Entry<String, UOMDataDTO> entry : uomOutMap.entrySet()) {
				addUOMData(entry.getKey(), entry.getValue(), psmt);
			}

			psmt.executeBatch();
			psmt.close();
			_Conn.commit();
			uomOutMap = null;

		} catch (Exception se) {
			logger.debug(se);
			throw new GeneralException("addMovementData", se);
		}

	}

	// public void addUOMData(String retailItemCode, int UomId,
	// PreparedStatement psmt) throws GeneralException {
	public void addUOMData(String retailItemCode, UOMDataDTO UOMDataDTO, PreparedStatement psmt) throws GeneralException {

		try {
			// psmt.setObject(1, UomId); // UOM Id
			// psmt.setObject(2, retailItemCode); // Retailer Item Code

			// Modified by RB
			psmt.setObject(1, UOMDataDTO.uomid);
			if (UOMDataDTO.preprice != 0.0000) {
				psmt.setObject(2, 1); // Pre Price
			} else {
				psmt.setObject(2, 0); // Pre Price
			}
			psmt.setObject(3, retailItemCode); // Retailer Item Code
			psmt.addBatch();

		} catch (Exception sql) {
			logger.debug(sql);
			throw new GeneralException("addUOMData", sql);

		}
	}

	private String UpdateUOMSql() {
		StringBuffer Sql = new StringBuffer();
		// Sql.append("UPDATE ITEM_LOOKUP SET UOM_ID =? WHERE RETAILER_ITEM_CODE
		// = ?");
		Sql.append("UPDATE ITEM_LOOKUP SET UOM_ID =?, PREPRICED_IND =? WHERE RETAILER_ITEM_CODE = ?");
		logger.debug("UpdateUOMSql " + Sql.toString());
		return Sql.toString();
	}

	/**
	 * 
	 * @param conn
	 * @return Map of retiler item code and list of items
	 * @throws GeneralException
	 */

	public HashMap<String, List<ItemDTO>> getItemcodeMap(Connection conn) throws GeneralException {
		HashMap<String, List<ItemDTO>> retailerItemCodeMap = new HashMap<String, List<ItemDTO>>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(GET_ITEM_CODE_MAP_WITH_LIR_INFO);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
				int itemCode = resultSet.getInt("ITEM_CODE");
				int retLirId = resultSet.getInt("RET_LIR_ID");
				ItemDTO itemDTO = new ItemDTO();
				itemDTO.itemCode = itemCode;
				itemDTO.likeItemId = retLirId;
				if (retailerItemCodeMap.get(retailerItemCode) != null) {
					List<ItemDTO> tempList = retailerItemCodeMap.get(retailerItemCode);
					tempList.add(itemDTO);
					retailerItemCodeMap.put(retailerItemCode, tempList);
				} else {
					List<ItemDTO> tempList = new ArrayList<ItemDTO>();
					tempList.add(itemDTO);
					retailerItemCodeMap.put(retailerItemCode, tempList);
				}
			}
		} catch (SQLException e) {
			logger.error("Error while getting retailer item code and item code mapping");
			throw new GeneralException("Error while getting retailer item code and item code mapping", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailerItemCodeMap;

	}

	public int updateUserAttributes(Connection conn, List<GiantEagleItemDTO> itemList, boolean updateTier) throws GeneralException {
		int updatedRows = 0;
		PreparedStatement statement = null;
		try {
			if (updateTier)
				statement = conn.prepareStatement(UPDATE_USER_ATTRIBUTES_TIER);
			else
				statement = conn.prepareStatement(UPDATE_USER_ATTRIBUTES);
			int rowCount = 0;
			for (GiantEagleItemDTO giantEagleItemDTO : itemList) {
				int counter = 0;
				statement.setString(++counter, giantEagleItemDTO.getPRC_GRP_CD());
				statement.setString(++counter, giantEagleItemDTO.getSYS_CD());
				statement.setString(++counter, giantEagleItemDTO.getSPLR_TYP_CD());
				if (updateTier) {
					if (giantEagleItemDTO.getTIER() == null || Constants.EMPTY.equals(giantEagleItemDTO.getTIER())) {
						statement.setNull(++counter, Types.NULL);
					} else {
						statement.setString(++counter, giantEagleItemDTO.getTIER());
					}
				}
				statement.setString(++counter, PrestoUtil.castUPC(giantEagleItemDTO.getUPC(), false));
				statement.setString(++counter, giantEagleItemDTO.getRITEM_NO());
				statement.addBatch();
				rowCount++;
				if (rowCount % Constants.BATCH_UPDATE_COUNT == 0) {
					int updateCount[] = statement.executeBatch();
					updatedRows += PristineDBUtil.getUpdateCount(updateCount);
					statement.clearBatch();
					rowCount = 0;
				}
			}

			if (rowCount > 0) {
				int updateCount[] = statement.executeBatch();
				updatedRows += PristineDBUtil.getUpdateCount(updateCount);
				statement.clearBatch();
				rowCount = 0;
			}
		} catch (SQLException e) {
			throw new GeneralException("Error -- updateUserAttributes()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return updatedRows;
	}

	public int updateUserAttributesForLig(Connection conn, List<GiantEagleItemDTO> itemList, boolean updateTier) throws GeneralException {
		int updatedRows = 0;
		PreparedStatement statement = null;
		try {
			if (updateTier)
				statement = conn.prepareStatement(UPDATE_USER_ATTRIBUTES_TIER);
			else
				statement = conn.prepareStatement(UPDATE_USER_ATTRIBUTES);
			int rowCount = 0;
			for (GiantEagleItemDTO giantEagleItemDTO : itemList) {
				int counter = 0;
				statement.setString(++counter, giantEagleItemDTO.getPRC_GRP_CD());
				statement.setString(++counter, giantEagleItemDTO.getSYS_CD());
				statement.setString(++counter, giantEagleItemDTO.getSPLR_TYP_CD());
				if (updateTier) {
					if (giantEagleItemDTO.getTIER() == null || Constants.EMPTY.equals(giantEagleItemDTO.getTIER())) {
						statement.setNull(++counter, Types.NULL);
					} else {
						statement.setString(++counter, giantEagleItemDTO.getTIER());
					}
				}
				statement.setString(++counter, "L" + PrestoUtil.castUPC(giantEagleItemDTO.getUPC(), false));
				statement.setString(++counter, "L" + giantEagleItemDTO.getRITEM_NO());
				statement.addBatch();
				rowCount++;
				if (rowCount % Constants.BATCH_UPDATE_COUNT == 0) {
					int updateCount[] = statement.executeBatch();
					updatedRows += PristineDBUtil.getUpdateCount(updateCount);
					statement.clearBatch();
					rowCount = 0;
				}
			}

			if (rowCount > 0) {
				int updateCount[] = statement.executeBatch();
				updatedRows += PristineDBUtil.getUpdateCount(updateCount);
				statement.clearBatch();
				rowCount = 0;
			}
		} catch (SQLException e) {
			throw new GeneralException("Error -- updateUserAttributes()", e);
		} finally {
			PristineDBUtil.close(statement);
		}
		return updatedRows;
	}

	/**
	 * 
	 * @param conn
	 * @return list of all active items
	 * @throws GeneralException
	 */
	public List<ItemDTO> getAllActiveItems(Connection conn) throws GeneralException {
		List<ItemDTO> itemLookup = new ArrayList<>();
		String qry = "SELECT ITEM_CODE, UPC,  RETAILER_ITEM_CODE, " + " RET_LIR_ID, ITEM_NAME, ITEM_SIZE,  USER_ATTR_3, "
				+ " USER_ATTR_4, USER_ATTR_5, ACTIVE_INDICATOR FROM ITEM_LOOKUP " + " WHERE LIR_IND = 'N'";
		 //+ " AND UPC IN ('007433336112')"; //for testing
		
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(qry);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				
				String activeInd = resultSet.getString("ACTIVE_INDICATOR");
				
				ItemDTO itemDTO = new ItemDTO();
				itemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				itemDTO.setUpc(PrestoUtil.castUPC(resultSet.getString("UPC"), false));
				itemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				itemDTO.setLikeItemId(resultSet.getInt("RET_LIR_ID"));
				itemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				itemDTO.setSize(resultSet.getString("ITEM_SIZE"));
				itemDTO.setPrcGrpCd(resultSet.getString("USER_ATTR_3"));
				itemDTO.setSysCd(resultSet.getString("USER_ATTR_4"));
				itemDTO.setSplrTypCd(resultSet.getString("USER_ATTR_5"));
				
				if(String.valueOf(Constants.YES).equalsIgnoreCase(activeInd)){
					itemDTO.setActive(true);
				} else {
					itemDTO.setActive(false);
				}
				
				itemLookup.add(itemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getAllActiveItems()", sqlE);
			throw new GeneralException("Error -- getAllActiveItems()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return itemLookup;
	}

	/**
	 * 
	 * @param conn
	 * @param collection
	 * @return Map of item codes and retailer names like Ahold, RiteAid etc..
	 * @throws GeneralException
	 */
	public HashMap<String, List<String>> getRetailerNamesForItems(Connection conn, Collection<String> collection) throws GeneralException {
		HashMap<String, List<String>> retailerMap = new HashMap<>();
		List<String> itemCodeList = new ArrayList<>();
		int limitCount = 0;
		for (String itemCode : collection) {
			itemCodeList.add(itemCode);
			limitCount++;
			if (limitCount % Constants.BATCH_UPDATE_COUNT == 0) {
				Object[] values = itemCodeList.toArray();
				retrieveRetailerNames(conn, retailerMap, values);
				itemCodeList.clear();
			}
		}

		if (itemCodeList.size() > 0) {
			Object[] values = itemCodeList.toArray();
			retrieveRetailerNames(conn, retailerMap, values);
			itemCodeList.clear();
		}

		return retailerMap;
	}

	/**
	 * for each 1000 items, fills the retailer map with retailer name
	 * 
	 * @param conn
	 * @param retailerMap
	 * @param values
	 * @throws GeneralException
	 */
	private void retrieveRetailerNames(Connection conn, HashMap<String, List<String>> retailerMap, Object... values) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(String.format(GET_RETAILER_NAMES_FOR_ITEMS, PristineDBUtil.preparePlaceHolders(values.length)));
			PristineDBUtil.setValues(statement, values);
			// statement.setFetchSize(10000);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String itemCode = resultSet.getString("ITEM_CODE");
				String retailerName = resultSet.getString("RETAILER_NAME").toLowerCase();
				if (retailerMap.get(itemCode) == null) {
					List<String> tempList = new ArrayList<>();
					tempList.add(retailerName);
					retailerMap.put(itemCode, tempList);
				} else {
					List<String> tempList = retailerMap.get(itemCode);
					tempList.add(retailerName);
					retailerMap.put(itemCode, tempList);
				}
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- retrieveRetailerNames()", sqlE);
			throw new GeneralException("Error -- retrieveRetailerNames()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);

		}
	}

	/**
	 * Inserts retailer names for each item
	 * 
	 * @param conn
	 * @param retailerMap
	 * @throws GeneralException
	 */
	public void insertRetailerNames(Connection conn, HashMap<String, List<String>> retailerMap) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_RETAILER_NAMES_FOR_ITEMS);
			int itemsInBatch = 0;
			for (Map.Entry<String, List<String>> entry : retailerMap.entrySet()) {
				for (String retailerName : entry.getValue()) {
					int colCount = 0;
					statement.setString(++colCount, entry.getKey());
					statement.setString(++colCount, retailerName);
					statement.addBatch();
					itemsInBatch++;
					if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
						statement.executeBatch();
						statement.clearBatch();
						itemsInBatch = 0;
					}
				}
			}
			if (itemsInBatch > 0) {
				statement.executeBatch();
				statement.clearBatch();
				itemsInBatch = 0;
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- insertRetailerNames()", sqlE);
			throw new GeneralException("Error -- insertRetailerNames()", sqlE);
		}
	}

	/**
	 * 
	 * @param conn
	 * @return List of all items from item_lookup
	 * @throws GeneralException
	 */
	public List<ItemDTO> getItemLookupForExport(Connection conn) throws GeneralException {
		List<ItemDTO> itemLookup = new ArrayList<>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {

			statement = conn.prepareStatement(EXPORT_ITEM_LOOKUP);
			statement.setFetchSize(100000);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				// A.DEPT_NAME DEPT_SHRT_NAME , A.DEPT_CODE, B.DESCRIPTION
				// DEPT_NAME,
				// + " A.CAT_NAME, A.CATEGORY_CODE, A.RETAILER_ITEM_CODE,
				// A.ITEM_NAME, "
				// + " A.ITEM_SIZE, A.UOM_NAME, A.PACK, A.PRIVATE_LABEL_CODE,
				// A.RET_LIR_NAME, "
				// + " A.RET_LIR_CODE, A.RETAILER_UPC, SRM.RETAILER_NAME,
				// BL.BRAND_NAME

				ItemDTO itemDTO = new ItemDTO();
				itemDTO.setDeptShortName(resultSet.getString("DEPT_SHRT_NAME"));
				itemDTO.setDeptCode(resultSet.getString("DEPT_CODE"));
				itemDTO.setDeptShortName(resultSet.getString("DEPT_NAME"));
				itemDTO.setCatName(resultSet.getString("CAT_NAME"));
				itemDTO.setCatCode(resultSet.getString("CATEGORY_CODE"));
				itemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				itemDTO.setItemName(resultSet.getString("ITEM_NAME"));
				itemDTO.setSize(resultSet.getString("ITEM_SIZE"));
				itemDTO.setUom(resultSet.getString("UOM_NAME"));
				itemDTO.setPack(resultSet.getString("PACK"));
				itemDTO.setPrivateLabelCode(resultSet.getString("PRIVATE_LABEL_CODE"));
				itemDTO.setLikeItemGrp(resultSet.getString("RET_LIR_NAME"));
				itemDTO.setLikeItemCode(resultSet.getString("RET_LIR_CODE"));
				itemDTO.setUpc(resultSet.getString("RETAILER_UPC"));
				itemDTO.setRetailerName(resultSet.getString("RETAILER_NAME"));
				itemDTO.setBrandName(resultSet.getString("BRAND_NAME"));
				itemLookup.add(itemDTO);
			}
		} catch (SQLException sqlE) {
			logger.info("Error -- getItemLookupForExport()", sqlE);
			throw new GeneralException("Error -- getItemLookupForExport()", sqlE);
		}

		return itemLookup;
	}

	/**
	 * To get sequence number which is not used in Item lookup table
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	private String getUnusedLirSeqFromItemLookup(Connection conn) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		String lirItemCode = null;
		try {
			statement = conn.prepareStatement(GET_UNUSED_LIR_ITEM_SEQ_FROM_ITEM_LOOKUP);
			statement.setString(1, String.valueOf(PropertyManager.getProperty("ITEMLOAD.MAX_LIR_ITEM_CODE", "999999")));
			statement.setString(2, String.valueOf(PropertyManager.getProperty("ITEMLOAD.MIN_LIR_ITEM_CODE", "920000")));
			statement.setString(3, String.valueOf(PropertyManager.getProperty("ITEMLOAD.MAX_LIR_ITEM_CODE", "999999")));
			statement.setString(4, String.valueOf(PropertyManager.getProperty("ITEMLOAD.MIN_LIR_ITEM_CODE", "920000")));
			resultSet = statement.executeQuery();
			if (resultSet.next()) {
				lirItemCode = resultSet.getString("MISSING_ITEM_CODE");
			}
		} catch (SQLException e) {
			logger.error("Error while executing getUnusedLirSeqFromItemLookup()", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return lirItemCode;
	}

	/**
	 * Returns Lir Item code sequence id
	 * 
	 * @return
	 * @throws GeneralException
	 */
	private String getLirItemCode(Connection conn) throws GeneralException {
		String lirItemCode = null;
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_LIR_ITEM_CODE_SEQ);
			rs = stmt.executeQuery();
			if (rs.next()) {
				lirItemCode = rs.getString("LIR_ITEM_CODE");
			}
		} catch (SQLException exception) {
			logger.error("Error when retrieving Lir Item code sequence - " + exception);
			throw new GeneralException("Exception in getLirItemCode() " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return lirItemCode;
	}

	/**
	 * Returns retailer_item_code for every upc in item_looup
	 * 
	 * @param conn
	 * @return
	 * @throws GeneralException
	 */
	public HashMap<String, List<String>> getRetItemCodeBasedOnUPC(Connection conn) throws GeneralException {
		HashMap<String, List<String>> itemHashMap = new HashMap<String, List<String>>();
		CachedRowSet itemRS = null;
		StringBuffer sb = new StringBuffer();
		sb.append("SELECT UPC, RETAILER_ITEM_CODE FROM RETAILER_ITEM_CODE_MAP");

		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getRetItemCodeBasedOnUPC");
			while (itemRS.next()) {
				List<String> tempList = new ArrayList<String>();
				if (itemHashMap.containsKey(itemRS.getString("UPC"))) {
					tempList.addAll(itemHashMap.get(itemRS.getString("UPC")));
				}
				tempList.add(itemRS.getString("RETAILER_ITEM_CODE"));
				itemHashMap.put(itemRS.getString("UPC"), tempList);
				int size = itemHashMap.size();
				if ((size % 25000) == 0) {
					logger.debug("getUPCAndRetailerItemCode: processed " + String.valueOf(size));
				}
			}
		} catch (SQLException e) {
			logger.error("Error while retrieving upc and retailer item code IN getRetItemCodeBasedOnUPC()");
			throw new GeneralException("Error while retrieving upc and retailer item code IN getRetItemCodeBasedOnUPC() - " + e);
		} finally {
			PristineDBUtil.close(itemRS);
		}

		return itemHashMap;
	}

	public String getQueryBasedOnActiveProductId(Connection conn, int productLevel, ProductService productService, int productHierarchy)
			throws GeneralException {
		StringBuffer sb = new StringBuffer();

		if (productService.productHierarchy.size() == 0)
			productService.getChildHierarchy(conn, productLevel);

		int productHierarchyCount = productService.productHierarchy.size();
		sb.append("UPDATE PRODUCT_GROUP SET ACTIVE_INDICATOR ='Y' WHERE PRODUCT_LEVEL_ID =? AND PRODUCT_ID IN (");
		sb.append("SELECT P_ID_").append(productHierarchy).append(" FROM (SELECT ITEM_CODE as ITEM_CODE_1,");
		for (int i = 1; i < productHierarchyCount; i++) {
			// sb.append("P_L_ID_" + i + ", P_ID_" + i + ", P" + i + ".NAME NAME_" + i + ",");
			sb.append("P_L_ID_" + i + ", P_ID_" + i + ",");
		}
		sb.delete((sb.length() - 1), sb.length());
		sb.append(" FROM ");
		sb.append(" (SELECT ");
		for (int j = 1; j < productHierarchyCount; j++) {
			if (j == 1) {
				sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j
						+ ".CHILD_PRODUCT_ID ITEM_CODE ,");
			} else {
				sb.append(" A" + j + ".PRODUCT_LEVEL_ID P_L_ID_" + j + ", A" + j + ".PRODUCT_ID P_ID_" + j + ", A" + j
						+ ".CHILD_PRODUCT_ID CHILD_P_ID_" + j + ",");
			}
		}
		sb.delete((sb.length() - 1), sb.length());

		sb.append(" FROM ");
		for (int x = 1; x < productHierarchyCount; x++) {
			int loop = productHierarchyCount - 1;
			if (x > loop) {
				break;
			}
			sb.append("( ");
		}

		for (int k = 1; k <= productHierarchyCount; k++) {
			if (k == 1) {
				sb.append(" PRODUCT_GROUP_RELATION A" + k + " ");
			} else if (k == 2) {
				sb.append(" LEFT JOIN PRODUCT_GROUP_RELATION A" + k + " ");
				break;
			}
		}

		String firstLevel = "";
		String nextLevel = "";
		for (int a = 1; a < productHierarchyCount + 1; a++) {
			if (a == 1) {
				firstLevel = "A" + a;
			} else if (a == 2) {
				nextLevel = "A" + a;
				break;
			}
		}
		sb.append("ON " + firstLevel + ".PRODUCT_ID = " + nextLevel + ".CHILD_PRODUCT_ID AND " + firstLevel + ".PRODUCT_LEVEL_ID = " + nextLevel
				+ ".CHILD_PRODUCT_LEVEL_ID) ");

		for (int b = 1; b < productHierarchyCount + 1; b++) {
			int loopCheck = b;
			if (b >= 3) {
				sb.append("LEFT JOIN PRODUCT_GROUP_RELATION A" + b + " ");
				sb.append(" ON A" + (loopCheck - 1) + ".PRODUCT_ID = A" + b + ".CHILD_PRODUCT_ID AND A" + (loopCheck - 1) + ".PRODUCT_LEVEL_ID = A"
						+ b + ".CHILD_PRODUCT_LEVEL_ID ) ");
			}
		}

		for (int c = 1; c < productHierarchyCount + 1; c++) {
			if (c == 1) {
				sb.append("WHERE A" + c + ".CHILD_PRODUCT_LEVEL_ID = 1 ");
				sb.append("AND A" + c + ".CHILD_PRODUCT_ID IN (SELECT ITEM_CODE FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR='Y') ");
				break;
			}
		}
		sb.append(") ");
		
		int chainId = Integer.parseInt(PropertyManager.getProperty("SUBSCRIBER_CHAIN_ID"));
		if(chainId == 53){
			sb.append(" WHERE P_L_ID_").append(productHierarchyCount - 1).append(" = ").append(Constants.LOBLEVELID).append("))");
		}
		else
		{
			sb.append(" WHERE P_L_ID_").append(productHierarchyCount - 1).append(" = ").append(Constants.DEPARTMENTLEVELID).append("))");
		}
		

		return sb.toString();
	}

	public HashMap<String, Integer> getUserAttrDetailsMap(Connection conn) throws GeneralException {
		HashMap<String, Integer> userAttrMap = new HashMap<String, Integer>();
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_USER_ATTR_LOOKUP_DETAILS_MAP);
			rs = stmt.executeQuery();
			while (rs.next()) {
				userAttrMap.put(rs.getString("ATTR_DISPLAY_NAME"), rs.getInt("ATTR_ID"));
			}
		} catch (SQLException exception) {
			logger.error("Error when retrieving USER ATTR Details - " + exception);
			throw new GeneralException("Exception in getUserAttrDetails() " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return userAttrMap;
	}

	public void insertUserAttrDetails(Connection conn, String attrDisplayName) throws GeneralException {
		PreparedStatement statement = null;
		try {
			statement = conn.prepareStatement(INSERT_INTO_USER_ATTR_LOOKUP);
			statement.setString(1, attrDisplayName);
			int updateCount = statement.executeUpdate();
			logger.info("No of rows inserted:" + updateCount);
		} catch (SQLException sqlE) {
			logger.error("Error -- insertUserAttrDetails()", sqlE);
			throw new GeneralException("Error -- insertUserAttrDetails()", sqlE);
		}
	}

	public void getUserAttrDetails(Connection conn, HashMap<String, Integer> getUserAttrDetails, String attrDisplayName) throws GeneralException {
		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_USER_ATTR_LOOKUP_DETAILS);
			stmt.setString(1, attrDisplayName);
			rs = stmt.executeQuery();
			while (rs.next()) {
				getUserAttrDetails.put(rs.getString("ATTR_DISPLAY_NAME"), rs.getInt("ATTR_ID"));
			}
		} catch (SQLException exception) {
			logger.error("Error when retrieving USER ATTR Details - " + exception);
			throw new GeneralException("Exception in getUserAttrDetails() " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
	}

	public void updateUserAttrInItemLookup(Connection conn, HashMap<String, Integer> itemWithAttrId) throws GeneralException {
		int noOfRecords = 0;
		int itemsInBatch = 0;
		try {
			PreparedStatement statement = conn.prepareStatement(UPDATE_USER_ATTR_DETAIL_IN_ITEM_LOOKUP);
			for (Map.Entry<String, Integer> finalEntry : itemWithAttrId.entrySet()) {
				itemsInBatch++;
				int colCount = 0;
				statement.setInt(++colCount, finalEntry.getValue());
				statement.setString(++colCount, finalEntry.getKey());
				statement.addBatch();
				if (itemsInBatch % Constants.BATCH_UPDATE_COUNT == 0) {
					int count[] = statement.executeBatch();
					noOfRecords += count.length;
					statement.clearBatch();
					itemsInBatch = 0;
				}

			}
			if (itemsInBatch > 0) {
				int count[] = statement.executeBatch();
				noOfRecords += count.length;
				statement.clearBatch();
				itemsInBatch = 0;
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- updateUserAttrInItemLookup()", sqlE);
			throw new GeneralException("Error -- updateUserAttrInItemLookup()", sqlE);
		}
		logger.info("# of records updated: " + noOfRecords);
	}

	public HashMap<String, List<ItemMappingDTO>> getBaseItemDetails(Connection conn, int calendarId) throws GeneralException {
		HashMap<String, List<ItemMappingDTO>> baseItemDetails = new HashMap<String, List<ItemMappingDTO>>();

		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_BASE_ITEM_DETAILS);
			int colCount = 0;
			stmt.setInt(++colCount, calendarId);
//			stmt.setInt(++colCount, 2021);
			rs = stmt.executeQuery();
			while (rs.next()) {
				List<ItemMappingDTO> itemMappingDTOs = new ArrayList<ItemMappingDTO>();
				ItemMappingDTO itemMappingDTO = new ItemMappingDTO();
				itemMappingDTO.setBaseItemName(rs.getString("ITEM_NAME"));
				itemMappingDTO.setBaseSize(rs.getDouble("ITEM_SIZE"));
				itemMappingDTO.setBaseRegPrice(rs.getDouble("REG_PRICE"));
				itemMappingDTO.setUpc(rs.getString("UPC"));
				itemMappingDTO.setRetailerItemCode(rs.getString("RETAILER_ITEM_CODE"));
				itemMappingDTO.setSubCatName(rs.getString("SUB_CATEGORY_NAME"));
				itemMappingDTO.setCategoryName(rs.getString("CATEGORY_NAME"));
				itemMappingDTO.setDepartmentName(rs.getString("DEPARTMENT_NAME"));
				itemMappingDTO.setBaseItemFriendlyName(rs.getString("FRIENDLY_NAME"));

				if (baseItemDetails.containsKey(itemMappingDTO.getCategoryName())) {
					itemMappingDTOs = baseItemDetails.get(itemMappingDTO.getCategoryName());
				}
				itemMappingDTOs.add(itemMappingDTO);
				baseItemDetails.put(itemMappingDTO.getCategoryName(), itemMappingDTOs);
			}
		} catch (SQLException exception) {
			logger.error("Error when retrieving getBaseItemDetails - " + exception);
			throw new GeneralException("Exception in getBaseItemDetails() " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return baseItemDetails;

	}

	public HashMap<String, List<Integer>> getItemCodeBasedOnUPC(Connection conn) throws GeneralException {
		HashMap<String, List<Integer>> baseItemDetails = new HashMap<String, List<Integer>>();

		PreparedStatement stmt = null;
		ResultSet rs = null;
		try {
			stmt = conn.prepareStatement(GET_ITEM_CODE_BASED_ON_UPC);
			rs = stmt.executeQuery();
			while (rs.next()) {
				List<Integer> items = new ArrayList<Integer>();
				int itemCode = rs.getInt("ITEM_CODE");
				String upc = rs.getString("UPC");

				if (baseItemDetails.containsKey(upc)) {
					items = baseItemDetails.get(upc);
				}
				items.add(itemCode);
				baseItemDetails.put(upc, items);
			}
		} catch (SQLException exception) {
			logger.error("Error when retrieving getItemCodeBasedOnUPC - " + exception);
			throw new GeneralException("Exception in getItemCodeBasedOnUPC() " + exception);
		} finally {
			PristineDBUtil.close(rs);
			PristineDBUtil.close(stmt);
		}
		return baseItemDetails;

	}

	public HashMap<String, List<String>> getPLCompCatMapping(Connection connection, String compName) throws GeneralException {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<String, List<String>> plCatMap = new HashMap<String, List<String>>();
		try {
			try {
				statement = connection.prepareStatement(GET_PL_COMP_CATEGORY_MAPPING);
				statement.setString(1, compName);
				resultSet = statement.executeQuery();
				while (resultSet.next()) {
					List<String> tempCompCat = new ArrayList<>();
					if (plCatMap.containsKey(resultSet.getString("BASE_CATEGORY"))) {
						tempCompCat = plCatMap.get(resultSet.getString("BASE_CATEGORY"));
					}
					tempCompCat.add(resultSet.getString("COMP_CATEGORY"));
					plCatMap.put(resultSet.getString("BASE_CATEGORY"), tempCompCat);
				}
			} catch (SQLException e) {
				logger.error("Error while executing getPLCompCatMapping() " + e);
				throw new GeneralException("Error while executing getPLCompCatMapping() " + e);
			}
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return plCatMap;
	}
	
	public HashMap<String, Set<ItemDTO>> getActiveItemsFromItemLookup(Connection conn) throws GeneralException {
		HashMap<String, Set<ItemDTO>> retailerItemCodeMap = new HashMap<String, Set<ItemDTO>>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		Set<ItemDTO> tempList = null;
		try {
			statement = conn.prepareStatement(GET_ITEM_LOOKUP_ACTIVE_ITEMS);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				ItemDTO itemDTO = new ItemDTO();
				itemDTO.setPrcGrpCd(resultSet.getString("USER_ATTR_3"));
				itemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				itemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				if (retailerItemCodeMap.get(itemDTO.getRetailerItemCode()) != null)
					tempList = retailerItemCodeMap.get(itemDTO.getRetailerItemCode());
				else
					tempList = new HashSet<ItemDTO>();

				tempList.add(itemDTO);
				retailerItemCodeMap.put(itemDTO.getRetailerItemCode(), tempList);
			}
		} catch (SQLException e) {
			logger.error("Error while getting retailer item code and item code mapping");
			throw new GeneralException("Error while getting retailer item code and item code mapping", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return retailerItemCodeMap;

	}
	
	
	/**
	 * 
	 * @param conn
	 * @return item code and category map
	 * @throws GeneralException
	 */
	public HashMap<Integer, Integer> getCategoryAndItemCodeMap(Connection conn, String itemCodes) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<Integer, Integer> categoryItemCodeMap= new HashMap<>();
		try{
			
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT ITEM_CODE, PROD.PRODUCT_ID AS CAT_ID ");
			sb.append(" FROM ITEM_LOOKUP IL JOIN (SELECT CAT.PRODUCT_ID, CAT.PRODUCT_LEVEL_ID, ");
			sb.append(" SUB_CAT.CHILD_PRODUCT_ID AS ITEM_C0DE, SUB_CAT.CHILD_PRODUCT_LEVEL_ID FROM ");
			sb.append(" (SELECT CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID, PRODUCT_LEVEL_ID, PRODUCT_ID ");
			sb.append(" FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_LEVEL_ID = " 
							+ Constants.CATEGORYLEVELID + ") CAT ");
			sb.append(" JOIN  (SELECT CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID, PRODUCT_LEVEL_ID, PRODUCT_ID ");
			sb.append(" FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_LEVEL_ID = " 
							+ Constants.SUBCATEGORYLEVELID + ") SUB_CAT ");
			sb.append(" ON SUB_CAT.PRODUCT_LEVEL_ID = CAT.CHILD_PRODUCT_LEVEL_ID ");
			sb.append(" AND SUB_CAT.PRODUCT_ID = CAT.CHILD_PRODUCT_ID) PROD ");
			sb.append(" ON PROD.ITEM_C0DE = IL.ITEM_CODE WHERE IL.LIR_IND = 'N' ");
			
			if(itemCodes != null)
				sb.append(" AND IL.ITEM_CODE IN (" + itemCodes + ") ");
			
			
			
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			
			while(resultSet.next()){
				int categoryId = resultSet.getInt("CAT_ID");
				int itemCode = resultSet.getInt("ITEM_CODE");
				categoryItemCodeMap.put(itemCode, categoryId);
			}
		}catch(SQLException sqlE){
			logger.error("Error while executing getCategoryAndItemCodeMap() " + sqlE);
			throw new GeneralException("Error while executing getCategoryAndItemCodeMap() " + sqlE);
		}finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return categoryItemCodeMap;
	}
	
	
	
	/**
	 * 
	 * @param conn
	 * @return item code and category map
	 * @throws GeneralException
	 */
	public HashMap<PriceGroupAndCategoryKey, Integer> getItemCountForPrcGrp(Connection conn) throws GeneralException{
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		HashMap<PriceGroupAndCategoryKey, Integer> prcCodeCountMap= new HashMap<>();
		try{
			
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT IL.USER_ATTR_3, PROD.PRODUCT_ID AS CAT_ID, COUNT(ITEM_CODE) ITEM_COUNT ");
			sb.append(" FROM ITEM_LOOKUP IL JOIN (SELECT CAT.PRODUCT_ID, CAT.PRODUCT_LEVEL_ID, ");
			sb.append(" SUB_CAT.CHILD_PRODUCT_ID AS ITEM_C0DE, SUB_CAT.CHILD_PRODUCT_LEVEL_ID FROM ");
			sb.append(" (SELECT CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID, PRODUCT_LEVEL_ID, PRODUCT_ID ");
			sb.append(" FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_LEVEL_ID = " 
							+ Constants.CATEGORYLEVELID + ") CAT ");
			sb.append(" JOIN  (SELECT CHILD_PRODUCT_LEVEL_ID, CHILD_PRODUCT_ID, PRODUCT_LEVEL_ID, PRODUCT_ID ");
			sb.append(" FROM PRODUCT_GROUP_RELATION WHERE PRODUCT_LEVEL_ID = " 
							+ Constants.SUBCATEGORYLEVELID + ") SUB_CAT ");
			sb.append(" ON SUB_CAT.PRODUCT_LEVEL_ID = CAT.CHILD_PRODUCT_LEVEL_ID ");
			sb.append(" AND SUB_CAT.PRODUCT_ID = CAT.CHILD_PRODUCT_ID) PROD ");
			sb.append(" ON PROD.ITEM_C0DE = IL.ITEM_CODE AND IL.LIR_IND = 'N' ");
			sb.append(" WHERE IL.USER_ATTR_3 <> '" + Constants.GE_PRC_GRP_CD_DSD + "' AND IL.USER_ATTR_3 IS NOT NULL ");
			sb.append(" GROUP BY IL.USER_ATTR_3, PROD.PRODUCT_ID ");
			
			
			
			statement = conn.prepareStatement(sb.toString());
			resultSet = statement.executeQuery();
			
			while(resultSet.next()){
				String prcGrpCode = resultSet.getString("USER_ATTR_3");
				int catId = resultSet.getInt("CAT_ID");
				int itemCount = resultSet.getInt("ITEM_COUNT");
				PriceGroupAndCategoryKey priceGroupAndCategoryKey = new PriceGroupAndCategoryKey(prcGrpCode, catId);				
				prcCodeCountMap.put(priceGroupAndCategoryKey, itemCount);
			}
		}catch(SQLException sqlE){
			logger.error("Error while executing getItemCountForPrcGrp() " + sqlE);
			throw new GeneralException("Error while executing getItemCountForPrcGrp() " + sqlE);
		}finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		
		return prcCodeCountMap;
	}
	
	
	public HashMap<String, String> getItemCodeMapForTest(Connection conn, String upcIn) throws GeneralException {
		HashMap<String, String> itemMap = new HashMap<String, String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String qry = "SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE "
					+ " FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR = 'Y' "
					+ " AND LIR_IND = 'N' AND UPC IN(" + upcIn + ")";
			
			statement = conn.prepareStatement(qry);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String upc = resultSet.getString("UPC");
				String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
				String itemCode = resultSet.getString("ITEM_CODE");
				itemMap.put(upc + "-" + retailerItemCode, itemCode);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ALL_ITEMS");
			throw new GeneralException("Error while executing GET_ALL_ITEMS", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemMap;
	}
	
	
	public HashMap<ItemDetailKey, String> getItemCodeMapKeyForTest(Connection conn, String upcIn) throws GeneralException {
		HashMap<ItemDetailKey, String> itemMap = new HashMap<ItemDetailKey, String>();
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			String qry = "SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE "
					+ " FROM ITEM_LOOKUP WHERE ACTIVE_INDICATOR = 'Y' "
					+ " AND LIR_IND = 'N' AND UPC IN(" + upcIn + ")";
			
			statement = conn.prepareStatement(qry);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				String upc = resultSet.getString("UPC");
				String retailerItemCode = resultSet.getString("RETAILER_ITEM_CODE");
				String itemCode = resultSet.getString("ITEM_CODE");
				ItemDetailKey itemDetailKey = new ItemDetailKey(upc, retailerItemCode);
				itemMap.put(itemDetailKey, itemCode);
			}
		} catch (SQLException e) {
			logger.error("Error while executing GET_ALL_ITEMS");
			throw new GeneralException("Error while executing GET_ALL_ITEMS", e);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return itemMap;
	}
	
	/**
	 * 
	 * @param conn
	 * @return list of all active items
	 * @throws GeneralException
	 */
	public List<ItemDTO> getItemsByOrigAndCurrItemNumber(Connection conn) throws GeneralException {
		List<ItemDTO> itemLookup = new ArrayList<>();
		String qry = "SELECT ITEM_CODE, RETAILER_ITEM_CODE, ALT_RETAILER_ITEM_CODE FROM ITEM_LOOKUP WHERE LIR_IND = 'N'";	
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		try {
			statement = conn.prepareStatement(qry);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				ItemDTO itemDTO = new ItemDTO();
				itemDTO.setItemCode(resultSet.getInt("ITEM_CODE"));
				itemDTO.setRetailerItemCode(resultSet.getString("RETAILER_ITEM_CODE"));
				itemDTO.setAltRetailerItemCode(resultSet.getString("ALT_RETAILER_ITEM_CODE"));
				itemLookup.add(itemDTO);
			}
		} catch (SQLException sqlE) {
			logger.error("Error -- getItemsByOrigAndCurrItemNumber()", sqlE);
			throw new GeneralException("Error -- getItemsByOrigAndCurrItemNumber()", sqlE);
		} finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}

		return itemLookup;
	}
	
	
	public List<RetailerLikeItemGroupDTO> getAllLIR(Connection conn) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;
		List<RetailerLikeItemGroupDTO> lirs = new ArrayList<RetailerLikeItemGroupDTO>();
		try {
			statement = conn.prepareStatement(GET_ALL_LIR);
			resultSet = statement.executeQuery();
			while (resultSet.next()) {
				RetailerLikeItemGroupDTO retailerLikeItemGroupDTO = new RetailerLikeItemGroupDTO();
				retailerLikeItemGroupDTO.setRetLirId(resultSet.getInt("RET_LIR_ID"));
				retailerLikeItemGroupDTO.setRetLirName(resultSet.getString("RET_LIR_NAME"));
				lirs.add(retailerLikeItemGroupDTO);
			}
		} catch (SQLException | NullPointerException e) {
			logger.error("Error while executing getAllLIR() " + e);
		}  
		finally {
			PristineDBUtil.close(resultSet);
			PristineDBUtil.close(statement);
		}
		return lirs;
	}
	
	/**
	 * Returns List of all items from ITEM_LOOKUP
	 * 
	 * @param conn
	 * @throws GeneralException Added for Autozone by Karishma
	 */

	public List<RetailPriceDTO> getAllItemsFromLookup(Connection conn) throws GeneralException {
		List<RetailPriceDTO> itemLookupMap = new ArrayList<RetailPriceDTO>();
		CachedRowSet itemRS = null;

		StringBuffer sb = new StringBuffer();
		sb.append("SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE FROM ITEM_LOOKUP WHERE LIR_IND='N'");
		//sb.append("SELECT UPC, RETAILER_ITEM_CODE, ITEM_CODE FROM ITEM_LOOKUP WHERE RETAILER_ITEM_CODE IN('2524','10274','85,'79','63')");
		logger.debug(sb);

		// execute the statement
		try {
			itemRS = PristineDBUtil.executeQuery(conn, sb, "ItemDAO - getUPCAndItem");
			while (itemRS.next()) {
				RetailPriceDTO priceAndCostDTO = new RetailPriceDTO();
				priceAndCostDTO.setUpc(itemRS.getString("UPC"));
				priceAndCostDTO.setRetailerItemCode(itemRS.getString("RETAILER_ITEM_CODE"));
				priceAndCostDTO.setItemcode(itemRS.getString("ITEM_CODE"));

				itemLookupMap.add(priceAndCostDTO);

			}

		} catch (SQLException e) {
			logger.error("Exception in getAllItemsFromLookup" + e);
		}
		return itemLookupMap;
	}
}
