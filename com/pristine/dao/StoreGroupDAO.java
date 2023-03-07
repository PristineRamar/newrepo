package com.pristine.dao;

import java.sql.Connection;

import org.apache.log4j.Logger;

import com.pristine.dto.LocationDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;

public class StoreGroupDAO implements IDAO
{
	private static Logger	logger	= Logger.getLogger(StoreGroupDAO.class);

	public int getZoneId(Connection conn, String zoneNum, String zoneName) throws GeneralException{
		// TODO Auto-generated method stub
		int zoneId = -1;
		
		if( zoneNum != null ) zoneNum = zoneNum.trim(); 
		if( zoneName != null ) zoneName = zoneName.trim();
		
		if( zoneNum == null || zoneNum.equals("") )
			zoneNum = zoneName;
		
		if( zoneName == null || zoneName.equals("") )
			zoneName = zoneNum;

		if( zoneNum != null && !zoneNum.equals("")){
			//get the zone Id. 
			StringBuffer sb = new StringBuffer ();
			sb.append("SELECT PRICE_ZONE_ID FROM RETAIL_PRICE_ZONE WHERE ZONE_NUM = ");
			PristineDBUtil.appendNoComa(sb, zoneNum);
			String val = PristineDBUtil.getSingleColumnVal(conn, sb, "getZoneId");
			if( val != null){
				zoneId = Integer.parseInt(val);
			}
			
			
		}
		return zoneId;
	}
	
	public int updatePriceZoneActiveIndicator(Connection conn) throws GeneralException{
		int result = 0;
	    StringBuffer sb = new StringBuffer("UPDATE RETAIL_PRICE_ZONE SET ACTIVE_INDICATOR = 'N' ");
	    PristineDBUtil.executeUpdate(conn, sb, "StrGrpDAO - Zone Active Indicator Update");
		return result;   
	}
	
   public int updatePriceZone(Connection conn, String zoneNum, String zoneName, String desc, int zoneId) throws GeneralException{
		
		int result = 0;

		if( zoneNum != null ) zoneNum = zoneNum.trim(); 
		if( zoneName != null ) zoneName = zoneName.trim();
		
		if( zoneNum == null || zoneNum.equals("") )
			zoneNum = zoneName;
		
		if( zoneName == null || zoneName.equals("") )
			zoneName = zoneNum;

		if( zoneNum != null && !zoneNum.equals("")){
			
			zoneName = zoneName.replace("'", "\''");
			StringBuffer sb = new StringBuffer("UPDATE RETAIL_PRICE_ZONE RPZ SET RPZ.ACTIVE_INDICATOR = 'Y', RPZ.NAME = '" + zoneName + "' WHERE RPZ.PRICE_ZONE_ID = " + zoneId);
			PristineDBUtil.executeUpdate(conn, sb, "Store DAO - update Zone New");
		}
		return result;
		
	}
	
	public int insertPriceZone(Connection conn, String zoneNum, String zoneName, String desc) throws GeneralException{
		
		int zoneId = -1;

		if( zoneNum != null ) zoneNum = zoneNum.trim(); 
		if( zoneName != null ) zoneName = zoneName.trim();
		
		if( zoneNum == null || zoneNum.equals("") )
			zoneNum = zoneName;
		
		if( zoneName == null || zoneName.equals("") )
			zoneName = zoneNum;

		if( zoneNum != null && !zoneNum.equals("")){
			zoneName = zoneName.replace("'", "\''");
			StringBuffer sb = new StringBuffer ();
			sb.append("INSERT INTO RETAIL_PRICE_ZONE ");
			sb.append("(PRICE_ZONE_ID, NAME, DESCRIPTION, ZONE_NUM)");
			sb.append(" VALUES ( PRICE_ZONE_ID_SEQ.NEXTVAL, ");
			PristineDBUtil.append(sb, zoneName);
			PristineDBUtil.append(sb, desc);
			PristineDBUtil.appendNoComa(sb, zoneNum);
			sb.append(')');
			PristineDBUtil.execute(conn, sb, "StrGrpDAO - Zone Insert");
			zoneId = getZoneId(conn, zoneNum, zoneName);
		}
		return zoneId;
		
	}
	
	public int getLocationId(Connection conn, String tableName, String parentColumn, LocationDTO location ) 	throws GeneralException{
		int locationId = -1;
		if( location.name != null)
			location.name = location.name.trim().toUpperCase();
		
		if( location.name != null && !location.name.equals("")){
			StringBuffer sb = new StringBuffer ();
			sb.append(" SELECT ID FROM ").append(tableName);
			sb.append( " WHERE NAME = ");
			PristineDBUtil.appendNoComa(sb, location.name );
			if( parentColumn != null && location.parentId > 0){
				sb.append( " AND ");
				sb.append( parentColumn ).append('=');
				sb.append( location.parentId );
			}
			String val = PristineDBUtil.getSingleColumnVal(conn, sb, "getLocationId");
			if( val != null){
				locationId = Integer.parseInt(val);
			}

		}
		
		return locationId; 
	}

	public int insertLocation(Connection conn, String tableName, String  parentColumn, LocationDTO location) 
		throws GeneralException {
		int locationId = -1;
		if( location.name != null)
			location.name = location.name.trim().toUpperCase();
		
		if( location.name != null && !location.name.equals("")){
			StringBuffer sb = new StringBuffer ();
			sb.append( " INSERT INTO ").append(tableName).append("( ");
			sb.append( " ID, NAME, DESCRIPTION, MANAGER, CONTACT_NO " );
			if( parentColumn != null )
				sb.append( ',').append(parentColumn);
			sb.append( ')').append(" VALUES (");
			sb.append("LOCATION_ID_SEQ.NEXTVAL," );
			PristineDBUtil.append(sb, location.name );
			PristineDBUtil.append(sb, location.desc);
			PristineDBUtil.append(sb, location.manager);
			PristineDBUtil.appendNoComa(sb, location.phoneNo );
			if( parentColumn != null ){
				sb.append( ',');
				sb.append( location.parentId > 0 ? location.parentId: "NULL");
			}
			sb.append( ')');
			//logger.debug( sb.toString());
			PristineDBUtil.execute(conn, sb, "StrGrpDAO - Location Insert");
			locationId = getLocationId(conn, tableName, parentColumn, location);
		}
		return locationId;
	}

	public void updateLocation(Connection conn, String tableName, String parentColumn,
			LocationDTO location) throws GeneralException {
		StringBuffer sb = new StringBuffer("update ").append(tableName).append(" set ");

		PristineDBUtil.appendUpdate(sb, "NAME", location.name);
		PristineDBUtil.appendUpdate(sb, "DESCRIPTION", location.desc);
		PristineDBUtil.appendUpdate(sb, "MANAGER", location.desc);
		sb.append(" CONTACT_NO = ");
		PristineDBUtil.appendNoComa(sb, location.phoneNo);
		if( parentColumn != null ){
			sb.append(',').append(parentColumn).append('=');
			sb.append(location.parentId > 0 ? location.parentId: "NULL");
		}
		sb.append(" WHERE ID = " + location.locationId);

		//logger.debug(sb.toString());
		PristineDBUtil.executeUpdate(conn, sb, "Store Grp DAO - update location");

		
	}
		
	
	
	
}
