package com.pristine.dataload;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import javax.sql.rowset.CachedRowSet;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.DBManager;
import com.pristine.dao.ItemDAO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;
import com.pristine.dto.SizeGroupRange;

public class ItemSizeGroup
{
	// ItemSizeGroup OPERATION=1 (create item size groups by segment)
	// ItemSizeGroup OPERATION=2 (create item size groups by brand and segment)
	// ItemSizeGroup OPERATION=3 (place item in item size groups by segment)
	// ItemSizeGroup OPERATION=4 (place item in item size groups by brand and segment)
	
	public ItemSizeGroup ()
	{
		try
		{
	        PropertyManager.initialize("analysis.properties");
	        _Conn = DBManager.getConnection();
	        _Conn.setAutoCommit(true);
	    }
		catch (GeneralException gex) {
	        logger.error("Error initializing", gex);
	    }
		catch (Exception ex) {
	        logger.error("Error initializing", ex);
	    }
	}
	
	public static void main(String[] args)
	{
		PropertyConfigurator.configure("log4j-item-size.properties");
		logCommand (args);
		
        try
        {
        	int operation = ARG_OPERATION_NONE;
    		
    		for ( int ii = 0; ii < args.length; ii++ )
    		{
        		String arg = args[ii];
        		if ( arg.startsWith (ARG_OPERATION) ) {
        			operation = Integer.parseInt(arg.substring(ARG_OPERATION.length()));
        		}
    		}

        	ItemSizeGroup sizeGroup = new ItemSizeGroup ();
        	sizeGroup._PercentDiff = Double.parseDouble(PropertyManager.getProperty("SIZEGROUP.PERCENT_DIFF", "0.05"));
        	switch ( operation )
        	{
	    		case ARG_OPERATION_ITEM_SIZE_GROUPING_BY_SEGMENT:
	    			sizeGroup.createSizeGroups();
	    			break;
	    		case ARG_OPERATION_ITEM_SIZE_GROUPING_BY_BRAND_SEGMENT:
	    			sizeGroup._DoBrandSegnemt = true;
	    			sizeGroup.createSizeGroups();
	    			break;
	    		case ARG_OPERATION_PLACE_ITEM_IN_SIZE_GROUP_BY_SEGMENT:
	    			sizeGroup.placeItemsInSizeGroups();
	    			break;
	    		case ARG_OPERATION_PLACE_ITEM_IN_SIZE_GROUP_BY_BRAND_SEGMENT:
	    			sizeGroup._DoBrandSegnemt = true;
	    			sizeGroup.placeItemsInSizeGroups();
	    			break;
        		default:
        			break;
        	}
        	
    		// Close the connection
    		PristineDBUtil.close(sizeGroup._Conn);
		}
        catch (Exception ex) {
            logger.error("Error", ex);
        }
        
		logger.info("Item size group finished");
    }

	private static void logCommand (String[] args)
	{
		logger.info("*****************************************");
		StringBuffer sb = new StringBuffer("Command: ItemSizeGroup ");
		for ( int ii = 0; ii < args.length; ii++ ) {
			if ( ii > 0 ) sb.append(' ');
			sb.append (args[ii]);
		}
		logger.info(sb.toString());
	}

	private void createSizeGroups ()
	{
		Map<String, List<Double>> sizeMap = new HashMap<String, List<Double>>();
		Map<String, List<String>> sizeGroupMap = new HashMap<String, List<String>>();
		createSizeGroups (sizeMap, sizeGroupMap);
		save (sizeMap, sizeGroupMap);		// save to database
	}
	
	private void placeItemsInSizeGroups ()
	{
		Map<String, List<Double>> sizeMap = new HashMap<String, List<Double>>();
		Map<String, List<String>> sizeGroupMap = new HashMap<String, List<String>>();
		createSizeGroups (sizeMap, sizeGroupMap);
		
		Map<String, List<SizeGroupRange>> sizeGroupRangeMap = new HashMap<String, List<SizeGroupRange>>();
		setupSizeGroupRanges (sizeGroupMap, sizeGroupRangeMap);
		placeItemsInSizeGroup (sizeGroupRangeMap);
	}
	
	private void createSizeGroups (Map<String, List<Double>> sizeMap, Map<String, List<String>> sizeGroupMap)
	{
		ItemDAO dao = new ItemDAO();
		try
		{
			CachedRowSet sizes = dao.getSizeBrandSegment(_Conn, _DoBrandSegnemt);
			int curBrandId = -1;
			String curUomId = "NONE";
			int curSegmentId = -1;
			double curItemSize = -1;
			List<Double> curSizeList = null;
			String curKey = null;
			while ( sizes.next() )
			{
				int brandId, segmentId;
				String uomId;
				double itemSize;

				try
				{
					uomId = sizes.getString("uom");
					brandId = sizes.getInt("brand_id");
					segmentId = sizes.getInt("segment_id");
					itemSize = sizes.getDouble("item_size");
					
					// uom different?
					if ( !uomId.equals(curUomId) )
					{
						if ( !curUomId.equals("NONE") ) {
							processSizes (sizeMap, curKey, curSizeList, sizeGroupMap);
						}
						
						curUomId = uomId;
						curBrandId = -1;
						curSegmentId = -1;
						curKey = null;
					}
					
					// brand different?
					if ( _DoBrandSegnemt )
					{
						if ( brandId != curBrandId ) {
							if ( curBrandId != -1 ) {
								processSizes (sizeMap, curKey, curSizeList, sizeGroupMap);
							}
							
							curBrandId = brandId;
							curSegmentId = -1;
						}
					}
					
					// segment different?
					if ( segmentId != curSegmentId )
					{
						if ( curSegmentId != -1 ) {
							processSizes (sizeMap, curKey, curSizeList, sizeGroupMap);
						}
						
						curKey = curUomId + ":" + segmentId;
						if ( _DoBrandSegnemt )
							curKey += ":" + curBrandId;
						curSizeList = new ArrayList<Double>();
						sizeMap.put(curKey, curSizeList);
						
						curSegmentId = segmentId;
						curItemSize = -1;
					}
					
					// size different?
					if ( itemSize != curItemSize )
					{
						if ( curItemSize > -1 ) {
						}
						
						curSizeList.add(itemSize);
						curItemSize = itemSize;
					}
				}
				catch (SQLException sex) {
				}
			}

			// last one
			processSizes (sizeMap, curKey, curSizeList, sizeGroupMap);
		}
		catch (GeneralException ex) {
		}
		catch (SQLException sex) {
		}
	}
	
	private void processSizes (Map<String, List<Double>> sizeMap, String key, List<Double> sizeList, Map<String, List<String>> sizeGroupMap)
	{
		StringBuffer sb = new StringBuffer("processSizes::'" + key + "' sizes: count ");
		int count = 0;
		count = sizeList.size();
		sb.append(count);
		for ( Double size : sizeList ) {
			sb.append(',').append(size);
		}
		logger.debug(sb.toString());
		
		if ( count > 1 )
		{
			List<String> groupList = new ArrayList<String>();
			for ( int ii = 1; ii < count-1; ii = ii + 2 )
			{
				double size = sizeList.get(ii);
				double sizePrev = sizeList.get(ii-1);
				double sizeNext = sizeList.get(ii+1);

				String sizes = "";
				// add to group if diff < 5%
				double withinPercent = size * _PercentDiff;
				double diff = Math.abs(size - sizePrev);
				if ( diff < withinPercent ) {
					sizes = doubleToString(sizePrev) + "," + doubleToString(size);
				}
				diff = Math.abs(sizeNext - size);
				if ( diff < withinPercent ) { 
					if ( sizes.length() > 0 ) {
						sizes += "," + String.valueOf(sizeNext);
					}
					else {
						sizes = doubleToString(size) + "," + doubleToString(sizeNext);
					}
					ii++;
				}
				
				if ( sizes.length() > 0 ) {
					groupList.add(sizes);
				}
			}
			
			if ( groupList.size() > 0 )
				sizeGroupMap.put(key, groupList);
		}
	}

	private void save (Map<String, List<Double>> sizeMap, Map<String, List<String>> sizeGroupMap)
	{
		logger.info ("save: begin");
		
		ItemDAO dao = new ItemDAO();
		
		Set<String> sizeKeys = sizeMap.keySet();
		Iterator<String> sizeIter = sizeKeys.iterator();
		int totalCount = 0;
		int maxSizesLen = 0, maxSizeGroupLen = 0; 
		while ( sizeIter.hasNext() )
		{
			String key = sizeIter.next();
			List<Double> sizeList = sizeMap.get(key);
			String[] strs = key.split(":");
			String uom = strs[0];
			int segment = Integer.parseInt(strs[1]);
			int brand = -1;
			
			if ( _DoBrandSegnemt )
				brand = Integer.parseInt(strs[2]);
			
			String sizes = doubleToDelimitedList (sizeList, ",");
			List<String> sizeGroupLiist = sizeGroupMap.get(key);
			String sizeGroups = sizeGroupLiist != null ? stringToDelimitedList (sizeGroupLiist, "; ") : null;
			
			if ( sizes != null && maxSizesLen < sizes.length() )
				maxSizesLen = sizes.length();
			if ( sizeGroups != null && maxSizeGroupLen < sizeGroups.length() )
				maxSizeGroupLen = sizeGroups.length();
			
			int res = dao.updateSizeGroup(_Conn, uom, brand, segment, sizeGroups, sizes);
			totalCount += res;
			if ( totalCount % 5000 == 0 ) {
				logger.info ("save: updated " + String.valueOf(totalCount));
			}
		}

		logger.info ("save: end updated " + String.valueOf(totalCount));
	}

	private void setupSizeGroupRanges (Map<String, List<String>> sizeGroupMap, Map<String, List<SizeGroupRange>> sizeGroupRangeMap)
	{
		logger.info ("setupSizeGroupRange: begin");
		
		Set<String> sizeKeys = sizeGroupMap.keySet();
		Iterator<String> sizeIter = sizeKeys.iterator();
		while ( sizeIter.hasNext() )
		{
			String key = sizeIter.next();
			List<String> sizeGroupList = sizeGroupMap.get(key);
			List<SizeGroupRange> rangeList = new ArrayList<SizeGroupRange>();
			for ( String sizeGroup : sizeGroupList ) {
				SizeGroupRange range = new SizeGroupRange (sizeGroup);
				rangeList.add(range);
			}
			sizeGroupRangeMap.put(key, rangeList);
		}

		logger.info ("setupSizeGroupRange: end");
	}
	
	private void placeItemsInSizeGroup (Map<String, List<SizeGroupRange>> sizeGroupRangeMap)
	{
		logger.info ("placeItemsInSizeGroup: begin");
		
		ItemDAO dao = new ItemDAO();
		try
		{
			CachedRowSet items = dao.getItemSizeBrandSegment (_Conn, _DoBrandSegnemt);
			int count = 0;
			logger.info ("placeItemsInSizeGroup: " + String.valueOf(items.size()) + " items");
			while ( items.next() )
			{
				int itemCode, brandId, segmentId;
				String uomId;
				double itemSize;

				try
				{
					itemCode = items.getInt("item_code");
					uomId = items.getString("uom");
					brandId = items.getInt("brand_id");
					segmentId = items.getInt("segment_id");
					itemSize = items.getDouble("item_size");

					String key = uomId + ":" + segmentId;
					if ( _DoBrandSegnemt )
						key += ":" + brandId;

					List<SizeGroupRange> sizeRangeList = sizeGroupRangeMap.get(key);
					SizeGroupRange itemSizeRange = null;
					if ( sizeRangeList != null )
					{
						for ( SizeGroupRange range : sizeRangeList ) {
							if ( range._Min <= itemSize && itemSize <= range._Max ) {
								itemSizeRange = range;
								break;
							}
						}
					}

					if ( itemSizeRange != null ) {
						int ret = dao.updateItemSizeGroup (_Conn, itemCode, uomId, _DoBrandSegnemt ? brandId : -1, segmentId, itemSizeRange);
						count += ret;
						
						if ( count % 10000 == 0 )
							logger.info ("placeItemsInSizeGroup: updated " + String.valueOf(count) + " records");
					}
				}
				catch (SQLException sqlEx) {
					logger.error("placeItemsInSizeGroup", sqlEx);
				}
			}
			
			logger.info ("placeItemsInSizeGroup: updated " + String.valueOf(count) + " records");
		}
		catch (GeneralException ex) {
			logger.error("placeItemsInSizeGroup", ex);
		}
		catch (SQLException sqlEx) {
			logger.error("placeItemsInSizeGroup", sqlEx);
		}

		logger.info ("placeItemsInSizeGroup: end");
	}
	
	private String doubleToDelimitedList (List<Double> sizeList, String delimitor)
	{
		int count = sizeList.size(); 
		StringBuffer sb = new StringBuffer();
		for ( int ii = 0; ii < count; ii++ ) {
			if ( ii > 0 ) sb.append(delimitor);
			double size = sizeList.get(ii);
			sb.append(doubleToString(size));
		}
		String sizes = sb.toString();
		return sizes;
	}
	
	private String stringToDelimitedList (List<String> sizeList, String delimitor)
	{
		int count = sizeList.size(); 
		StringBuffer sb = new StringBuffer();
		for ( int ii = 0; ii < count; ii++ ) {
			if ( ii > 0 ) sb.append(delimitor);
			String size = sizeList.get(ii);
			sb.append(size);
		}
		String sizes = sb.toString();
		return sizes;
	}
	
	private String doubleToString (double d)
	{
		String dStr = String.valueOf(d);
		return dStr;
	}
	
	static Logger logger = Logger.getLogger("ItemSizeGroup");

    private Connection	_Conn = null;
    private boolean		_DoBrandSegnemt = false;
    private double		_PercentDiff = 0.05;
    
	// Main command flags
	private static final String ARG_OPERATION	= "OPERATION=";
	
	// Secondary command flags
	private static final int ARG_OPERATION_NONE = 0;
	private static final int ARG_OPERATION_ITEM_SIZE_GROUPING_BY_SEGMENT = 1;
	private static final int ARG_OPERATION_ITEM_SIZE_GROUPING_BY_BRAND_SEGMENT = 2;
	private static final int ARG_OPERATION_PLACE_ITEM_IN_SIZE_GROUP_BY_SEGMENT = 3;
	private static final int ARG_OPERATION_PLACE_ITEM_IN_SIZE_GROUP_BY_BRAND_SEGMENT = 4;
}

/*
CREATE TABLE SIZE_GROUP
(
    UOM_ID       		CHAR(3 BYTE) NOT NULL,
    BRAND_ID       		NUMBER(5,0) DEFAULT NULL,
    SEGMENT_ID       	NUMBER(4,0) NOT NULL,
    SIZE_GROUP			VARCHAR2(1500) DEFAULT NULL, 
    SIZES				VARCHAR2(1500) NOT NULL 
);

ALTER TABLE SIZE_GROUP ADD (
	CONSTRAINT FK_SIZE_GROUP_ITEMCODE
    FOREIGN KEY (ITEM_CODE) 
    REFERENCES ITEM_LOOKUP (ITEM_CODE));
ALTER TABLE SIZE_GROUP ADD (
	CONSTRAINT FK_SIZE_GROUP_UOMID
    FOREIGN KEY (UOM_ID) 
    REFERENCES UOM_LOOKUP (ID));
ALTER TABLE SIZE_GROUP ADD (
	CONSTRAINT FK_SIZE_GROUP_BRANDID
    FOREIGN KEY (BRAND_ID) 
    REFERENCES BRAND_LOOKUP (BRAND_ID));
ALTER TABLE SIZE_GROUP ADD (
	CONSTRAINT FK_SIZE_GROUP_SEGID
    FOREIGN KEY (SEGMENT_ID) 
    REFERENCES ITEM_SEGMENT (ID));

CREATE TABLE ITEM_SIZE_GROUP
(
    ITEM_CODE       	NUMBER(6,0) NOT NULL,
    UOM_ID       		CHAR(3 BYTE) NOT NULL,
    BRAND_ID       		NUMBER(5,0) DEFAULT NULL,
    SEGMENT_ID       	NUMBER(4,0) NOT NULL,
    SIZE_MIN       		NUMBER(5,2) NOT NULL,
    SIZE_MAX       		NUMBER(5,2) NOT NULL,
    SIZE_GROUP			VARCHAR2(40) NOT NULL 
);

ALTER TABLE ITEM_SIZE_GROUP ADD (
	CONSTRAINT FK_ITEM_SIZE_GROUP_ITEMCODE
    FOREIGN KEY (ITEM_CODE) 
    REFERENCES ITEM_LOOKUP (ITEM_CODE));
ALTER TABLE ITEM_SIZE_GROUP ADD (
	CONSTRAINT FK_ITEM_SIZE_GROUP_UOMID
    FOREIGN KEY (UOM_ID) 
    REFERENCES UOM_LOOKUP (ID));
ALTER TABLE ITEM_SIZE_GROUP ADD (
	CONSTRAINT FK_ITEM_SIZE_GROUP_BRANDID
    FOREIGN KEY (BRAND_ID) 
    REFERENCES BRAND_LOOKUP (BRAND_ID));
ALTER TABLE ITEM_SIZE_GROUP ADD (
	CONSTRAINT FK_ITEM_SIZE_GROUP_SEGID
    FOREIGN KEY (SEGMENT_ID) 
    REFERENCES ITEM_SEGMENT (ID));
*/
