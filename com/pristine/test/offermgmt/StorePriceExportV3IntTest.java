package com.pristine.test.offermgmt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import com.pristine.dao.ChainDAO;
import com.pristine.dao.ItemLoaderDAO;
import com.pristine.dao.RetailCalendarDAO;
import com.pristine.dao.RetailPriceDAO;
import com.pristine.dao.RetailPriceZoneDAO;
import com.pristine.dao.offermgmt.ItemDAO;
import com.pristine.dao.offermgmt.PriceExportDAOV2;
import com.pristine.dataload.offermgmt.StorePriceExportV3;
import com.pristine.dataload.tops.PriceDataLoad;
import com.pristine.dto.RetailPriceDTO;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.PriceExportDTO;
import com.pristine.exception.GeneralException;
import com.pristine.service.offermgmt.NotificationService;
import com.pristine.util.Constants;

public class StorePriceExportV3IntTest {
	
	private static StorePriceExportV3 storePriceExportV3;
	private static StorePriceExportIntTestDataProvider dataProvider;
	
	private static PriceDataLoad priceDataDAO;
	private static PriceExportDAOV2 priceExportDAOV2;
	private static ItemLoaderDAO itemLoaderDAO;	
	private static ItemDAO itemDAO;	
	private static RetailPriceDAO retailPriceDAO;
	private static RetailCalendarDAO retailCalendarDAO;
	private static ChainDAO chainDAO;
	private static RetailPriceZoneDAO retailPriceZoneDAO;
	
	private static NotificationService notificationService;
	
	@BeforeClass
	public static void generateCache() {

//		//Enable the line below to run as simulator
//		PropertyManager.initialize("analysis.properties");
		
		priceDataDAO = Mockito.mock(PriceDataLoad.class);
		priceExportDAOV2 = Mockito.mock(PriceExportDAOV2.class);
		itemLoaderDAO = Mockito.mock(ItemLoaderDAO.class);
		itemDAO = Mockito.mock(ItemDAO.class);
		retailPriceDAO = Mockito.mock(RetailPriceDAO.class);
		retailCalendarDAO = Mockito.mock(RetailCalendarDAO.class);
		chainDAO = Mockito.mock(ChainDAO.class);
		retailPriceZoneDAO = Mockito.mock(RetailPriceZoneDAO.class);
		notificationService = Mockito.mock(NotificationService.class);
		
		storePriceExportV3 = new StorePriceExportV3(true, priceDataDAO, priceExportDAOV2, itemLoaderDAO, itemDAO, retailPriceDAO, 
				retailCalendarDAO, chainDAO, retailPriceZoneDAO);
		
		dataProvider = new StorePriceExportIntTestDataProvider();
		
		try {
			Mockito.when(retailPriceZoneDAO.getStoreZoneMapping(storePriceExportV3.conn)).thenReturn(dataProvider.getStoreZoneMapping());
			Mockito.when(itemDAO.getActiveItems(storePriceExportV3.conn)).thenReturn(dataProvider.getItemData());
			Mockito.when(priceExportDAOV2.getRUofItems(storePriceExportV3.conn)).thenReturn(dataProvider.getItemRUData());
			Mockito.when(retailPriceZoneDAO.getZoneIdAndNoMap(storePriceExportV3.conn)).thenReturn(dataProvider.getZoneIdAndNoMap());
			Mockito.when(chainDAO.getBaseChainId(storePriceExportV3.conn)).thenReturn(dataProvider.getBaseChainId());
			Mockito.when(priceExportDAOV2.getZoneNameForVirtualZone(Mockito.any(Connection.class), Mockito.anyString()))
			.thenReturn("VIRTUAL-ZONE-TEST");
			Mockito.when(retailCalendarDAO.getCalendarId(Mockito.any(Connection.class), Mockito.anyString(), Mockito.anyString()))
			.thenReturn(dataProvider.getRetailCalendarDTO());
			Mockito.when(priceDataDAO.getRetailPrice(Mockito.any(Connection.class), Mockito.anyString(), Mockito.anyList(), 
					Mockito.anyInt(), Mockito.any(), Mockito.anyInt()))
			.thenReturn(new HashMap<String, RetailPriceDTO>());
			Mockito.doNothing().when(priceExportDAOV2).insertECDataToHeader(Mockito.any(Connection.class), Mockito.anySet());
			Mockito.doNothing().when(priceExportDAOV2).insertECDataToDetail(Mockito.any(Connection.class), Mockito.anySet());
			Mockito.when(priceExportDAOV2.updateClearanceItemsStatusInBatches(Mockito.any(Connection.class), Mockito.anySet()))
			.thenReturn(new int[]{1,2});
			
			Mockito.when(priceExportDAOV2.getHardpartItemLeftoutV3(Mockito.any(Connection.class), Mockito.anyList()))
			.thenReturn(new ArrayList<PriceExportDTO>());
			Mockito.when(priceExportDAOV2.getSalesFloorItemsLeftoutV3(Mockito.any(Connection.class), Mockito.anyList()))
			.thenReturn(new ArrayList<PriceExportDTO>());
			Mockito.when(priceExportDAOV2.getLIGItemsPostDated(Mockito.any(Connection.class), Mockito.anyList()))
			.thenReturn(new ArrayList<PriceExportDTO>());
			Mockito.when(priceExportDAOV2.getAllSalesFloorItemsV3(Mockito.any(Connection.class), Mockito.anyList()))
			.thenReturn(new ArrayList<PriceExportDTO>());
			Mockito.when(priceExportDAOV2.getRunIdWithStatusCode(Mockito.any(Connection.class), Mockito.anyList()))
			.thenReturn(new HashMap<Long, Integer>());
			Mockito.when(notificationService.addNotificationsBatch(Mockito.any(Connection.class), Mockito.anyList(), Mockito.anyBoolean()))
			.thenReturn(1);
			Mockito.when(priceExportDAOV2.getProductLocationDetail(Mockito.any(Connection.class), Mockito.anyList()))
			.thenReturn(new HashMap<Long, List<PRItemDTO>>());
			
			Mockito.doNothing().when(priceExportDAOV2).deleteProcessedRunIdsV3(Mockito.any(Connection.class), Mockito.anyList());
			Mockito.doNothing().when(priceExportDAOV2).updateExportItemsV3(Mockito.any(Connection.class), Mockito.anyList());
			Mockito.doNothing().when(priceExportDAOV2).updateExportLigItemsV3(Mockito.any(Connection.class), Mockito.anyList());
			Mockito.doNothing().when(priceExportDAOV2).updateExportStatusV3(Mockito.any(Connection.class), Mockito.anyList(), 
					Mockito.anyList(), Mockito.any());
			Mockito.doNothing().when(priceExportDAOV2).insertExportStatusV3(Mockito.any(Connection.class), Mockito.anyList(), 
					Mockito.anyList(), Mockito.any());
			Mockito.doNothing().when(priceExportDAOV2).insertExportTrackIdV3(Mockito.any(Connection.class), Mockito.anyList());
			Mockito.doNothing().when(priceExportDAOV2).changeEffectiveDateV3(Mockito.any(Connection.class), Mockito.anyList(), 
					Mockito.anyInt());
			Mockito.doNothing().when(priceExportDAOV2).updateLIGItemRegEffectiveDate(Mockito.any(Connection.class), Mockito.anyMap());
			
			
		} catch (GeneralException | ParseException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Mockito.when(priceExportDAOV2.getStoreData(storePriceExportV3.conn)).thenReturn(dataProvider.getStoreData());
		Mockito.when(priceExportDAOV2.getZoneData(storePriceExportV3.conn, false)).thenReturn(dataProvider.getAllZoneData());
		Mockito.when(priceExportDAOV2.getZoneData(storePriceExportV3.conn, true)).thenReturn(dataProvider.getGlobalZoneData());
		Mockito.when(priceExportDAOV2.getZoneNoForZoneId(storePriceExportV3.conn)).thenReturn(dataProvider.getVirtualZoneIdToNumMap());
		Mockito.when(priceExportDAOV2.getZoneIdMapForVirtualZone(Mockito.any(Connection.class), Mockito.anyString(), Mockito.anyList()))
		.thenReturn(dataProvider.getIndividualZoneNumToIdMap());
		
	}
	
	@Test
	public void sample() throws GeneralException {
		String exportType = "SH", exportDate = "01072050"; //2050 starts on a Saturday. 01/07 is a Friday.
		
		List<PriceExportDTO> q = dataProvider.approvedItemsForSample();
		
		Mockito.when(priceExportDAOV2.getItemCountByRunIdFromExportQueue(storePriceExportV3.conn))
		.thenReturn(dataProvider.getRunIdToItemCountMapFromExportQueue(q));
		
		//Depending on the run types expected: (Emergency [E] or Normal [N]) -------
		Mockito.when(priceExportDAOV2.getRunIdsByType(storePriceExportV3.conn, exportDate, Constants.EMERGENCY))
		.thenReturn(new ArrayList<>());

		Mockito.when(priceExportDAOV2.getRunIdsByType(storePriceExportV3.conn, exportDate, Constants.NORMAL))
		.thenReturn(Arrays.asList(0L));
		//------- make either or both of these mocks return a non-empty list -------
		Mockito.when(priceExportDAOV2.separateTestZoneAndRegularZoneRunIds(Mockito.any(Connection.class), Mockito.anyList()))
		.thenReturn(dataProvider.segregateRunIdsByNormalAndTestZones(q));
		//------- for a call to be placed to the method mocked above during runtime.
		
//		//Export Q.
		Mockito.when(priceExportDAOV2.getItemsFromApprovedRecommendationsV3(Mockito.any(Connection.class), 
				Mockito.anyList(), Mockito.anyString(), Mockito.anyBoolean(), Mockito.anyString()))
		.thenReturn(q);
		
		//Store number to (zone number to DTO map) map.
		Mockito.when(priceExportDAOV2.getExcludeStoresFromStoreLockListV2(Mockito.any(Connection.class), 
				Mockito.anySet(), Mockito.anySet(), Mockito.anyInt(), Mockito.anyInt(), Mockito.anyString()))
		.thenReturn(new HashMap<Integer, HashMap<Integer, List<PriceExportDTO>>>());
		
		String exportFile = storePriceExportV3.generateStorePriceExport(exportType, exportDate);
		
		Mockito.verify(retailPriceZoneDAO).getStoreZoneMapping(storePriceExportV3.conn);
		Mockito.verify(priceExportDAOV2).getStoreData(storePriceExportV3.conn);
		Mockito.verify(priceExportDAOV2).getZoneData(storePriceExportV3.conn, false);
		Mockito.verify(priceExportDAOV2).getZoneData(storePriceExportV3.conn, true);
		Mockito.verify(itemDAO).getActiveItems(storePriceExportV3.conn);
		Mockito.verify(priceExportDAOV2).getRUofItems(storePriceExportV3.conn);
		Mockito.verify(priceExportDAOV2).getItemCountByRunIdFromExportQueue(storePriceExportV3.conn);
//		//Will not be invoked for export type SH
//		Mockito.verify(priceExportDAOV2).getRunIdsByType(storePriceExportV3.conn, exportDate, Constants.EMERGENCY);
		Mockito.verify(priceExportDAOV2).getRunIdsByType(storePriceExportV3.conn, exportDate, Constants.NORMAL);
		Mockito.verify(priceExportDAOV2).separateTestZoneAndRegularZoneRunIds(Mockito.any(Connection.class), Mockito.anyList());
		
		Assert.assertFalse(exportFile.isEmpty());
		try {
			BufferedReader br = new BufferedReader(new FileReader(exportFile));
			String line = br.readLine(); // burn the header line.
			String[] tokens;
			HashMap<String, Integer> zoneNumberToCountMap = new HashMap<String, Integer>();
			HashMap<String, Integer> itemCodeToCountMap = new HashMap<String, Integer>();
            while ((line = br.readLine()) != null) {
                tokens = line.split("\\|");
                if(zoneNumberToCountMap.containsKey(tokens[1])) {
                	int zoneNumberCount = zoneNumberToCountMap.get(tokens[1]);
                	zoneNumberToCountMap.put(tokens[1], ++zoneNumberCount);
                }
                else
                	zoneNumberToCountMap.put(tokens[1], 1);
                if(itemCodeToCountMap.containsKey(tokens[6])) {
                	int itemCodeCount = itemCodeToCountMap.get(tokens[6]);
                	itemCodeToCountMap.put(tokens[6], ++itemCodeCount);
                }
                else
                	itemCodeToCountMap.put(tokens[6], 1);
            }
            br.close();
            Assert.assertTrue(zoneNumberToCountMap.get("4")==4);
            Assert.assertTrue(zoneNumberToCountMap.get("16")==4);
            Assert.assertTrue(zoneNumberToCountMap.get("30")==7);
            
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
