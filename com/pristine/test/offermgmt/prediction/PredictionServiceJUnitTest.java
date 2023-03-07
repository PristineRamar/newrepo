package com.pristine.test.offermgmt.prediction;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.pristine.dto.offermgmt.ExecutionTimeLog;
import com.pristine.dto.offermgmt.MultiplePrice;
import com.pristine.dto.offermgmt.PRItemDTO;
import com.pristine.dto.offermgmt.prediction.PredictionDetailDTO;
import com.pristine.dto.offermgmt.prediction.PredictionInputDTO;
import com.pristine.dto.offermgmt.prediction.PredictionItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.exception.OfferManagementException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.service.offermgmt.ItemService;
import com.pristine.service.offermgmt.prediction.PredictionDetailKey;
import com.pristine.service.offermgmt.prediction.PredictionEngineInput;
import com.pristine.service.offermgmt.prediction.PredictionEngineItem;
import com.pristine.service.offermgmt.prediction.PredictionServiceHelper;
import com.pristine.service.offermgmt.prediction.PredictionStatus;
import com.pristine.util.Constants;

public class PredictionServiceJUnitTest {
	int productLevelId = Constants.CATEGORYLEVELID, productId = 1238;
	int locationLevelId = 6, locationId = 49;
	int RET_LIR_ID1 = 10000, RET_LIR_ID2 = 20000;
	int NON_LIG1 = 1000, NON_LIG2 = 1001, NON_LIG3 = 1002, NON_LIG4 = 1003;
	String NON_LIG1_UPC = "1000", NON_LIG2_UPC = "1000", NON_LIG3_UPC = "1000", NON_LIG4_UPC = "1000";
	int LIG1_MEMBER1 = 10001, LIG1_MEMBER2 = 10002, LIG1_MEMBER3 = 10003;
	int LIG2_MEMBER1 = 20001, LIG2_MEMBER2 = 20002, LIG2_MEMBER3 = 20003;
	
	String LIG1_MEMBER1_UPC = "1000", LIG1_MEMBER2_UPC = "10002", LIG1_MEMBER3_UPC = "10003";
	String LIG2_MEMBER1_UPC = "1000", LIG2_MEMBER2_UPC = "20002", LIG2_MEMBER3_UPC = "20003";
	
	boolean isPassMissingLigMembers = true;
	Connection conn = null;
	
	PredictionServiceHelper predictionServiceHelper = new PredictionServiceHelper();
	ItemService itemServiceMock;
	HashMap<PredictionDetailKey, PredictionDetailDTO> predictionDetails = null;
	HashMap<Integer, List<PRItemDTO>> ligMap = null;
	List<PredictionInputDTO> predictionInputDTOs = null;
	boolean isLIGModel = false;
	
	@Before
	public void init() {
		// Mockup
		itemServiceMock = new ItemService(new ArrayList<ExecutionTimeLog>());
		itemServiceMock = EasyMock.createMock(ItemService.class);
		isLIGModel = true;
	}

	/**
	 * Test prediction cache 
	 * 1 non LIG, all authorized LIG members (3) are passed to the prediction. No prediction is cached 
	 * Exp:1 non LIG, all authorized LIG members (3) of LIG
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase1() throws GeneralException, Exception, OfferManagementException {
		// no predictions are cached
		predictionDetails = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
		predictionInputDTOs = new ArrayList<PredictionInputDTO>();

		PredictionInputDTO predictionInputDTO = TestHelper.getPredictionInputDTO(locationLevelId, locationId, productLevelId, productId);

		//Add item and price points
		PredictionItemDTO nonLig1 = TestHelper.getPredictionItemDTO(NON_LIG1, NON_LIG1_UPC , 0);
		TestHelper.addPricePoint(nonLig1, new MultiplePrice(1, 2.19), new MultiplePrice(1, 1.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(), 0);
		
		PredictionItemDTO lig1Member1 = TestHelper.getPredictionItemDTO(LIG1_MEMBER1, LIG1_MEMBER1_UPC, RET_LIR_ID1);
		TestHelper.addPricePoint(lig1Member1, new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(), 0);
		
		PredictionItemDTO lig1Member2 = TestHelper.getPredictionItemDTO(LIG1_MEMBER2, LIG1_MEMBER2_UPC, RET_LIR_ID1);
		TestHelper.addPricePoint(lig1Member2, new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(), 0);
		
		PredictionItemDTO lig1Member3 = TestHelper.getPredictionItemDTO(LIG1_MEMBER3, LIG1_MEMBER3_UPC, RET_LIR_ID1);
		TestHelper.addPricePoint(lig1Member3, new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(), 0);
		
		
		predictionInputDTO.predictionItems.add(nonLig1);
		predictionInputDTO.predictionItems.add(lig1Member1);
		predictionInputDTO.predictionItems.add(lig1Member2);
		predictionInputDTO.predictionItems.add(lig1Member3);
		predictionInputDTOs.add(predictionInputDTO);

		PredictionEngineInput predictionEngineInput = predictionServiceHelper.findItemsToBePredicted(productLevelId, productId, predictionDetails,
				predictionInputDTOs, isLIGModel);

		//Exp: must have all input items (i.e. all items to be predicted)
		for(PredictionItemDTO predictionItemDTO: predictionInputDTO.predictionItems) {
			boolean isItemMatch = false;
			for(PredictionEngineItem predictionEngineItem : predictionEngineInput.getPredictionEngineItems()) {
				if(predictionItemDTO.itemCodeOrLirId == predictionEngineItem.getItemCode()) {
					isItemMatch = true;
					break;
				}
			}
			assertEquals("Mismatch", true, isItemMatch);
		}
		
	}
	
	/**
	 * Test prediction cache 
	 * 1 non LIG, all authorized LIG members (3) are passed to the prediction. 
	 * 1 non Lig and 1 LIG member prediction is cached 
	 * Exp: all authorized LIG members (3) of LIG
	 * 
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase2() throws GeneralException, Exception {
		// no predictions are cached
		predictionDetails = new HashMap<PredictionDetailKey, PredictionDetailDTO>();
		predictionInputDTOs = new ArrayList<PredictionInputDTO>();
		PredictionDetailKey pdk = null;
		PredictionDetailDTO predDetailDTO = null;
		
		PredictionInputDTO predictionInputDTO = TestHelper.getPredictionInputDTO(locationLevelId, locationId, productLevelId, productId);

		//Add item and price points
		PredictionItemDTO nonLig1 = TestHelper.getPredictionItemDTO(NON_LIG1, NON_LIG1_UPC , 0);
		TestHelper.addPricePoint(nonLig1, new MultiplePrice(1, 2.19), new MultiplePrice(1, 1.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(), 0);
		
		PredictionItemDTO lig1Member1 = TestHelper.getPredictionItemDTO(LIG1_MEMBER1, LIG1_MEMBER1_UPC, RET_LIR_ID1);
		TestHelper.addPricePoint(lig1Member1, new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(), 0);
		
		PredictionItemDTO lig1Member2 = TestHelper.getPredictionItemDTO(LIG1_MEMBER2, LIG1_MEMBER2_UPC, RET_LIR_ID1);
		TestHelper.addPricePoint(lig1Member2, new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(), 0);
		
		PredictionItemDTO lig1Member3 = TestHelper.getPredictionItemDTO(LIG1_MEMBER3, LIG1_MEMBER3_UPC, RET_LIR_ID1);
		TestHelper.addPricePoint(lig1Member3, new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(), 0);
		
		
		predictionInputDTO.predictionItems.add(nonLig1);
		predictionInputDTO.predictionItems.add(lig1Member1);
		predictionInputDTO.predictionItems.add(lig1Member2);
		predictionInputDTO.predictionItems.add(lig1Member3);
		predictionInputDTOs.add(predictionInputDTO);
		
		// Add prediction cache
		pdk = TestHelper.getPredictionDetailKey(locationLevelId, locationId, nonLig1, nonLig1.pricePoints.get(0));
		predDetailDTO = TestHelper.getPredictionDetailDTO(10d, PredictionStatus.SUCCESS);
		predictionDetails.put(pdk, predDetailDTO);
		
		pdk = TestHelper.getPredictionDetailKey(locationLevelId, locationId, lig1Member1, lig1Member1.pricePoints.get(0));
		predDetailDTO = TestHelper.getPredictionDetailDTO(20d, PredictionStatus.SUCCESS);
		predictionDetails.put(pdk, predDetailDTO);

		PredictionEngineInput predictionEngineInput = predictionServiceHelper.findItemsToBePredicted(productLevelId, productId, predictionDetails,
				predictionInputDTOs, isLIGModel);

		//Exp: must have only lig items
		for(PredictionItemDTO predictionItemDTO: predictionInputDTO.predictionItems) {
			boolean isItemToBePredicted = false;
			for(PredictionEngineItem predictionEngineItem : predictionEngineInput.getPredictionEngineItems()) {
				if(predictionItemDTO.itemCodeOrLirId == predictionEngineItem.getItemCode()) {
					isItemToBePredicted = true;
					break;
				}
			}
			if(predictionItemDTO.itemCodeOrLirId == NON_LIG1) {
				assertEquals("Mismatch", false, isItemToBePredicted);
			} else {
				assertEquals("Mismatch", true, isItemToBePredicted);	
			}
		}
	}
	
	/***
	 * Check if missing LIG members are added properly
	 * Only one authorized item of a LIG with 3 members is passed
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase3() throws GeneralException, OfferManagementException {
		predictionInputDTOs = new ArrayList<PredictionInputDTO>();
		PredictionInputDTO predictionInputDTO = TestHelper.getPredictionInputDTO(locationLevelId, locationId, productLevelId, productId);
		ligMap = new HashMap<Integer, List<PRItemDTO>>();
		
		PredictionItemDTO lig1Member1 = TestHelper.getPredictionItemDTO(LIG1_MEMBER1, LIG1_MEMBER1_UPC, 0);
		TestHelper.addPricePoint(lig1Member1, new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.99), 0, 0, PromoTypeLookup.STANDARD.getPromoTypeId(),
				0);

		predictionInputDTO.predictionItems.add(lig1Member1);
		predictionInputDTOs.add(predictionInputDTO);

		TestHelper.addItemToLig(ligMap, LIG1_MEMBER1, LIG1_MEMBER1_UPC, RET_LIR_ID1);
		TestHelper.addItemToLig(ligMap, LIG1_MEMBER2, LIG1_MEMBER2_UPC, RET_LIR_ID1);
		TestHelper.addItemToLig(ligMap, LIG1_MEMBER3, LIG1_MEMBER3_UPC, RET_LIR_ID1);
		
		List<Integer> priceZoneStoresMock = new ArrayList<Integer>();
		EasyMock.expect(itemServiceMock.getPriceZoneStores(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt(), EasyMock.anyInt(),
				EasyMock.anyInt())).andReturn(priceZoneStoresMock).anyTimes();
//		EasyMock.replay(itemServiceMock);
		
		List<PRItemDTO> authorizedItemsMock = new ArrayList<PRItemDTO>();
		EasyMock.expect(itemServiceMock.getAuthorizedItemsOfZoneAndStore(EasyMock.anyObject(), EasyMock.anyInt(), EasyMock.anyInt(), EasyMock.anyObject(),
				EasyMock.anyObject())).andReturn(authorizedItemsMock).anyTimes();
		EasyMock.replay(itemServiceMock);

		predictionServiceHelper.addMissingLigMembers(conn, predictionInputDTOs, itemServiceMock, isPassMissingLigMembers, ligMap);

		for (List<PRItemDTO> items : ligMap.values()) {
			for (PRItemDTO item : items) {
				boolean isItemToBePredicted = false;
				for (PredictionItemDTO predictionItemDTO : predictionInputDTO.predictionItems) {
					if (predictionItemDTO.itemCodeOrLirId == item.getItemCode()) {
						isItemToBePredicted = true;
						break;
					}
				}
				assertEquals("Mismatch", true, isItemToBePredicted);
			}
		}
		
	}
	
	/**
	 * Items in multiple batch test
	 * 1 non-lig, 1 lig with 3 members, prediction batch count is 3
	 * Exp: items will be send in 2 batches
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase4() {
		int predictionBatchCount = 2;
		PredictionEngineInput predictionEngineInput = new PredictionEngineInput();
		predictionEngineInput.setPredictionEngineItems(new ArrayList<PredictionEngineItem>());
		List<PredictionEngineItem> nonLigItems = new ArrayList<PredictionEngineItem>();
		List<PredictionEngineItem> ligMembers = new ArrayList<PredictionEngineItem>();
		
		nonLigItems.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", NON_LIG1, NON_LIG1_UPC,
				new MultiplePrice(1, 2.19), new MultiplePrice(1, 1.29), 0, 0, 0, 0, 0));
		predictionEngineInput.getPredictionEngineItems().addAll(nonLigItems);
		
		ligMembers.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", LIG1_MEMBER1, LIG1_MEMBER1_UPC,
				new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.29), 0, 0, 0, 0, RET_LIR_ID1));
		ligMembers.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", LIG1_MEMBER2, LIG1_MEMBER2_UPC,
				new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.29), 0, 0, 0, 0, RET_LIR_ID1));
		ligMembers.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", LIG1_MEMBER3, LIG1_MEMBER3_UPC,
				new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.29), 0, 0, 0, 0, RET_LIR_ID1));
		predictionEngineInput.getPredictionEngineItems().addAll(ligMembers);
		
		HashMap<Integer, PredictionEngineInput> iteminBatches = predictionServiceHelper.breakItemsToBatches(predictionBatchCount, predictionEngineInput);
		
		//Check batch count
		assertEquals("Mismatch", 2, iteminBatches.size());
	
		boolean nonLigPresent = false;
		//Check non lig
		for (Map.Entry<Integer, PredictionEngineInput> itemsInBatch : iteminBatches.entrySet()) {
			if(nonLigItems.equals(itemsInBatch.getValue().getPredictionEngineItems())) {
				nonLigPresent = true;
			}
		}
		assertEquals("Mismatch", true, nonLigPresent);
		
		boolean ligPresent = false;
		//Check LIG
		for (Map.Entry<Integer, PredictionEngineInput> itemsInBatch : iteminBatches.entrySet()) {
			if (nonLigItems.equals(itemsInBatch.getValue().getPredictionEngineItems())) {
				ligPresent = true;
			}
		}
		assertEquals("Mismatch", true, ligPresent);
	}
	
	
	/**
	 * Items in multiple batch test
	 * 1 non-lig, 1 lig with 3 members, prediction count is 4
	 * Exp: items will be send in 1 batch
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase5() {
		int predictionBatchCount = 4;
		PredictionEngineInput predictionEngineInput = new PredictionEngineInput();
		predictionEngineInput.setPredictionEngineItems(new ArrayList<PredictionEngineItem>());
		List<PredictionEngineItem> nonLigItems = new ArrayList<PredictionEngineItem>();
		List<PredictionEngineItem> ligMembers = new ArrayList<PredictionEngineItem>();
		
		nonLigItems.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", NON_LIG1, NON_LIG1_UPC,
				new MultiplePrice(1, 2.19), new MultiplePrice(1, 1.29), 0, 0, 0, 0, 0));
		predictionEngineInput.getPredictionEngineItems().addAll(nonLigItems);
		
		ligMembers.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", LIG1_MEMBER1, LIG1_MEMBER1_UPC,
				new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.29), 0, 0, 0, 0, RET_LIR_ID1));
		ligMembers.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", LIG1_MEMBER2, LIG1_MEMBER2_UPC,
				new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.29), 0, 0, 0, 0, RET_LIR_ID1));
		ligMembers.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", LIG1_MEMBER3, LIG1_MEMBER3_UPC,
				new MultiplePrice(1, 3.19), new MultiplePrice(1, 2.29), 0, 0, 0, 0, RET_LIR_ID1));
		predictionEngineInput.getPredictionEngineItems().addAll(ligMembers);
		
		HashMap<Integer, PredictionEngineInput> iteminBatches = predictionServiceHelper.breakItemsToBatches(predictionBatchCount, predictionEngineInput);
		
		//Check batch count
		assertEquals("Mismatch", 1, iteminBatches.size());
	
		List<PredictionEngineItem> allItems = new ArrayList<PredictionEngineItem>();
		allItems.addAll(nonLigItems);
		allItems.addAll(ligMembers);
		
		boolean allItemsPresent = false;
		//Check non lig
		for (Map.Entry<Integer, PredictionEngineInput> itemsInBatch : iteminBatches.entrySet()) {
			if(allItems.equals(itemsInBatch.getValue().getPredictionEngineItems())) {
				allItemsPresent = true;
			}
		}
		assertEquals("Mismatch", true, allItemsPresent);
		
	}
	
	/**
	 * Items in multiple batch test
	 * 4 non-lig
	 * Exp: items will be send in 1 batch
	 * @throws GeneralException
	 * @throws Exception
	 * @throws OfferManagementException
	 */
	@Test
	public void testCase6() {
		int predictionBatchCount = 4;
		PredictionEngineInput predictionEngineInput = new PredictionEngineInput();
		predictionEngineInput.setPredictionEngineItems(new ArrayList<PredictionEngineItem>());
		List<PredictionEngineItem> nonLigItems = new ArrayList<PredictionEngineItem>();
		
		nonLigItems.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", NON_LIG1, NON_LIG1_UPC,
				new MultiplePrice(1, 2.19), new MultiplePrice(1, 1.29), 0, 0, 0, 0, 0));
		nonLigItems.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", NON_LIG2, NON_LIG2_UPC,
				new MultiplePrice(1, 2.19), new MultiplePrice(1, 1.29), 0, 0, 0, 0, 0));
		nonLigItems.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", NON_LIG3, NON_LIG3_UPC,
				new MultiplePrice(1, 2.19), new MultiplePrice(1, 1.29), 0, 0, 0, 0, 0));
		nonLigItems.add(TestHelper.getPredictionEngineItem(locationLevelId, locationId, "", NON_LIG4, NON_LIG4_UPC,
				new MultiplePrice(1, 2.19), new MultiplePrice(1, 1.29), 0, 0, 0, 0, 0));
		predictionEngineInput.getPredictionEngineItems().addAll(nonLigItems);
		
		HashMap<Integer, PredictionEngineInput> iteminBatches = predictionServiceHelper.breakItemsToBatches(predictionBatchCount, predictionEngineInput);
		
		//Check batch count
		assertEquals("Mismatch", 1, iteminBatches.size());
		for (Map.Entry<Integer, PredictionEngineInput> itemsInBatch : iteminBatches.entrySet()) {
			assertEquals("Mismatch", 4, itemsInBatch.getValue().getPredictionEngineItems().size());
		}
		
		List<PredictionEngineItem> allItems = new ArrayList<PredictionEngineItem>();
		allItems.addAll(nonLigItems);
		
	
		//Check non lig
		for(PredictionEngineItem predictionEngineItem : allItems) {
			boolean itemPresent = false;
			for (Map.Entry<Integer, PredictionEngineInput> itemsInBatch : iteminBatches.entrySet()) {
				for(PredictionEngineItem pei : itemsInBatch.getValue().getPredictionEngineItems()) {
					if(predictionEngineItem.equals(pei)) {
						itemPresent = true;
						break;
					}
				}
			}
			assertEquals("Mismatch", true, itemPresent);
		}
		
	}
}
