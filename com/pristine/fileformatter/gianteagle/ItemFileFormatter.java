package com.pristine.fileformatter.gianteagle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.pristine.dao.ItemDAO;
import com.pristine.dto.ItemDetailKey;
import com.pristine.dto.fileformatter.gianteagle.GiantEagleItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.parsinginterface.PristineFileParser;
import com.pristine.util.Constants;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import com.pristine.util.PropertyManager;

@SuppressWarnings("rawtypes")
public class ItemFileFormatter extends PristineFileParser {

	private static Logger logger = Logger.getLogger("ItemFileFormatterGE");
	private final static String INPUT_FOLDER = "INPUT_FOLDER=";
	private final static String OUTPUT_FOLDER = "OUTPUT_FOLDER=";
	private final static String MODE = "MODE=";
	private final static String UPDATE_TIER = "UPDATE_TIER=";
	private final static String FORMAT_MODE = "FORMAT";
	private final static String UPDATE_ATTR_MODE = "UPDATE";
	private String rootPath;
	static TreeMap<Integer, String> allColumns = new TreeMap<Integer, String>();
	private int recordCount = 0;
	private int skippedCount = 0;
	private int discontinuedItemsCount = 0;
	private int updateCount = 100000;
	private FileWriter fw = null;
	private PrintWriter pw = null;
	private static String relativeInputPath, relativeOutputPath, mode = FORMAT_MODE;
	private ItemDAO itemDAO = null;
	List<GiantEagleItemDTO> _itemList = null;
	private boolean updateTier = false;
	List<GiantEagleItemDTO> giantEagleItemDTOs = new ArrayList<>();

	public static void main(String[] args) {
		boolean tier = false;
		for (String arg : args) {
			if (arg.startsWith(INPUT_FOLDER)) {
				relativeInputPath = arg.substring(INPUT_FOLDER.length());
			}
			if (arg.startsWith(OUTPUT_FOLDER)) {
				relativeOutputPath = arg.substring(OUTPUT_FOLDER.length());
			}
			if (arg.startsWith(MODE)) {
				mode = arg.substring(MODE.length());
			}
			if (arg.startsWith(UPDATE_TIER)) {
				tier = Boolean.parseBoolean(arg.substring(UPDATE_TIER.length()));
			}
		}
		PropertyConfigurator.configure("log4j-item-file-formatter.properties");
		ItemFileFormatter fileFormatter = new ItemFileFormatter();
		fileFormatter.updateTier = tier;
		fileFormatter.processFile();
	}

	/**
	 * Fill all columns in list and processing the file.
	 */
	@SuppressWarnings("unchecked")
	private void processFile() {

		itemDAO = new ItemDAO();
		_itemList = new ArrayList<>();
		super.headerPresent = false;
		rootPath = PropertyManager.getProperty("DATALOAD.ROOTPATH", "");
		fillAllColumns();
		String fieldNames[] = new String[allColumns.size()];
		int i = 0;
		for (Map.Entry<Integer, String> columns : allColumns.entrySet()) {
			fieldNames[i] = columns.getValue();
			i++;
		}

		logger.info("Formatting Item file Started... ");
		try {
			parseFile(fieldNames);

		} catch (IOException | GeneralException e) {
			logger.error("Error -- processFile()", e);
		}
		logger.info("Total Number of Items " + recordCount);
		logger.info("Formatting Item file Completed... ");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void processRecords(List listobj) throws GeneralException {
		List<GiantEagleItemDTO> giantEagleDTOList = (List<GiantEagleItemDTO>) listobj;
		if (mode.equals(UPDATE_ATTR_MODE)) {
			_itemList.addAll(giantEagleDTOList);

		} else if (mode.equals(FORMAT_MODE)) {
			giantEagleItemDTOs.addAll(giantEagleDTOList);
			// writeFile(giantEagleDTOList);
		}
	}

	private void updateUserAttributes() throws GeneralException {
		List<GiantEagleItemDTO> tempList = new ArrayList<>();
		int counter = 0;
		for (GiantEagleItemDTO giantEagleItemDTO : _itemList) {
			counter++;
			tempList.add(giantEagleItemDTO);
			if (counter % updateCount == 0) {
				logger.info("updateUserAttributes() - # of items to update -> " + tempList.size());
				int rowsAffected = itemDAO.updateUserAttributes(getOracleConnection(), tempList, updateTier);
				logger.info("updateUserAttributes() - # of items updated -> " + rowsAffected);

				rowsAffected = itemDAO.updateUserAttributesForLig(getOracleConnection(), tempList, updateTier);
				logger.info("updateUserAttributes() - # of LIGs updated -> " + rowsAffected);
				tempList.clear();
				counter = 0;
			}
		}
		if (counter > 0) {
			logger.info("updateUserAttributes() - # of items to update -> " + tempList.size());
			int rowsAffected = itemDAO.updateUserAttributes(getOracleConnection(), tempList, updateTier);
			logger.info("updateUserAttributes() - # of items updated -> " + rowsAffected);

			rowsAffected = itemDAO.updateUserAttributesForLig(getOracleConnection(), tempList, updateTier);
			logger.info("updateUserAttributes() - # of LIGs updated -> " + rowsAffected);
			tempList.clear();
			counter = 0;
		}
	}

	private void writeFile(List<GiantEagleItemDTO> itemList) {
		for (int j = 0; j < itemList.size(); j++) {
			GiantEagleItemDTO giantEagleItemDTO = itemList.get(j);
			double x = 0;
			
			if(giantEagleItemDTO.getITM_STAT().equals("D"))
			{
				discontinuedItemsCount++;
			}
			else
			{
				try {
					if (giantEagleItemDTO.getITM_SZ().length() > 0
							&& giantEagleItemDTO.getITM_SZ().trim().charAt(giantEagleItemDTO.getITM_SZ().length() - 1) == '.') {
						String tempSize = giantEagleItemDTO.getITM_SZ().trim().substring(0, giantEagleItemDTO.getITM_SZ().length() - 1);
						giantEagleItemDTO.setITM_SZ(tempSize);
					}
					x = Double.parseDouble(giantEagleItemDTO.getITM_SZ()); // Parse string into number
					if (x > 9999) {
						giantEagleItemDTO.setITM_SZ("1");
						//giantEagleItemDTO.setITM_SZ_UOM(Constants.EMPTY);
					}
				} catch (NumberFormatException e) {
					giantEagleItemDTO.setITM_SZ("1");
					//giantEagleItemDTO.setITM_SZ_UOM(Constants.EMPTY);
				}

				recordCount++;

				pw.print(giantEagleItemDTO.getCAT_DSCR()); // Major Department # 1
				pw.print("|");
				pw.print(giantEagleItemDTO.getCAT_CD()); // Major Dept Code # 2
				pw.print("|");
				pw.print(giantEagleItemDTO.getCAT_DSCR()); // Department # 3
				pw.print("|");
				pw.print(giantEagleItemDTO.getCAT_CD()); // Dept Code # 4
				pw.print("|");
				pw.print(""); // EMPTY - Dept Short Name # 5
				pw.print("|");
				pw.print(giantEagleItemDTO.getGRP_DSCR()); // Category Name # 6
				pw.print("|");
				pw.print(giantEagleItemDTO.getGRP_CD()); // Category Code # 7
				pw.print("|");
				pw.print(giantEagleItemDTO.getSGRP_DSCR()); // Sub Category Name # 8
				pw.print("|");
				pw.print(giantEagleItemDTO.getSGRP_CD()); // Sub Category Code # 9
				pw.print("|");
				pw.print(""); // EMPTY - Segment name # 10
				pw.print("|");
				pw.print(""); // EMPTY - Segment Code # 11
				pw.print("|");
				pw.print(giantEagleItemDTO.getRITEM_NO()); // Item Code # 12
				pw.print("|");
				pw.print(giantEagleItemDTO.getITM_DSCR()); // Item Name or Description # 13
				pw.print("|");
				pw.print(giantEagleItemDTO.getITM_SZ());
				pw.print("|");
				pw.print(giantEagleItemDTO.getITM_SZ_UOM()); // UOM # 14
				pw.print("|");
				pw.print(giantEagleItemDTO.getITM_PAK()); // Pack # 15
				pw.print("|");
				if (giantEagleItemDTO.getBRND_LBL_DSCR().trim().toLowerCase().equals("NOT GE BRAND".toLowerCase()))
					pw.print("NB"); // Private Label Code # 16
				else
					pw.print(giantEagleItemDTO.getCORP_BRAND_ID().trim()); // Private Label Code # 16
				pw.print("|");
				// pw.print(giantEagleItemDTO.getFMLY_NAME() == null ?
				// Constants.EMPTY : giantEagleItemDTO.getFMLY_NAME()); // EMPTY - Like Item Group Name # 17
				// Modified code to add Family code with Description to avoid Different LIG members under same Family By
				// Dinesh(07/13/2017)
				if (giantEagleItemDTO.getFMLY_DSCR() != null && !giantEagleItemDTO.getFMLY_DSCR().isEmpty() && giantEagleItemDTO.getFMLY_CD() != null
						&& !giantEagleItemDTO.getFMLY_CD().isEmpty()) {
					pw.print(giantEagleItemDTO.getFMLY_DSCR() + "(" + giantEagleItemDTO.getFMLY_CD() + ")");// Like Item Group Name #
																											// 17
				} else {
					pw.print(giantEagleItemDTO.getFMLY_DSCR());// Like Item Group Name # 17
				}
				pw.print("|");
				pw.print(giantEagleItemDTO.getFMLY_CD()); // Pack # 18
				pw.print("|");
				pw.print(giantEagleItemDTO.getUPC()); // UPC # 19
				pw.print("|");
				pw.print("0"); // Level Type # 20
				pw.print("|");
				pw.print(""); // EMPTY - # 21
				pw.print("|");
				pw.print("0"); // Portfolio Name - # 22
				pw.print("|");
				pw.print("0"); // Portfolio Code - # 23
				pw.print("|");
				if (giantEagleItemDTO.getBRND_LBL_DSCR().trim().toLowerCase().equals("NOT GE BRAND".toLowerCase()))
					pw.print("NATIONAL BRAND"); // Brand Name # 24
				else
					pw.print(giantEagleItemDTO.getBRND_LBL_DSCR());// Brand Name # 24
				pw.print("|");
				pw.print(giantEagleItemDTO.getLOB_DSCR()); // LOB Name - # 26
				pw.print("|");
				pw.print(giantEagleItemDTO.getLOB_CD()); // LOB Code - # 27
				pw.println("       "); // spaces

				if (recordCount % Constants.LOG_RECORD_COUNT == 0) {
					logger.info("No of records processed - " + recordCount);
				}
			}
		}
	}

	/**
	 * Get the Path of Input file, and delimiting.
	 * 
	 * @param fieldNames
	 * @throws GeneralException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private void parseFile(String fieldNames[]) throws GeneralException, IOException {

		try {
			// getzip files
			ArrayList<String> zipFileList = getZipFiles(relativeInputPath);
			headerPresent = true;
			// Start with -1 so that if any regular files are present, they are processed first
			int curZipFileCount = -1;
			boolean processZipFile = false;

			String zipFilePath = getRootPath() + "/" + relativeInputPath;
			do {
				ArrayList<String> fileList = null;
				boolean commit = true;

				try {
					if (processZipFile)
						PrestoUtil.unzip(zipFileList.get(curZipFileCount), zipFilePath);

					fileList = getFiles(relativeInputPath);

					for (int i = 0; i < fileList.size(); i++) {

						long fileProcessStartTime = System.currentTimeMillis();

						recordCount = 0;
						skippedCount = 0;
						discontinuedItemsCount = 0;

						String files = fileList.get(i);
						logger.info("File Name - " + files);
						int stopCount = Integer.parseInt(PropertyManager.getProperty("DATALOAD.STOP_AFTER", "-1"));

						String outputFileName[] = files.split("/");
						logger.info("Output File name - " + outputFileName[outputFileName.length - 1]);

						String outputPath = rootPath + "/" + relativeOutputPath + "/" + outputFileName[outputFileName.length - 1];

						File file = new File(outputPath);
						if (!file.exists())
							fw = new FileWriter(outputPath);
						else
							fw = new FileWriter(outputPath, true);

						pw = new PrintWriter(fw);

						logger.info("Processing Retail Records ...");
						super.parseDelimitedFile(GiantEagleItemDTO.class, fileList.get(i), '|', fieldNames, stopCount);

						List<GiantEagleItemDTO> finalItemList = processItemDetails();

						writeFile(finalItemList);
						if (mode.equals(UPDATE_ATTR_MODE)) {
							updateUserAttributes();
							PristineDBUtil.commitTransaction(getOracleConnection(), "User attr update");
							// Clear cache
							_itemList = null;
						}

						logger.info("No of records processed - " + recordCount);
						logger.info("No of records skipped - " + skippedCount);
						logger.info("No of discontinued items - " + discontinuedItemsCount);

						pw.flush();
						fw.flush();
						pw.close();
						fw.close();

						long fileProcessEndTime = System.currentTimeMillis();

						logger.info("Time taken to process the file - " + (fileProcessEndTime - fileProcessStartTime) + "ms");
					}
				} catch (GeneralException | Exception ex) {
					logger.error("GeneralException", ex);
					commit = false;
					PristineDBUtil.rollbackTransaction(getOracleConnection(), "Error updating user attr");
				} finally {

				}

				if (processZipFile) {
					PrestoUtil.deleteFiles(fileList);
					fileList.clear();
					fileList.add(zipFileList.get(curZipFileCount));
				}
				String archivePath = getRootPath() + "/" + relativeInputPath + "/";

				if (commit) {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.COMPLETED_FOLDER);
				} else {
					PrestoUtil.moveFiles(fileList, archivePath + Constants.BAD_FOLDER);
				}
				curZipFileCount++;
				processZipFile = true;
			} while (curZipFileCount < zipFileList.size());

		} catch (GeneralException ex) {
			logger.error("Outer Exception - JavaException", ex);
			ex.printStackTrace();
		} catch (Exception ex) {
			logger.error("Outer Exception - JavaException", ex);
			ex.printStackTrace();
		}
	}

	/**
	 * Fill the values in list
	 */
	private void fillAllColumns() {
		allColumns.put(1, "RITEM_NO");
		allColumns.put(2, "UPC");
		allColumns.put(3, "WHITEM_NO");
		allColumns.put(4, "ITM_DSCR");
		allColumns.put(5, "SPLR_NO");
		allColumns.put(6, "SPLR_TYP_CD");
		allColumns.put(7, "ITM_SZ");
		allColumns.put(8, "ITM_SZ_UOM");
		allColumns.put(9, "ITM_PAK");
		allColumns.put(10, "CAT_CD");
		// allColumns.put(11, "FMLY_CD");
		allColumns.put(11, "CAT_DSCR");
		allColumns.put(12, "GRP_CD");
		allColumns.put(13, "GRP_DSCR");
		allColumns.put(14, "SGRP_CD");
		allColumns.put(15, "SGRP_DSCR");
		allColumns.put(16, "PRC_GRP_CD");
		allColumns.put(17, "PRC_GRP_DSCR");
		allColumns.put(18, "SYS_CD");
		allColumns.put(19, "CORP_BRAND_ID");
		allColumns.put(20, "BRND_LBL_DSCR");
		allColumns.put(21, "FMLY_CD");
		allColumns.put(22, "FMLY_DSCR");
		allColumns.put(23, "TIER");
		allColumns.put(24, "ITM_STAT");
		allColumns.put(25, "EFCT_DTE");
		allColumns.put(26, "DW_LOB_CD");
		allColumns.put(27, "DW_LOB_DSCR");
	}

	private List<GiantEagleItemDTO> processItemDetails() {
		List<GiantEagleItemDTO> finalItemList = new ArrayList<>();
		List<GiantEagleItemDTO> itemList = getUniqueItemList(giantEagleItemDTOs);

		List<GiantEagleItemDTO> ligItemList = getUniqueLigItems(itemList);
		finalItemList.addAll(ligItemList);

		List<GiantEagleItemDTO> nonLigItemList = getNonLigItemList(itemList);
		finalItemList.addAll(nonLigItemList);

		return finalItemList;
	}

	/**
	 * TO remove duplicate items(Same item with different SPLR NO), or if same item with LIG and NON LIG items, consider only LIG
	 * items
	 * 
	 * @param itemList
	 * @return
	 */
	private List<GiantEagleItemDTO> getUniqueItemList(List<GiantEagleItemDTO> itemList) {

		HashMap<ItemDetailKey, List<GiantEagleItemDTO>> ligItems = new HashMap<>();
		HashMap<ItemDetailKey, GiantEagleItemDTO> nonLigItems = new HashMap<>();
		List<GiantEagleItemDTO> uniqueItemList = new ArrayList<>();
		itemList.forEach(item -> {
			ItemDetailKey key = new ItemDetailKey(item.getUPC(), item.getRITEM_NO());

			List<GiantEagleItemDTO> tempList = new ArrayList<>();
			if (item.getFMLY_CD() != null && !item.getFMLY_CD().trim().isEmpty()) {
				if (ligItems.containsKey(key)) {
					tempList = ligItems.get(key);
				}
				tempList.add(item);
				ligItems.put(key, tempList);
			} else {
				nonLigItems.put(key, item);
			}
		});

		// If Non LIG items is available in LIG Group, then Skip those items else add it to unique list
		nonLigItems.forEach((key, item) -> {
			if (!ligItems.containsKey(key)) {
				uniqueItemList.add(item);
			}
		});

		// Add all the items from LIG Map
		ligItems.forEach((key, items) -> {
			uniqueItemList.addAll(items);
		});
		return uniqueItemList;

	}

	private HashMap<ItemDetailKey, List<String>> getFamilyCodesBasedOnItem(List<GiantEagleItemDTO> itemList) {

		HashMap<ItemDetailKey, List<String>> familyCodesBasedOnItem = new HashMap<>();
		itemList.stream().filter(item -> item.getFMLY_CD() != null && !item.getFMLY_CD().trim().isEmpty()).forEach(item -> {
			ItemDetailKey key = new ItemDetailKey(item.getUPC(), item.getRITEM_NO());
			List<String> familyCodes = new ArrayList<>();
			if (familyCodesBasedOnItem.containsKey(key)) {
				familyCodes = familyCodesBasedOnItem.get(key);
			}
			familyCodes.add(item.getFMLY_CD());
			familyCodesBasedOnItem.put(key, familyCodes);
		});
		return familyCodesBasedOnItem;
	}

	private HashMap<String, List<GiantEagleItemDTO>> getItemsBasedOnFamilyCode(List<GiantEagleItemDTO> itemList) {

		HashMap<String, List<GiantEagleItemDTO>> itemsBasedOnFamilyCode = new HashMap<>();
		itemList.stream().filter(item -> item.getFMLY_CD() != null && !item.getFMLY_CD().trim().isEmpty()).forEach(item -> {
			List<GiantEagleItemDTO> items = new ArrayList<>();
			if (itemsBasedOnFamilyCode.containsKey(item.getFMLY_CD())) {
				items = itemsBasedOnFamilyCode.get(item.getFMLY_CD());
			}
			items.add(item);
			itemsBasedOnFamilyCode.put(item.getFMLY_CD(), items);
		});
		return itemsBasedOnFamilyCode;
	}

	/**
	 * Group different items which has same LIG or items with more than one LIG and it's items
	 * 
	 * @param itemList
	 * @return
	 */
	private List<GiantEagleItemDTO> getUniqueLigItems(List<GiantEagleItemDTO> itemList) {

		// HashMap<"UPC+RetailerItemCode", List<"FamilyCodes">>
		HashMap<ItemDetailKey, List<String>> familyCodesBasedOnItem = getFamilyCodesBasedOnItem(itemList);

		HashMap<String, List<GiantEagleItemDTO>> itemsBasedOnFamilyCode = getItemsBasedOnFamilyCode(itemList);

		// Get list of items which has more than one family codes
		List<ItemDetailKey> itemsWithDiffFamilyCode = new ArrayList<>();
		List<ItemDetailKey> itemsWithSingleFamilyCode = new ArrayList<>();
		familyCodesBasedOnItem.forEach((key, values) -> {
			if (values.size() > 1) {
				itemsWithDiffFamilyCode.add(key);
			} else if (values.size() == 1) {
				itemsWithSingleFamilyCode.add(key);
			}
		});

		List<GiantEagleItemDTO> itemFinalList = groupLIGMembersFromDiffFamilies(familyCodesBasedOnItem.keySet(), familyCodesBasedOnItem,
				itemsBasedOnFamilyCode);

		return itemFinalList;
	}

	private List<GiantEagleItemDTO> groupLIGMembersFromDiffFamilies(Set<ItemDetailKey> itemsWithDiffFamilyCode,
			HashMap<ItemDetailKey, List<String>> familyCodesBasedOnItem, HashMap<String, List<GiantEagleItemDTO>> itemsBasedOnFamilyCode) {

		HashMap<ItemDetailKey, GiantEagleItemDTO> finalLIGItemMap = new HashMap<>();
		// Process Each items to group diff lig into a single Lig
		itemsWithDiffFamilyCode.forEach(itemKey -> {
			List<GiantEagleItemDTO> ligMembers = new ArrayList<>();

			if (familyCodesBasedOnItem.containsKey(itemKey)) {
				familyCodesBasedOnItem.get(itemKey).forEach(familyCodeKey -> {
					CopyOnWriteArraySet<String> relatedFamilyCodeList = new CopyOnWriteArraySet<>();
					relatedFamilyCodeList.add(familyCodeKey);
					do {
						// Check current processing Family code is available in itemsBasedOnFamilyCode HashMap
						// Consider all items based on family code and get list of Family code from those items and add those
						// items to list
						// Repeat same process for new family codes list until all the Family codes items were added into list

						HashSet<String> familyListToBeDeleted = new HashSet<>();

						relatedFamilyCodeList.forEach(familyCode -> {
							familyListToBeDeleted.add(familyCode);
							if (itemsBasedOnFamilyCode.containsKey(familyCode)) {

								itemsBasedOnFamilyCode.get(familyCode).forEach(item -> {

									// Add List of family codes related to current processing item
									ItemDetailKey key = new ItemDetailKey(item.getUPC(), item.getRITEM_NO());
									if (familyCodesBasedOnItem.containsKey(key)) {
										// Add all new Family codes were obtained from
										relatedFamilyCodeList.addAll(familyCodesBasedOnItem.get(key));
									}
									ligMembers.add(item);
								});

								// Remove item based on family code from Map
								itemsBasedOnFamilyCode.remove(familyCode);
							}
						});

						// Delete Family code which is processed
						relatedFamilyCodeList.removeAll(familyListToBeDeleted);

					} while (relatedFamilyCodeList.size() > 0);
				});
			}

			if (ligMembers.size() > 0) {

				Set<Integer> familyCodes = ligMembers.stream().filter(item -> item.getFMLY_CD() != null && !item.getFMLY_CD().trim().isEmpty())
						.map(item -> Integer.valueOf(item.getFMLY_CD())).collect(Collectors.toSet());

				Optional<Integer> minValue = familyCodes.stream().min(Comparator.comparing(Integer::valueOf));

				// TO find Family desc based on Family code
				Optional<GiantEagleItemDTO> familyDesc = ligMembers.stream().filter(item -> item.getFMLY_CD().equals(String.valueOf(minValue.get())))
						.findAny();

				ligMembers.forEach(item -> {
					ItemDetailKey key = new ItemDetailKey(item.getUPC(), item.getRITEM_NO());
					if (familyDesc.isPresent()) {
						item.setFMLY_DSCR(familyDesc.get().getFMLY_DSCR());
					}
					item.setFMLY_CD(String.valueOf(minValue.get()));
					finalLIGItemMap.put(key, item);
				});

			}
		});

		List<GiantEagleItemDTO> itemFinalList = new ArrayList<>();

		finalLIGItemMap.forEach((key, value) -> {
			itemFinalList.add(value);
		});

		return itemFinalList;
	}

	private List<GiantEagleItemDTO> getNonLigItemList(List<GiantEagleItemDTO> itemList) {

		List<GiantEagleItemDTO> nonLigItems = itemList.parallelStream()
				.filter(item -> item != null && (item.getFMLY_CD() == null || item.getFMLY_CD().equals(""))).collect(Collectors.toList());
		return nonLigItems;
	}
}
