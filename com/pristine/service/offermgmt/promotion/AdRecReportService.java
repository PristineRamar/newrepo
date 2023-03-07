package com.pristine.service.offermgmt.promotion;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.pristine.dto.offermgmt.promotion.AdDetail;
import com.pristine.dto.offermgmt.promotion.BlockDetail;
import com.pristine.dto.offermgmt.promotion.PageDetail;
import com.pristine.dto.offermgmt.promotion.PromoItemDTO;
import com.pristine.exception.GeneralException;
import com.pristine.lookup.offermgmt.promotion.PromoTypeLookup;
import com.pristine.util.offermgmt.PRCommonUtil;
import com.pristine.util.offermgmt.PRFormatHelper;

public class AdRecReportService {

	private static Logger logger = Logger.getLogger("ReportGenService");

	public void generateAdRecExcelReport(String outputPath, String recWeek, AdDetail actualAdDetail, AdDetail recAdDetail,
			Set<Integer> departmentList) throws GeneralException {

		String pgTempFile = outputPath + "/Template/ad_rec_template.xlsx";
		
		String timeStamp = new SimpleDateFormat("ddMMyyyy_HHmmss").format(new java.util.Date());

		String pgDestFile = outputPath + "/" + (departmentList.size() == 1 ? departmentList.iterator().next() + "_" : "") + recWeek.replace("/", "_") + "_"
				+ timeStamp + ".xlsx";
		
		boolean copyStatus = copyExcel(pgTempFile, pgDestFile);

		if (!copyStatus) {
			logger.error("Unable to create Excel file");
			return;
		}

		try {
			logger.info("Output File name - " + pgDestFile);

			InputStream inp = new FileInputStream(pgDestFile);
			XSSFWorkbook wb = new XSSFWorkbook(inp);
			fillAdRecSummarySheet(wb, actualAdDetail, recAdDetail);

			fillAdRecDetailedSheet(wb, actualAdDetail, recAdDetail);
			FileOutputStream outputStream = new FileOutputStream(pgDestFile);
			wb.write(outputStream);
			wb.close();
			outputStream.close();
		} catch (Exception fe) {
			logger.error("File Processing Exception : ", fe);
			throw new GeneralException(fe.getMessage(), fe);
		}

	}
	
	private void fillAdRecSummarySheet(XSSFWorkbook wb, AdDetail actualAdDetail, AdDetail recAdDetail) {

		ExcelStyle excelStyle = new ExcelStyle();
		XSSFSheet sheet = wb.getSheetAt(0);
		int rowToStart = 2;

		int colCount = -1;

		for (PageDetail pageDetail : actualAdDetail.getPageMap().values()) {
			
			Map<Integer, BlockDetail> blockSortedByDeptName = pageDetail.getBlockMap().entrySet().stream()
					.sorted((e1, e2) -> e1.getValue().getDepartmentName().compareTo(e2.getValue().getDepartmentName()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
			
			for (Map.Entry<Integer, BlockDetail> actualAdBlockMap : blockSortedByDeptName.entrySet()) {
				colCount = -1;
				XSSFRow itemRow = sheet.getRow(rowToStart);
				if (itemRow == null) {
					itemRow = sheet.createRow(rowToStart);
				}
				rowToStart++;

				setCellValue(wb, itemRow, ++colCount, actualAdBlockMap.getValue().getPageBlockNoKey().getPageNumber(), excelStyle);
				setCellValue(wb, itemRow, ++colCount, actualAdBlockMap.getValue().getPageBlockNoKey().getBlockNumber(), excelStyle);
				setCellValue(wb, itemRow, ++colCount, actualAdBlockMap.getValue().getDepartmentName(), excelStyle);

				BlockDetail recBlockDetail = null;
				// Prediction of System Rec Items
				for (PageDetail recPageDetail : recAdDetail.getPageMap().values()) {
					if (recPageDetail.getPageNo() == actualAdBlockMap.getValue().getPageBlockNoKey().getPageNumber()) {
						recBlockDetail = recPageDetail.getBlockMap().get(actualAdBlockMap.getValue().getPageBlockNoKey().getBlockNumber());
					}
				}
				setCellValue(wb, itemRow, ++colCount,
						PRCommonUtil.roundDouble(actualAdBlockMap.getValue().getSalePricePredictionMetrics().getPredMov()), excelStyle);
				setCellValue(wb, itemRow, ++colCount,
						PRCommonUtil.roundDouble(actualAdBlockMap.getValue().getSalePricePredictionMetrics().getPredRev()), excelStyle);
				setCellValue(wb, itemRow, ++colCount,
						PRCommonUtil.roundDouble(actualAdBlockMap.getValue().getSalePricePredictionMetrics().getPredMar()), excelStyle);
				setCellValue(wb, itemRow, ++colCount, actualAdBlockMap.getValue().getTotalHHCnt(), excelStyle);

				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getSalePricePredictionMetrics().getPredMov()),
						excelStyle);
				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getSalePricePredictionMetrics().getPredRev()),
						excelStyle);
				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getSalePricePredictionMetrics().getPredMar()),
						excelStyle);
				setCellValue(wb, itemRow, ++colCount, recBlockDetail.getTotalHHCnt(), excelStyle);
				
				//Actual vs recommendation
				setCellValue(wb, itemRow, ++colCount,
						doubleToTwoDigitStringPCT(PRCommonUtil.getLiftPCT(actualAdBlockMap.getValue().getSalePricePredictionMetrics().getPredMov(),
								recBlockDetail.getSalePricePredictionMetrics().getPredMov())),
						excelStyle);
				setCellValue(wb, itemRow, ++colCount,
						doubleToTwoDigitStringPCT(PRCommonUtil.getLiftPCT(actualAdBlockMap.getValue().getSalePricePredictionMetrics().getPredRev(),
								recBlockDetail.getSalePricePredictionMetrics().getPredRev())),
						excelStyle);
				setCellValue(wb, itemRow, ++colCount,
						doubleToTwoDigitStringPCT(PRCommonUtil.getLiftPCT(actualAdBlockMap.getValue().getSalePricePredictionMetrics().getPredMar(),
								recBlockDetail.getSalePricePredictionMetrics().getPredMar())),
						excelStyle);
				setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(
						PRCommonUtil.getLiftPCT(actualAdBlockMap.getValue().getTotalHHCnt(), recBlockDetail.getTotalHHCnt())), excelStyle);
				
			}
		}

		// Total summary details
		XSSFRow itemRow = sheet.getRow(rowToStart);
		if (itemRow == null) {
			itemRow = sheet.createRow(rowToStart);
		}
		rowToStart++;
		colCount = 2;
		// Actual Ad Items
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(actualAdDetail.getPredictionMetrics().getPredMov()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(actualAdDetail.getPredictionMetrics().getPredRev()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(actualAdDetail.getPredictionMetrics().getPredMar()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, actualAdDetail.getTotalHHReachCnt(), excelStyle);

		// System Rec Items
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recAdDetail.getPredictionMetrics().getPredMov()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recAdDetail.getPredictionMetrics().getPredRev()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recAdDetail.getPredictionMetrics().getPredMar()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, recAdDetail.getTotalHHReachCnt(), excelStyle);

		// Prediction of Actual Ad Items
		rowToStart++;
		itemRow = sheet.getRow(rowToStart);
		if (itemRow == null) {
			itemRow = sheet.createRow(rowToStart);
		}
		rowToStart++;
		colCount = -1;
		setCellValue(wb, itemRow, ++colCount, "Prediction of Actual Ad Items", excelStyle);
		setCellValue(wb, itemRow, ++colCount, null, excelStyle);
		setCellValue(wb, itemRow, ++colCount, null, excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(actualAdDetail.getPredictionMetrics().getPredMov()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(actualAdDetail.getPredictionMetrics().getPredRev()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(actualAdDetail.getPredictionMetrics().getPredMar()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, actualAdDetail.getTotalHHReachCnt(), excelStyle);

		// Prediction of System Rec Items
		itemRow = sheet.getRow(rowToStart);
		if (itemRow == null) {
			itemRow = sheet.createRow(rowToStart);
		}
		rowToStart++;
		colCount = -1;
		setCellValue(wb, itemRow, ++colCount, "Prediction of System Rec Items *", excelStyle);
		setCellValue(wb, itemRow, ++colCount, null, excelStyle);
		setCellValue(wb, itemRow, ++colCount, null, excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recAdDetail.getPredictionMetrics().getPredMov()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recAdDetail.getPredictionMetrics().getPredRev()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recAdDetail.getPredictionMetrics().getPredMar()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, recAdDetail.getTotalHHReachCnt(), excelStyle);

		// Total Gain w.r.t. Actual Ad
		itemRow = sheet.getRow(rowToStart);
		if (itemRow == null) {
			itemRow = sheet.createRow(rowToStart);
		}
		rowToStart++;
		colCount = -1;
		setCellValue(wb, itemRow, ++colCount, "Total Gain w.r.t. Actual Ad", excelStyle);
		setCellValue(wb, itemRow, ++colCount, null, excelStyle);
		setCellValue(wb, itemRow, ++colCount, null, excelStyle);
		setCellValue(wb, itemRow, ++colCount,
				doubleToTwoDigitStringPCT(
						PRCommonUtil.getLiftPCT(actualAdDetail.getPredictionMetrics().getPredMov(), recAdDetail.getPredictionMetrics().getPredMov())),
				excelStyle);
		setCellValue(wb, itemRow, ++colCount,
				doubleToTwoDigitStringPCT(
						PRCommonUtil.getLiftPCT(actualAdDetail.getPredictionMetrics().getPredRev(), recAdDetail.getPredictionMetrics().getPredRev())),
				excelStyle);
		setCellValue(wb, itemRow, ++colCount,
				doubleToTwoDigitStringPCT(
						PRCommonUtil.getLiftPCT(actualAdDetail.getPredictionMetrics().getPredMar(), recAdDetail.getPredictionMetrics().getPredMar())),
				excelStyle);
		setCellValue(wb, itemRow, ++colCount,
				doubleToTwoDigitStringPCT(PRCommonUtil.getLiftPCT(actualAdDetail.getTotalHHReachCnt(), recAdDetail.getTotalHHReachCnt())),
				excelStyle);
	}

	private void fillAdRecDetailedSheet(XSSFWorkbook wb, AdDetail actualAdDetail, AdDetail recAdDetail) {

		ExcelStyle excelStyle = new ExcelStyle();
		XSSFSheet sheet = wb.getSheetAt(1);
		int rowToStart = 2;

		for (Map.Entry<Integer, PageDetail> pageDetail : actualAdDetail.getPageMap().entrySet()) {
			
			Map<Integer, BlockDetail> blockSortedByDeptName = pageDetail.getValue().getBlockMap().entrySet().stream()
					.sorted((e1, e2) -> e1.getValue().getDepartmentName().compareTo(e2.getValue().getDepartmentName()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
			
			for (BlockDetail blockDetail : blockSortedByDeptName.values()) {
				BlockDetail recBlockDetail = null;
				// Prediction of System Rec Items
				for (PageDetail recPageDetail : recAdDetail.getPageMap().values()) {
					if (recPageDetail.getPageNo() == blockDetail.getPageBlockNoKey().getPageNumber()) {
						recBlockDetail = recPageDetail.getBlockMap().get(blockDetail.getPageBlockNoKey().getBlockNumber());
					}
				}
				int colCount = -1;
				XSSFRow itemRow = getRow(sheet, rowToStart);
				rowToStart++;
				List<String> departmentNames = new ArrayList<>();
				blockDetail.getDepartments().values().forEach(depName -> {
					departmentNames.add(depName);
				});
				String pageAndBlockInfo = "Page: " + blockDetail.getPageBlockNoKey().getPageNumber() + ",Block: "
						+ blockDetail.getPageBlockNoKey().getBlockNumber() + ",Departments:"
						+ PRCommonUtil.getCommaSeperatedStringFromStrArray(departmentNames);
				setCellValue(wb, itemRow, ++colCount, pageAndBlockInfo, excelStyle);

				itemRow = getRow(sheet, rowToStart++);
				colCount = -1;
				setCellValue(wb, itemRow, ++colCount, "Actual", excelStyle);

				for (PromoItemDTO promoItemDTO : blockDetail.getItems()) {
					if (promoItemDTO.isActive()) {
						itemRow = getRow(sheet, rowToStart++);
						updatePageBlockItemDetails(wb, itemRow, promoItemDTO, excelStyle, false);
					}
				}
				itemRow = getRow(sheet, rowToStart++);
				colCount = 8;
				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(blockDetail.getSalePricePredictionMetrics().getPredMov()), excelStyle);
				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(blockDetail.getSalePricePredictionMetrics().getPredRev()), excelStyle);
				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(blockDetail.getSalePricePredictionMetrics().getPredMar()), excelStyle);
				setCellValue(wb, itemRow, ++colCount, blockDetail.getTotalHHCnt(), excelStyle);

				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(blockDetail.getRegPricePredictionMetrics().getPredMov()),
						excelStyle);
				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(blockDetail.getRegPricePredictionMetrics().getPredRev()),
						excelStyle);
				setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(blockDetail.getRegPricePredictionMetrics().getPredMar()),
						excelStyle);

				setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(blockDetail.getSaleUnitsLiftPCTAgainstRegUnits()), excelStyle);
				setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(blockDetail.getSaleRevLiftPCTAgainstRegRev()), excelStyle);
				setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(blockDetail.getSaleMarLiftPCTAgainstRegMar()), excelStyle);
				
				itemRow = getRow(sheet, rowToStart++);
				colCount = -1;
				setCellValue(wb, itemRow, ++colCount, "Recommendations", excelStyle);

				if (recBlockDetail.getItems().size() > 0) {
					for (PromoItemDTO promoItemDTO : recBlockDetail.getItems()) {
						itemRow = getRow(sheet, rowToStart++);
						updatePageBlockItemDetails(wb, itemRow, promoItemDTO, excelStyle, true);
					}
					itemRow = getRow(sheet, rowToStart++);
					colCount = 8;
					setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getSalePricePredictionMetrics().getPredMov()),
							excelStyle);
					setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getSalePricePredictionMetrics().getPredRev()),
							excelStyle);
					setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getSalePricePredictionMetrics().getPredMar()),
							excelStyle);
					setCellValue(wb, itemRow, ++colCount, recBlockDetail.getTotalHHCnt(), excelStyle);

					setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getRegPricePredictionMetrics().getPredMov()),
							excelStyle);
					setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getRegPricePredictionMetrics().getPredRev()),
							excelStyle);
					setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(recBlockDetail.getRegPricePredictionMetrics().getPredMar()),
							excelStyle);

					setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(recBlockDetail.getSaleUnitsLiftPCTAgainstRegUnits()), excelStyle);
					setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(recBlockDetail.getSaleRevLiftPCTAgainstRegRev()), excelStyle);
					setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(recBlockDetail.getSaleMarLiftPCTAgainstRegMar()), excelStyle);
				} else {
					itemRow = getRow(sheet, rowToStart);
					rowToStart++;
					colCount = -1;
					setCellValue(wb, itemRow, ++colCount, "No Recommendations", excelStyle);
				}
				rowToStart++;

			}
		}
	}

	private void updatePageBlockItemDetails(XSSFWorkbook wb, XSSFRow itemRow, PromoItemDTO promoItemDTO, ExcelStyle excelStyle,
			boolean isRecommendationItem) {
		int colCount = -1;
		String itemName = (promoItemDTO.getRetLirName() != null && !promoItemDTO.getRetLirName().isEmpty()) ? promoItemDTO.getRetLirName()
				: promoItemDTO.getItemName();
		setCellValue(wb, itemRow, ++colCount, itemName, excelStyle);
		setCellValue(wb, itemRow, ++colCount, promoItemDTO.getRegPrice().multiple, excelStyle);
		setCellValue(wb, itemRow, ++colCount, promoItemDTO.getRegPrice().price, excelStyle);
		setCellValue(wb, itemRow, ++colCount, promoItemDTO.getSaleInfo().getSalePrice().multiple, excelStyle);
		setCellValue(wb, itemRow, ++colCount, promoItemDTO.getSaleInfo().getSalePrice().price, excelStyle);
		setCellValue(wb, itemRow, ++colCount, PromoTypeLookup.get(promoItemDTO.getSaleInfo().getPromoTypeId()).name(),
				excelStyle);
		setCellValue(wb, itemRow, ++colCount, promoItemDTO.getDisplayInfo(), excelStyle);
		setCellValue(wb, itemRow, ++colCount, promoItemDTO.getListCost(), excelStyle);
		setCellValue(wb, itemRow, ++colCount, promoItemDTO.getFinalCost(), excelStyle);

		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(promoItemDTO.getPredMov()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(promoItemDTO.getPredRev()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(promoItemDTO.getPredMar()), excelStyle);
		setCellValue(wb, itemRow, ++colCount, promoItemDTO.getNoOfHHRecommendedTo(), excelStyle);
//		if (isRecommendationItem) {
			setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(promoItemDTO.getPredMovReg()), excelStyle);
			setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(promoItemDTO.getPredRevReg()), excelStyle);
			setCellValue(wb, itemRow, ++colCount, PRCommonUtil.roundDouble(promoItemDTO.getPredMarReg()), excelStyle);

			setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(promoItemDTO.getSaleUnitsLiftPCTAgainstRegUnits()), excelStyle);
			setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(promoItemDTO.getSaleRevLiftPCTAgainstRegRev()), excelStyle);
			setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(promoItemDTO.getSaleMarLiftPCTAgainstRegMar()), excelStyle);
			setCellValue(wb, itemRow, ++colCount, doubleToTwoDigitStringPCT(promoItemDTO.getSaleDiscountPCT()), excelStyle);

//		}
	}

	private XSSFRow getRow(XSSFSheet sheet, int rowToStart) {
		XSSFRow itemRow = sheet.getRow(rowToStart);
		if (itemRow == null) {
			itemRow = sheet.createRow(rowToStart);
		}
		return itemRow;

	}

	public boolean copyExcel(String inputFile, String outputFile) {

		boolean copyStatus = false;
		File source = new File(inputFile);
		File dest = new File(outputFile);

		try {
			FileUtils.copyFile(source, dest);
			copyStatus = true;
		} catch (IOException ie) {
			ie.printStackTrace();
			copyStatus = false;
		}
		return copyStatus;
	}

	/**
	 * Sets string value to cell
	 * 
	 * @param row
	 * @param colCount
	 * @param value
	 * @param sheet
	 */
	private void setCellValue(XSSFWorkbook workbook, XSSFRow row, int colCount, Object value, ExcelStyle excelStyle) {

		XSSFCell cell;
		if (row.getCell(colCount) == null) {
			cell = row.createCell(colCount);
		} else {
			cell = row.getCell(colCount);
		}

		if (excelStyle.cloneStyle) {
			XSSFCellStyle cellStyle = workbook.createCellStyle();
			cellStyle = (XSSFCellStyle) cellStyle.clone();
			cellStyle.setIndention(excelStyle.indention);
			cellStyle.setBorderTop(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderBottom(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderRight(XSSFCellStyle.BORDER_THIN);
			cellStyle.setBorderLeft(XSSFCellStyle.BORDER_THIN);
			cellStyle.setAlignment(XSSFCellStyle.ALIGN_RIGHT);
			cell.setCellStyle(cellStyle);
		}

		if (value instanceof String) {
			cell.setCellValue(String.valueOf(value));
		} else if (value instanceof Double) {
			cell.setCellValue((Double) value);
		} else if (value instanceof Long) {
			cell.setCellValue((Long) value);
		} else if (value instanceof Integer) {
			cell.setCellValue((Integer) value);
		}
	}
	
	private String doubleToTwoDigitStringPCT(double input) {
		return PRFormatHelper.doubleToTwoDigitString(input) + "%";
	}
}

class ExcelStyle {
	public short indention = 0;
	public boolean cloneStyle = false;
}
