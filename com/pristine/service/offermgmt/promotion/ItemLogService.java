package com.pristine.service.offermgmt.promotion;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
//import java.util.HashSet;
import java.util.List;

import org.apache.log4j.Logger;

import com.pristine.dto.offermgmt.promotion.PromoItemDTO;

public class ItemLogService {

	private static Logger logger = Logger.getLogger("ItemLogService");

	private FileWriter fw = null;
	private PrintWriter pw = null;

	private char delimitor = ',';
	
	public void writeToCSVFile(String outputPath, List<PromoItemDTO> promoItems) {
		try {
			File file = new File(outputPath);
			if (!file.exists()) {
				fw = new FileWriter(outputPath);
				pw = new PrintWriter(fw);
				writeHeader();
			}
			else {
				fw = new FileWriter(outputPath, true);
				pw = new PrintWriter(fw);
			}

			writeContent(promoItems);

			pw.flush();
			fw.flush();
			pw.close();
			fw.close();
		} catch (Exception ex) {
			logger.error("JavaException", ex);
		}
	}

	private void writeHeader() {
		pw.print("Dept Id");
		pw.print(delimitor);
		pw.print("Dept Name");
		pw.print(delimitor);
		pw.print("PPG Group Id");
		pw.print(delimitor);
		pw.print("IS PPG Lead");
		pw.print(delimitor);
		pw.print("Product Level Id");
		pw.print(delimitor);
		pw.print("Product Id");
		pw.print(delimitor);
		pw.print("SubCategory Id");
		pw.print(delimitor);
		pw.print("Brand Id");
		pw.print(delimitor);
		pw.print("Brand Name");
		pw.print(delimitor);
		pw.print("Active");
		pw.print(delimitor);
		pw.print("Ret Lir Id");
		pw.print(delimitor);
		pw.print("Item Name");
		pw.print(delimitor);
		pw.print("LIG Name");
		pw.print(delimitor);
		pw.print("Cat Id");
		pw.print(delimitor);
		pw.print("Reg Quantity");
		pw.print(delimitor);
		pw.print("Reg Price");
		pw.print(delimitor);
		pw.print("Sale Quantity");
		pw.print(delimitor);
		pw.print("Sale Price");
		pw.print(delimitor);
		pw.print("Sale Promo Type");
		pw.print(delimitor);
		pw.print("Sale Start");
		pw.print(delimitor);
		pw.print("Sale End");
		pw.print(delimitor);
		pw.print("Ad Week");
		pw.print(delimitor);
		pw.print("Ad No");
		pw.print(delimitor);
		pw.print("Block No");
		pw.print(delimitor);
		pw.print("Display Type Id");
		pw.print(delimitor);
		pw.print("List Cost");
		pw.print(delimitor);
		pw.print("Deal Cost");
		pw.print(delimitor);
		pw.print("Min Deal Cost");
		pw.print(delimitor);
		pw.print("Derived Deal Cost");
		pw.print(delimitor);
		pw.print("Final Cost");
		pw.print(delimitor);
		pw.print("Comp Price");
		pw.print(delimitor);
		pw.print("# of HH recommended");
		pw.print(delimitor);
		
		pw.print("Pred Status(Sale)");
		pw.print(delimitor);
		pw.print("Pred Mov(Sale)");
		pw.print(delimitor);
		pw.print("Pred Rev(Sale)");
		pw.print(delimitor);
		pw.print("Pred Mar(Sale)");
		pw.print(delimitor);
		pw.print("Total Cost(Sale)");
		pw.print(delimitor);
		pw.print("Mar PCT(Sale)");
		pw.print(delimitor);
		
		pw.print("Pred Status(Reg)");
		pw.print(delimitor);		
		pw.print("Pred Mov(Reg)");
		pw.print(delimitor);		
		pw.print("Pred Rev(Reg)");
		pw.print(delimitor);		
		pw.print("Pred Mar(Reg)");
		pw.print(delimitor);
		
		pw.print("Is Ad in Pre Week");
		pw.print(delimitor);
		pw.print("IsOnTPR");
		pw.print(delimitor);
		pw.print("Is Ad in Fut Week");
		pw.print(delimitor);
		pw.print("Is Price>Comp");
		pw.print(delimitor);
		pw.print("Is Item in Promo");
		pw.print(delimitor);
		pw.print("Addtional Detail");
		pw.println(" ");
	}

	private void writeContent(List<PromoItemDTO> promoItems) {
		for (PromoItemDTO promoItem : promoItems) {
//			HashSet<Long> ppgGroups = new HashSet<Long>();
			
//			if(promoItem.getPpgGroupIds() != null && promoItem.getPpgGroupIds().size() > 0){
//				ppgGroups.clear();
//				ppgGroups.addAll(promoItem.getPpgGroupIds());
//			} else {
//				ppgGroups.clear();
//				ppgGroups.add(0l); //non PPG items
//			}
			
//			for(Long ppgGroupId : ppgGroups){
				pw.print(promoItem.getDeptId());
				pw.print(delimitor);
				pw.print(promoItem.getDeptName());
				pw.print(delimitor);
				pw.print(promoItem.getPpgGroupId());
				pw.print(delimitor);
				pw.print((promoItem.isPPGLeadItem()) ? "Y" : "");
				pw.print(delimitor);
				pw.print(promoItem.getProductKey().getProductLevelId());
				pw.print(delimitor);
				pw.print(promoItem.getProductKey().getProductId());
				pw.print(delimitor);
				pw.print(promoItem.getSubCategoryId());
				pw.print(delimitor);
				pw.print(promoItem.getBrandId());
				pw.print(delimitor);
				pw.print(promoItem.getBrandName());
				pw.print(delimitor);
				pw.print(promoItem.isActive());
				pw.print(delimitor);
				pw.print(promoItem.getRetLirId() > 0 ? promoItem.getRetLirId() : "");
				pw.print(delimitor);
				pw.print(promoItem.getItemName() != null ? promoItem.getItemName().replace(",", " ") : "");
				pw.print(delimitor);
				pw.print(promoItem.getRetLirName() != null ? promoItem.getRetLirName().replace(",", " ") : "");
				pw.print(delimitor);
				pw.print(promoItem.getCategoryId());
				pw.print(delimitor);
				pw.print((promoItem.getRegPrice() != null ? promoItem.getRegPrice().multiple : ""));
				pw.print(delimitor);
				pw.print((promoItem.getRegPrice() != null ? promoItem.getRegPrice().price : ""));
				pw.print(delimitor);
				pw.print((promoItem.getSaleInfo() != null ? (promoItem.getSaleInfo().getSalePrice() != null ? 
						promoItem.getSaleInfo().getSalePrice().multiple : "") : ""));
				pw.print(delimitor);
				pw.print((promoItem.getSaleInfo() != null ? (promoItem.getSaleInfo().getSalePrice() != null ?
						promoItem.getSaleInfo().getSalePrice().price : "") : "" ));
				pw.print(delimitor);
				pw.print((promoItem.getSaleInfo() != null ? (promoItem.getSaleInfo() != null ? 
						promoItem.getSaleInfo().getPromoTypeId() : "") : ""));
				pw.print(delimitor);
				pw.print((promoItem.getSaleInfo() != null ? promoItem.getSaleInfo().getSaleStartDate() : ""));
				pw.print(delimitor);
				pw.print((promoItem.getSaleInfo() != null ? promoItem.getSaleInfo().getSaleEndDate() : ""));
				pw.print(delimitor);
				pw.print((promoItem.getAdInfo() != null ? promoItem.getAdInfo().getWeeklyAdStartDate() : ""));
				pw.print(delimitor);
				pw.print((promoItem.getAdInfo() != null ? promoItem.getAdInfo().getAdPageNo() : ""));
				pw.print(delimitor);
				pw.print((promoItem.getAdInfo() != null ? promoItem.getAdInfo().getAdBlockNo() : ""));
				pw.print(delimitor);
				pw.print((promoItem.getDisplayInfo() != null ? (promoItem.getDisplayInfo().getDisplayTypeLookup() != null ?
						promoItem.getDisplayInfo().getDisplayTypeLookup().getDisplayTypeId() : "" ) : ""));
				pw.print(delimitor);
				pw.print(promoItem.getListCost() != null ? promoItem.getListCost() : "");
				pw.print(delimitor);
				pw.print(promoItem.getDealCost() != null ? promoItem.getDealCost() : "");
				pw.print(delimitor);
				pw.print(promoItem.getMinDealCost() != null ? promoItem.getMinDealCost() : "");
				pw.print(delimitor);
				pw.print(promoItem.getDerivedDealCost() != null ? promoItem.getDerivedDealCost() : "");
				pw.print(delimitor);
				pw.print(promoItem.getFinalCost() != null ? promoItem.getFinalCost() : "");
				pw.print(delimitor);
				pw.print(promoItem.getCompPrice() != null ? promoItem.getCompPrice() : "");
				pw.print(delimitor);
				pw.print(promoItem.getNoOfHHRecommendedTo());
				pw.print(delimitor);
				
				pw.print(promoItem.getPredStatus() != null ? promoItem.getPredStatus().getStatusCode() : "");
				pw.print(delimitor);
				pw.print(promoItem.getPredMov() != null ? promoItem.getPredMov() : "");
				pw.print(delimitor);
				pw.print(promoItem.getPredRev());
				pw.print(delimitor);
				pw.print(promoItem.getPredMar());
				pw.print(delimitor);
				pw.print(promoItem.getPredTotalCostOnSalePrice());
				pw.print(delimitor);
				pw.print(promoItem.getSaleMarginPCT());
				pw.print(delimitor);
				
				
				pw.print(promoItem.getPredStatusReg() != null ? promoItem.getPredStatusReg().getStatusCode() : "");
				pw.print(delimitor);				
				pw.print(promoItem.getPredMovReg());
				pw.print(delimitor);				
				pw.print(promoItem.getPredRevReg());
				pw.print(delimitor);				
				pw.print(promoItem.getPredMarReg());
				pw.print(delimitor);
				
				pw.print(promoItem.isPresentInPreviousWeekAd());
				pw.print(delimitor);
				pw.print(promoItem.isOnTPR());
				pw.print(delimitor);
				pw.print(promoItem.isPresentInFutureWeekAd());
				pw.print(delimitor);
				pw.print(promoItem.isPriceGreaterThanCompPrice());
				pw.print(delimitor);
				pw.print(promoItem.isItemCurrentlyOnPromo());
				pw.print(delimitor);
				pw.print(promoItem.getAdditionalDetailForLog());
				pw.println(" ");
			}
//		}
	}

}
