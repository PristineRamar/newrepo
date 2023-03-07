package com.pristine.priceChangePerformance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.csvreader.CsvReader;
import com.pristine.dao.priceChangePerformance.CommonTaskDAO;
import com.pristine.dao.priceChangePerformance.ReportDataInsertDAO;
import com.pristine.dao.priceChangePerformance.TlogDataDAO;
import com.pristine.dto.priceChangePerformance.CatContriSummaryBO;
import com.pristine.dto.priceChangePerformance.CatForecastAccBO;
import com.pristine.dto.priceChangePerformance.FinalDataDTO;
import com.pristine.dto.priceChangePerformance.IMSDataDTO;
import com.pristine.dto.priceChangePerformance.ItemInfoDTO;
import com.pristine.dto.priceChangePerformance.TlogDataDTO;
import com.pristine.dto.priceChangePerformance.WeekGraphDataDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PropertyManager;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class PriceChangePerformanceServices {
	private static Logger logger = Logger.getLogger("PriceChangePerformanceServices");

	public List<Integer> GetUniqueItemCodes(Map<Integer, ItemInfoDTO> iteminfo){
		
		List<Integer> UItemCodes = new ArrayList<Integer>();
		
		iteminfo.forEach((k,v) -> {
			if(!UItemCodes.contains(v.getItemCode())) 
			{
				UItemCodes.add(v.getItemCode());
			}
			
		});
		
		
		return UItemCodes;
	
	}
	
	
	public String CreateItemCodeString(int From, int To, List<Integer> iteminfo)
	{	
		logger.info("Creating ItemCode String" );
		StringBuilder ItemCodeString = new StringBuilder();
		List<Integer>sList = iteminfo.subList(From, To);
		sList.forEach(item -> {
					if(ItemCodeString.length()>0) {
						ItemCodeString.append(","+ String.valueOf(item)); 
					}
					else {ItemCodeString.append(String.valueOf(item));}
			});
		
		return ItemCodeString.toString();
		
	}

	public List<TlogDataDTO> FilterTlogOnCustType(List<TlogDataDTO> TlogData){
		  
		Map<Integer, Map<LocalDate , Float>> RegPerc = new HashMap();
		Map<Integer, Map<LocalDate , Integer>> Total = new HashMap();
		Map<Integer ,Map<LocalDate, Float>> RegMovement = new HashMap(); 
		
		
		TlogData.forEach(tl->{
			int RIC =tl.getProductId();
			LocalDate WSD = tl.getWeekStartDate();
			if(tl.getCustomerType()=="REGULAR") {
				if(RegMovement.containsKey(RIC)) {
					if(RegMovement.get(RIC).containsKey(WSD)) {
						RegMovement.get(RIC).replace(WSD, RegMovement.get(RIC).get(WSD)+1);
					}
					else {
						RegMovement.get(RIC).put(WSD, 1f);
					}
				}else {
					Map<LocalDate, Float> m = new HashMap();
					m.put(WSD, 1f);
					RegMovement.put(RIC, m);
				}
	
			}
						
			if(Total.containsKey(RIC)) {
				if(Total.get(RIC).containsKey(WSD)) {
					Total.get(RIC).replace(WSD, Total.get(RIC).get(WSD)+1);
				}
				else {
					Total.get(RIC).put(WSD, 1);
				}
			}else {
				Map<LocalDate, Integer> m = new HashMap();
				m.put(WSD, 1);
				Total.put(RIC, m);
			}
			
		});
	
	RegMovement.forEach((k,v)->{
			Map<LocalDate, Float> m = new HashMap();
			RegPerc.put(k, m);
			v.forEach((k1,v1)->{
				RegPerc.get(k).put(k1, (v1/Total.get(k).get(k1))*100) ;
			});
		});
		
		
		TlogData.forEach(t ->{
			int RIC = t.getProductId();
			LocalDate WSD = t.getWeekStartDate();
			if(RegMovement.containsKey(RIC)&&RegMovement.get(RIC).containsKey(WSD)) {
			t.setReg_Perc(RegPerc.get(RIC).get(WSD));
			t.setReg_Mov(RegMovement.get(RIC).get(WSD));
			
			//t.setTotal_Mov(Total.get(RIC).get(WSD));
			}else {
				t.setReg_Perc(0);	
				t.setReg_Mov(0);
			}
				
		});
		/*
		Iterator<TlogDataDTO> i = TlogData.iterator();
		TlogDataDTO tlog = new TlogDataDTO();
		
		
		while(i.hasNext()) {
			tlog =i.next();
			if(tlog.getReg_Perc()<50) {
				i.remove();
			}
		 }
		 */
		return TlogData;
	}


	public List<TlogDataDTO> AddOtherPerc(List<TlogDataDTO>TlogData) {
		
		Map<Integer, Map<LocalDate , Float>> GoldMov = new HashMap();
		Map<Integer, Map<LocalDate , Float>> SilverMov = new HashMap();
		Map<Integer, Map<LocalDate , Float>> OtherMov = new HashMap();
		Map<Integer, Map<LocalDate , Float>> NoCardMov = new HashMap();
		Map<Integer, Map<LocalDate , Integer>> Total = new HashMap();
		

		Map<Integer, Map<LocalDate , Float>> GoldPerc = new HashMap();
		Map<Integer, Map<LocalDate , Float>> SilverPerc = new HashMap();
		Map<Integer, Map<LocalDate , Float>> OtherPerc = new HashMap();
		Map<Integer, Map<LocalDate , Float>> NoCardPerc = new HashMap();
		

		TlogData.forEach(tl->{
			String CustType = String.valueOf(tl.getCustomerType());
			int RIC =tl.getProductId();
			LocalDate WSD = tl.getWeekStartDate();
			if(!CustType.equals("REGULAR")) {
			
			
			if(CustType.equals("OTHER")) {
				if(OtherMov.containsKey(RIC)) {
					if(OtherMov.get(RIC).containsKey(WSD)) {
						OtherMov.get(RIC).replace(WSD, OtherMov.get(RIC).get(WSD)+1);
					}
					else {
						OtherMov.get(RIC).put(WSD, 1f);
					}
				}else {
					Map<LocalDate, Float> m = new HashMap();
					m.put(WSD, 1f);
					OtherMov.put(RIC, m);
				}
	
			}else if(CustType.equals("SILVER")) {
				if(SilverMov.containsKey(RIC)) {
					if(SilverMov.get(RIC).containsKey(WSD)) {
						SilverMov.get(RIC).replace(WSD, SilverMov.get(RIC).get(WSD)+1);
					}
					else {
						SilverMov.get(RIC).put(WSD, 1f);
					}
				}else {
					Map<LocalDate, Float> m = new HashMap();
					m.put(WSD, 1f);
					SilverMov.put(RIC, m);
				}
	
			}else if(CustType.equals("GOLD")) {
				if(GoldMov.containsKey(RIC)) {
					if(GoldMov.get(RIC).containsKey(WSD)) {
						GoldMov.get(RIC).replace(WSD, GoldMov.get(RIC).get(WSD)+1);
					}
					else {
						GoldMov.get(RIC).put(WSD, 1f);
					}
				}else {
					Map<LocalDate, Float> m = new HashMap();
					m.put(WSD, 1f);
					GoldMov.put(RIC, m);
				}
	
			}else if(CustType.equals("NOCARD")) {
				if(NoCardMov.containsKey(RIC)) {
					if(NoCardMov.get(RIC).containsKey(WSD)) {
						NoCardMov.get(RIC).replace(WSD, NoCardMov.get(RIC).get(WSD)+1);
					}
					else {
						NoCardMov.get(RIC).put(WSD, 1f);
					}
				}else {
					Map<LocalDate, Float> m = new HashMap();
					m.put(WSD, 1f);
					NoCardMov.put(RIC, m);
				}
			}
						
			}
			
			if(Total.containsKey(RIC)) {
				if(Total.get(RIC).containsKey(WSD)) {
					Total.get(RIC).replace(WSD, Total.get(RIC).get(WSD)+1);
				}
				else {
					Total.get(RIC).put(WSD, 1);
				}
			}else {
				Map<LocalDate, Integer> m = new HashMap();
				m.put(WSD, 1);
				Total.put(RIC, m);
			}
		});

	OtherMov.forEach((k,v)->{
			
		Map<LocalDate, Float> m = new HashMap();
		OtherPerc.put(k, m);
		v.forEach((k1,v1)->{
			OtherPerc.get(k).put(k1, (v1/Total.get(k).get(k1))*100) ;
		});
		});
		
	SilverMov.forEach((k,v)->{
		
		Map<LocalDate, Float> m = new HashMap();
		SilverPerc.put(k, m);
		v.forEach((k1,v1)->{
			SilverPerc.get(k).put(k1, (v1/Total.get(k).get(k1))*100) ;
		});
	});
	
	GoldMov.forEach((k,v)->{
		Map<LocalDate, Float> m = new HashMap();
		GoldPerc.put(k, m);
		v.forEach((k1,v1)->{
			GoldPerc.get(k).put(k1, (v1/Total.get(k).get(k1))*100) ;
		});
	});
	
	NoCardMov.forEach((k,v)->{
		Map<LocalDate, Float> m = new HashMap();
		NoCardPerc.put(k, m);
		v.forEach((k1,v1)->{
			NoCardPerc.get(k).put(k1, (v1/Total.get(k).get(k1))*100) ;
		});
	});
	
	
				
			
					

		TlogData.forEach(t->{
			int RIC = t.getProductId();
			LocalDate WSD = t.getWeekStartDate();
			if(OtherPerc.containsKey(RIC)&&OtherPerc.get(RIC).containsKey(WSD)) {
			
				t.setSale_Perc(OtherPerc.get(RIC).get(WSD));
				
				}else {
					
					t.setSale_Perc(0);
				}
			if(GoldMov.containsKey(RIC)&&GoldMov.get(RIC).containsKey(WSD)) {
				
				t.setGold_Mov(GoldMov.get(RIC).get(WSD));
				t.setGold_Perc(GoldPerc.get(RIC).get(WSD));
				
				}else {
				
					t.setGold_Perc(0);
					t.setGold_Mov(0);
				}
			if(SilverMov.containsKey(RIC)&&SilverMov.get(RIC).containsKey(WSD)) {
				
				t.setSil_Perc(SilverPerc.get(RIC).get(WSD));
				t.setSil_Mov(SilverMov.get(RIC).get(WSD));
				}else {
					t.setSil_Perc(0);
					t.setSil_Mov(0);
				}
			if(NoCardPerc.containsKey(RIC)&&NoCardPerc.get(RIC).containsKey(WSD)) {
				
				t.setNo_Card_Perc(NoCardPerc.get(RIC).get(WSD));
	
				}else {
					
					t.setNo_Card_Perc(0);
				
				}
			
		});
		
			
		return TlogData;
	}


	public List<TlogDataDTO> AggregateTlogBySum(List<TlogDataDTO> TlogData) {
		Map<Integer,Map<LocalDate,Double>> TotalRevenue = new HashMap();
		Map<Integer,Map<LocalDate,Double>> TotalMovement = new HashMap();
		Map<Integer,Map<LocalDate,Double>> NetMargin = new HashMap();
		Map<Integer,Map<LocalDate,Float>> RegMov = new HashMap();
		Map<Integer,Map<LocalDate,Float>> SilMov = new HashMap();
		Map<Integer,Map<LocalDate,Float>> GoldMov = new HashMap();
		
		
		TlogData.forEach(t->{
			int RIC = t.getProductId();
			LocalDate WSD = t.getWeekStartDate();
			if(TotalRevenue.containsKey(RIC)) {
				if(TotalRevenue.get(RIC).containsKey(WSD)) {
					TotalRevenue.get(RIC).replace(WSD, TotalRevenue.get(RIC).get(WSD)+t.getTotalRevenue());
					TotalMovement.get(RIC).replace(WSD, TotalMovement.get(RIC).get(WSD)+t.getTotalMovement());
					NetMargin.get(RIC).replace(WSD,NetMargin.get(RIC).get(WSD)+t.getNetMargin());
					
					RegMov.get(RIC).replace(WSD, RegMov.get(RIC).get(WSD)+t.getReg_Mov());
					GoldMov.get(RIC).replace(WSD, GoldMov.get(RIC).get(WSD)+t.getGold_Mov());
					SilMov.get(RIC).replace(WSD, SilMov.get(RIC).get(WSD)+t.getSil_Mov());

				}
				else {
					TotalRevenue.get(RIC).put(WSD,t.getTotalRevenue());
					TotalMovement.get(RIC).put(WSD,t.getTotalMovement());
					NetMargin.get(RIC).put(WSD,t.getNetMargin());
					
					RegMov.get(RIC).put(WSD,t.getReg_Mov());
					GoldMov.get(RIC).put(WSD, t.getGold_Mov());
					SilMov.get(RIC).put(WSD, t.getSil_Mov());

				}
			}else {
				Map<LocalDate, Double> mov = new HashMap();
				mov.put(WSD, t.getTotalMovement());
				TotalMovement.put(RIC, mov);
				
				Map<LocalDate, Double> marg = new HashMap();
				marg.put(WSD, t.getNetMargin());
				NetMargin.put(RIC, marg);
				
				Map<LocalDate, Double> Rev = new HashMap();
				Rev.put(WSD, t.getTotalRevenue());
				TotalRevenue.put(RIC, Rev);		
				
				Map<LocalDate, Float> GMov = new HashMap();
				GMov.put(WSD, t.getGold_Mov());
				GoldMov.put(RIC, GMov);		
				
				Map<LocalDate, Float> SMov = new HashMap();
				SMov.put(WSD, t.getSil_Mov());
				SilMov.put(RIC, SMov);		
				
				Map<LocalDate, Float> RMov = new HashMap();
				RMov.put(WSD, t.getReg_Mov());
				RegMov.put(RIC, RMov);		
				
			}
		});
		
		TlogData.forEach(t-> {
			int RIC = t.getProductId();
			LocalDate WSD = t.getWeekStartDate();
			t.setNetMargin(NetMargin.get(RIC).get(WSD));
			t.setTotalMovement(TotalMovement.get(RIC).get(WSD));
			t.setTotalRevenue(TotalRevenue.get(RIC).get(WSD));
			t.setReg_Mov(RegMov.get(RIC).get(WSD));
			t.setSil_Mov(GoldMov.get(RIC).get(WSD));
			t.setGold_Mov(SilMov.get(RIC).get(WSD));
			
		});
	
		return TlogData;
	}


	public List<TlogDataDTO> AggregateTlogByMean(List<TlogDataDTO> TlogData,String Func) {
		
		Map<Integer, TlogDataDTO> Mean = new HashMap();
		Map<Integer, Integer> Count = new HashMap();
		 BigDecimal bd=null;

		TlogData.forEach(t->{
			int RIC = t.getProductId();
			
			if(Mean.containsKey(RIC)) {
					if(Func=="OLD") {
						Mean.get(RIC).setLY_RegularPrice(Mean.get(RIC).getLY_RegularPrice()+t.getLY_RegularPrice());
						Mean.get(RIC).setLY_ListCost(Mean.get(RIC).getLY_ListCost()+t.getLY_ListCost());
					}else if(Func=="NEW") {
						
						Mean.get(RIC).setPrediction(Mean.get(RIC).getPrediction()+t.getPrediction());
						Mean.get(RIC).setPredictedSale(Mean.get(RIC).getPredictedSale()+t.getPredictedSale());
						Mean.get(RIC).setPredictedMargin(Mean.get(RIC).getPredictedMargin()+t.getPredictedMargin());
						
					}
					Mean.get(RIC).setRegularPrice(Mean.get(RIC).getRegularPrice()+t.getRegularPrice());
					Mean.get(RIC).setListCost(Mean.get(RIC).getListCost()+t.getListCost());
					Mean.get(RIC).setTotalMovement(Mean.get(RIC).getTotalMovement()+t.getTotalMovement());
					Mean.get(RIC).setTotalRevenue(Mean.get(RIC).getTotalRevenue()+t.getTotalRevenue());
					Mean.get(RIC).setNetMargin(Mean.get(RIC).getNetMargin()+t.getNetMargin());
					Mean.get(RIC).setGold_Perc(Mean.get(RIC).getGold_Perc()+t.getGold_Perc());
					Mean.get(RIC).setGold_Mov(Mean.get(RIC).getGold_Mov()+t.getGold_Mov());
					Mean.get(RIC).setGold_Mar(Mean.get(RIC).getGold_Mar()+t.getGold_Mar());
					Mean.get(RIC).setGold_Rev(Mean.get(RIC).getGold_Rev()+t.getGold_Rev());
					
					Mean.get(RIC).setSil_Perc(Mean.get(RIC).getSil_Perc()+t.getSil_Perc());
					Mean.get(RIC).setSil_Mov(Mean.get(RIC).getSil_Mov()+t.getSil_Mov());
					Mean.get(RIC).setSil_Mar(Mean.get(RIC).getSil_Mar()+t.getSil_Mar());
					Mean.get(RIC).setSil_Rev(Mean.get(RIC).getSil_Rev()+t.getSil_Rev());
					
					Mean.get(RIC).setReg_Perc(Mean.get(RIC).getReg_Perc()+t.getReg_Perc());
					Mean.get(RIC).setReg_Mov(Mean.get(RIC).getReg_Mov()+t.getReg_Mov());
					Mean.get(RIC).setReg_Mar(Mean.get(RIC).getReg_Mar()+t.getReg_Mar());
					Mean.get(RIC).setReg_Rev(Mean.get(RIC).getReg_Rev()+t.getReg_Rev());
					
					
					Mean.get(RIC).setSale_Perc(Mean.get(RIC).getSale_Perc()+t.getSale_Perc());

					Mean.get(RIC).setNo_Card_Perc(Mean.get(RIC).getNo_Card_Perc()+t.getNo_Card_Perc());
					
					
					Count.replace(RIC,Count.get(RIC)+1);
			
			}else {
				
				
				TlogDataDTO Tl = new TlogDataDTO();
				if(Func=="OLD") {
					Tl.setLY_RegularPrice(t.getLY_RegularPrice());
					Tl.setLY_ListCost(t.getLY_ListCost());	
				}else if(Func=="NEW") {
					
					Tl.setPrediction(t.getPrediction());
					Tl.setPredictedSale(t.getPredictedSale());
					Tl.setPredictedMargin(t.getPredictedMargin());
				}
				Tl.setRegularPrice(t.getRegularPrice());
				Tl.setListCost(t.getListCost());
				Tl.setTotalMovement(t.getTotalMovement());
				Tl.setTotalRevenue(t.getTotalRevenue());
				Tl.setNetMargin(t.getNetMargin());
				Tl.setGold_Perc(t.getGold_Perc());
				Tl.setGold_Mov(t.getGold_Mov());
				Tl.setGold_Mar(t.getGold_Mar());	
				Tl.setGold_Rev(t.getGold_Rev());
				
				Tl.setSil_Perc(t.getSil_Perc());
				Tl.setSil_Mov(t.getSil_Mov());
				Tl.setSil_Mar(t.getSil_Mar());	
				Tl.setSil_Rev(t.getSil_Rev());
				
				Tl.setReg_Perc(t.getReg_Perc());
				Tl.setReg_Mov(t.getReg_Mov());
				Tl.setReg_Mar(t.getReg_Mar());	
				Tl.setReg_Rev(t.getReg_Rev());
				
				Tl.setSale_Perc(t.getSale_Perc());

				Tl.setNo_Card_Perc(t.getNo_Card_Perc());
				
				
				Mean.put(RIC, Tl);
				Count.put(RIC,1);
						
			}
		});
			
		
		for(TlogDataDTO t:TlogData) {
				int RIC = t.getProductId();
				//LocalDate WSD = t.getWeekStartDate();
				int count = Count.get(RIC);
				if(Func=="OLD") {
					t.setLY_RegularPrice(Mean.get(RIC).getLY_RegularPrice()/count);
					t.setLY_ListCost(Mean.get(RIC).getLY_ListCost()/count);
					bd= new BigDecimal(t.getLY_RegularPrice()).setScale(2, RoundingMode.HALF_UP);
					t.setLY_RegularPrice(bd.floatValue());
					bd= new BigDecimal(t.getLY_ListCost()).setScale(2, RoundingMode.HALF_UP);
					t.setLY_ListCost(bd.floatValue());
					
					
				}else if(Func=="NEW") {
				
					t.setPrediction(Mean.get(RIC).getPrediction()/count);
					t.setPredictedSale(Mean.get(RIC).getPredictedSale()/count);
					t.setPredictedMargin(Mean.get(RIC).getPredictedMargin()/count);
					
				}
				t.setRegularPrice(Mean.get(RIC).getRegularPrice()/count);
				t.setListCost(Mean.get(RIC).getListCost()/count);
				bd= new BigDecimal(t.getRegularPrice()).setScale(2, RoundingMode.HALF_UP);
				t.setRegularPrice(bd.floatValue());
				bd= new BigDecimal(t.getListCost()).setScale(2, RoundingMode.HALF_UP);
				t.setListCost(bd.floatValue());
			
				t.setTotalMovement(Mean.get(RIC).getTotalMovement()/count);
				t.setTotalRevenue(Mean.get(RIC).getTotalRevenue()/count);
				t.setNetMargin(Mean.get(RIC).getNetMargin()/count);
				t.setGold_Perc(Mean.get(RIC).getGold_Perc()/count);
				t.setGold_Mov(Mean.get(RIC).getGold_Mov()/count);
				t.setGold_Mar(Mean.get(RIC).getGold_Mar()/count);
				t.setGold_Rev(Mean.get(RIC).getGold_Rev()/count);
				
				t.setSil_Perc(Mean.get(RIC).getSil_Perc()/count);
				t.setSil_Mov(Mean.get(RIC).getSil_Mov()/count);
				t.setSil_Mar(Mean.get(RIC).getSil_Mar()/count);
				t.setSil_Rev(Mean.get(RIC).getSil_Rev()/count);
				
				t.setReg_Perc(Mean.get(RIC).getReg_Perc()/count);
				t.setReg_Mov(Mean.get(RIC).getReg_Mov()/count);
				t.setReg_Mar(Mean.get(RIC).getReg_Mar()/count);
				t.setReg_Rev(Mean.get(RIC).getReg_Rev()/count);
				
				t.setSale_Perc(Mean.get(RIC).getSale_Perc()/count);
				t.setNo_Card_Perc(Mean.get(RIC).getNo_Card_Perc()/count);
				
			}
		return TlogData;
	
	}
	
	public HashMap<Integer, HashMap<LocalDate, Float>> readPredictionsFile(String rootPath, String ProdId , String LocId, LocalDate StartWeekDate, LocalDate EndWeekDate, Map<Long, ItemInfoDTO> ligInfo, Map<Integer, ItemInfoDTO> iteminfo) throws Exception {
		
		String MyFile="";
		File myDirectory = new File(rootPath);
		String[] fileList = myDirectory.list();
		String M = "score_panel_"+ProdId+"_"+LocId+".CSV";
		for (String fileName : fileList) {
			//logger.info(fileName);
			if(fileName.equals(M)) {
				logger.info(fileName);
				MyFile=String.valueOf(fileName);
				break;
			}
		}
		if(MyFile=="") {
			throw new Exception("Prediction File Not Found");
		}
		
		HashMap<Integer,HashMap<LocalDate,Float>> NewPred= new HashMap();
		 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		CsvReader csvReader = readFilecheck(myDirectory+"\\"+MyFile, ',');
		String line[];
		int counter = 0;
		int predIndex = -1;
		int RICIndex = -1;
		int WSDIndex = -1;
		while ( csvReader.readRecord()) {
			if (counter != 0) {

				line = csvReader.getValues();
				try {
						
						
					
					if (line[0] != "") {
						LocalDate PredDate = LocalDate.parse(line[WSDIndex],dtf);
					
						if(PredDate.compareTo(StartWeekDate)>=0 && PredDate.compareTo(EndWeekDate)<=0)
						{
							int RIC = Integer.parseInt(line[RICIndex]);
							float Pred = Float.parseFloat(line[predIndex]);
							if(NewPred.containsKey(RIC)) {
								if(NewPred.get(RIC).containsKey(PredDate)) 
								{
									NewPred.get(RIC).replace(PredDate, NewPred.get(RIC).get(PredDate)+Pred);
								}
								else {
									NewPred.get(RIC).put(PredDate, Pred);
									}
							}else {
								HashMap<LocalDate, Float> m = new HashMap();
								m.put(PredDate, Pred);
								NewPred.put(RIC, m);
							}
						}
						

					}
				}
				catch (Exception ex) {
					logger.info("Ignored record" + line[0] + "as its not a valid number");
					continue;
				}
			}else {
				line=csvReader.getValues();
				for (int i=0;i<line.length;i++) {
				    if (line[i].contentEquals("pred")) {
				        predIndex = i;
				        if(predIndex!=-1&&RICIndex!=-1&&WSDIndex!=-1)
				        {
				        	break;
				        }
				    }else if(line[i].contentEquals("PRESTO_ITEM_CODE")) {
				        RICIndex = i;
				        if(predIndex!=-1&&RICIndex!=-1&&WSDIndex!=-1) 
				        {
				        	break;
				        	}
				        }else if(line[i].contentEquals("WEEK_START_DATE")) {
				        	  WSDIndex = i;
						        
				        	  if(predIndex!=-1&&RICIndex!=-1&&WSDIndex!=-1) {
						        	break;}
				        }
				}
						
			}
			
		
			counter++;
		}
		logger.info("RECOM COUNT: "+NewPred.size());
		
	//	RollUp Logic
		HashMap<Integer,HashMap<LocalDate,Float>> RollupAtLig = new HashMap();
		
		NewPred.forEach((k,v)->{
			if(iteminfo.containsKey(k)) {
			long RLI = iteminfo.get(k).getRetLIRId();
			if(RLI!=0&&ligInfo.containsKey(RLI)) {
			v.forEach((w,p)->{
				int ProdIdLir = ligInfo.get(RLI).getItemCode();
				if(RollupAtLig.containsKey(ProdIdLir)) {
					
					if(RollupAtLig.get(ProdIdLir).containsKey(w)) {			
					RollupAtLig.get(ProdIdLir).replace(w,RollupAtLig.get(ProdIdLir).get(w)+p);
					}else {
						RollupAtLig.get(ProdIdLir).put(w,p);
					}
				}else {
					HashMap<LocalDate,Float> m = new HashMap();
					m.put(w, p);
					RollupAtLig.put(ProdIdLir,m);
				}
			});	
			}
			}
		});
		logger.info("RECOM COUNT: "+RollupAtLig.size());

	NewPred.putAll(RollupAtLig);
	logger.info("RECOM COUNT: "+NewPred.size());

		return NewPred;
	}

	private CsvReader readFilecheck(String fileName, char delimiter) throws Exception {

		CsvReader reader = null;
		try {
			reader = new CsvReader(new FileReader(fileName));
			if (delimiter != '0') {
				reader.setDelimiter(delimiter);
			}
		} catch (Exception e) {
			throw new Exception("File read error ", e);
		}
		return reader;

	}


	public List<TlogDataDTO> AddNewPredData(HashMap<Integer, HashMap<LocalDate, Float>> newRecom, List<TlogDataDTO> TlogData) throws Exception {
		if(newRecom!=null) {
			TlogData.forEach(t->{
				int RIC = t.getProductId();
			//	int RIC = Integer.parseInt(t.getRetailerItemCode());
				LocalDate WSD = t.getWeekStartDate();
				if(newRecom.containsKey(RIC)&&newRecom.get(RIC).containsKey(WSD)) {
				t.setPrediction(newRecom.get(RIC).get(WSD));
				t.setIsPredPresent(true);
				float GoldPred = (t.getGold_Perc()*t.getPrediction())/100;
				float SilPred = (t.getSil_Perc()*t.getPrediction())/100;
				float RegPred = (t.getReg_Perc()*t.getPrediction())/100;
				float OtherPred = (t.getSale_Perc()*t.getPrediction())/100;
				float NoCardPred = (t.getNo_Card_Perc()*t.getPrediction())/100;
				
				float GoldRev = (float) (GoldPred*t.getRegularPrice()*0.8);
				float SilRev = (float) (SilPred*t.getRegularPrice()*0.9);
				float RegRev = (float) (RegPred*t.getRegularPrice()*1);
				float OtherRev = (float) (OtherPred*t.getRegularPrice()*1);
				float NoCardRev = (float) (NoCardPred*t.getRegularPrice()*1);
				
				t.setPredictedSale(GoldRev+SilRev+RegRev+OtherRev+NoCardRev);
				t.setPredictedMargin(t.getPredictedSale()-t.getListCost()*t.getPrediction());
				
				}else
				{
					t.setPrediction(0);
					t.setPredictedSale(0);
					t.setPredictedMargin(0);
					t.setIsPredPresent(false);
				}
			});
				
		}else {
			throw new Exception("No New Prediction Data");
		}
		return TlogData;
	}


	public List<FinalDataDTO> CombineOldAndNewData(List<TlogDataDTO> oldTlogData, List<TlogDataDTO> newTlogData) {
	 Map<Integer, TlogDataDTO> OldItems = new HashMap();
	 Map<Integer,TlogDataDTO> NewItems = new HashMap();
	 List<FinalDataDTO> Final = new ArrayList();
	 oldTlogData.forEach(item -> {
		 if(!OldItems.containsKey(item.getProductId())) {
			 OldItems.put(item.getProductId(), item);
			 
		 }
	 });
	 newTlogData.forEach(item -> {
		 if(!NewItems.containsKey(item.getProductId())) {
			 NewItems.put(item.getProductId(), item);
		 }
	 });
	 
	OldItems.forEach((k,v)->{
		FinalDataDTO Item = new FinalDataDTO();
		Item.setOldData(v);
		Item.setOldDataPresent(true);
		if(NewItems.containsKey(k)) {
			Item.setNewData(NewItems.get(k));
			Item.setNewDataPresent(true);
			Item.setRetailerItemCode(v.getRetailerItemCode());
			Item.setProductId(v.getProductId());
			Item.setRetailerLIRName(v.getRetLIRName());
			//if(v.isListCostNull()&&v.isRegularPriceNull()) {
				//Item.setOldDataPresent(false);
			//}
			NewItems.remove(k);
		}else {
			Item.setNewDataPresent(false);
			Item.setRetailerItemCode(v.getRetailerItemCode());
			Item.setProductId(v.getProductId());
			Item.setRetailerLIRName(v.getRetLIRName());
		}
	
		Item.setProductLevelId(v.getProductLevelId());
		Item.setLigMember(v.isLigMember());
		Final.add(Item);
	});
	
	if(!NewItems.isEmpty()) {
		NewItems.forEach((k,v)->{
			FinalDataDTO Item = new FinalDataDTO();
			Item.setOldDataPresent(false);
			Item.setNewDataPresent(true);
			Item.setNewData(v);
			Item.setRetailerItemCode(v.getRetailerItemCode());
			Item.setProductId(v.getProductId());
			Item.setRetailerLIRName(v.getRetLIRName());
			Item.setProductLevelId(v.getProductLevelId());
			Item.setLigMember(v.isLigMember());
			Final.add(Item);
		});
		
	}
	 
		
		
		return Final;
	}


	public List<TlogDataDTO> SortUniqueData(List<TlogDataDTO> TlogData, String Type, boolean ConsiderWeekStartDate, boolean isPredictionAdded) {
		Iterator<TlogDataDTO> i = TlogData.iterator();
		TlogDataDTO tlog = new TlogDataDTO();
		int count= 1;
		while(i.hasNext()) {
			tlog =i.next();
			count = 0;

			for(TlogDataDTO t: TlogData) {
				if(tlog.IsEqual(t, Type, ConsiderWeekStartDate,isPredictionAdded)) {
					count=count+1;
					if(count>1) {
							i.remove();
							break;
					}
				}	
			}
			
		
		 }
		return TlogData;
	}


	public List<FinalDataDTO> CalculateDiffMetrics(List<FinalDataDTO> finalData) {
	
		finalData.forEach(Item->{
			if(Item.isNewDataPresent()&&Item.isOldDataPresent()) {
				if(Item.getOldData().getTotalMovement()!=0) {
				Item.setMovementDiffPerc(((Item.getNewData().getTotalMovement()-Item.getOldData().getTotalMovement())/Item.getOldData().getTotalMovement())*100.0);
				}else {
					Item.setMovementDiffPerc(0);
				}
				if(Item.getOldData().getTotalRevenue()!=0) {
				Item.setRevenueDiffPerc(((Item.getNewData().getTotalRevenue()-Item.getOldData().getTotalRevenue())/Item.getOldData().getTotalRevenue())*100.0);
				}else {
					Item.setRevenueDiffPerc(0);
				}
				if(Item.getOldData().getNetMargin()!=0) {
				Item.setMarginDiffPerc(((Item.getNewData().getNetMargin()-Item.getOldData().getNetMargin())/Item.getOldData().getNetMargin())*100.0);
				}else {
					Item.setMarginDiffPerc(0);
				}
				if(Item.getOldData().getLY_RegularPrice()!=0) {
				Item.setPriceDiff(((Item.getNewData().getRegularPrice()-Item.getOldData().getLY_RegularPrice())/Item.getOldData().getLY_RegularPrice())*100.0);
				}else {
					Item.setPriceDiff(0);
				}
				Item.setMovementVariance(Item.getNewData().getTotalMovement()-Item.getOldData().getTotalMovement());
				Item.setRevenueVariance(Item.getNewData().getTotalRevenue()-Item.getOldData().getTotalRevenue());
				Item.setMarginVariance(Item.getNewData().getNetMargin()-Item.getOldData().getNetMargin());
				
				if(Item.getNewData().getTotalMovement()!=0&&Item.getNewData().getPrediction()!=0) {
				Item.setPredictionAccuracyPerc(Math.abs(((Item.getNewData().getPrediction()-Item.getNewData().getTotalMovement())/Item.getNewData().getPrediction())*100.0));
				}else{Item.setPredictionAccuracyPerc(0);}
				
				if(Item.getNewData().getTotalRevenue()!=0&&Item.getNewData().getPredictedSale()!=0) {
					Item.setSalePredictionAccuracyPerc(Math.abs(((Item.getNewData().getPredictedSale()-Item.getNewData().getTotalRevenue())/Item.getNewData().getPredictedSale())*100.0));
					}else{Item.setSalePredictionAccuracyPerc(0);}
		
				if(Item.getNewData().getNetMargin()!=0&&Item.getNewData().getPredictedMargin()!=0) {
					Item.setMarginPredictionAccuracyPerc(Math.abs(((Item.getNewData().getPredictedMargin()-Item.getNewData().getNetMargin())/Item.getNewData().getPredictedMargin())*100.0));
					}else{Item.setMarginPredictionAccuracyPerc(0);}	
				
				Item.setGoldMovVariance(Item.getNewData().getGold_Mov()-Item.getOldData().getGold_Mov());
				Item.setGoldMarVariance(Item.getNewData().getGold_Mar()-Item.getOldData().getGold_Mar());
				Item.setGoldRevVariance(Item.getNewData().getGold_Rev()-Item.getOldData().getGold_Rev());

				Item.setSilMovVariance(Item.getNewData().getSil_Mov()-Item.getOldData().getSil_Mov());
				Item.setSilMarVariance(Item.getNewData().getSil_Mar()-Item.getOldData().getSil_Mar());
				Item.setSilRevVariance(Item.getNewData().getSil_Rev()-Item.getOldData().getSil_Rev());
				
				Item.setRegMovVariance(Item.getNewData().getReg_Mov()-Item.getOldData().getReg_Mov());
				Item.setRegMarVariance(Item.getNewData().getReg_Mar()-Item.getOldData().getReg_Mar());
				Item.setRegRevVariance(Item.getNewData().getReg_Rev()-Item.getOldData().getReg_Rev());
			}
		});
		
		return finalData;
		
	}


	public List<FinalDataDTO> MarkAnomalousItems(List<FinalDataDTO> finalData) {
		finalData.forEach(item->{
			if(item.isNewDataPresent()&&item.isOldDataPresent()) {
				
				if((item.getOldData().getLY_RegularPrice()==0)||(item.getOldData().isTotalMovementNull())||(item.getOldData().isTotalRevenueNull())||(item.getNewData().isTotalRevenueNull())||(item.getNewData().isTotalMovementNull())||(item.getNewData().getTotalMovement()<0)||(item.getNewData().getTotalRevenue()<0)||(item.getNewData().isTotalMovementNull())||(item.getOldData().getTotalMovement()<0)||(item.getOldData().getTotalRevenue()<0)) {
					item.setToConsiderRecord(false);
				}else {
					if(Math.abs(item.getMovementDiffPerc())>30) {
						item.setToConsiderRecord(false);
					}else {
						item.setToConsiderRecord(true);
					}

					
					/*
					if(Math.abs(item.getMovementVariance())>=10) {
						double LYMovement=item.getOldData().getTotalMovement();
						double TotMovement = item.getNewData().getTotalMovement();
						double minMovement = Math.min(LYMovement, TotMovement);
						double maxMovement = Math.max(TotMovement, LYMovement);
						double perc = (Math.abs(item.getMovementVariance())/minMovement)*100;
						if(minMovement<100 && perc > 20 && maxMovement>=2*minMovement) {
							item.setToConsiderRecord(false);
						}else {
							item.setToConsiderRecord(true);
						}
					}else {
						item.setToConsiderRecord(true);
					}*/
				}
				
			}else {
				item.setToConsiderRecord(false);
			}
		});
		
		finalData.forEach(f->{
			if(f.isOldDataPresent()&&f.isNewDataPresent()) {
				if(f.getPriceDiff()>0) {
					f.setPriceChangeIndicator(2);
				}else if(f.getPriceDiff()<0) {
					f.setPriceChangeIndicator(3);
				}else {
					f.setPriceChangeIndicator(1);
				}
			}
		});
		return finalData;
		
	}
	
	public void writeToCSV(List<FinalDataDTO> FinalData , String ProdId, String LocId) throws IOException, GeneralException {
		String separtor = ",";
		logger.info("WriteToCsvFile:Writing started");
		System.out.println("WriteToCSVFile:Writing Started");
		
		FileWriter fw ;
		PrintWriter pw;
		String csvOutputPath = PropertyManager.getProperty("NEW_PREDICTION.OUTPUTPATH", "")+ "\\" + "PriceChangePerformanceReport"+ProdId+"_"+LocId+".csv";

		 fw = new FileWriter(csvOutputPath);
		 pw = new PrintWriter(fw);

		 writeHeader(pw);
		 
		 for(FinalDataDTO I:FinalData){
			 boolean NDP =I.isNewDataPresent();
			 boolean ODP =I.isOldDataPresent();
			 //is Consider
			 pw.print(I.isToConsiderRecord());
			 pw.print(separtor);
			 
			 //OLD DATA
			 pw.print(ODP);
			 pw.print(separtor);
			 
			 //New Data
			 pw.print(NDP);
			 pw.print(separtor);
			 
			 //RIC
			 pw.print(I.getRetailerItemCode());
			 pw.print(separtor);
			 //RETLIRNAME
			 pw.print(I.getRetailerLIRName());
			 pw.print(separtor);
			 //LY REG PRICE
			 if(ODP) {
				 pw.print(I.getOldData().getLY_RegularPrice());
			 }
			 else {
				 pw.print("NA");
			 }
			 pw.print(separtor);
			 
			 //NEW REG PRICE
			 if(NDP) {
			 pw.print(I.getNewData().getRegularPrice());
			 }
			 else {
				 pw.print("NA");
			 }
			 pw.print(separtor);
			 
			 //%Diff Price
			 pw.print(I.getPriceDiff());
			 pw.print(separtor);
			 
			 //LY LIST COST
			 if(ODP) {
			 pw.print(I.getOldData().getLY_ListCost());
			 }
			 else
			 {
				 pw.print("NA");
			 }
				 pw.print(separtor);
				 
			//New List Cost	 
			 if(NDP) {
			 pw.print(I.getNewData().getListCost());
			 }
			 else {
				 pw.print("NA");
			 }
			 pw.print(separtor);
			 
			 //LY WEEK AVERAGE REG Movement
			 if(ODP&&!I.getOldData().isTotalMovementNull())
			 {
				 pw.print(I.getOldData().getTotalMovement());
			 }
			else {
			 pw.print("NA");
			}
			 pw.print(separtor);
			 
			 //New Week AVerage Reg Movement
			 if(NDP&&!I.getNewData().isTotalMovementNull()) {
			 pw.print(I.getNewData().getTotalMovement());}
			 else {
				 pw.print("NA");}
			 pw.print(separtor);
			 
			 pw.print(I.getMovementVariance());
			 pw.print(separtor);
			 pw.print(I.getMovementDiffPerc());
			 pw.print(separtor);
			 
			 if(ODP&&!I.getOldData().isTotalRevenueNull()) {
			 pw.print(I.getOldData().getTotalRevenue());}
			 else {
				 pw.print("NA");}
			 pw.print(separtor);
			 if(NDP&&!I.getNewData().isTotalRevenueNull()) {
			 pw.print(I.getNewData().getTotalRevenue());}
			 else {
				 pw.print("NA");}
			 pw.print(separtor);
			 
			 pw.print(I.getRevenueVariance());
			 pw.print(separtor);
			 pw.print(I.getRevenueDiffPerc());
			 pw.print(separtor);
			 
			 if(ODP) {
			 pw.print(I.getOldData().getNetMargin());}
			 else {
				 pw.print("NA");}
			 pw.print(separtor);
			 if(NDP) {
			 pw.print(I.getNewData().getNetMargin());}
			 else {
				 pw.print("NA");}
			 pw.print(separtor);
			 pw.print(I.getMarginVariance());
			 pw.print(separtor);
			 pw.print(I.getMarginDiffPerc());
			 pw.print(separtor);
			 
			 if(ODP) {
			 pw.print(I.getOldData().getGold_Perc());}
			 else {
				 pw.print("NA");}
			 pw.print(separtor);
			 
			 if(NDP) {
			 pw.print(I.getNewData().getGold_Perc());
			 }else
				 {pw.print("NA");}
			 pw.print(separtor);
			 
			 pw.print(I.getGoldMovVariance());
			 pw.print(separtor);
			 pw.print(I.getGoldRevVariance());
			 pw.print(separtor);
			 pw.print(I.getGoldMarVariance());
			 pw.print(separtor);
			 
			 
			 if(ODP) {
			 pw.print(I.getOldData().getReg_Perc());}
			 else {
				 pw.print("NA");
			 }
			 pw.print(separtor);
			 
			 if(NDP) {
			 pw.print(I.getNewData().getReg_Perc());
			 }else
				 {pw.print("NA");}
			 pw.print(separtor);
			 pw.print(I.getRegMovVariance());
			 pw.print(separtor);
			 pw.print(I.getRegRevVariance());
			 pw.print(separtor);
			 pw.print(I.getRegMarVariance());
			 pw.print(separtor);
			 
			 
			 if(ODP) {
			 pw.print(I.getOldData().getSil_Perc());
			 }else
				 {pw.print("NA");}
			 pw.print(separtor);
			 if(NDP) {
			 pw.print(I.getNewData().getSil_Perc());
			 }else {
				 pw.print("NA");}
			 pw.print(separtor);
			 pw.print(I.getSilMovVariance());
			 pw.print(separtor);
			 pw.print(I.getSilRevVariance());
			 pw.print(separtor);
			 pw.print(I.getSilMarVariance());
			 pw.print(separtor);
			 
			 if(ODP) {
			 pw.print(I.getOldData().getSale_Perc());}
			 else {
				 pw.print("NA");}
			 pw.print(separtor);
			 if(NDP) {
			 pw.print(I.getNewData().getSale_Perc());}
			 else {
				 pw.print("NA");
			 }
			 pw.print(separtor);
			 
			 
			 if(ODP) {
				 pw.print(I.getOldData().getNo_Card_Perc());}
				 else {
					 pw.print("NA");}
				 pw.print(separtor);
				 if(NDP) {
				 pw.print(I.getNewData().getNo_Card_Perc());}
				 else {
					 pw.print("NA");
				 }
				 pw.print(separtor);
			 
			 
			 if(NDP) {
			 pw.print(I.getNewData().getPrediction());
			 }else
			 { pw.print("NA");}
			 pw.print(separtor);
			 
			
			 pw.print(I.getPredictionAccuracyPerc());
			 pw.print(separtor);
			
			 
			 if(NDP) {
				 pw.print(I.getNewData().getPredictedSale());
				 }else
				 { pw.print("NA");}
				 pw.print(separtor);
				 
				
				 pw.print(I.getSalePredictionAccuracyPerc());
				 pw.print(separtor);
			 
				 if(NDP) {
					 pw.print(I.getNewData().getPredictedMargin());
					 }else
					 { pw.print("NA");}
					 pw.print(separtor);
					 
					
					 pw.print(I.getMarginPredictionAccuracyPerc());
					 	 
				 
			 pw.println();
			
			 
		 }

		
		 pw.flush();
		 fw.flush();
		 logger.info("WriteToCsvFile:Writing Ended");
			System.out.println("WriteToCSVFile:Writing Ended");
			
		 
		}


	private void writeHeader(PrintWriter pw) {
	
		String separtor = ",";
		pw.print("To Consider");
		pw.print(separtor);
		pw.print("LY DATA");
		pw.print(separtor);
		pw.print("NEW DATA");
		pw.print(separtor);
		 pw.print("RETAILER_ITEM_CODE");
		 pw.print(separtor);
		 pw.print("ITEM");
		 pw.print(separtor);
		 pw.print("LY_REGULAR_PRICE");
		 pw.print(separtor);
		 pw.print("CURR_REGULAR_PRICE");
		 pw.print(separtor);
		 pw.print("%_PRICE_DIFFERENCE");
		 pw.print(separtor);
		 pw.print("LY_LIST_COST");
		 pw.print(separtor);
		 pw.print("CURR_WEEK_LIST_COST");
		 pw.print(separtor);
		 pw.print("LY_WEEK_AVERAGE_REGULAR_MOVEMENT");
		 pw.print(separtor);
		 pw.print("NEW_WEEK_REGULAR_MOMENT");
		 pw.print(separtor);
		 pw.print("MOVEMENT_VARIANCE");
		 pw.print(separtor);
		 pw.print("% MOVEMENT_VARIANCE");
		 pw.print(separtor);
		 pw.print("LY_WEEK_AVERAGE_REGULAR_REVENUE");
		 pw.print(separtor);
		 pw.print("NEW_WEEK_REVENUE");
		 pw.print(separtor);
		 pw.print("REVENUE_VARIANCE");
		 pw.print(separtor);
		 pw.print("% REVENUE_VARIANCE");
		 pw.print(separtor);
		 pw.print("LY_WEEK_AVERAGE_REGULAR_MARGIN");
		 pw.print(separtor);
		 pw.print("NEW_WEEK_MARGIN");
		 pw.print(separtor);
		 pw.print("MARGIN_VARIANCE");
		 pw.print(separtor);
		 pw.print("% MARGIN_VARIANCE");
		 pw.print(separtor);
		 pw.print("LY_WEEK_AVERAGE_GOLD_%");
		 pw.print(separtor);
		 pw.print("NEW_WEEK_AVERAGE_GOLD_%");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_GOLD_MOV_VAR");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_GOLD_REV_VAR");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_GOLD_MAR_VAR");
		 pw.print(separtor);
		 
		 pw.print("LY_WEEK_AVERAGE_REGULAR_%");
		 pw.print(separtor);
		 pw.print("NEW_WEEK_AVERAGE_REGULAR_%");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_REG_MOV_VAR");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_REG_REV_VAR");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_REG_MAR_VAR");
		 pw.print(separtor);
		 
		 pw.print("LY_WEEK_AVERAGE_SILVER_%");
		 pw.print(separtor);
		 pw.print("NEW_WEEK_AVERAGE_SILVER_%");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_SIL_MOV_VAR");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_SIL_REV_VAR");
		 pw.print(separtor);
		 pw.print("WEEK_AVERAGE_SIL_MAR_VAR%");
		 pw.print(separtor);
		 
		 pw.print("LY_WEEK_AVERAGE_OTHER_TRANS_%");
		 pw.print(separtor);
		 pw.print("NEW_WEEK_AVERAGE_OTHER_TRANS_%");
		 pw.print(separtor);
		 
		 pw.print("LY_WEEK_AVERAGE_NO_CARD_%");
		 pw.print(separtor);
		 pw.print("NEW_WEEK_AVERAGE_NO_CARD_%");
		 pw.print(separtor);
		 
		 pw.print("VERSION_5.1_PREDICTIONS");
		 pw.print(separtor);
		 pw.print("VERSION_5.1_ACCURACY_%");
		 pw.print(separtor);

		 pw.print("VERSION_5.1_PREDICTIONS_SALES");
		 pw.print(separtor);
		 pw.print("VERSION_5.1_SALES_ACCURACY_%");
		 pw.print(separtor);

		 pw.print("VERSION_5.1_PREDICTIONS_MARGIN");
		 pw.print(separtor);
		 pw.print("VERSION_5.1_MARGIN_ACCURACY_%");
		 
		 
		 pw.println();
		 
	}


	public List<TlogDataDTO> FilterTlogOnNegativeUnitPrice(List<TlogDataDTO> TlogData) {
		Iterator<TlogDataDTO> i = TlogData.iterator();
		TlogDataDTO tlog = new TlogDataDTO();
		
		
		while(i.hasNext()) {
			tlog =i.next();
			if(!(tlog.getUnitPrice()>0)) {
				i.remove();
			}
		 }
		 
		return TlogData;
	}


	public void writeTlogToCSV(String string, List<TlogDataDTO> TlogData,String prodId,String locId) {
		String separtor = ",";
		logger.info("WriteToCsvFile:Writing started");
		System.out.println("WriteToCSVFile:Writing Started");
		
		FileWriter fw ;
		PrintWriter pw;
		String csvOutputPath = PropertyManager.getProperty("NEW_PREDICTION.OUTPUTPATH", "")+ "\\"+string +"_"+prodId+"_"+locId+".csv";
try {
		 fw = new FileWriter(csvOutputPath);
		 pw = new PrintWriter(fw);
		 
		 pw.print("RETAILER_ITEM_CODE");
		 pw.print(separtor);
		 
		 pw.print("WEEK_START_DATE");
		 pw.print(separtor);
		 
		 pw.print("RETAILER_LIR_NAME");
		 pw.print(separtor);
		 
		 pw.print("REG_PRICE");
		 pw.print(separtor);
		 
		 pw.print("LIST_COST");
		 pw.print(separtor);
		 
		 pw.print("TOTAL_MOVEMENT");
		 pw.print(separtor);
		 
		 pw.print("TOTAL_REVENUE");
		 pw.print(separtor);
		 
		 pw.print("NET_MARGIN");
		 pw.print(separtor);
		 
		 pw.print("GOLD_PERC");
		 pw.print(separtor);
		 
		 pw.print("GOLD_MOVEMENT");
		 pw.print(separtor);
		 
		 pw.print("GOLD_REVENUE");
		 pw.print(separtor);
		 
		 pw.print("GOLD_MARGIN");
		 pw.print(separtor);
		 
		 pw.print("REGULAR_PERC");
		 pw.print(separtor);
		 
		 pw.print("REG_MOVEMENT");
		 pw.print(separtor);
		 
		 pw.print("REG_REVENUE");
		 pw.print(separtor);
		 
		 pw.print("REG_MARGIN");
		 pw.print(separtor);
		 
		 pw.print("SILVER_PERC");
		 pw.print(separtor);
		 
		 pw.print("SIL_MOVEMENT");
		 pw.print(separtor);
		 
		 pw.print("SIL_REVENUE");
		 pw.print(separtor);
		 
		 pw.print("SIL_MARGIN");
		 pw.print(separtor);
		 
		 pw.print("OTHER_PERC");
		 pw.print(separtor);
		 
		 pw.print("NO_CARD_PERC");
		 pw.print(separtor);
		 
		 
		 if(string=="NEW") {
		 pw.print("PREDICTION");
		 pw.print(separtor);
		 pw.print("SALES PREDICTION");
		 pw.print(separtor);
		 pw.print("SALES PREDICTION"); 
		 
		 pw.println();
		 
		 }
		 else {
			 pw.println();
		 }

		 TlogData.forEach(t->{
			 pw.print(t.getRetailerItemCode());
			 pw.print(separtor);
			 
			 pw.print(t.getWeekStartDate());
			 pw.print(separtor);
			 
			 pw.print(t.getRetLIRName());
			 pw.print(separtor);
			 if(string=="NEW") {
			 pw.print(t.getRegularPrice());
			 pw.print(separtor);
			 
			 pw.print(t.getListCost());
			 pw.print(separtor);
			 
			 }else {
				 pw.print(t.getLY_RegularPrice());
				 pw.print(separtor);
				 
				 pw.print(t.getLY_ListCost());
				 pw.print(separtor);
			 }
			 
			 pw.print(t.getTotalMovement());
			 pw.print(separtor);
			 
			 pw.print(t.getTotalRevenue());
			 pw.print(separtor);
			 
			 pw.print(t.getNetMargin());
			 pw.print(separtor);
			 
			 pw.print(t.getGold_Perc());
			 pw.print(separtor);
			 
			 pw.print(t.getGold_Mov());
			 pw.print(separtor);
			 
			 pw.print(t.getGold_Rev());
			 pw.print(separtor);
			 
			 pw.print(t.getGold_Mar());
			 pw.print(separtor);	 
			 
			 pw.print(t.getReg_Perc());
			 pw.print(separtor);
			 
			 pw.print(t.getReg_Mov());
			 pw.print(separtor);
			 
			 pw.print(t.getReg_Rev());
			 pw.print(separtor);
			 
			 pw.print(t.getReg_Mar());
			 pw.print(separtor);	 
			 
			 pw.print(t.getSil_Perc());
			 pw.print(separtor);
			 
			 pw.print(t.getSil_Mov());
			 pw.print(separtor);
			 
			 pw.print(t.getSil_Rev());
			 pw.print(separtor);
			 
			 pw.print(t.getSil_Mar());
			 pw.print(separtor);	 
			 
			 pw.print(t.getSale_Perc());
			 pw.print(separtor);
			 
			 pw.print(t.getNo_Card_Perc());
			 pw.print(separtor);
			 
			 if(string=="NEW") {
			 pw.print(t.getPrediction());
			 pw.print(separtor);
			 pw.print(t.getPredictedSale());
			 pw.print(separtor);
			 pw.print(t.getPredictedMargin());
			 pw.print(separtor);

			 
			 }
			 pw.println();
		 });
		 
		 pw.flush();
		 fw.flush();
		 logger.info("WriteTlogToCsvFile:Writing Ended");
			System.out.println("WriteTlogToCSVFile:Writing Ended");
		 

}catch(Exception Ex) {
logger.info("Failed to output CSV before mean");	
}
}


	public List<TlogDataDTO> mergeTlogAndIMS(Map<Integer, Map<LocalDate, IMSDataDTO>> IMSData,
			List<TlogDataDTO> TlogData) {
		
		logger.info("Merging TLOG and IMS Data started");
		Iterator<TlogDataDTO> i = TlogData.iterator();
		TlogDataDTO tlog = new TlogDataDTO();
		while(i.hasNext()) {
			tlog =i.next();
			
			int Id = tlog.getProductId();
			LocalDate Dt = tlog.getWeekStartDate();
			
			if(IMSData.containsKey(Id)) {
				if(IMSData.get(Id).containsKey(Dt)) {
				//int ind = IndexIMS.get(Id).get(Dt);
				IMSDataDTO IDT = IMSData.get(Id).get(Dt);
				tlog.setRegularPrice(IDT.getRegularPrice());
				tlog.setRegular_M_Price(IDT.getRegular_M_Price());
				tlog.setRegular_M_Pack(IDT.getRegular_M_Pack());
				//tlog.setFinalprice(IDT.getFinalprice());
				tlog.setListCost(IDT.getListCost());
				tlog.setTotalRevenue(IDT.getTotalRevenue());
				tlog.setTotalMovement(IDT.getTotalMovement());
				tlog.setNetMargin(IDT.getNetMargin());
				tlog.setTotalMovementNull(IDT.isTotalMovementNull());
				tlog.setTotalRevenueNull(IDT.isTotalRevenueNull());
				//tlog.setCalendarId(IDT.getCalendarId());
				//tlog.setSalePrice(IDT.getSalePrice());
				//tlog.setSale_M_Price(IDT.getSale_M_Price());
				//tlog.setSale_M_Pack(IDT.getSale_M_Pack());
				//tlog.setWeekEndDate(IDT.getWeekEndDate());
				
				
				}else {
					i.remove();
				}
				
			}else {
			i.remove();
			}
		}
		logger.info("Merging TLOG and IMS Data Ended");
		return TlogData;
	}


	public List<TlogDataDTO> CopyListToTrends(String Func_Type, List<TlogDataDTO> TlogData) {
		List<TlogDataDTO> Result = new ArrayList();
		
		TlogData.forEach(t->{
			TlogDataDTO tlog = new TlogDataDTO();
			//'RETAILER_ITEM_CODE','RET_LIR_NAME','TOT_MOVEMENT','TOT_REVENUE','NET_MARGIN','FEB_10_LIST_COST','FEB_10_REG_PRICE',
            //'GOLDPER','REGPER','SILPER','SALEPER','WEEK_START_DATE'
			tlog.setRetailerItemCode(t.getRetailerItemCode());
			tlog.setProductId(t.getProductId());
			tlog.setLigMember(t.isLigMember());
			tlog.setProductLevelId(t.getProductLevelId());
			tlog.setRetLIRName(String.valueOf(t.getRetLIRName()));
			tlog.setTotalMovement(t.getTotalMovement());
			tlog.setTotalRevenue(t.getTotalRevenue());
			tlog.setNetMargin(t.getNetMargin());
			tlog.setWeekStartDate(t.getWeekStartDate());
			tlog.setWeekCalendarId(t.getWeekCalendarId());
			tlog.setGold_Perc(t.getGold_Perc());
			tlog.setSil_Perc(t.getSil_Perc());
			tlog.setReg_Perc(t.getReg_Perc());
			
			tlog.setReg_Mov(t.getReg_Mov());
			tlog.setReg_Mar(t.getReg_Mar());
			tlog.setReg_Rev(t.getReg_Rev());
			
			tlog.setSil_Mov(t.getSil_Mov());
			tlog.setSil_Mar(t.getSil_Mar());
			tlog.setSil_Rev(t.getSil_Rev());
			
			tlog.setGold_Mov(t.getGold_Mov());
			tlog.setGold_Mar(t.getGold_Mar());
			tlog.setGold_Rev(t.getGold_Rev());
			
			tlog.setSale_Perc(t.getSale_Perc());
			tlog.setNo_Card_Perc(t.getNo_Card_Perc());
			
			if(Func_Type=="OLD") {
				tlog.setLY_ListCost(t.getLY_ListCost());
				tlog.setLY_RegularPrice(t.getLY_RegularPrice());
			}else if(Func_Type=="NEW"){
				tlog.setRegularPrice(t.getRegularPrice());
				tlog.setListCost(t.getListCost());
				tlog.setPrediction(t.getPrediction());
				tlog.setPredictedSale(t.getPredictedSale());
				tlog.setPredictedMargin(t.getPredictedMargin());
			}
		Result.add(tlog);	
		});
		return Result;
	}


	public Map<Integer, WeekGraphDataDTO> getGraphdata(int i, List<FinalDataDTO> finalData, List<TlogDataDTO> trendsNewData,
			List<TlogDataDTO> trendsOldData, HashMap<Integer, HashMap<LocalDate, Float>> newRecom, Connection conn) {
		 Map<Integer, WeekGraphDataDTO> result = new HashMap();
		 Map<Integer,FinalDataDTO> RICList = new HashMap();
		 Map<LocalDate, List<Integer>> WRICPairs= new HashMap();
		 Map<LocalDate, List<Integer>> FinalWRICPairs= new HashMap();
		 Map<LocalDate,Integer> TYWeeksCalId = new HashMap();
		 if(i==1) {
			 finalData.forEach(f->{if(f.isToConsiderRecord()&&!f.isLigMember()) {
				 if(f.getPriceChangeIndicator()==2||f.getPriceChangeIndicator()==3) {
					 RICList.put(f.getProductId(),f);
				 }}
			 });
		 }else if(i==2||i==3) {
			 finalData.forEach(f->{if(f.isToConsiderRecord()&&!f.isLigMember()) {
				 if(f.getPriceChangeIndicator()==i) {
					 RICList.put(f.getProductId(),f);
				 }}
			 });
		 }
		 
		 trendsNewData.forEach(t->{
			 if(RICList.containsKey(t.getProductId())) {
				 if(WRICPairs.containsKey(t.getWeekStartDate())) {
					 if(!WRICPairs.get(t.getWeekStartDate()).contains(t.getProductId())) {
					 WRICPairs.get(t.getWeekStartDate()).add(t.getProductId());
					 }
				 }else {
					 List<Integer> l = new ArrayList();
					 l.add(t.getProductId());
					 WRICPairs.put(t.getWeekStartDate(), l);
					 TYWeeksCalId.put(t.getWeekStartDate(),t.getWeekCalendarId());
				 }
			 }
		 });
		 
		 trendsOldData.forEach(t->{
			 LocalDate d= t.getWeekStartDate().plusDays(364);
			 if(WRICPairs.containsKey(d)){
				 if(WRICPairs.get(d).contains(t.getProductId())) {
					 if(FinalWRICPairs.containsKey(d)) {
						 if(!FinalWRICPairs.get(d).contains(t.getProductId())) {
						 FinalWRICPairs.get(d).add(t.getProductId());
						 }
					 }else {
						 List<Integer> l = new ArrayList();
						 l.add(t.getProductId());
						 FinalWRICPairs.put(d, l);
					 }
				 }
			
			 }
		 });
		 
		 trendsNewData.forEach(tnew->{
			// List<Integer> AddedItemCodes = new ArrayList();
			 if((FinalWRICPairs.containsKey(tnew.getWeekStartDate()))&&(FinalWRICPairs.get(tnew.getWeekStartDate()).contains(tnew.getProductId()))){
			 if(RICList.containsKey(tnew.getProductId())) {
				 int WCId = tnew.getWeekCalendarId();
				 if(result.containsKey(WCId)) {
					 result.get(WCId).setNewMovement(result.get(WCId).getNewMovement()+tnew.getTotalMovement());
					 result.get(WCId).setNewRevenue(result.get(WCId).getNewRevenue()+tnew.getTotalRevenue());
					 result.get(WCId).setNewMargin(result.get(WCId).getNewMargin()+tnew.getNetMargin());
					// result.get(WCId).setPrediction_Movement(result.get(WCId).getPrediction_Movement()+tnew.getPrediction());
					// int RIC=tnew.getProductId();
					 //double RegPrice = tnew.getRegularPrice();
					 //double GoldPrice = RegPrice*0.80;
					 //double SilPrice = RegPrice*0.90;
					 //double pred = tnew.getPrediction();
					 //double GoldMov = (pred*tnew.getGold_Perc())/100;
					 //double SilverMov = (pred*tnew.getSil_Perc())/100;
					 //double RegularMov = (pred*tnew.getReg_Perc())/100;
					 
					 //double PredTotalSales = (GoldMov*GoldPrice)+(SilPrice*SilverMov)+(RegPrice*RegularMov);
					 //double PredTotalMargin = PredTotalSales -(GoldMov+SilverMov+RegularMov)*tnew.getListCost();
					 					 
					 result.get(WCId).setPrediction_Movement(result.get(WCId).getPrediction_Movement()+tnew.getPrediction());
					 result.get(WCId).setPrediction_Sales(result.get(WCId).getPrediction_Sales()+tnew.getPredictedSale());
					 result.get(WCId).setPrediction_Margin(result.get(WCId).getPrediction_Margin()+tnew.getPredictedMargin());
					 
					 if(!(result.get(WCId).getRetailerItemCodes().contains(tnew.getProductId()))) {
						 result.get(WCId).getRetailerItemCodes().add(tnew.getProductId());
					 }
					 }else {
						 WeekGraphDataDTO t = new WeekGraphDataDTO();
						 t.setIsFutureWeek(0);
					 t.setNewMovement(tnew.getTotalMovement());
					 t.setNewRevenue(tnew.getTotalRevenue());
					 t.setNewMargin(tnew.getNetMargin());
					 t.setOldMargin(0);
					 t.setOldMovement(0);
					 t.setOldRevenue(0);
					 int RIC=tnew.getProductId();
					 // double RegPrice = tnew.getRegularPrice();
					 // double GoldPrice = RegPrice*0.80;
					 // double SilPrice = RegPrice*0.90;
					 //double pred = tnew.getPrediction();
					 // double GoldMov = (pred*tnew.getGold_Perc())/100;
					 //double SilverMov = (pred*tnew.getSil_Perc())/100;
					 //double RegularMov = (pred*tnew.getReg_Perc())/100;
					 
					 // double PredTotalSales = (GoldMov*GoldPrice)+(SilPrice*SilverMov)+(RegPrice*RegularMov);
					 // double PredTotalMargin = PredTotalSales -(GoldMov+SilverMov+RegularMov)*tnew.getListCost();
					 					 
					 t.setPrediction_Movement(tnew.getPrediction());
					 t.setPrediction_Sales(tnew.getPredictedSale());
					 t.setPrediction_Margin(tnew.getPredictedMargin());
					 
					 List<Integer> Code = new ArrayList();
					 Code.add(tnew.getProductId());
					 t.setRetailerItemCodes(Code);
					 t.setWeekStartDate(tnew.getWeekStartDate());
					 t.setWeekCalendarID(tnew.getWeekCalendarId());
					 result.put(WCId, t);
					 
				 }
				 
			 }
			 }
		 });
		 /*
		 result.forEach((k,v)->{
			 v.getRetailerItemCodes().forEach(I->{
				 	 if(RICList.containsKey(I)) {
				 result.get(k).setOldMovement(result.get(k).getOldMovement()+RICList.get(I).getOldData().getTotalMovement());
				 result.get(k).setOldRevenue(result.get(k).getOldRevenue()+RICList.get(I).getOldData().getTotalRevenue());
				 result.get(k).setOldMargin(result.get(k).getOldMargin()+RICList.get(I).getOldData().getNetMargin());
					 }
				 
				 });
		 });
		*/ 
		 trendsOldData.forEach(tOld->{
			// List<Integer> AddedItemCodes = new ArrayList();
			 if(RICList.containsKey(tOld.getProductId())) {
				 LocalDate d = tOld.getWeekStartDate().plusDays(364);
				 if((FinalWRICPairs.containsKey(d))&&(FinalWRICPairs.get(d).contains(tOld.getProductId()))){
				 int WCId = TYWeeksCalId.get(d);
				 if(result.containsKey(WCId)) {
					 if(result.get(WCId).getRetailerItemCodes().contains(tOld.getProductId())) {
					 result.get(WCId).setOldMovement(result.get(WCId).getOldMovement()+tOld.getTotalMovement());
					 result.get(WCId).setOldRevenue(result.get(WCId).getOldRevenue()+tOld.getTotalRevenue());
					 result.get(WCId).setOldMargin(result.get(WCId).getOldMargin()+tOld.getNetMargin());
					 }
				}
				 
			 }
				 }
		 });
		// Map<LocalDate,Float> Dates = new HashMap();
		 if(result.size()!=0) {
		 List<LocalDate> d = new ArrayList();
		 int max[]= {0};
		 result.keySet().forEach(k->{if(max[0]<=k){
			 max[0]=k;
		 }});
		 logger.info("Max value: "+max[0]);
		 logger.info("Result count"+result.size());
		LocalDate LastDate = result.get(max[0]).getWeekStartDate();
		newRecom.forEach((k,v)->{
			
			if(RICList.containsKey(k)) {
				v.forEach((k1,v1)->{
					if(k1.compareTo(LastDate)>0) {
						if(!(d.contains(k1))){
							d.add(k1);
						}
					}
				});
			}
		});
		 CommonTaskDAO common = new CommonTaskDAO();
		 
		 if(!d.isEmpty()) {
		Map<LocalDate,Integer> CalendarIds = common.getCalendarIds(d,conn);
		newRecom.forEach((k,v)->{
			
			if(RICList.containsKey(k)) {
			v.forEach((k1,v1)->{
				
			if(CalendarIds.containsKey(k1)){
				int WCId = CalendarIds.get(k1);
				if(result.containsKey(WCId)) {
					//int RIC = id[0];
					 int RIC=k;
					 double RegPrice = RICList.get(RIC).getNewData().getRegularPrice();
					 double GoldPrice = RegPrice*0.80;
					 double SilPrice = RegPrice*0.90;
					 double pred = v1;
					 
					 double GoldMov = (pred*RICList.get(RIC).getNewData().getGold_Perc())/100;
					 double SilverMov = (pred*RICList.get(RIC).getNewData().getSil_Perc())/100;
					 double RegularMov = (pred*RICList.get(RIC).getNewData().getReg_Perc())/100;
					 double OtherMov = (pred*RICList.get(RIC).getNewData().getSale_Perc())/100;
					 double NoCardMov = (pred*RICList.get(RIC).getNewData().getNo_Card_Perc())/100;
					 
					 
					 double PredTotalSales = (GoldMov*GoldPrice)+(SilPrice*SilverMov)+(RegPrice*RegularMov)+(OtherMov*RegPrice)+(NoCardMov*RegPrice);
					 double PredTotalMargin = PredTotalSales -(GoldMov+SilverMov+RegularMov+OtherMov+NoCardMov)*RICList.get(RIC).getNewData().getListCost();
					 					 
					 result.get(WCId).setPrediction_Movement(result.get(WCId).getPrediction_Movement()+GoldMov+SilverMov+RegularMov+OtherMov+NoCardMov);
					 result.get(WCId).setPrediction_Sales(result.get(WCId).getPrediction_Sales()+PredTotalSales);
					 result.get(WCId).setPrediction_Margin(result.get(WCId).getPrediction_Margin()+PredTotalMargin);
					 
					 if(!(result.get(WCId).getRetailerItemCodes().contains(RIC))) {
						 result.get(WCId).getRetailerItemCodes().add(RIC);
					 }
					
					
				}else {
					int ID = CalendarIds.get(k1);
					 WeekGraphDataDTO t = new WeekGraphDataDTO();
					 t.setIsFutureWeek(1);
					 t.setNewMovement(0);
					 t.setNewRevenue(0);
					 t.setNewMargin(0);
					 t.setOldMargin(0);
					 t.setOldMovement(0);
					 t.setOldRevenue(0);
					 
					 
					// int RIC =id[0];
					 //////////////Uncomment below line and comment upper line for item code usage
					 
					 
					 int RIC=k;
					 double RegPrice = RICList.get(RIC).getNewData().getRegularPrice();
					 double GoldPrice = RegPrice*0.80;
					 double SilPrice = RegPrice*0.90;
					 double pred = v1;
					 double GoldMov = (pred*RICList.get(RIC).getNewData().getGold_Perc())/100;
					 double SilverMov = (pred*RICList.get(RIC).getNewData().getSil_Perc())/100;
					 double RegularMov = (pred*RICList.get(RIC).getNewData().getReg_Perc())/100;
					 double OtherMov = (pred*RICList.get(RIC).getNewData().getSale_Perc())/100;
					 double NoCardMov = (pred*RICList.get(RIC).getNewData().getNo_Card_Perc())/100;
					 
					 double PredTotalSales = (GoldMov*GoldPrice)+(SilPrice*SilverMov)+(RegPrice*RegularMov)+(OtherMov*RegPrice)+(NoCardMov*RegPrice);
					 double PredTotalMargin = PredTotalSales -(GoldMov+SilverMov+RegularMov+OtherMov+NoCardMov)*RICList.get(RIC).getNewData().getListCost();
					 				 
					 t.setPrediction_Movement(GoldMov+SilverMov+RegularMov+OtherMov+NoCardMov);
					 t.setPrediction_Sales(PredTotalSales);
					 t.setPrediction_Margin(PredTotalMargin);
					 
					 List<Integer> Code = new ArrayList();
					 Code.add(RIC);
					 t.setRetailerItemCodes(Code);
					 t.setWeekStartDate(k1);
					 t.setWeekCalendarID(ID);
					 result.put(ID, t);
				}
			
		
				
			}
		});
			}
		});
		 }
		 }
		return result;
	}


	public void writeGraphDataToCSV(int i, Map<Integer, WeekGraphDataDTO> all, String productId, String locationId) {
		if(all.size()!=0) {
		String separtor = ",";
		logger.info("WriteGraphDataToCsvFile:Writing started");
		System.out.println("WriteGraphDataToCSVFile:Writing Started");
		String string=null;
		if(i==3) {
			string = "PriceDecreasedItemsTrend_"+productId+"_"+locationId;
		}else if(i==2) {
			string = "PriceIncreasedItemsTrend_"+productId+"_"+locationId;
		}else if(i==1) {
			string = "OverallItemsTrend_"+productId+"_"+locationId;
		}
		FileWriter fw ;
		PrintWriter pw;
		String csvOutputPath = PropertyManager.getProperty("NEW_PREDICTION.OUTPUTPATH", "")+ "\\"+string +".csv";
try {
		 fw = new FileWriter(csvOutputPath);
		 pw = new PrintWriter(fw);
		 
		 pw.print("WEEK_START_DATE");
		 pw.print(separtor);
		 
		 pw.print("WEEK_CALENDAR_ID");
		 pw.print(separtor);
		 
		 pw.print("NO_OF_ITEMS");
		 pw.print(separtor);
		 
		 pw.print("NEW_MOVEMENT");
		 pw.print(separtor);
		 
		 pw.print("NEW_REVENUE");
		 pw.print(separtor);
		 
		 pw.print("NEW_MARGIN");
		 pw.print(separtor);
		 
		 pw.print("PREDICTION_MOVEMENT");
		 pw.print(separtor);
		 
		 pw.print("PREDICTION_SALES");
		 pw.print(separtor);
		 
		 pw.print("PREDICTION_MARGIN");
		 pw.print(separtor);
		 
		 pw.print("OLD_MOVEMENT");
		 pw.print(separtor);
		 
		 pw.print("OLD_REVENUE");
		 pw.print(separtor);
		 
		 pw.print("OLD_MARGIN");
		 pw.println();
		 
		 all.forEach((k,v)->{
			 pw.print(v.getWeekStartDate().toString());
			 pw.print(separtor);
			 pw.print(v.getWeekCalendarID());
			 pw.print(separtor);
			 pw.print(v.getRetailerItemCodes().size());
			 pw.print(separtor);
			 pw.print(v.getNewMovement());
			 pw.print(separtor);
			 pw.print(v.getNewRevenue());
			 pw.print(separtor);
			 pw.print(v.getNewMargin());
			 pw.print(separtor);
			 pw.print(v.getPrediction_Movement());
			 pw.print(separtor);
			 pw.print(v.getPrediction_Sales());
			 pw.print(separtor);
			 pw.print(v.getPrediction_Margin());
			 pw.print(separtor);
			 pw.print(v.getOldMovement());
			 pw.print(separtor);
			 pw.print(v.getOldRevenue());
			 pw.print(separtor);
			 pw.print(v.getOldMargin());
			 pw.println();
		 });
		 
		 pw.flush();
		 fw.flush();
		 logger.info("WriteGraphDataToCsvFile:Writing Ended");
			System.out.println("WriteGraphDataToCsvFile:Writing Ended");
		 

}catch(Exception Ex) {
logger.info("Failed to output CSV for graph");	
}
		
		}
		}


	public List<TlogDataDTO> AddSaleAndRevByCustType(String FuncType, List<TlogDataDTO> TlogData) {
		
		TlogData.forEach(t->{
			double RegPrice;
			double ListCost;
			double GoldRegPrice;
			double SilRegPrice;
			
			if(FuncType=="OLD") {
			 RegPrice = t.getLY_RegularPrice();
			 ListCost = t.getLY_ListCost();
			}
			else {
				 RegPrice = t.getRegularPrice();
				 ListCost = t.getListCost();
			}
			
			GoldRegPrice = RegPrice*0.8;
			SilRegPrice = RegPrice*0.9;
			
			
				float GoldSale = (float)GoldRegPrice*t.getGold_Mov();
				float GoldMargin =(float)(GoldSale - ListCost*t.getGold_Mov());
				t.setGold_Rev(GoldSale);
				t.setGold_Mar(GoldMargin);
				
				float SilSale = (float)SilRegPrice*t.getSil_Mov();
				float SilMargin =(float)(SilSale - ListCost*t.getSil_Mov());
				t.setSil_Rev(SilSale);
				t.setSil_Mar(SilMargin);
				
				float RegSale = (float)RegPrice*t.getReg_Mov();
				float RegMargin =(float)(RegSale - ListCost*t.getReg_Mov());
				t.setReg_Rev(RegSale);
				t.setReg_Mar(RegMargin);
				
		});
		
		
		return TlogData;
	}


	public CatContriSummaryBO getCategoryContributionSummary(int type, Map<Integer, WeekGraphDataDTO> graphData, List<FinalDataDTO> finalData) {
		 Map<Integer,FinalDataDTO> RICList = new HashMap();
 
		
		if(type==1) {
			 finalData.forEach(f->{if(f.isToConsiderRecord()&&!f.isLigMember()) {
				 if(f.getPriceChangeIndicator()==2||f.getPriceChangeIndicator()==3) {
					 RICList.put(f.getProductId(),f);
				 }}
			 });
		 }else if(type==2||type==3) {
			 finalData.forEach(f->{if(f.isToConsiderRecord()&&!f.isLigMember()) {
				 if(f.getPriceChangeIndicator()==type) {
					 RICList.put(f.getProductId(),f);
				 }}
			 });
		 }
		
		
		CatContriSummaryBO result = new CatContriSummaryBO();
		result.setLyWeekAverageMargin(0);
		result.setLyWeekAverageSales(0);
		result.setLyWeekAverageUnits(0);
	    
		result.setNewWeekAverageMargin(0);
		result.setNewWeekAverageSales(0);
		result.setNewWeekAverageUnits(0);
	    
		result.setPredAverageUnits(0);
		result.setPredAverageSales(0);
		result.setPredAverageMargin(0);
		
	//	int counter[] = { 0,0};
		
		RICList.forEach((k,f)->{
			result.setLyWeekAverageMargin(result.getLyWeekAverageMargin()+f.getOldData().getNetMargin());
			result.setLyWeekAverageUnits(result.getLyWeekAverageUnits()+f.getOldData().getTotalMovement());
			result.setLyWeekAverageSales(result.getLyWeekAverageSales()+f.getOldData().getTotalRevenue());
			
			result.setNewWeekAverageMargin(result.getNewWeekAverageMargin()+f.getNewData().getNetMargin());
			result.setNewWeekAverageUnits(result.getNewWeekAverageUnits()+f.getNewData().getTotalMovement());
			result.setNewWeekAverageSales(result.getNewWeekAverageSales()+f.getNewData().getTotalRevenue());
			
		//	result.setPredAverageMargin(result.getPredAverageMargin()+v.getPrediction_Margin());
		//	result.setPredAverageUnits(result.getPredAverageUnits()+f.getNewData().getPrediction());
		//	result.setPredAverageSales(result.getPredAverageSales()+v.getPrediction_Sales());
			
			//counter[0]=counter[0]+1;
			
		});
	
		/*
		graphData.forEach((k,v)->{
			if(v.getIsFutureWeek()==0) {
				result.setLyWeekAverageMargin(result.getLyWeekAverageMargin()+v.getOldMargin());
				result.setLyWeekAverageUnits(result.getLyWeekAverageUnits()+v.getOldMovement());
				result.setLyWeekAverageSales(result.getLyWeekAverageSales()+v.getOldRevenue());
				
				result.setNewWeekAverageMargin(result.getNewWeekAverageMargin()+v.getNewMargin());
				result.setNewWeekAverageUnits(result.getNewWeekAverageUnits()+v.getNewMovement());
				result.setNewWeekAverageSales(result.getNewWeekAverageSales()+v.getNewRevenue());
				
				counter[0]=counter[0]+1;
			}

			result.setPredAverageMargin(result.getPredAverageMargin()+v.getPrediction_Margin());
			result.setPredAverageUnits(result.getPredAverageUnits()+v.getPrediction_Movement());
			result.setPredAverageSales(result.getPredAverageSales()+v.getPrediction_Sales());
			counter[1]=counter[1]+1;
			
		});
		result.setLyWeekAverageMargin(result.getLyWeekAverageMargin()/counter[0]);
		result.setLyWeekAverageSales(result.getLyWeekAverageSales()/counter[0]);
		result.setLyWeekAverageUnits(result.getLyWeekAverageUnits()/counter[0]);
	    
		result.setNewWeekAverageMargin(result.getNewWeekAverageMargin()/counter[0]);
		result.setNewWeekAverageSales(result.getNewWeekAverageSales()/counter[0]);
		result.setNewWeekAverageUnits(result.getNewWeekAverageUnits()/counter[0]);
	    
		result.setPredAverageUnits(result.getPredAverageUnits()/counter[1]);
		result.setPredAverageSales(result.getPredAverageSales()/counter[1]);
		result.setPredAverageMargin(result.getPredAverageMargin()/counter[1]);
		*/
		result.setLyWeekAverageMargin(result.getLyWeekAverageMargin());
		result.setLyWeekAverageSales(result.getLyWeekAverageSales());
		result.setLyWeekAverageUnits(result.getLyWeekAverageUnits());
	    
		result.setNewWeekAverageMargin(result.getNewWeekAverageMargin());
		result.setNewWeekAverageSales(result.getNewWeekAverageSales());
		result.setNewWeekAverageUnits(result.getNewWeekAverageUnits());
	    
		result.setPredAverageUnits(result.getPredAverageUnits());
		result.setPredAverageSales(result.getPredAverageSales());
		result.setPredAverageMargin(result.getPredAverageMargin());
		
		result.setSalesVar(result.getNewWeekAverageSales()-result.getLyWeekAverageSales());
		result.setUnitsVar(result.getNewWeekAverageUnits()-result.getLyWeekAverageUnits());
		result.setMarginVar(result.getNewWeekAverageMargin()-result.getLyWeekAverageMargin());
		
		return result;
	}


	public CatForecastAccBO getForecastAccuracy(int i, List<FinalDataDTO> finalData) {
		CatForecastAccBO r = new CatForecastAccBO();
		
		r.setUnitForecast(0);
		r.setUnitActual(0);
		r.setUnitVar(0);
		r.setSalesForecast(0);
		r.setSalesActual(0);
		r.setSalesVar(0);
		r.setMarginForecast(0);
		r.setMarginActual(0);
		r.setMargingVar(0);
		finalData.forEach(f->{
			if(f.isToConsiderRecord()&&!f.isLigMember()) {
				if((i==1?(f.getPriceChangeIndicator()==2||f.getPriceChangeIndicator()==3):f.getPriceChangeIndicator()==i)) {
					double Unitsforecaste = f.getNewData().getPrediction();
					double UnitsActual = f.getNewData().getTotalMovement();
			//		double RegPrice = f.getNewData().getRegularPrice();
				//	double ListCost = f.getNewData().getListCost();
				//	double GoldRegPrice = RegPrice*0.8;
				//	double SilverRegPrice = RegPrice*0.9;
					
					//double goldSalePred =((f.getNewData().getGold_Perc()*Unitsforecaste)/100)*GoldRegPrice;
					//double SilverSalePred =((f.getNewData().getSil_Perc()*Unitsforecaste)/100)*SilverRegPrice;
					//double RegSalePred = ((f.getNewData().getReg_Perc()*Unitsforecaste)/100)*RegPrice;
					
					//double goldMarginPred =goldSalePred - (((f.getNewData().getGold_Perc()*Unitsforecaste)/100)*ListCost);
					//double SilverMarginPred =SilverSalePred - (((f.getNewData().getSil_Perc()*Unitsforecaste)/100)*ListCost);
					//double RegMarginPred = RegSalePred - (((f.getNewData().getReg_Perc()*Unitsforecaste)/100)*ListCost);
					
					
					r.setUnitForecast(r.getUnitForecast()+f.getNewData().getPrediction());
					r.setUnitActual(r.getUnitActual()+UnitsActual);
					
					r.setSalesForecast(r.getSalesForecast()+f.getNewData().getPredictedSale());
					r.setSalesActual(r.getSalesActual()+f.getNewData().getTotalRevenue());
				
					r.setMarginForecast(r.getMarginForecast()+f.getNewData().getPredictedMargin());
					r.setMarginActual(r.getMarginActual()+f.getNewData().getNetMargin());
				
					
				}
			}
		});
		
		r.setSalesVar(r.getSalesActual()-r.getSalesForecast());
		r.setUnitVar(r.getUnitActual()-r.getUnitForecast());
		r.setMargingVar(r.getMarginActual()-r.getMarginForecast());
		return r;
	}


	public void WriteDataToDB(Connection conn, String productId, String productLevelId, String locationId, String locationLevelId,
			Map<Long, ItemInfoDTO> ligInfo, String strStartWeekDate, String strEndWeekDate, String strPriceStartWeekDate, String strPriceEndWeekDate,
			List<FinalDataDTO> finalData, List<TlogDataDTO> trendsNewData, List<TlogDataDTO> trendsOldData,
			Map<Integer, WeekGraphDataDTO> all, Map<Integer, WeekGraphDataDTO> increase,
			Map<Integer, WeekGraphDataDTO> decrease, CatContriSummaryBO allSummary, CatContriSummaryBO increasedSummary,
			CatContriSummaryBO decreasedSummary, CatForecastAccBO allFAcc, CatForecastAccBO incFAcc,
			CatForecastAccBO decFAcc,Map<LocalDate,Integer> CalendarIds, int totalCount, int countIncrease, int countDecrease) throws Exception {
		 DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		ReportDataInsertDAO DAO = new ReportDataInsertDAO();
		LocalDate StartDate = LocalDate.parse(strStartWeekDate,dtf);
		LocalDate EndDate = LocalDate.parse(strEndWeekDate,dtf);
		//Check For Active Run Header
		//Mark it As Not Active 
		//Create New Active Run Header
		
		int RunId = DAO.createNewRunId(conn,productId, productLevelId, locationId, locationLevelId,
				strStartWeekDate, strEndWeekDate,strPriceStartWeekDate, strPriceEndWeekDate);
		
		logger.info("RunId Created : "+RunId);
		if(totalCount!=0) {
		//Insert Category Contribution Table summary data for Inc, Dec, All
		//logger.info("Inserting Category Contribution...............");
		//DAO.InsertCategoryContribution(conn,allSummary,increasedSummary,decreasedSummary,RunId,CalendarIds.get(StartDate),CalendarIds.get(EndDate));
		//logger.info("Completed Inserting Category Contribution!");
		//Insert WeekGraphData for all , Inc ,Dec
		logger.info("Inserting WeeklyGraphData...............");
		DAO.InsertWeekGraphData(conn,"C",all,RunId);
		if(countIncrease!=0&&increase.size()!=0) {
		DAO.InsertWeekGraphData(conn,"I",increase,RunId);
		}
		if(countDecrease!=0&&decrease.size()!=0) {
		DAO.InsertWeekGraphData(conn,"D",decrease,RunId);
		}
		logger.info("Completed Inserting Weekly Graph Data!");
		//Insert Category Forecaste Table Summary Data
		logger.info("Inserting Category Summary...............");
		
		DAO.InsertCatForecastSummary(conn,"C",allFAcc,allSummary,RunId,CalendarIds.get(StartDate),CalendarIds.get(EndDate));
		if(countIncrease!=0) {
		DAO.InsertCatForecastSummary(conn,"I",incFAcc,increasedSummary,RunId,CalendarIds.get(StartDate),CalendarIds.get(EndDate));
		}
		if(countDecrease!=0) {
		DAO.InsertCatForecastSummary(conn,"D",decFAcc,decreasedSummary,RunId,CalendarIds.get(StartDate),CalendarIds.get(EndDate));
		}
		logger.info("Completed Inserting Category Summary!");
		//Insert Final Data
		logger.info("Inserting Final Report Data...............");
		DAO.InsertFinalReportData(conn,RunId,CalendarIds.get(StartDate),CalendarIds.get(EndDate),finalData,ligInfo);
		logger.info("Completed Inserting Final Report Data!");
		//Insert New And Old Item Level Data
		logger.info("Inserting Item Level Weekly Data for Item Level Graph...............");
		DAO.InsertItemLevelWeeklyData(conn,"OLD",RunId,trendsOldData,CalendarIds);
		DAO.InsertItemLevelWeeklyData(conn,"NEW",RunId,trendsNewData,CalendarIds);
		logger.info("Completed Inserting Item Level Weekly Data for Item Level Graph!");
		}else {
			logger.info("No Price Change Items found");
		}
	}


	public Map<LocalDate, Integer> getWeekCalendarIds(Connection conn, String strStartWeekDate, String strEndWeekDate) throws Exception {
		Map<LocalDate, Integer> cId= new HashMap();
		
		 CommonTaskDAO common = new CommonTaskDAO();
		 cId=common.getWeekCalId( conn,strStartWeekDate,strEndWeekDate);
		
		return cId;
	}


	public List<FinalDataDTO> CalculateTiersMatrices(List<FinalDataDTO> finalData) {
		
		
		finalData.forEach(f->{
			if(f.isOldDataPresent()) {
			
			float LyGoldPerc = f.getOldData().getGold_Perc(); 
			float LySilPerc = f.getOldData().getSil_Perc(); 
			float LyRegPerc = f.getOldData().getReg_Perc(); 

			double LyTotalUnits = f.getOldData().getTotalMovement();
			
			float LyRegPrice = f.getOldData().getRegularPrice();
			
			float LyListCost = f.getOldData().getListCost();
			
			double LygoldMov = (LyGoldPerc*LyTotalUnits)/100;
			double LySilMov = (LySilPerc*LyTotalUnits)/100;
			double LyRegMov = (LyRegPerc*LyTotalUnits)/100;

			
			
			double LygoldSales = LygoldMov*(LyRegPrice*0.8);
			double LySilSales = LySilMov*(LyRegPrice*0.9);
			double LyRegSales = LyRegMov*(LyRegPrice*1);
			
			
			double LygoldMar = LygoldSales-LygoldMov*LyListCost;
			double LySilMar = LySilSales-LySilMov*LyListCost;
			double LyRegMar = LyRegSales-LyRegMov*LyListCost;
			
			f.getOldData().setGold_Mov((float)LygoldMov);
			f.getOldData().setSil_Mov((float)LySilMov);
			f.getOldData().setReg_Mov((float)LyRegMov);
			
			
			f.getOldData().setGold_Rev((float)LygoldSales);
			f.getOldData().setSil_Rev((float)LySilSales);
			f.getOldData().setReg_Rev((float)LyRegSales);
			
			
			f.getOldData().setGold_Mar((float)LygoldMar);
			f.getOldData().setSil_Mar((float)LySilMar);
			f.getOldData().setReg_Mar((float)LyRegMar);
			
			
			}
			
			if(f.isNewDataPresent()) {
				float TyGoldPerc = f.getNewData().getGold_Perc(); 
				float TySilPerc = f.getNewData().getSil_Perc(); 
				float TyRegPerc = f.getNewData().getReg_Perc(); 
				
				double TyTotalUnits = f.getNewData().getTotalMovement();

				float TyRegPrice = f.getNewData().getRegularPrice();
				float TyListCost = f.getNewData().getListCost();


				double TygoldMov = (TyGoldPerc*TyTotalUnits)/100;
				double TySilMov = (TySilPerc*TyTotalUnits)/100;
				double TyRegMov = (TyRegPerc*TyTotalUnits)/100;
				
				double TygoldSales = TygoldMov*(TyRegPrice*0.8);
				double TySilSales = TySilMov*(TyRegPrice*0.9);
				double TyRegSales = TyRegMov*(TyRegPrice*1);
				
				double TygoldMar = TygoldSales-TygoldMov*TyListCost;
				double TySilMar = TySilSales-TySilMov*TyListCost;
				double TyRegMar = TyRegSales-TyRegMov*TyListCost;
			
				f.getNewData().setGold_Mov((float)TygoldMov);
				f.getNewData().setSil_Mov((float)TySilMov);
				f.getNewData().setReg_Mov((float)TyRegMov);

				f.getNewData().setGold_Rev((float)TygoldSales);
				f.getNewData().setSil_Rev((float)TySilSales);
				f.getNewData().setReg_Rev((float)TyRegSales);
				
				f.getNewData().setGold_Mar((float)TygoldMar);
				f.getNewData().setSil_Mar((float)TySilMar);
				f.getNewData().setReg_Mar((float)TyRegMar);
				
			}
			
			
			
		});
		
		
		return finalData;
	}


	public List<TlogDataDTO> RemoveFutureWeeksData(List<TlogDataDTO> oldTlogData, List<TlogDataDTO> newTlogData, LocalDate oldStartWeekDate) {
		LocalDate MaxDate[] = {oldStartWeekDate};
		newTlogData.forEach(n->{
			LocalDate OldDate = n.getWeekStartDate().minusDays(364);
				if(MaxDate[0].compareTo(OldDate)<0) {
					MaxDate[0] = OldDate;
				}
				
			
		});
		
		Iterator<TlogDataDTO> Old = oldTlogData.iterator();
		TlogDataDTO tlog = new TlogDataDTO();

		while(Old.hasNext()) {
			tlog = Old.next();
			if(tlog.getWeekStartDate().compareTo(MaxDate[0])>0) {
				Old.remove();
			}
			
		}
		return oldTlogData;
	}


	public List<TlogDataDTO> RollupToLIGLevel(String Type,Map<Long, ItemInfoDTO> ligInfo, Map<Integer, ItemInfoDTO> iteminfo,
			List<TlogDataDTO> TlogData) {
		Map<Long,Map<LocalDate,TlogDataDTO>> LIGlevelRollupData = new HashMap();
		Map<Long,Map<LocalDate,Float>> Count = new HashMap();
		TlogData.forEach(t->{
			int Id = t.getProductId();
			long RLI = iteminfo.get(Id).getRetLIRId();
			if(ligInfo.containsKey(RLI)) {
				t.setLigMember(true);
				LocalDate WSD = t.getWeekStartDate();
				if(LIGlevelRollupData.containsKey(RLI)) {
					if(LIGlevelRollupData.get(RLI).containsKey(WSD)) {
						Count.get(RLI).replace(WSD, Count.get(RLI).get(WSD)+1f);
						if(Type=="OLD") {
						LIGlevelRollupData.get(RLI).get(WSD).setLY_RegularPrice(LIGlevelRollupData.get(RLI).get(WSD).getLY_RegularPrice()+t.getLY_RegularPrice());
						LIGlevelRollupData.get(RLI).get(WSD).setLY_ListCost(LIGlevelRollupData.get(RLI).get(WSD).getLY_ListCost()+t.getLY_ListCost());
						}
						else if(Type=="NEW"){
							LIGlevelRollupData.get(RLI).get(WSD).setPrediction(LIGlevelRollupData.get(RLI).get(WSD).getPrediction()+t.getPrediction());
							LIGlevelRollupData.get(RLI).get(WSD).setPredictedSale(LIGlevelRollupData.get(RLI).get(WSD).getPredictedSale()+t.getPredictedSale());
							LIGlevelRollupData.get(RLI).get(WSD).setPredictedMargin(LIGlevelRollupData.get(RLI).get(WSD).getPredictedMargin()+t.getPredictedMargin());
							
						}
						LIGlevelRollupData.get(RLI).get(WSD).setRegularPrice(LIGlevelRollupData.get(RLI).get(WSD).getRegularPrice()+t.getRegularPrice());
						LIGlevelRollupData.get(RLI).get(WSD).setListCost(LIGlevelRollupData.get(RLI).get(WSD).getListCost()+t.getListCost());
						
						LIGlevelRollupData.get(RLI).get(WSD).setTotalMovement(LIGlevelRollupData.get(RLI).get(WSD).getTotalMovement()+t.getTotalMovement());
						LIGlevelRollupData.get(RLI).get(WSD).setTotalRevenue(LIGlevelRollupData.get(RLI).get(WSD).getTotalRevenue()+t.getTotalRevenue());
						LIGlevelRollupData.get(RLI).get(WSD).setNetMargin(LIGlevelRollupData.get(RLI).get(WSD).getNetMargin()+t.getNetMargin());
						
						LIGlevelRollupData.get(RLI).get(WSD).setReg_Perc(LIGlevelRollupData.get(RLI).get(WSD).getReg_Perc()+t.getReg_Perc());
						LIGlevelRollupData.get(RLI).get(WSD).setGold_Perc(LIGlevelRollupData.get(RLI).get(WSD).getGold_Perc()+t.getGold_Perc());
						LIGlevelRollupData.get(RLI).get(WSD).setSil_Perc(LIGlevelRollupData.get(RLI).get(WSD).getSil_Perc()+t.getSil_Perc());
						LIGlevelRollupData.get(RLI).get(WSD).setSale_Perc(LIGlevelRollupData.get(RLI).get(WSD).getSale_Perc()+t.getSale_Perc());
						LIGlevelRollupData.get(RLI).get(WSD).setNo_Card_Perc(LIGlevelRollupData.get(RLI).get(WSD).getNo_Card_Perc()+t.getNo_Card_Perc());

					}else {
						TlogDataDTO d = new TlogDataDTO();
						Count.get(RLI).put(WSD, 1f);
						d.setWeekCalendarId(t.getWeekCalendarId());
						//d.setProductId(ligInfo.get(RLI).getItemCode());We store Ret Lir Id for lig in db
						d.setProductId((int) RLI);
						d.setRetailerItemCode(ligInfo.get(RLI).getRetailerItemCode());
						d.setWeekStartDate(WSD);
						d.setProductLevelId(11);
						d.setRetLIRName(t.getRetLIRName());
						d.setLigMember(false);
						
						if(Type=="OLD") {
						d.setLY_RegularPrice(t.getLY_RegularPrice());
						d.setLY_ListCost(t.getLY_ListCost());
						}
						else if(Type=="NEW"){
							d.setPrediction(t.getPrediction());
							d.setPredictedSale(t.getPredictedSale());
							d.setPredictedMargin(t.getPredictedMargin());
						}
						d.setRegularPrice(t.getRegularPrice());
						d.setListCost(t.getListCost());
						
						d.setTotalMovement(t.getTotalMovement());
						d.setTotalRevenue(t.getTotalRevenue());
						d.setNetMargin(t.getNetMargin());
						
						d.setReg_Perc(t.getReg_Perc());
						d.setGold_Perc(t.getGold_Perc());
						d.setSil_Perc(t.getSil_Perc());
						d.setSale_Perc(t.getSale_Perc());
						d.setNo_Card_Perc(t.getNo_Card_Perc());
						LIGlevelRollupData.get(RLI).put(WSD, d);
					}
				}else {
					TlogDataDTO d = new TlogDataDTO();
					Map<LocalDate,TlogDataDTO> mt = new HashMap();
					Map<LocalDate,Float> mc = new HashMap();
					mc.put(WSD, 1f);
					Count.put(RLI,mc);
					d.setWeekCalendarId(t.getWeekCalendarId());
					//d.setProductId(ligInfo.get(RLI).getItemCode());
					d.setProductId((int)RLI);
					d.setRetailerItemCode(ligInfo.get(RLI).getRetailerItemCode());
					d.setWeekStartDate(WSD);
					d.setProductLevelId(11);
					d.setRetLIRName(t.getRetLIRName());
					d.setLigMember(false);
					
					if(Type=="OLD") {
					d.setLY_RegularPrice(t.getLY_RegularPrice());
					d.setLY_ListCost(t.getLY_ListCost());
					}
					else if(Type=="NEW"){
						d.setPrediction(t.getPrediction());
						d.setPredictedSale(t.getPredictedSale());
						d.setPredictedMargin(t.getPredictedMargin());
					}
					d.setRegularPrice(t.getRegularPrice());
					d.setListCost(t.getListCost());
					
					d.setTotalMovement(t.getTotalMovement());
					d.setTotalRevenue(t.getTotalRevenue());
					d.setNetMargin(t.getNetMargin());
					
					d.setReg_Perc(t.getReg_Perc());
					d.setGold_Perc(t.getGold_Perc());
					d.setSil_Perc(t.getSil_Perc());
					d.setSale_Perc(t.getSale_Perc());
					d.setNo_Card_Perc(t.getNo_Card_Perc());
					mt.put(WSD,d);
					LIGlevelRollupData.put(RLI,mt);
					
				}
				
			}else {
				t.setLigMember(false);
			}
		});
		
		LIGlevelRollupData.forEach((k,m)->{
			m.forEach((w,t)->{
				float c =Count.get(k).get(w);
				if(Type=="OLD") {
					t.setLY_RegularPrice(t.getLY_RegularPrice()/c);
					t.setLY_ListCost(t.getLY_ListCost()/c);
					}
					t.setRegularPrice(t.getRegularPrice()/c);
					t.setListCost(t.getListCost()/c);
					
					t.setGold_Perc(t.getGold_Perc()/c);
					t.setSil_Perc(t.getSil_Perc()/c);
					t.setReg_Perc(t.getReg_Perc()/c);
					t.setSale_Perc(t.getSale_Perc()/c);
					t.setNo_Card_Perc(t.getNo_Card_Perc()/c);
						
			});
		});
		
		LIGlevelRollupData.forEach((k,m)->{
			m.forEach((w,t)->{
				TlogData.add(t);
			});
		});
		logger.info("LIG COUNT: " +LIGlevelRollupData.size() );
		return TlogData;
	}


	public Map<Long, Map<LocalDate, List<Integer>>> identifyCommonWeeklyLigMembers(List<TlogDataDTO> oldTlogData,
			List<TlogDataDTO> newTlogData, Map<Long, ItemInfoDTO> ligInfo, Map<Integer, ItemInfoDTO> iteminfo) {
		 
		Map<Long, Map<LocalDate, List<Integer>>> Output = new HashMap();
		Map<Long, Map<LocalDate, List<Integer>>> temp = new HashMap();
		
		oldTlogData.forEach(t->{
			int Id = t.getProductId();
			long RLI = iteminfo.get(Id).getRetLIRId();
			if(ligInfo.containsKey(RLI)) {
				t.setLigMember(true);
				LocalDate WSD = t.getWeekStartDate();
				if(temp.containsKey(RLI)) {
					if(temp.get(RLI).containsKey(WSD)) {
						temp.get(RLI).get(WSD).add(Id);
					}else {
						List<Integer> l = new ArrayList();
						l.add(Id);
						temp.get(RLI).put(WSD,l);
					}
				}else {
						Map<LocalDate,List<Integer>> m = new HashMap();
						List<Integer> l = new ArrayList();
						l.add(Id);
						m.put(WSD, l);	
						temp.put(RLI, m);
				}
				
			}else {
				t.setLigMember(false);
			}
			
		});
		
		newTlogData.forEach(t->{
			int Id = t.getProductId();
			long RLI = iteminfo.get(Id).getRetLIRId();
			if(ligInfo.containsKey(RLI)) {
				t.setLigMember(true);
				LocalDate WSD = t.getWeekStartDate().minusDays(364);
				if(temp.containsKey(RLI)&&temp.get(RLI).containsKey(WSD)&&temp.get(RLI).get(WSD).contains(Id)) {
				if(Output.containsKey(RLI)) {
					if(Output.get(RLI).containsKey(WSD)) {
						Output.get(RLI).get(WSD).add(Id);
					}else {
						List<Integer> l = new ArrayList();
						l.add(Id);
						Output.get(RLI).put(WSD,l);
					
					}
				}else {
						Map<LocalDate,List<Integer>> m = new HashMap();
						List<Integer> l = new ArrayList();
						l.add(Id);
						m.put(WSD, l);
						Output.put(RLI, m);
				}
				}
			}else {
				t.setLigMember(false);
			}
			
		});
		
		return Output;
	}


	public List<TlogDataDTO> FilterWeeklyLigMembers(String type, List<TlogDataDTO> TlogData,
			Map<Long, Map<LocalDate, List<Integer>>> weeklyLigMembers, Map<Long, ItemInfoDTO> ligInfo,
			Map<Integer, ItemInfoDTO> iteminfo) {
		Iterator<TlogDataDTO> Tlog = TlogData.iterator();
		TlogDataDTO tlog = new TlogDataDTO();

		while(Tlog.hasNext()) {
			tlog = Tlog.next();
			int Id = tlog.getProductId();
			long RLI = iteminfo.get(Id).getRetLIRId();
			
			if(ligInfo.containsKey(RLI)) {
				tlog.setLigMember(true);
				LocalDate WSD = tlog.getWeekStartDate();
				if(type=="NEW") {
					WSD = WSD.minusDays(364);
				}
				if(weeklyLigMembers.containsKey(RLI)) {
				if(weeklyLigMembers.get(RLI).containsKey(WSD)) {
					if(!weeklyLigMembers.get(RLI).get(WSD).contains(Id)){
						Tlog.remove();
					}
				}else {
					Tlog.remove();
				}
				}else {
					Tlog.remove();
				}
			}else {
				tlog.setLigMember(false);
			}
			
		}
		return TlogData;
	}


	public List<TlogDataDTO> FilterNonPriceChangedWeeks(List<TlogDataDTO> TlogData) {
	
		Map<Integer,Float> LatestPricesForItems = new HashMap();
		Map<Integer,LocalDate> date = new HashMap();
		TlogData.forEach(t->{
			int prodId = t.getProductId();
			float RegPrice = t.getRegularPrice();
			LocalDate WSD = t.getWeekStartDate();
			if(LatestPricesForItems.containsKey(prodId)) {
				if(LatestPricesForItems.get(prodId)!=RegPrice) {
					if(date.get(prodId).isBefore(WSD)) {
						LatestPricesForItems.replace(prodId, RegPrice);
						date.replace(prodId, WSD);
					}
				}
			}else {
				LatestPricesForItems.put(prodId,RegPrice);
				date.put(prodId, WSD);
			}
		});
		
		Iterator<TlogDataDTO> TI = TlogData.iterator();
		TlogDataDTO tlog = new TlogDataDTO();

		while(TI.hasNext()) {
			tlog = TI.next();
			int Id = tlog.getProductId();
			float Reg = tlog.getRegularPrice();
			if(LatestPricesForItems.containsKey(Id)) {
				if(Reg!=LatestPricesForItems.get(Id)) {
					TI.remove();
				}
			}
		}
		
		
		
		
		return TlogData;
	}


	public List<FinalDataDTO> DecideLIGRegPriceBasedOnPerformance(List<FinalDataDTO> finalData, Map<Long, ItemInfoDTO> ligInfo, Map<Integer, ItemInfoDTO> iteminfo) {
		logger.info("Final Data in Reg Price Func:"+finalData.size());
        finalData.removeAll(Collections.singletonList(null));
		logger.info("Final Data in Reg Price Func:"+finalData.size());

		for(FinalDataDTO f : finalData){
			logger.info("Checking Ppoduct level ID");
			if(f.getProductLevelId()==11) {
					long retLirId=f.getProductId();

					logger.info("getting Ret LIR ID");
					List<Integer> ItemsInLig = new ArrayList();
					iteminfo.forEach((k,i)->{
						if(i.getRetLIRId()==retLirId) {
							ItemsInLig.add(i.getItemCode());
						}
					});
					Map<Integer,TlogDataDTO> Impact = new HashMap();
					int count[] = {0};
					logger.info("Items in current LIG in Reg Price Func:"+ItemsInLig.size());
					if(ItemsInLig.size()!=0) {
						finalData.forEach(f2->{
							if(ItemsInLig.contains(f2.getProductId())&&f2.getProductLevelId()==1) {
								logger.info("Inside First If");
								if(f2.isNewDataPresent()&&f2.isOldDataPresent()) {
									logger.info("Inside Second If");
									TlogDataDTO t = new TlogDataDTO();
									t.setProductId(f2.getProductId());
									t.setTotalMovement(f2.getNewData().getTotalMovement());
									t.setRegularPrice(f2.getNewData().getRegularPrice());
									t.setListCost(f2.getNewData().getListCost());
									t.setLY_RegularPrice(f2.getOldData().getLY_RegularPrice());
									t.setLY_ListCost(f2.getOldData().getLY_ListCost());
									Impact.put(f2.getProductId(),t);
									count[0]++;
								}
							}
						});
						int ID[] = {0};
						double max[] = {0};
						logger.info("Impact Data in Reg Price Func:"+Impact.size());
						logger.info("Actual Impact Data in Reg Price Func:"+count[0]);
						if(count[0] > 0) {
							Impact.forEach((k,i)->{
								if(i.getTotalMovement()>max[0]) {
									ID[0] = i.getProductId();
									max[0] = i.getTotalMovement();
								}
							});
							logger.info("ID "+ID[0]);
							logger.info("TotalMovement"+max[0]);
							if(f.isNewDataPresent()&&f.isOldDataPresent()&&ID[0]!=0) { 	
								f.getNewData().setRegularPrice(Impact.get(ID[0]).getRegularPrice());
								f.getNewData().setListCost(Impact.get(ID[0]).getListCost());
							
								f.getOldData().setLY_RegularPrice(Impact.get(ID[0]).getLY_RegularPrice());
								f.getOldData().setLY_ListCost(Impact.get(ID[0]).getLY_ListCost());
					
							}
						}
					}
				}
			}
		return finalData;
	}


	public List<TlogDataDTO> GroupItemsWithMultiUPCRetItemCode(List<TlogDataDTO> TlogData, String type) {
		
		HashMap<String,List<Integer>> mulUPClist = new HashMap();
		HashMap<String,TlogDataDTO> mulUPCObjlist = new HashMap();
		for(TlogDataDTO t : TlogData) {
			String key = t.getRetailerItemCode()+"_"+t.getWeekCalendarId();
			if(mulUPClist.containsKey(key)&&t.getProductLevelId()!=11) {
				mulUPClist.get(key).add(t.getProductId());
			
				mulUPCObjlist.get(key).setGold_Mar(mulUPCObjlist.get(key).getGold_Mar()+t.getGold_Mar());
				mulUPCObjlist.get(key).setGold_Mov(mulUPCObjlist.get(key).getGold_Mov()+t.getGold_Mov());
				mulUPCObjlist.get(key).setGold_Perc(mulUPCObjlist.get(key).getGold_Perc()+t.getGold_Perc());
				mulUPCObjlist.get(key).setGold_Rev(mulUPCObjlist.get(key).getGold_Rev()+t.getGold_Rev());
				
				mulUPCObjlist.get(key).setSil_Mar(mulUPCObjlist.get(key).getSil_Mar()+t.getSil_Mar());
				mulUPCObjlist.get(key).setSil_Mov(mulUPCObjlist.get(key).getSil_Mov()+t.getSil_Mov());
				mulUPCObjlist.get(key).setSil_Perc(mulUPCObjlist.get(key).getSil_Perc()+t.getSil_Perc());
				mulUPCObjlist.get(key).setSil_Rev(mulUPCObjlist.get(key).getSil_Rev()+t.getSil_Rev());
				
				mulUPCObjlist.get(key).setReg_Mar(mulUPCObjlist.get(key).getReg_Mar()+t.getReg_Mar());
				mulUPCObjlist.get(key).setReg_Mov(mulUPCObjlist.get(key).getReg_Mov()+t.getReg_Mov());
				mulUPCObjlist.get(key).setReg_Perc(mulUPCObjlist.get(key).getReg_Perc()+t.getReg_Perc());
				mulUPCObjlist.get(key).setReg_Rev(mulUPCObjlist.get(key).getReg_Rev()+t.getReg_Rev());
				
				mulUPCObjlist.get(key).setSale_Perc(mulUPCObjlist.get(key).getSale_Perc()+t.getSale_Perc());
				
				mulUPCObjlist.get(key).setNo_Card_Perc(mulUPCObjlist.get(key).getNo_Card_Perc()+t.getNo_Card_Perc());
				
				if(type=="NEW"&&t.isPredPresent()) {
				mulUPCObjlist.get(key).setPrediction(mulUPCObjlist.get(key).getPrediction()+t.getPrediction()); 
				mulUPCObjlist.get(key).setPredictedSale(mulUPCObjlist.get(key).getPredictedSale()+t.getPredictedSale());
				mulUPCObjlist.get(key).setPredictedMargin(mulUPCObjlist.get(key).getPredictedMargin()+t.getPredictedMargin());
				
				}
				
				mulUPCObjlist.get(key).setTotal_Mov(mulUPCObjlist.get(key).getTotal_Mov()+t.getTotal_Mov());
				
				mulUPCObjlist.get(key).setTotalMovement(mulUPCObjlist.get(key).getTotalMovement()+t.getTotalMovement());
				mulUPCObjlist.get(key).setTotalRevenue(mulUPCObjlist.get(key).getTotalRevenue()+t.getTotalRevenue());
				mulUPCObjlist.get(key).setNetMargin(mulUPCObjlist.get(key).getNetMargin()+t.getNetMargin());
				t.setIsDeleteForMulUPC(true);
				
			}else {
				List<Integer> prodIds = new ArrayList();
				prodIds.add(t.getProductId());
				mulUPClist.put(key, prodIds);
				mulUPCObjlist.put(key, t);
				t.setIsDeleteForMulUPC(false);
			}
		}
		
		mulUPCObjlist.forEach((k,v)->{
			int count = mulUPClist.get(k).size();
			if(count>1) {
				v.setGold_Perc(v.getGold_Perc()/count);
				v.setSil_Perc(v.getSil_Perc()/count);
				v.setReg_Perc(v.getReg_Perc()/count);
				v.setNo_Card_Perc(v.getNo_Card_Perc()/count);
				v.setSale_Perc(v.getSale_Perc()/count);
			}
		});
		
		Iterator<TlogDataDTO> i = TlogData.iterator();
		TlogDataDTO tlog = new TlogDataDTO();
		HashMap<String, TlogDataDTO> RepresItems = new HashMap();
		
		while(i.hasNext()) {
			tlog =i.next();
			if(tlog.isIsDeleteForMulUPC()) {
				i.remove();
			}
		 }
		
		return TlogData;
	}


	public HashMap<Integer, HashMap<LocalDate, Float>> rollUpPredDataToMulUPC(HashMap<Integer, HashMap<LocalDate, Float>> newRecom,
			Map<Integer, ItemInfoDTO> iteminfo, Map<Long, ItemInfoDTO> ligInfo, List<FinalDataDTO> finalData) {
		
		HashMap<Integer,String> RetItemCode = new HashMap();
		HashMap<Integer, HashMap<LocalDate, Float>> FinalOP = new HashMap();
		
		HashMap<Integer, List<Integer>> ItemsWithSameRetItemCode = new HashMap();
		
		iteminfo.forEach((k,v)->{
			int ic = v.getItemCode();
	        String RIC = v.getRetailerItemCode();
	        List<Integer> l = new ArrayList();
	        iteminfo.forEach((k1,v1)->{
	        	if(v1.getRetailerItemCode()==RIC) {
	        		l.add(v1.getItemCode());
	        	}
	        });
	        ItemsWithSameRetItemCode.put(k,l);
		});
		
		ItemsWithSameRetItemCode.forEach((k,l)->{
			HashMap<LocalDate,Float> f = new HashMap();
			l.forEach(I->{
				if(newRecom.containsKey(I)) {
				newRecom.get(I).forEach((ld,pred)->{
					if(f.containsKey(ld)) {
						f.replace(ld,f.get(ld)+pred);
					}else {
						f.put(ld, pred);
					}
				});
				}
			});
			
			if(f.size()>0)
			FinalOP.put(k,f);
		});
		
		ligInfo.forEach((c,i)->{
			long RLI = c;
			List<String> RIClist = new ArrayList();
			List<Integer> ItemCodes = new ArrayList();
			iteminfo.forEach((k,v)->{
				if(v.getRetLIRId()==RLI&&!RIClist.contains(v.getRetailerItemCode())) {
					ItemCodes.add(k);
					RIClist.add(v.getRetailerItemCode());
				}
			});
			HashMap<LocalDate,Float> p = new HashMap();
			
			ItemCodes.forEach(ic->{				
				if(newRecom.containsKey(ic)) {
					newRecom.get(ic).forEach((ld,pred)->{
						if(p.containsKey(ld)) {
							p.replace(ld, p.get(ld)+pred);
						}else {
							p.put(ld,pred);
						}
					});
				}
			});
			
			if(p.size()>0) {
				FinalOP.put((int)RLI, p);
			}
		});
		
		return FinalOP;
	}




	
	/*public LocalDate GetLastYearDate(LocalDate input) {
		LocalDate output = input;
		
		if(input.isLeapYear()) {
			if(input.getMonthValue()>2) {
				output = input.minusDays(365);
			}else {
				output = input.minusDays(364);
			}
		}else {
			output = input.minusDays(364);
			if(output.isLeapYear()) {
				if(output.getMonthValue()<=2) {
					output = output.minusDays(1);
				}
			}
		}
		return output;
		
	}
	
	public LocalDate GetNextYearDate(LocalDate input) {
		LocalDate output = input;
		
		if(input.isLeapYear()) {
			if(input.getMonthValue()>2) {
				output = input.plusDays(364);
			}else {
				output = input.plusDays(365);
			}
		}else {
			output = input.plusDays(364);
			if(output.isLeapYear()) {
				if(output.getMonthValue()>2) {
					output = output.plusDays(1);
				}
			}
		}
		return output;
		
	}*/
}
