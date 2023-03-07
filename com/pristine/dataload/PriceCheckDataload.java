/*    */ package com.pristine.dataload;
/*    */ 
/*    */ import com.pristine.dao.DBManager;
/*    */ import com.pristine.exception.GeneralException;
/*    */ import com.pristine.util.DateUtil;
/*    */ import com.pristine.util.PristineDBUtil;
/*    */ import com.pristine.util.PropertyManager;
/*    */ import java.sql.Connection;
/*    */ import javax.sql.rowset.CachedRowSet;
/*    */ import org.apache.log4j.Logger;
/*    */ import org.apache.log4j.PropertyConfigurator;
/*    */ 
/*    */ public class PriceCheckDataload
/*    */ {
/* 14 */   private static Logger logger = Logger.getLogger("PriceCheckDataload");
/* 15 */   private static String dateForSchedule = "";
/*    */   
/*    */   public static void main(String[] args)
/*    */   {
/* 19 */     PropertyConfigurator.configure("log4j-PRload.properties");
/* 20 */     PropertyManager.initialize("analysis.properties");
/* 21 */     Connection conn = null;
/* 22 */     StringBuffer sb = new StringBuffer();
/* 23 */     if (args.length == 0)
/*    */     {
/* 24 */       logger.debug("Insufficient Arguments - PriceCheckDataload [No. of days]");
/* 25 */       System.exit(1);
/*    */     }
/* 27 */     int noOfDays = Integer.parseInt(args[0]);
/* 28 */     dateForSchedule = DateUtil.getDateFromCurrentDate(-noOfDays);
/* 29 */     logger.info("Before " + noOfDays + " days, Date was::" + dateForSchedule);
/*    */     try
/*    */     {
/* 32 */       conn = DBManager.getConnection();
/* 33 */       sb.append("SELECT SCHEDULE_ID FROM SCHEDULE WHERE ");
/* 34 */       sb.append("STATUS_CHG_DATE >= to_date('" + dateForSchedule + "','MM/dd/yyyy') AND CURRENT_STATUS IN (2,4) ");
/* 35 */       sb.append("AND COMP_STR_ID IN (SELECT COMP_STR_ID FROM COMPETITOR_STORE WHERE COMP_CHAIN_ID NOT IN (52,47))");
/* 36 */       logger.debug("Query for fetching schdules ---> " + sb);
/* 37 */       CachedRowSet crs = PristineDBUtil.executeQuery(conn, sb, "");
/* 38 */       processSchedules(crs, conn);
/*    */     }
/*    */     catch (GeneralException e)
/*    */     {
/* 41 */       logger.error("Error in getting schdules -->", e);
/* 42 */       System.exit(1);
/*    */     }
/*    */   }
/*    */   
/*    */   public static void processSchedules(CachedRowSet crs, Connection conn)
/*    */     throws GeneralException
/*    */   {
/* 47 */     logger.info("No. of schedules to be processed --> " + crs.size());
/*    */     try
/*    */     {
/* 49 */       CompDataLIGRollup Lig = new CompDataLIGRollup();
/* 50 */       CompDataPISetup pi = new CompDataPISetup();
/* 51 */       SuspectDetector sus = new SuspectDetector();
/* 52 */       if (crs.size() > 0) {
/* 53 */         while (crs.next())
/*    */         {
/* 55 */           int scheduleId = Integer.parseInt(crs.getString("SCHEDULE_ID"));
/* 56 */           logger.info("Processing Suspect Analysis for -->" + crs.getString("SCHEDULE_ID"));
/* 57 */           sus.markSupectItems(conn, scheduleId);
/* 58 */           logger.info("Processing LIGLevelRollUp for -->" + crs.getString("SCHEDULE_ID"));
/* 59 */           Lig.LIGLevelRollUp(conn, scheduleId);
/* 60 */           logger.info("Processing PISetup for -->" + crs.getString("SCHEDULE_ID"));
/* 61 */           pi.setupSchedule(conn, scheduleId);
/*    */           
/* 63 */           PristineDBUtil.commitTransaction(conn, "Committing");
/*    */         }
/*    */       }
/* 66 */       logger.info("No. of schdules processed -->" + crs.size());
/*    */     }
/*    */     catch (Exception e)
/*    */     {
/* 68 */       logger.info("Error processing LIGLevelRollUp and PI setup for schdules -->" + e);
/*    */     }
/*    */   }
/*    */ }


