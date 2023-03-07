package com.pristine.dao;

import com.pristine.dto.StoreCouponDTO;
import com.pristine.exception.GeneralException;
import com.pristine.util.PrestoUtil;
import com.pristine.util.PristineDBUtil;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.log4j.Logger;

public class TopsStoreCouponDAO
  implements IDAO
{
  static Logger logger = Logger.getLogger("StoreCouponDAO");
  static HashMap<String, String> movementUPCList = new HashMap();
  
  private String InsertCouponSql(boolean isMulti)
  {
    StringBuffer Sql = new StringBuffer();
    Sql.append("insert into ");
    if (isMulti)
    {
      Sql.append("MOV_HOURLY_COUPON_INFO (");
      logger.debug("Loading records into hourly table...");
    }
    else
    {
      Sql.append("MOV_DAILY_COUPON_INFO (");
      logger.debug("Loading records into daily table...");
    }
    Sql.append(" COMP_STR_NO, UPC, POS_TIMESTAMP, ");
    Sql.append(" PRICE, TRANSACTION_NO, ");
    Sql.append(" CUSTOMER_CARD_NO, ITEM_CODE, CALENDAR_ID, POS_DEPT, CPN_TYPE,CPN_NUMBER, FAMILY_CODE, CPN_QTY, CPN_WEIGHT ) values (");
    
    Sql.append(" ?, ?, To_DATE(?, 'YYYYMMDDHH24MI'), ?, ?, ?, ?, ?, ?, ?,?,?,?,? )");
    return Sql.toString();
  }
  
  public int insertCoupons(Connection conn, ArrayList<StoreCouponDTO> couponList, boolean isMultiCpnLoad)
    throws GeneralException
  {
    int processedCount = 0;
    try
    {
      PreparedStatement psmt = conn.prepareStatement(InsertCouponSql(isMultiCpnLoad));
      
      boolean runBatch = false;
      for (int recordCnt = 0; recordCnt < couponList.size(); recordCnt++)
      {
        StoreCouponDTO couponDTO = (StoreCouponDTO)couponList.get(recordCnt);
        if (IsStoreLevelCoupon(conn, couponDTO))
        {
          processedCount++;
          addCouponDataToBatch(couponDTO, psmt);
          runBatch = true;
        }
      }
      if (runBatch) {
        psmt.executeBatch();
      }
      psmt.close();
      conn.commit();
    }
    catch (Exception se)
    {
      logger.error("Error in coupon batch" + se);
      throw new GeneralException("addCouponData", se);
    }
    return processedCount;
  }
  
  private void addCouponDataToBatch(StoreCouponDTO couponDTO, PreparedStatement psmt)
    throws GeneralException
  {
    try
    {
      psmt.setObject(1, couponDTO.getStore());
      

      psmt.setObject(2, couponDTO.getItemUPC());
      

      SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmm");
      String dateStr = formatter.format(couponDTO.getItemDateTime());
      psmt.setObject(3, dateStr);
      

      psmt.setDouble(4, PrestoUtil.round(couponDTO.getItemNetPrice(), 2));
      

      psmt.setInt(5, couponDTO.getTransactionNo());
      
      psmt.setObject(6, couponDTO.getCustomerId());
      if (couponDTO.getItemCode() > 0) {
        psmt.setInt(7, couponDTO.getItemCode());
      } else {
        psmt.setObject(7, null);
      }
      psmt.setInt(8, couponDTO.getCalendarId());
      

      psmt.setInt(9, couponDTO.getPosDept());
      
      psmt.setInt(10, Integer.parseInt(couponDTO.getCouponType()));
      
      psmt.setString(11, couponDTO.getCouponNumber());
      psmt.setInt(12, couponDTO.getCpnFamilyCurr());
      psmt.setDouble(13, PrestoUtil.round(couponDTO.getCpnQty(), 2));
      psmt.setDouble(14, PrestoUtil.round(couponDTO.getCpnWeight(), 2));
      psmt.addBatch();
    }
    catch (Exception e)
    {
      throw new GeneralException("CouponDAO - Error in prepared Statement", e);
    }
  }
  
  private boolean IsStoreLevelCoupon(Connection conn, StoreCouponDTO couponDTO)
    throws GeneralException
  {
    boolean retVal = true;
    return retVal;
  }
  
  public void deleteCouponData(Connection conn, int calendarId, String storeNum)
    throws GeneralException
  {
    StringBuffer sb = new StringBuffer("delete from MOV_DAILY_COUPON_INFO where");
    sb.append(" CALENDAR_ID = ").append(calendarId);
    if (storeNum != null) {
      sb.append(" AND COMP_STR_NO = '").append(storeNum).append("'");
    }
    PristineDBUtil.execute(conn, sb, "TopsStoreCouponDAO - Delete");
  }
}
