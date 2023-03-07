package com.pristine.dto.offermgmt.promotion;

import com.pristine.dto.offermgmt.ProductKey;

public class PromoAnalysisDTO {

	private int promo_analysis_id;
	private int location_level_id;
	private int location_id;
	private int product_level_id;
	private int product_id;
	private long promo_definition_id;
	private String promo_group_id;
	private int promo_type_id;
	private String offer_type;
	private double sale_price;
	private int sale_qty;
	private double reg_unit_price;
	private double sale_unit_price;
	private int total_units_avg;
	private double total_revenue_avg;
	private double total_margin_avg;
	private int total_households_avg;
	private int unit_ch_avg;
	private double revenue_ch_avg;
	private double margin_ch_avg;
	private int unit_ch_analyzed_avg;
	private double revenue_ch_analyzed_avg;
	private int margin_ch_analyzed_avg;
	private int households_analyzed_avg;
	private int gross_incremental_units_avg;
	private int giu_a_avg;
	private int giu_b_avg;
	private int giu_c_avg;
	private int giu_d_avg;
	private int giu_e_avg;
	private int giu_f_avg;
	private double gross_incremental_sales_avg;
	private double gross_incremental_margin_avg;
	private int net_incremental_units_avg;
	private double net_incremental_sales_avg;
	private double net_incremental_margin_avg;
	private int base_units_per_hh_avg;
	private int units_per_hh_avg;
	private int hh_avaling_promo_avg;
	private int hh_availing_same_lir_avg;
	private int start_calendar_id;
	private int end_calendar_id;
	private int base_units_per_hh_typical;
	private int unit_per_hh_typical;
	private String updated;
	private String is_active;
	private String offer_name;
	private double offer_value;
	private int no_of_weeks;
	private boolean isGroupPromotion;
	private ProductKey productKey;
	public int getPromo_analysis_id() {
		return promo_analysis_id;
	}

	public void setPromo_analysis_id(int promo_analysis_id) {
		this.promo_analysis_id = promo_analysis_id;
	}

	public int getLocation_level_id() {
		return location_level_id;
	}

	public void setLocation_level_id(int location_level_id) {
		this.location_level_id = location_level_id;
	}

	public int getLocation_id() {
		return location_id;
	}

	public void setLocation_id(int location_id) {
		this.location_id = location_id;
	}

	public int getProduct_level_id() {
		return product_level_id;
	}

	public void setProduct_level_id(int product_level_id) {
		this.product_level_id = product_level_id;
	}

	public int getProduct_id() {
		return product_id;
	}

	public void setProduct_id(int product_id) {
		this.product_id = product_id;
	}

	public long getPromo_definition_id() {
		return promo_definition_id;
	}

	public void setPromo_definition_id(long promo_definition_id) {
		this.promo_definition_id = promo_definition_id;
	}

	public String getPromo_group_id() {
		return promo_group_id;
	}

	public void setPromo_group_id(String promo_group_id) {
		this.promo_group_id = promo_group_id;
	}

	public int getPromo_type_id() {
		return promo_type_id;
	}

	public void setPromo_type_id(int promo_type_id) {
		this.promo_type_id = promo_type_id;
	}

	public String getOffer_type() {
		return offer_type;
	}

	public void setOffer_type(String offer_type) {
		this.offer_type = offer_type;
	}

	public double getSale_price() {
		return sale_price;
	}

	public void setSale_price(double sale_price) {
		this.sale_price = sale_price;
	}

	public int getSale_qty() {
		return sale_qty;
	}

	public void setSale_qty(int sale_qty) {
		this.sale_qty = sale_qty;
	}

	public double getReg_unit_price() {
		return reg_unit_price;
	}

	public void setReg_unit_price(double reg_unit_price) {
		this.reg_unit_price = reg_unit_price;
	}

	public double getSale_unit_price() {
		return sale_unit_price;
	}

	public void setSale_unit_price(double sale_unit_price) {
		this.sale_unit_price = sale_unit_price;
	}

	public int getTotal_units_avg() {
		return total_units_avg;
	}

	public void setTotal_units_avg(int total_units_avg) {
		this.total_units_avg = total_units_avg;
	}

	public double getTotal_revenue_avg() {
		return total_revenue_avg;
	}

	public void setTotal_revenue_avg(double total_revenue_avg) {
		this.total_revenue_avg = total_revenue_avg;
	}

	public double getTotal_margin_avg() {
		return total_margin_avg;
	}

	public void setTotal_margin_avg(double total_margin_avg) {
		this.total_margin_avg = total_margin_avg;
	}

	public int getTotal_households_avg() {
		return total_households_avg;
	}

	public void setTotal_households_avg(int total_households_avg) {
		this.total_households_avg = total_households_avg;
	}

	public int getUnit_ch_avg() {
		return unit_ch_avg;
	}

	public void setUnit_ch_avg(int unit_ch_avg) {
		this.unit_ch_avg = unit_ch_avg;
	}

	public double getRevenue_ch_avg() {
		return revenue_ch_avg;
	}

	public void setRevenue_ch_avg(double revenue_ch_avg) {
		this.revenue_ch_avg = revenue_ch_avg;
	}

	public double getMargin_ch_avg() {
		return margin_ch_avg;
	}

	public void setMargin_ch_avg(double margin_ch_avg) {
		this.margin_ch_avg = margin_ch_avg;
	}

	public int getUnit_ch_analyzed_avg() {
		return unit_ch_analyzed_avg;
	}

	public void setUnit_ch_analyzed_avg(int unit_ch_analyzed_avg) {
		this.unit_ch_analyzed_avg = unit_ch_analyzed_avg;
	}

	public double getRevenue_ch_analyzed_avg() {
		return revenue_ch_analyzed_avg;
	}

	public void setRevenue_ch_analyzed_avg(double revenue_ch_analyzed_avg) {
		this.revenue_ch_analyzed_avg = revenue_ch_analyzed_avg;
	}

	public int getMargin_ch_analyzed_avg() {
		return margin_ch_analyzed_avg;
	}

	public void setMargin_ch_analyzed_avg(int margin_ch_analyzed_avg) {
		this.margin_ch_analyzed_avg = margin_ch_analyzed_avg;
	}

	public int getHouseholds_analyzed_avg() {
		return households_analyzed_avg;
	}

	public void setHouseholds_analyzed_avg(int households_analyzed_avg) {
		this.households_analyzed_avg = households_analyzed_avg;
	}

	public int getGross_incremental_units_avg() {
		return gross_incremental_units_avg;
	}

	public void setGross_incremental_units_avg(int gross_incremental_units_avg) {
		this.gross_incremental_units_avg = gross_incremental_units_avg;
	}

	public int getGiu_a_avg() {
		return giu_a_avg;
	}

	public void setGiu_a_avg(int giu_a_avg) {
		this.giu_a_avg = giu_a_avg;
	}

	public int getGiu_b_avg() {
		return giu_b_avg;
	}

	public void setGiu_b_avg(int giu_b_avg) {
		this.giu_b_avg = giu_b_avg;
	}

	public int getGiu_c_avg() {
		return giu_c_avg;
	}

	public void setGiu_c_avg(int giu_c_avg) {
		this.giu_c_avg = giu_c_avg;
	}

	public int getGiu_d_avg() {
		return giu_d_avg;
	}

	public void setGiu_d_avg(int giu_d_avg) {
		this.giu_d_avg = giu_d_avg;
	}

	public int getGiu_e_avg() {
		return giu_e_avg;
	}

	public void setGiu_e_avg(int giu_e_avg) {
		this.giu_e_avg = giu_e_avg;
	}

	public int getGiu_f_avg() {
		return giu_f_avg;
	}

	public void setGiu_f_avg(int giu_f_avg) {
		this.giu_f_avg = giu_f_avg;
	}

	public double getGross_incremental_sales_avg() {
		return gross_incremental_sales_avg;
	}

	public void setGross_incremental_sales_avg(double gross_incremental_sales_avg) {
		this.gross_incremental_sales_avg = gross_incremental_sales_avg;
	}

	public double getGross_incremental_margin_avg() {
		return gross_incremental_margin_avg;
	}

	public void setGross_incremental_margin_avg(double gross_incremental_margin_avg) {
		this.gross_incremental_margin_avg = gross_incremental_margin_avg;
	}

	public int getNet_incremental_units_avg() {
		return net_incremental_units_avg;
	}

	public void setNet_incremental_units_avg(int net_incremental_units_avg) {
		this.net_incremental_units_avg = net_incremental_units_avg;
	}

	public double getNet_incremental_sales_avg() {
		return net_incremental_sales_avg;
	}

	public void setNet_incremental_sales_avg(double net_incremental_sales_avg) {
		this.net_incremental_sales_avg = net_incremental_sales_avg;
	}

	public double getNet_incremental_margin_avg() {
		return net_incremental_margin_avg;
	}

	public void setNet_incremental_margin_avg(double net_incremental_margin_avg) {
		this.net_incremental_margin_avg = net_incremental_margin_avg;
	}

	public int getBase_units_per_hh_avg() {
		return base_units_per_hh_avg;
	}

	public void setBase_units_per_hh_avg(int base_units_per_hh_avg) {
		this.base_units_per_hh_avg = base_units_per_hh_avg;
	}

	public int getUnits_per_hh_avg() {
		return units_per_hh_avg;
	}

	public void setUnits_per_hh_avg(int units_per_hh_avg) {
		this.units_per_hh_avg = units_per_hh_avg;
	}

	public int getHh_avaling_promo_avg() {
		return hh_avaling_promo_avg;
	}

	public void setHh_avaling_promo_avg(int hh_avaling_promo_avg) {
		this.hh_avaling_promo_avg = hh_avaling_promo_avg;
	}

	public int getHh_availing_same_lir_avg() {
		return hh_availing_same_lir_avg;
	}

	public void setHh_availing_same_lir_avg(int hh_availing_same_lir_avg) {
		this.hh_availing_same_lir_avg = hh_availing_same_lir_avg;
	}

	public int getStart_calendar_id() {
		return start_calendar_id;
	}

	public void setStart_calendar_id(int start_calendar_id) {
		this.start_calendar_id = start_calendar_id;
	}

	public int getEnd_calendar_id() {
		return end_calendar_id;
	}

	public void setEnd_calendar_id(int end_calendar_id) {
		this.end_calendar_id = end_calendar_id;
	}

	public boolean isGroupPromotion() {
		return isGroupPromotion;
	}

	public void setGroupPromotion(boolean isGroupPromotion) {
		this.isGroupPromotion = isGroupPromotion;
	}


	public ProductKey getProductKey() {
		if(productKey == null) {
			this.productKey = new ProductKey(product_level_id, product_id);
		}
		return 	productKey;
	}

	public void setProductKey(ProductKey productKey) {
		this.productKey = productKey;
	}
	
	@Override
	public String toString(){
		return "PromoAnalysisDTO [avgIncrementalUnits=" + net_incremental_units_avg + ", avgIncrementalSales="
				+ net_incremental_sales_avg + ", avgIncrementalMargin=" + net_incremental_margin_avg + "]";
	}

	public int getBase_units_per_hh_typical() {
		return base_units_per_hh_typical;
	}

	public void setBase_units_per_hh_typical(int base_units_per_hh_typical) {
		this.base_units_per_hh_typical = base_units_per_hh_typical;
	}

	public int getUnit_per_hh_typical() {
		return unit_per_hh_typical;
	}

	public void setUnit_per_hh_typical(int unit_per_hh_typical) {
		this.unit_per_hh_typical = unit_per_hh_typical;
	}

	public String getUpdated() {
		return updated;
	}

	public void setUpdated(String updated) {
		this.updated = updated;
	}

	public String getIs_active() {
		return is_active;
	}

	public void setIs_active(String is_active) {
		this.is_active = is_active;
	}

	public String getOffer_name() {
		return offer_name;
	}

	public void setOffer_name(String offer_name) {
		this.offer_name = offer_name;
	}

	public double getOffer_value() {
		return offer_value;
	}

	public void setOffer_value(double offer_value) {
		this.offer_value = offer_value;
	}

	public int getNo_of_weeks() {
		return no_of_weeks;
	}

	public void setNo_of_weeks(int no_of_weeks) {
		this.no_of_weeks = no_of_weeks;
	}
}
