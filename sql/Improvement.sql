-- Table: sales_aggr_daily_movement_temp

-- DROP TABLE sales_aggr_daily_movement_temp;

CREATE TABLE sales_aggr_daily_movement_temp
(
  calendar_id numeric,
  comp_str_no character(4),
  product_level_id numeric(4,0),
  product_id numeric(10,0),
  item_code numeric(10,0),
  sale_flag character(1),
  uom_id character(3),
  pos_department numeric(4,0),
  volume numeric,
  quantity numeric(6,0),
  weight_actual numeric,
  weight numeric(6,0),
  revenue numeric(10,2),
  grossrevenue numeric(10,2),
  salequantity numeric(6,0),
  salemovementvolume numeric,
  salegrossrevenue numeric(10,2),
  revenuesale numeric(10,2),
  regularquantity numeric(6,0),
  regmovementvolume numeric,
  reggrossrevenue numeric(10,2),
  revenueregular numeric(10,2),
  actualweight numeric(6,0),
  actualquantity numeric(6,0),
  reg_movement numeric(10,2),
  sale_movement numeric(10,2),
  reg_margin numeric(10,2),
  sale_margin numeric(10,2),
  reg_movement_vol numeric(15,2),
  sale_movement_vol numeric(15,2),
  reg_igvol_revenue numeric(15,2),
  sale_igvol_revenue numeric(15,2),
  reg_deal_cost numeric(10,2),
  sale_deal_cost numeric(10,2),
  tot_movement numeric(10,2),
  tot_revenue numeric(10,2),
  tot_margin numeric(10,2),
  tot_movement_vol numeric(15,2),
  tot_igvol_revenue numeric(15,2),
  store_type character(1),
  loyalty_card_saving numeric(8,2),
  lst_summary_daily_id numeric(12,0)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE sales_aggr_daily_movement_temp
  OWNER TO postgres;

-- Index: itemcodeindex

-- DROP INDEX itemcodeindex;

CREATE INDEX itemcodeindex
  ON sales_aggr_daily_movement_temp
  USING btree
  (item_code);

---------------------------------------------------------------------------------------------------------