define the_store_id = 5670;
--define ad_calendar_id = 3693; 
define curr_schedule_id = 6370; 
define ddod_id = 5;

select comp_str_no,analysis_date, dept_name, cat_name, seg_name, TableA.retailer_item_code, item_desc, tod_desc, sale_flag,
TableA.quantity,
case
when TableA.oos_method = 'last 7 days' then TableA.last_7day_exp
when TableA.oos_method = 'last 28 days' then TableA.last_28day_exp
when TableA.oos_method = 'last 120 days' then TableA.last_180day_exp
when TableA.oos_method = 'last 42 days avg' then TableA.last_7day_avg_exp
when TableA.oos_method = 'prev day' then TableA.prev_day_exp
else 0
end exp_used
,TableA.oos_method,
--,round(exp_quantity), 
case when TableA.last_7day_obs >= 3 then TableA.last_7day_exp else null end last_7day_exp, 
case when TableA.last_28day_obs >=7 then TableA.last_28day_exp else null end last_28day_exp, 
case when TableA.last_180day_obs >=14 then TableA.last_180day_exp else null end last_180day_exp, 
prev_day_exp,
TableA.last_7day_avg_exp,
TableA.last_7day_obs,TableA.last_7day_days_moved, 
TableA.last_28day_obs,TableA.last_28day_days_moved,
TableA.last_180day_obs,TableA.last_180day_days_moved,TableA.last_7day_avg_obs,
TableA.oos_ind,
TableA.unit_price, TableA.unit_reg_price,
TableA.purchase_count
--, exp_purchase_count, 
,TableA.total_visit_count,
--, exp_total_visit_count
--days_moved_last_week,
--round(q1a) avgQ,

MVD_TWO_PLUS_LAST_N_DAYS
,ooscl
--, TableA.oos_score + days_moved_last_week score
, TableA.presto_item_code
--, round(exp_quantityOld)
, ship.shipment_date, ship.quantity
,mape,sigma_used,sixty_four_exp
, ad.retailer_item_code, ad.store_location,ad.item_description, ad.org_unit_ad_price, ad.org_ad_retail, ad.display_type


from 
(select  cmp.comp_str_no comp_str_no,  sc.start_date + &&ddod_id  -1 analysis_date,dep.name dept_name, cat.name cat_name, seg.name seg_name, 
s.item_code presto_item_code, i.retailer_item_code retailer_item_code,item_name  || ' ' || item_size || ' ' || u.name  item_desc, 
s.tod_id tod, t.tod_desc tod_desc, s.promotional_ind sale_flag,
(s.quantity_sale + s.quantity_reg ) quantity,
--(case when s.promotional_ind = 'Y' then s.quantity_sale else s.quantity_reg end) quantity,
s.quantity_sale q1,s.quantity_reg q2,
--(case when s.promotional_ind = 'Y' then e.exp_quantity_sale else e.exp_quantity_reg end) exp_quantityOld,
(case when s.promotional_ind = 'Y' then s.unit_sale_price else s.unit_reg_price  end) unit_price,
s.unit_reg_price  unit_reg_price,
s.visit_count purchase_count,
--(case when s.promotional_ind = 'Y' then e.exp_sale_visit_count else e.exp_reg_visit_count end) exp_purchase_count ,
s.total_visit_count total_visit_count,
--e.exp_total_visit_count exp_total_visit_count,
s.oos_ind oos_ind
,MVD_TWO_PLUS_LAST_N_DAYS, oosl.name ooscl
, s.oos_score, last_7day_exp,last_7day_obs,last_28day_exp,last_28day_obs,last_7day_avg_exp,last_7day_avg_obs,
last_180day_exp,last_180day_obs,last_7day_days_moved,last_28day_days_moved, last_180day_days_moved,oos_method,
--(case when s.promotional_ind = 'Y' then e.OOS_MOV_VISIT_RATIO_SALE*s.total_visit_count else e.OOS_MOV_VISIT_RATIO_REG*s.total_visit_count end) exp_quantity,
prev_day_exp, mape, s.tod_id,sigma_used,sixty_four_exp
from summary_daily_mov s, 
schedule sc, 
item_lookup i,
time_of_day_lookup t,
--summary_daily_mov_exp e,
department dep, 
category cat, 
item_segment seg, 
competitor_store cmp
, oos_classification_lookup oosl
, uom_lookup u
where 
s.schedule_id = &&curr_schedule_id and s.dow_id = &&ddod_id
--and s.item_code = e.item_code
and s.schedule_id = sc.schedule_id
--and s.dow_id = e.dow_id
--and s.tod_id = e.tod_id
and s.item_code = i.item_code
and s.tod_id = t.tod_id
and i.dept_id = dep.id
and i.category_id = cat.id
and i.segment_id = seg.id
and sc.comp_str_id = cmp.comp_str_id
--and s.oos_ind = 'Y'
--and e.store_id = &&the_store_id
and s.oos_classification_id = oosl.id (+)
and i.uom_id = u.id (+)
order by s.item_code, s.tod_id
) tableA
,
(Select shipment_date, item_code, quantity from shipping_info where 
store_id = &&the_store_id and latest_ind = 'Y' and quantity >0
) ship
,


(select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, presto_item_code from
(select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, presto_item_code 
from ad_info a, retail_calendar rc, schedule ws where ws.schedule_id = &&curr_schedule_id and a.calendar_id = rc.calendar_id and rc.start_date = ws.start_date
union
select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, item_code presto_item_code 
from ad_info a, ad_info_item_list b, retail_calendar rc, schedule ws 
where a.ad_info_id = b.ad_info_id and ws.schedule_id = &&curr_schedule_id and a.calendar_id = rc.calendar_id and rc.start_date = ws.start_date
)) ad


where  tableA.presto_item_code = ship.item_code (+)
and tableA.presto_item_code = ad.presto_item_code (+)
order by dept_name,cat_name,seg_name,item_desc, tableA.tod_id
/*
select * from schedule where start_date = to_date('03/17/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('03/24/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('03/31/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('04/07/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('04/14/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('04/21/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('04/28/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('05/05/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('05/12/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('05/19/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('05/26/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('06/02/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('06/09/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('06/16/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('06/23/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('06/30/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('07/07/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('07/14/2013','MM/DD/YYYY') and comp_str_id = 5670
select * from schedule where start_date = to_date('07/21/2013','MM/DD/YYYY') and comp_str_id = 5670
*/
/*

*/
