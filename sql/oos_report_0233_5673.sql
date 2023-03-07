define the_store_id = 5673;
--define curr_date = to_date('04/03/2013','MM/DD/YYYY');

define curr_schedule_id = 6274; 
define ddod_id = 1;
define prev_schedule_id =  6271;
define sprev_schedule_id =  6271;  
define prev_dod_id = case when &&ddod_id = 1 then 7 else &&ddod_id - 1 end ;
/*
define curr_cal_start_date = &&curr_date;


select start_date as curr_cal_start_date from retail_calendar where start_date <= &&curr_date and end_date >= &&curr_date and row_type='W';
define ddod_id = (&&curr_date - &&curr_cal_start_date) +1; 
select schedule_id as curr_schedule_id from schedule s, retail_calendar rc where rc.start_date = &&curr_cal_start_date and s.start_date = rc.start_date and row_type = 'W' and comp_str_id = &&the_store_id; 


define prev_dod_id = case when &&ddod_id = 1 then 7 else &&ddod_id - 1 end ;
select schedule_id as prev_schedule_id from schedule s, retail_calendar rc where rc.start_date = (&&curr_cal_start_date-7) and s.start_date = rc.start_date and row_type = 'W' and comp_str_id = &&the_store_id; 
*/

select comp_str_no,analysis_date, dept_name, cat_name, seg_name, retailer_item_code, item_desc, tod_desc, sale_flag,
TableA.quantity,
--,round(exp_quantity), 
case when TableA.last_7day_obs > 3 then TableA.last_7day_exp else null end last_7day_exp, 
TableA.last_7day_obs,TableA.last_7day_days_moved, TableA.oos_method,
case when TableA.last_28day_obs >=7 then TableA.last_28day_exp else null end last_28day_exp, 
TableA.last_28day_obs,TableA.last_28day_days_moved,
case when TableA.last_180day_obs >=14 then TableA.last_180day_exp else null end last_180day_exp, 
TableA.last_180day_obs,TableA.last_180day_days_moved,
prev_day_exp,
TableA.oos_ind,
TableA.unit_price, TableA.unit_reg_price,
TableA.purchase_count
--, exp_purchase_count, 
,TableA.total_visit_count,
--, exp_total_visit_count


/*
case when 
sprev.promotional_ind = sale_flag and 
((TableA.unit_price = sprev.unit_sale_price and sprev.promotional_ind = 'Y')
or
(TableA.unit_price = sprev.unit_reg_price and sprev.promotional_ind = 'N'))
then 
(case when sprev.promotional_ind = 'Y' then sprev.quantity_sale else sprev.quantity_reg end)  
else null end  prev_day_quantity,
*/

--days_moved_last_week,
--round(q1a) avgQ,

(case when presto_item_code in (select item_code from price_check_list_items where price_check_list_id = 223) then 'Y' else 'N' end) top95
,ooscl
--, TableA.oos_score + days_moved_last_week score
, presto_item_code
--, round(exp_quantityOld)
, ship.shipment_date, ship.quantity
,mape,sigma_used
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
, oosl.name ooscl
, s.oos_score, last_7day_exp,last_7day_obs,last_28day_exp,last_28day_obs,
last_180day_exp,last_180day_obs,last_7day_days_moved,last_28day_days_moved, last_180day_days_moved,oos_method,
--(case when s.promotional_ind = 'Y' then e.OOS_MOV_VISIT_RATIO_SALE*s.total_visit_count else e.OOS_MOV_VISIT_RATIO_REG*s.total_visit_count end) exp_quantity,
prev_day_exp, mape, s.tod_id,sigma_used
/*
,
sprev.quantity_sale + sprev.quantity_reg prev_day_quantity
*/
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
/*
,
(
select ic, pi, unit_priceP, count(*) days_moved_last_week , avg(q1) q1a from
(select distinct item_code ic, schedule_id si, dow_id d, (quantity_sale+quantity_reg) q1, promotional_ind pi, 
(case when promotional_ind = 'Y' then unit_sale_price else unit_reg_price  end) unit_priceP from
summary_daily_mov wprev 
where 
(
(schedule_id = &&prev_schedule_id and (  dow_id >= &ddod_id   )) 
or
(schedule_id = &&curr_schedule_id and (  dow_id < &&ddod_id    )) 
)
and tod_id = 4
)
group by ic, pi,unit_priceP
)
tableB
*/
,
(Select shipment_date, item_code, quantity from shipping_info where 
store_id = &&the_store_id and latest_ind = 'Y' and quantity >0
) ship
/*
where tableA.presto_item_code = tableB.ic (+)
and tableA.sale_flag = tableB.pi (+)
and tableA.unit_price = tableB.unit_priceP (+)
*/

where  tableA.presto_item_code = ship.item_code (+)
order by dept_name,cat_name,seg_name,item_desc, tableA.tod_id
/*
select * from schedule where start_date = to_date('03/17/2013','MM/DD/YYYY') and comp_str_id = 5673
select * from schedule where start_date = to_date('03/24/2013','MM/DD/YYYY') and comp_str_id = 5673
select * from schedule where start_date = to_date('03/31/2013','MM/DD/YYYY') and comp_str_id = 5673
select * from schedule where start_date = to_date('04/07/2013','MM/DD/YYYY') and comp_str_id = 5673
select * from schedule where start_date = to_date('04/14/2013','MM/DD/YYYY') and comp_str_id = 5673
select * from schedule where start_date = to_date('04/21/2013','MM/DD/YYYY') and comp_str_id = 5673
select * from schedule where start_date = to_date('04/28/2013','MM/DD/YYYY') and comp_str_id = 5673
select * from schedule where start_date = to_date('05/05/2013','MM/DD/YYYY') and comp_str_id = 5673
select * from schedule where start_date = to_date('05/12/2013','MM/DD/YYYY') and comp_str_id = 5673
*/