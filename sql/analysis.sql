select avg(quantity_total) from 
summary_daily_mov s, schedule ss
where s.schedule_id = ss.schedule_id 
and item_code = 40023 and ss.comp_str_id = 5651
and tod_id = 4
group by dow_id

select * from summary_daily_mov

select to_date(ss.start_date) + dow_id -1 datadate , quantity_total from 
summary_daily_mov s, schedule ss
where s.schedule_id = ss.schedule_id 
and item_code = 9779 and ss.comp_str_id = 5651
and tod_id = 4
order by s.schedule_id, dow_id

select item_code, tod_id, 
sum(quantity_total - case when oos_method = 'prev day' then prev_day_exp
  when oos_method = 'last 7 days'  then last_7day_exp
  when oos_method = 'last 28 days' then last_28day_exp
  when oos_method = 'last 180 days' then last_180day_exp 
  else 0 end ) mape,
count(*) from summary_daily_mov s, schedule ss 
where 
s.schedule_id = ss.schedule_id 
and ss.comp_str_id = 5651
and (OOS_METHOD = 'prev day' or OOS_METHOD = 'last 7 days' or OOS_METHOD = 'last 28 days' or OOS_METHOD = 'last 180 days')
and item_code = 15159
group by item_code, tod_id

select * from weekly_ad_data

select start_date week_start_date, end_date week_end_date, page, block, w.retailer_item_code,
presto_item_code, item_description,
ad_location, reg_price, on_tpr, org_unit_ad_price, org_ad_retail,
display_type, upc from weekly_ad_data w, retail_calendar rc, item_lookup i where w.calendar_id = rc.calendar_id and w.presto_item_code = i.item_code

select * from summary_daily_mov where schedule_id = 6273 and item_code = 9395 and tod_id = 4

select * from item_lookup where retailer_item_code  like  '%17896' and active_indicator = 'Y'

SELECT item_code, item_name, upc, dept_id, category_id,  RETAILER_ITEM_CODE, STANDARD_UPC, sub_category_id, SEGMENT_ID from ITEM_LOOKUP WHERE  RETAILER_ITEM_CODE like '%21824' AND ACTIVE_INDICATOR='Y'



select item_code from movement_daily_OOS where item_code in ( 

select count(item_Code) from item_lookup where dept_id In (
22, /* BEAUTY CARE */
23, /*  BEVERAGE/SODA/SNACKS */
15, /*  BREAKFAST  BAKING  */
35, /*  BULK  */
36, /*  COFFEE COCOA CANDY JUICE PBJ  */
32, /*  CONDIMENT COOKIE CRACKER BREAD  */
19, /*  DAIRY  */
14, /*  ENHANCERS  */
16, /*  ETHNIC/SPEC/KOSHER/NO  */
17, /*  FROZEN  */
26, /*  GENERAL MERCHANDISE  */
28, /*  HEALTH CARE  */
18, /*  HOUSEHOLD  */
29 /*  MAIN MEAL */ )
and item_code in (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  (comp_str_no = 0108) and c.start_date > to_date(SYSDATE)-28)

and item_code in (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  (comp_str_no = 0108 or comp_str_no = 0227 or comp_str_no = 0233 or comp_str_no = 0363) and c.start_date > to_date(SYSDATE)-28)
and item_code in (select distinct item_code from movement_daily_oos m where  (comp_str_no = 0108 or comp_str_no = 0227 or comp_str_no = 0233 or comp_str_no = 0363))


select count(*) from (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0108 and c.start_date > to_date(SYSDATE)-42)
select count(*) from (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0227 and c.start_date > to_date(SYSDATE)-42)
select count(*) from (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0233 and c.start_date > to_date(SYSDATE)-42)
select count(*) from (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0363 and c.start_date > to_date(SYSDATE)-42)

select count(*) from (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0108 and c.start_date > to_date(SYSDATE)-28)
select count(*) from (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0227 and c.start_date > to_date(SYSDATE)-28)
select count(*) from (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0233 and c.start_date > to_date(SYSDATE)-28)
select count(*) from (select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0363 and c.start_date > to_date(SYSDATE)-28)


select min(pos_timestamp) from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0108 and c.start_date > to_date(SYSDATE)-14
select count(distinct pos_timestamp) from movement_daily_oos m where comp_str_no = 0108


describe item_lookup

describe weekly_ad_data

delete from weekly_ad_data where calendar_id = 0

select count(*) from weekly_ad_data
select * from weekly_ad_data where calendar_id <> 0

select distinct i.item_code from retailer_item_code_map r, item_lookup i where r.item_code = i.item_code and r.retailer_item_code like '%595150' and i.active_indicator ='Y'

SELECT I.item_code, I.item_name, I.upc, I.dept_id, I.category_id,  I.RETAILER_ITEM_CODE, I.STANDARD_UPC, I.sub_category_id, I.SEGMENT_ID from RETAILER_ITEM_CODE_MAP RM, ITEM_LOOKUP I WHERE  RM.RETAILER_ITEM_CODE like '%087303'  and RM.ITEM_CODE = I.ITEM_CODE 

Select s.item_code from summary_daily_mov s 
where s.schedule_id = 6278 and dow_id = 3 and tod_id = 4  and 
item_code not in (select presto_item_code from price_temp)

describe price_temp

Select count(distinct s.item_code) from summary_daily_mov s , movement_daily_oos m
where s.schedule_id = 6278 and dow_id = 3 and tod_id = 4 and oos_method ='prev day' and oos_ind = 'Y' and m.calendar_id = 3450 and m.comp_str_no = 0108 
and m.item_code = s.item_code
and (unit_price >=0 and price >=0) 
and m.sale_flag = s.promotional_ind and ((price <> unit_reg_price) and (unit_price <> unit_sale_price))

Select count(distinct s.item_code) from summary_daily_mov s , movement_daily_oos m
where s.schedule_id = 6278 and dow_id = 3 and tod_id = 4 and oos_method ='prev day' and oos_ind = 'Y' and m.calendar_id = 3450 and m.comp_str_no = 0108 
and m.item_code = s.item_code 
and (unit_price >=0 and price >=0) 
and m.sale_flag <> s.promotional_ind 


select count(distinct item_code) from movement_daily_oos where calendar_id = 3450 and comp_str_no = 0108



Select s.item_code, quantity_total, pred from summary_daily_mov s , sixty_four_exp e
where s.schedule_id = 6278 and dow_id = 5 and tod_id = 4
and s.item_code = e.item_code
and e.comp_str_id = 5651

select count(*) from sixty_four_exp where to_char(upc,'FM000000000000') in(select upc from item_lookup)
select * from item_lookup
select to_char(upc,'FM000000000000') from sixty_four_exp

select * from sixty_four_exp

update sixty_four_exp s set s.item_code = (select l.item_code from item_lookup l where l.upc =  to_char(s.upc,'FM000000000000'))
update sixty_four_exp set comp_str_id = 5651 where comp_str_no = '108'
update sixty_four_exp set comp_str_id = 5673 where comp_str_no = '233'
--truncate table sixty_four_exp

select * from sixty_four_exp e where comp_str_id = 5651 and to_date(pred_date,'yyyy-MM-dd') = to_date('06/01/2013','MM/dd/yyyy')

update summary_daily_mov s set s.sixty_four_exp = 
(select pred from sixty_four_exp e 
where s.item_code = e.item_code and e.comp_str_id = 5651 and to_date(pred_date,'yyyy-MM-dd') = to_date('05/30/2013','MM/dd/yyyy')
)
where s.schedule_id = 6282 and s.dow_id = 5 and s.tod_id = 4

update summary_daily_mov s set s.sixty_four_exp = 
(select pred from sixty_four_exp e 
where s.item_code = e.item_code and e.comp_str_id = 5673 and to_date(pred_date,'yyyy-MM-dd') = to_date('05/30/2013','MM/dd/yyyy')
)
where s.schedule_id = 6283 and s.dow_id = 5 and s.tod_id = 4

describe summary_daily_mov

select count(*) from summary_daily_mov where sixty_four_exp is not null and schedule_id = 6278 and dow_id = 5 and tod_id = 4
select count(*) from summary_daily_mov where sixty_four_exp is not null and schedule_id = 6279 and dow_id = 5 and tod_id = 4





select * from retail_calendar where calendar_id = 3694
select distinct(calendar_id)  from weekly_ad_data
select w.*, i.upc from weekly_ad_data w, item_lookup i where calendar_id = 3693 and w.presto_item_code = i.item_code

select ad.*, TabA.* from
(
select s.item_code item_code, rc.calendar_id calendar_id from summary_daily_mov s, schedule ss, retail_calendar rc
where 
s.schedule_id = 6283 and dow_id = 1 and tod_id = 4
and s.schedule_id = ss.schedule_id
and ss.start_date = rc.start_date 
and rc.row_type = 'W'
) TabA, 

(select w.*,il. from weekly_ad_data w, item_lookup il where ad.item_code = il.item_code) ad

where TabA.calendar_id = ad.calendar_id (+)
and TabA.item_code = ad.presto_item_code (+)


Select min(ss.start_date)  , sum(quantity_total), avg(case when promotional_Ind = 'Y' then unit_sale_price else unit_reg_price end) price from summary_daily_mov s, schedule ss where s.schedule_id  = ss.schedule_id and comp_str_id = 5670 and item_code = 40039 and tod_id = 4 group by s.schedule_id  order by s.schedule_id 
Select min(ss.start_date)  , sum(quantity_total), avg(case when promotional_Ind = 'Y' then unit_sale_price else unit_reg_price end) price from summary_daily_mov s, schedule ss where s.schedule_id  = ss.schedule_id and comp_str_id = 5670 and item_code = 35316 and tod_id = 4 group by s.schedule_id  order by s.schedule_id 
Select min(ss.start_date)  , sum(quantity_total), avg(case when promotional_Ind = 'Y' then unit_sale_price else unit_reg_price end) price from summary_daily_mov s, schedule ss where s.schedule_id  = ss.schedule_id and comp_str_id = 5670 and item_code = 15089 and tod_id = 4 group by s.schedule_id  order by s.schedule_id 

Select min(ss.start_date)  , sum(quantity_total) from summary_daily_mov s, schedule ss where s.schedule_id  = ss.schedule_id and comp_str_id = 5670 and item_code = 35316 and tod_id = 4 group by s.schedule_id  order by s.schedule_id 
Select min(ss.start_date)  , sum(quantity_total) from summary_daily_mov s, schedule ss where s.schedule_id  = ss.schedule_id and comp_str_id = 5670 and item_code = 15089 and tod_id = 4 group by s.schedule_id  order by s.schedule_id 

Select ss.start_date + dow_id -1 , quantity_total from summary_daily_mov s, schedule ss where s.schedule_id  = ss.schedule_id and comp_str_id = 5670 and item_code = 35316 and tod_id = 4 order by s.schedule_id
Select ss.start_date + dow_id -1 , quantity_total, promotional_ind, unit_sale_price, unit_reg_price from summary_daily_mov s, schedule ss where s.schedule_id  = ss.schedule_id and comp_str_id = 5670 and item_code = 15089 and tod_id = 4 order by s.schedule_id

select count(*) from sixty_four_exp 
-- 17680

--duplicates elimination in sixty four exp
select  from sixty_four_exp s
where (s.item_code,s.comp_str_id,s.pred_date) in 
(select item_code c1, comp_str_id c2, pred_date  from
(select item_code, comp_str_id, pred_date, count(pred) cc from sixty_four_exp
group by pred_date, comp_str_id, item_code)
where cc > 1
) 

order by s.item_code
select count(*) from item_lookup

select distinct item_code from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0233 and c.start_date > to_date(SYSDATE)-42
and item_code not in (select distinct item_code from summary_daily_mov s, schedule ss where s.schedule_id = ss.schedule_id and ss.comp_str_id = 5673
and ss.start_date >=  to_date(SYSDATE)-42)

select aI from 
(select tabA.item_code aI, tabB.item_code bI from
(select distinct item_code  from movement_daily_oos m, retail_calendar c where m.calendar_id = c.calendar_id and  comp_str_no = 0233 and c.start_date > to_date(SYSDATE)-42) tabA,
(select distinct item_code from summary_daily_mov s, schedule ss where s.schedule_id = ss.schedule_id and ss.comp_str_id = 5673
and ss.start_date >=  to_date(SYSDATE)-42) tabB
where tabA.item_code = tabB.item_code (+))
where bI is null

truncate table ad_info

select * from movement_daily_oos where item_code = 548409 and comp_str_no = 0233 
select * from movement_daily_oos where item_code = 797700 and comp_str_no = 0233 
select * from item_lookup where item_code = 797700
select * from item_lookup where item_code = 548409

describe weekly_ad_data

insert into ad_info_item_list 

-- expand to LIG

select ad_info_id, item_code from
(select tabAD.ad_info_id,tabAD.presto_item_code, tabAD.ret_lir_id, tabAD.category_id, il.item_code, il.item_name , tabAD.item_name from 
(select w.ad_info_id, w.presto_item_code, i.ret_lir_id, i.category_id, i.item_name 
from ad_info w, item_lookup i where w.presto_item_code = i.item_code and ret_lir_id is not null 
and  calendar_id = 3694) tabAD,
item_lookup il
where tabAD.ret_lir_id = il.ret_lir_id 
and tabAD.category_id = il.category_id
and tabAD.presto_item_code <> il.item_code
and il.item_name like concat(substr(tabAD.item_name,1,instr(tabAD.item_name,' ',1,1)),'%')
)

-- delete duplicates

delete from ad_info_item_list where item_code in
(select item_code from
(select item_code, count(ad_info_id) cc from ad_info_item_list where ad_info_id in (select ad_info_id from ad_info where calendar_id = 3694) group by item_code)
where cc > 1
)
group by item_code, ad_info_id

select count(*) from ad_info_item_list

select count(presto_item_code) from
(select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, presto_item_code from ad_info 
where calendar_id = 3694)

union
select count(distinct presto_item_code) from(
select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, item_code presto_item_code from ad_info a, ad_info_item_list b where a.ad_info_id = b.ad_info_id and a.calendar_id = 3694 
)

select * from item_lookup where retailer_item_code like '%080994'
SELECT item_code, item_name, upc, dept_id, category_id,  RETAILER_ITEM_CODE, STANDARD_UPC, sub_category_id, SEGMENT_ID from ITEM_LOOKUP WHERE  RETAILER_ITEM_CODE like '%001180'  and active_indicator = 'Y'

select * from item_lookup where retailer_item_code like '%080994'
select * from item_lookup where item_code in (select distinct item_code from retailer_item_code_map where retailer_item_code like '%080994')

select * from ad_info

select ss.start_date + dow_id -1, unit_sale_price, unit_reg_price, oos_ind, quantity_total from summary_daily_mov s, schedule ss where s.schedule_id = ss.schedule_id and ss.comp_str_id = 5670 and item_code = 40087 and oos_IND = 'Y'
order by s.schedule_id, dow_id

--------------------------------------------------
-- query to fetch regular moving items

select il.item_code, il.item_name, il.upc from
(select item_code from 
(select count(ss.start_date + dow_id -1) cc,item_code 
from summary_daily_mov s, schedule ss 
where s.schedule_id = ss.schedule_id and ss.start_date + dow_id -1 >= to_date('06/06/2013','MM/dd/yyyy') - 28
and ss.comp_str_id = 5651
and tod_id = 4
and quantity_total > 1
group by item_code
) where cc = 28) tabA, item_lookup il
where tabA.item_code = il.item_code

select item_code, item_name, upc  from item_lookup where ret_lir_id in(
select ret_lir_id from
(
select count(dt) cc, ret_lir_id from 
(select ss.start_date + dow_id -1 dt ,ret_lir_id , sum(quantity_total) qty
from summary_daily_mov s, schedule ss , item_lookup il
where s.schedule_id = ss.schedule_id and ss.start_date + dow_id -1 >= to_date('06/06/2013','MM/dd/yyyy') - 28
and ss.comp_str_id = 5651
and s.item_code = il.item_code
and il.ret_lir_id is not null
and tod_id = 4
group by ret_lir_id, ss.start_date + dow_id -1) 
where qty > 12
group by ret_lir_id
) where cc = 28
)



-----------------------------------
-- using tlog

select * from retail_calendar where to_date(start_date) = to_date('06/10/2013','MM/dd/yyyy')-14 and row_type = 'D'



select * from movement_daily_oos

--create a temp table if needed or a view.. and do it systematically. just have one months data

select distinct item_code, item_name, upc  from 
(
select il.item_code, il.item_name, il.upc from
(select item_code from
(select item_code, count(dt) dc from
(select item_code,to_date(pos_timestamp,'dd-MM-yy') dt, sum(quantity) cc 
from movement_daily_oos where comp_str_no = 0108 and 
to_date(pos_timestamp,'dd-MM-yy') >= to_date('06/09/2013','MM/dd/yyyy') - 28
group by item_code,to_date(pos_timestamp,'dd-MM-yy')
order by to_date(pos_timestamp,'dd-MM-yy'), item_code
) where cc > 1
group by item_code)
where dc >= 28) tabA, item_lookup il
where tabA.item_code = il.item_code

union 
-- LIG based
select item_code, item_name, upc  from item_lookup where ret_lir_id in(
select ret_lir_id from
(select count(dt) dc, ret_lir_id from
(select ret_lir_id,to_date(pos_timestamp,'dd-MM-yy') dt, sum(quantity) cc 
from movement_daily_oos m, item_lookup il where comp_str_no = 0108 and 
to_date(pos_timestamp,'dd-MM-yy') >= to_date('06/09/2013','MM/dd/yyyy') - 28
and m.item_code = il.item_code
and il.ret_lir_id is not null
group by ret_lir_id,to_date(pos_timestamp,'dd-MM-yy')
order by to_date(pos_timestamp,'dd-MM-yy'), ret_lir_id)
where cc > 12
group by ret_lir_id)
where dc = 28)
and item_code in
(select distinct item_code from movement_daily_oos where comp_str_no  = 0108 and 
to_date(pos_timestamp,'dd-MM-yy') >= to_date('06/09/2013','MM/dd/yyyy') - 14 and quantity > 0)
)

select * from category



select total_visit_count, mov_visit_ratio, ss.start_date + dow_id -1, tod_id, promotional_ind,unit_sale_price, unit_reg_price, oos_ind, quantity_total 
from summary_daily_mov s, schedule ss where s.schedule_id = ss.schedule_id and ss.comp_str_id = 5651 and item_code = 893175
and tod_id = 4
order by s.schedule_id, dow_id

select * from retail_calendar

select * from movement_daily_oos where item_code = 36212  and calendar_id = 3505 and comp_str_no = 0227

select total_visit_count, mov_visit_ratio, ss.start_date + dow_id -1, tod_id, promotional_ind,unit_sale_price, unit_reg_price, oos_ind, quantity_total 
, ad.*
from summary_daily_mov s, schedule ss , retail_calendar rc ,
(select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, presto_item_code, calendar_id from
(select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, presto_item_code, calendar_id 
from ad_info a 

union
select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, item_code presto_item_code , calendar_id
from ad_info a, ad_info_item_list b 
where a.ad_info_id = b.ad_info_id 
)) ad
where 
s.schedule_id = ss.schedule_id 
and ss.start_date = rc.start_date
and ss.comp_str_id = 5651 
and item_code = 893175
--23107
 --893172
and s.item_code = ad.presto_item_code (+)
and rc.calendar_id = ad.calendar_id 
and tod_id = 4
order by s.schedule_id, dow_id

