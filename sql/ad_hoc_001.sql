

select  distinct item_code, item_name, upc  from 
(
select il.item_code, il.item_name, il.upc from
(select item_code from
(select item_code, count(dt) dc from
(select item_code,calendar_id dt, sum(quantity) cc 
from movement_daily_oos where comp_str_no = 0233 and 
calendar_id >= 3442 and calendar_id < 3470
group by item_code,calendar_id
order by calendar_id, item_code
) where cc > 1
group by item_code)
where dc = 28) tabA, item_lookup il
where tabA.item_code = il.item_code

union all
-- LIG based
select  tabAA.item_code, item_name, upc from
(select item_code, item_name, upc  from item_lookup where (ret_lir_id, segment_id,substr(item_name,1,6)) in(
select ret_lir_id , segment_id, nm from
(select count(dt) dc, ret_lir_id , segment_id, nm from
(
select ret_lir_id,segment_id, substr(item_name,1,6) nm,calendar_id dt, sum(quantity) cc 
from movement_daily_oos m, item_lookup il where comp_str_no = 0233 and 
calendar_id >= 3442 and calendar_id < 3470
and m.item_code = il.item_code
and il.ret_lir_id is not null
group by ret_lir_id,segment_id, substr(item_name,1,6),calendar_id

) 
where cc > 8
group by ret_lir_id, segment_id, nm)
where dc =28)) tabAA
--, 

--(select distinct item_code from movement_daily_oos where comp_str_no  = 0233 and 
--calendar_id >= 3456 and calendar_id < 3470 and quantity > 0)tabBB
--where tabAA.item_code = tabBB.item_code

)

---108



select  distinct item_code, item_name, upc  from 
(
select  dep.name dept_name, cat.name cat_name, seg.name seg_name,il.item_name, il.upc, il.item_code from
(select item_code from
(select item_code, count(dt) dc from
(select item_code,calendar_id dt, sum(quantity) cc 
from movement_daily_oos where comp_str_no = 0108 and 
calendar_id >= 3442 and calendar_id < 3470
group by item_code,calendar_id
order by calendar_id, item_code
) where cc >= 1
group by item_code)
where dc = 28) tabA, item_lookup il, department dep, 
category cat, 
item_segment seg 

where tabA.item_code = il.item_code
and il.dept_id = dep.id
and il.category_id = cat.id
and il.segment_id = seg.id

and 
il.dept_id In (
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

union all
-- LIG based
select  dept_name, cat_name, seg_name,tabAA.item_code, item_name, upc from
(select dep.name dept_name, cat.name cat_name, seg.name seg_name,item_code, item_name, upc  from item_lookup, department dep, 
category cat, 
item_segment seg 
where  item_lookup.dept_id = dep.id
and item_lookup.category_id = cat.id
and item_lookup.segment_id = seg.id 
and (ret_lir_id, segment_id,substr(item_name,1,6)) in(
select ret_lir_id , segment_id, nm from
(select count(dt) dc, ret_lir_id , segment_id, nm from
(
select ret_lir_id,segment_id, substr(item_name,1,6) nm,calendar_id dt, sum(quantity) cc 
from movement_daily_oos m, item_lookup il where comp_str_no = 0108 and 
calendar_id >= 3442 and calendar_id < 3470
and m.item_code = il.item_code
and il.ret_lir_id is not null
and
dept_id In (
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
group by ret_lir_id,segment_id, substr(item_name,1,6),calendar_id

) 
where cc > 22
group by ret_lir_id, segment_id, nm)
where dc >=28)) tabAA, 

(select distinct item_code from movement_daily_oos where comp_str_no  = 0108 and 
calendar_id >= 3456 and calendar_id < 3470 and quantity > 0)tabBB
where tabAA.item_code = tabBB.item_code

)





