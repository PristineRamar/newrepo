
insert into ad_info_item_list 

-- expand to LIG

select ad_info_id, item_code from
(select tabAD.ad_info_id,tabAD.presto_item_code, tabAD.ret_lir_id, tabAD.category_id, il.item_code, il.item_name , tabAD.item_name from 
(select w.ad_info_id, w.presto_item_code, i.ret_lir_id, i.category_id, i.item_name 
from ad_info w, item_lookup i where w.presto_item_code = i.item_code and ret_lir_id is not null 
and  calendar_id = 3688 and presto_item_code = 23695) tabAD,
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


delete from ad_info_item_list where item_code in (select presto_item_code from ad_info where calendar_id = 3694)

select count(distinct presto_item_code) from
(select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, presto_item_code from ad_info 
where calendar_id = 3691

union

select retailer_item_code, store_location, item_description, org_unit_ad_price, org_ad_retail, display_type, item_code presto_item_code 
from ad_info a, ad_info_item_list b where a.ad_info_id = b.ad_info_id and a.calendar_id = 3691
)

select * from ad_info_item_list where ad_info_id = 2553

select distinct(calendar_id) from ad_info
select * from ad_info where calendar_id = 3694 and presto_item_code = 899076
select * from ad_info_item_list where ad_info_id = 1218
select * from item_lookup where item_code = 899076


select start_date week_start_date, end_date week_end_date, page, block, w.retailer_item_code,
presto_item_code, item_description,
ad_location, reg_price, on_tpr, org_unit_ad_price, org_ad_retail,
display_type, upc from weekly_ad_data w, retail_calendar rc, item_lookup i 
where w.calendar_id = rc.calendar_id and w.presto_item_code = i.item_code


select start_date week_start_date, end_date week_end_date,page, block, adinfo.retailer_item_code, presto_item_code, i.item_name,ad_location, 
reg_price, on_tpr, org_unit_ad_price, org_ad_retail, 
display_type,upc from

(select calendar_id, page, block, a.retailer_item_code, presto_item_code, ad_location, reg_price, on_tpr, org_unit_ad_price, org_ad_retail, 
display_type from ad_info a

union

select a.calendar_id,page, block, a.retailer_item_code, b.item_code presto_item_code, ad_location, reg_price, on_tpr, org_unit_ad_price, org_ad_retail, 
display_type 
from ad_info a, ad_info_item_list b  where a.ad_info_id = b.ad_info_id
) adinfo, retail_calendar rc, item_lookup i
where adinfo.calendar_id = rc.calendar_id and
adinfo.presto_item_code = i.item_code
and adinfo.calendar_id = 3702
--and rc.start_date > to_date('01/01/2013','MM/DD/YYYY')
order by adinfo.calendar_id
