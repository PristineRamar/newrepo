select * from retail_calendar where start_date = to_date('10/13/2012','MM/dd/yyyy')


select location_id, product_id,  sum(reg_revenue), sum(sale_movement), avg(avg_order_size), sum(tot_visit_cnt) 
from item_metric_summary where location_id = 5651 and calendar_id >= 281 and calendar_id <= 287
--and product_id = 15159
group by location_id, product_id

select * from item_metric_summary where location_id = 5651 and calendar_id >= 281 and calendar_id <= 287
and product_id = 15159
group by location_id, product_id