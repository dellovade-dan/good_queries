-- BDMS
with bdms as (
select distinct 
bdm_id
,bdm_name
,bdm_email
,bdm_abbv_email
,bdm_type
,territory
,area
,region
from ADL_DW_PROD.ANALYTICS_360.DIM_BDM
WHERE 
BDM_TYPE = 'Facial-BDM'
)
, bdms_shipto_soldto as (
select distinct 
b.shipto_id
,b.soldto_id
,b.user_id
,b.user_name
from 
ADL_DW_UAT.AA_CORE_METRICS_BASE.GEO_USER_SHIPTO b 
join bdms on b.user_id = bdms.bdm_id
)
,
py_cte as (
select distinct 
user_id
,a.shipto_id
,master_brand_name
,dim_consumer_id
,date_agg_type

from 
ADL_DW_UAT.AA_CORE_METRICS_BASE.SOLDTO_SHIPTO_METRICS_TIME_PERIOD a 
join bdms_shipto_soldto b on a.shipto_id = b.shipto_id
where pyflag = 1 
)
,
new_py_cte as ( -- new to practice and brand 
select 
b.user_id
,a.shipto_id
,master_brand_name
,dim_consumer_id
,date_agg_type

from 
ADL_DW_uat.AA_CORE_METRICS_BASE.SOLDTO_SHIPTO_METRICS_TIME_PERIOD a 
join bdms_shipto_soldto b on a.shipto_id = b.shipto_id
where py_new_to_practice_brand is not null
)
,
new_brand_py_cte as ( -- new to just brand 
select 
b.user_id
,a.shipto_id
,master_brand_name
,dim_consumer_id
,date_agg_type

from 
ADL_DW_uat.AA_CORE_METRICS_BASE.SOLDTO_SHIPTO_METRICS_TIME_PERIOD a 
join bdms_shipto_soldto b on a.shipto_id = b.shipto_id
where py_new_to_brand is not null
)
,cy_cte as ( -- current year any treatment 
select distinct 
b.user_id
,a.shipto_id
,master_brand_name
,dim_consumer_id
,date_agg_type

from 
ADL_DW_UAT.AA_CORE_METRICS_BASE.SOLDTO_SHIPTO_METRICS_TIME_PERIOD a 
join bdms_shipto_soldto b on a.shipto_id = b.shipto_id
where cyflag = 1 
)
, pre as ( -- total retention 
select distinct 
py_cte.user_id
,py_cte.master_brand_name
,py_cte.dim_consumer_id
,py_cte.date_agg_type
,cy_cte.dim_consumer_id  as retained_dim_consumer_id
from py_cte 
left join cy_cte on py_cte.dim_consumer_id   = cy_cte.dim_consumer_id 
                and py_cte.user_id           = cy_cte.user_id
                and py_cte.shipto_id         = cy_cte.shipto_id
                and py_cte.master_brand_name = cy_cte.master_brand_name    
)
, new_pre as ( -- new to practice and brand 
select distinct 
new_py_cte.USER_ID
,new_py_cte.master_brand_name
,new_py_cte.dim_consumer_id
,new_py_cte.date_agg_type
,cy_cte.dim_consumer_id  as retained_dim_consumer_id
from new_py_cte 
left join cy_cte on new_py_cte.dim_consumer_id    = cy_cte.dim_consumer_id 
                 and new_py_cte.user_id           = cy_cte.user_id
                 and new_py_cte.shipto_id         = cy_cte.shipto_id
                 and new_py_cte.master_brand_name = cy_cte.master_brand_name    

)
, new_brand_pre as (  -- new to just brand 
select distinct 
new_brand_py_cte.USER_ID
,new_brand_py_cte.master_brand_name
,new_brand_py_cte.dim_consumer_id
,new_brand_py_cte.date_agg_type
,cy_cte.dim_consumer_id  as retained_dim_consumer_id
from new_brand_py_cte 
left join cy_cte on new_brand_py_cte.dim_consumer_id    = cy_cte.dim_consumer_id 
                 and new_brand_py_cte.user_id           = cy_cte.user_id
                 and new_brand_py_cte.shipto_id         = cy_cte.shipto_id
                 and new_brand_py_cte.master_brand_name = cy_cte.master_brand_name    
)
,total_retention as (
select distinct 
user_id 
,master_brand_name
,count(distinct dim_consumer_id) as N_total_consumers
,count(distinct retained_dim_consumer_id) as n_retained_consumers
from pre 

where 
master_brand_name = 'BOTOXCOSMETIC'
group by all 
)
,new_practice_brand as ( -- new to practice and brand 
select distinct 
user_id 
,master_brand_name
,count(distinct dim_consumer_id) as N_total_new_practice_brand
,count(distinct retained_dim_consumer_id) as n_retained_new_practice_brand

from new_pre 

where 
master_brand_name = 'BOTOXCOSMETIC'
group by all 
)
, new_brand as (  -- new to just brand 
select distinct 
user_id 
,master_brand_name
,count(distinct dim_consumer_id) as N_total_new_to_brand
,count(distinct retained_dim_consumer_id) as n_retained_new_to_brand

from new_brand_pre 
where 
master_brand_name = 'BOTOXCOSMETIC'
group by all 
)

select distinct 
bdm_id
,bdm_name
,bdm_email
,bdm_abbv_email
,bdm_type
,territory
,area
,region

,N_total_consumers
,n_retained_consumers
,n_retained_consumers/nullifzero(N_total_consumers)                     as retention_total 

,N_total_new_to_brand
,n_retained_new_to_brand
,n_retained_new_to_brand/nullifzero(N_total_new_to_brand)               as retention_new_to_brand

,N_total_new_practice_brand
,n_retained_new_practice_brand
,n_retained_new_practice_brand/nullifzero(N_total_new_practice_brand)   as retention_new_to_practice_and_brand
from bdms 
left join total_retention on bdms.bdm_id = total_retention.user_id 
left join new_brand on bdms.bdm_id = new_brand.user_id 
left join new_practice_brand on bdms.bdm_id = new_practice_brand.user_id

order by n_total_consumers desc
