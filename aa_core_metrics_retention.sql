with prior_py_cte_shipto as (
select distinct 
soldto_id
,shipto_id
,master_brand_name 
,dim_consumer_id
,max(calendar_date) as last_date
from 
ADL_DW_prod.AA_CORE_METRICS_BASE.SOLDTO_SHIPTO_METRICS_TIME_PERIOD

where 
priorpyflag = 1 
group by all
)
,py_cte_shipto as (
select distinct 
soldto_id
,shipto_id
,master_brand_name 
,dim_consumer_id
,max(calendar_date) as last_date
from 
ADL_DW_prod.AA_CORE_METRICS_BASE.SOLDTO_SHIPTO_METRICS_TIME_PERIOD

where 
pyflag = 1 
group by all
)
,
cy_cte_shipto as (
select distinct 
soldto_id
,shipto_id
,dim_consumer_id
,master_brand_name 
,min(calendar_date) as retention_date
from 
ADL_DW_prod.AA_CORE_METRICS_BASE.SOLDTO_SHIPTO_METRICS_TIME_PERIOD 
where
cyflag = 1

group by all
)
, pre_shipto as (
select distinct 
py_cte_shipto.soldto_id
,py_cte_shipto.shipto_id
,py_cte_shipto.dim_consumer_id
,py_cte_shipto.master_brand_name
,cy_cte_shipto.dim_consumer_id as dim_consumer_id_retained
from py_cte_shipto 
left join cy_cte_shipto on py_cte_shipto.dim_consumer_id = cy_cte_shipto.dim_consumer_id 
                    and py_cte_shipto.shipto_id = cy_cte_shipto.shipto_id
                    and py_cte_shipto.master_brand_name = cy_cte_shipto.master_brand_name
)
,cp_shipto_retention as (
select distinct 
'Current 12 Month Rolling'                           as metric
,soldto_id                                           as soldto_id
,shipto_id                                           as shipto_id
,master_brand_name                                   as master_brand_name
,count(distinct dim_consumer_id)                     as n_total_consumers
,count(distinct dim_consumer_id_retained)            as n_retained_consumers
,round(n_retained_consumers/n_total_consumers,3)     as retention_pct
from pre_shipto
group by all 
)
,cp_national_retention as (
select distinct 
'Current 12 Month Rolling'                           as metric
,'NATIONAL'                                          as soldto_id
,'NATIONAL'                                          as shipto_id
,py_cte_shipto.master_brand_name                     as master_brand_name
,count(distinct py_cte_shipto.dim_consumer_id)       as n_total_consumers
,count(distinct cy_cte_shipto.dim_consumer_id)       as n_retained_consumers
,round(n_retained_consumers/n_total_consumers,3)     as retention_pct
from py_cte_shipto 
left join cy_cte_shipto on py_cte_shipto.dim_consumer_id = cy_cte_shipto.dim_consumer_id 
                    --and py_cte_shipto.shipto_id = cy_cte_shipto.shipto_id
                    and py_cte_shipto.master_brand_name = cy_cte_shipto.master_brand_name
                    group by all 

 )
 -- previous rolling 12 months
 , prior_period_pre_shipto as (
select distinct 
prior_py_cte_shipto.soldto_id
,prior_py_cte_shipto.shipto_id
,prior_py_cte_shipto.dim_consumer_id
,prior_py_cte_shipto.master_brand_name
,py_cte_shipto.dim_consumer_id as dim_consumer_id_retained
from prior_py_cte_shipto 
left join py_cte_shipto on prior_py_cte_shipto.dim_consumer_id = py_cte_shipto.dim_consumer_id 
                    and prior_py_cte_shipto.shipto_id = py_cte_shipto.shipto_id
                    and prior_py_cte_shipto.master_brand_name = py_cte_shipto.master_brand_name
)
,pp_shipto_retention as (
select distinct 
'Prior 12 Month Rolling'                             as metric
,soldto_id                                           as soldto_id
,shipto_id                                           as shipto_id
,master_brand_name                                   as master_brand_name
,count(distinct dim_consumer_id)                     as n_total_consumers
,count(distinct dim_consumer_id_retained)            as n_retained_consumers
,round(n_retained_consumers/n_total_consumers,3)     as retention_pct
from prior_period_pre_shipto
group by all 
)
,pp_national_retention as (
select distinct 
'Prior 12 Month Rolling'                             as metric
,'NATIONAL'                                          as soldto_id
,'NATIONAL'                                          as shipto_id
,prior_period_pre_shipto.master_brand_name                     as master_brand_name
,count(distinct prior_period_pre_shipto.dim_consumer_id)       as n_total_consumers
,count(distinct py_cte_shipto.dim_consumer_id)       as n_retained_consumers
,round(n_retained_consumers/n_total_consumers,3)     as retention_pct
from prior_period_pre_shipto 
left join py_cte_shipto on prior_period_pre_shipto.dim_consumer_id = py_cte_shipto.dim_consumer_id 
                    --and prior_period_pre_shipto.shipto_id = py_cte_shipto.shipto_id
                    and prior_period_pre_shipto.master_brand_name = py_cte_shipto.master_brand_name
                    group by all 

 )
 select distinct 
 *
 from cp_national_retention
 union all 
 select distinct 
 *
 from cp_shipto_retention
 union all 
 select distinct 
 *
 from pp_national_retention
 union all 
 select distinct 
 *
 from pp_shipto_retention
