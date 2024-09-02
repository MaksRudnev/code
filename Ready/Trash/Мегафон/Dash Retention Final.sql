/*Create table maxim_rudnev.mark_retention
(
Campaign_Start_Month DATE,
Start_date DATE,
End_date DATE,
Partner VARCHAR(50),
Service_code VARCHAR(50),
RUN_ID VARCHAR(50),
Collateral_id VARCHAR(50),
Collateral_ver VARCHAR(50),
Collateral_name VARCHAR(100),
Campaign_type VARCHAR(10),
Response_channel VARCHAR(50),
OT3 NUMBER, 
Revenue NUMBER,
Suspend_days NUMBER,
Lifetime_days NUMBER,
paid NUMBER,
been_suspend NUMBER,
M0 NUMBER, 
M1 NUMBER, 
M2 NUMBER, 
M3 NUMBER, 
M4 NUMBER, 
M5 NUMBER, 
M6 NUMBER, 
M7 NUMBER, 
M8 NUMBER, 
M9 NUMBER, 
M10 NUMBER, 
M11 NUMBER, 
M12 NUMBER
)*/

--;
delete from maxim_rudnev.mark_retention;
commit
;
insert into maxim_rudnev.mark_retention 

with

partners as
(
    select 
      comp.start_date,
      comp.end_date,
      comp.RUN_ID,
      comp.COLLATERAL_ID, 
      comp.collateral_name,
      listagg(distinct partner, ', ') within group(order by partner) as partner,
      listagg(distinct service_code, ', ') within group(order by service_code) as service_code
    from 
    (
        select /*+ parallel(8)*/
          run_id, 
          collateral_id, 
          collateral_name, 
          trunc(start_date, 'dd') as start_date, 
          trunc(finish_date, 'dd') as end_date 
        from  pub_ds.D_CIM_CAMPAIGN_RUN
        Where 1=1 
          and IS_TEST_FLG<>1
          and trunc(start_date,'dd')>='01.01.2022'
          and USECASE_ID in ('11','18','27','33','37','39','40','41') 
          and IS_B2B_FLG<>'1'
          and IS_DAUGHTER_FLG<>'1'
    ) comp
    left join
    (
        select 
          RUN_ID, 
          COLLATERAL_ID,
          case when d.pack_id is not null then d.VASP_SERV_ID 
               else BASIC_PROD_ID 
          end BASIC_PROD_ID
        from 
        pub_ds.D_CIM_CAMPAIGN_PRODUCTS p 
        left join 
        dmitry_osinin.marker d
        on      p.BASIC_PROD_ID = d.PACK_ID
    ) p 
    on p.run_id = comp.run_id and p.collateral_id = comp.collateral_id
    left join 
    (
        select 
          s.start_date,
          s.end_date,
          trunc(VASP_SERV_ID) as BASIC_PROD_ID, 
          s.SERVICE_CODE,
          e.Partner
        from 
        pub_ds.D_vasp_service s 
        join 
        NW_DEV.IS_SERVICE_CODE e 
        on e.service_code=s.service_code
        union all
        select 
          start_date,
          end_date,
          trunc(SERVICE_CODE) as BASIC_PROD_ID,
          SERVICE_NAME as SERVICE_CODE,
          Partner  
        from 
        NW_DEV.IS_SERVICE_CODE 
        where ssource = 'BILLING'
    ) d 
    on p.BASIC_PROD_ID = d.BASIC_PROD_ID and comp.start_date between d.start_date and d.end_date
Group by 
      comp.start_date,
      comp.end_date,
      comp.RUN_ID,
      comp.COLLATERAL_ID, 
      comp.collateral_name
)
,

Ret
as
(
Select    
    Month_Run_Date,
    p.start_date,
    p.end_date,
    ret.partner,
    service_code,
    clc.Run_id,
    clc.Collateral_id,
    collateral_name,
    Response_Channel,
    trunc(Response_dttm, 'dd') as resp_day,
    clc.msisdn as clc_msisdn,
    ret.msisdn as ret_msisdn,
    startd,
    endd,
    LTV_LESS_VAT,
    suspend_days,
    LT
from CVM_DS.F_CIM_GEN_RESPONSE clc --есть Run_dttm
left join partners p
on clc.collateral_id=p.collateral_id and clc.run_id=p.run_id
left join 
(Select 
  partner, 
  first_service_code,
  trunc(start_day,'dd') as startd,
  trunc(end_day, 'dd') as endd,
  msisdn,
  LTV_LESS_VAT,
  suspend_days,
  LT
from rep_b2b.rkub_lifetime lft
) ret
on ret.msisdn=concat('7', clc.msisdn) and ret.partner=p.partner and trunc(clc.Response_dttm, 'dd') between ret.startd-1 and ret.startd+1 and p.service_code = ret.first_service_code
)



select *
from
(
Select 
    Ret0.Month_Run_Date as Campaign_Start_Month,
    Ret0.start_date,
    Ret0.end_date,
    Ret0.partner,
    Ret0.service_code,
    Ret0.run_id,
    Ret0.collateral_id,
    null as collatral_ver,
    collateral_name,
    'CIM' as Campaign_type,
    Ret0.response_channel,
    Case when M=0 then 'M0' when M=1 then 'M1' when M=2 then 'M2' when M=3 then 'M3' when M=4 then 'M4' when M=5 then 'M5' when M=6 then 'M6' when M=7 then 'M7' when M=8 then 'M8' when M=9 then 'M9' when M=10 then 'M10' when M=11 then 'M11' when M=12 then 'M12' end as M,
    R0 as OT3,
    Revenue,
    Suspend_days,
    Lifetime_days,
    paid,
    been_suspend,
    CASE When M=0 and ROUND((R0-Rp)/R0, 4) is null then 1 else ROUND((R0-Rp)/R0, 4) end as RetRate
from
(
Select 
    Month_Run_Date,
    partner,
    service_code,
    run_id,
    collateral_id,
    response_channel,
    M,
sum(Rp) over (Partition by Month_Run_Date, partner, service_code, run_id, collateral_id, response_channel order by M ) as Rp
from
     (
  Select
    Month_Run_Date,
    partner,
    service_code,
    run_id,
    collateral_id,
    response_channel,
    FLOOR((endd-startd)/30) as M,
    count(distinct ret_msisdn) Rp
  from Ret
  Group by 
    Month_Run_Date,
    partner,
    service_code,
    run_id,
    collateral_id,
    response_channel,
    FLOOR((endd-startd)/30)
    )
) RetN
left join 
(
Select
    Month_Run_Date,
    start_date,
    end_date,
    partner,
    service_code,
    run_id,
    collateral_id,
    collateral_name,
    response_channel,
    count(distinct clc_msisdn) as R0,
    count(distinct case when LTV_LESS_VAT>0 then clc_msisdn end) as paid,
    count(distinct case when Suspend_days>0 then clc_msisdn end) as been_suspend,
    SUM(Suspend_days) as Suspend_days,
    SUM(LTV_LESS_VAT) as Revenue,
    SUM(LT) as Lifetime_days
  from Ret
  Group by 
    Month_Run_Date,
    start_date,
    end_date,
    partner,
    service_code,
    run_id,
    collateral_id,
    collateral_name,
    response_channel
) Ret0
on      Ret0.Month_Run_Date=RetN.Month_Run_Date
    and Ret0.partner=RetN.partner 
    and Ret0.service_code=RetN.service_code
    and Ret0.run_id=RetN.run_id
    and Ret0.collateral_id=RetN.collateral_id
    and Ret0.response_channel=RetN.response_channel
Where M<=12
)
pivot
(
SUM(RetRate)
for M in ('M0', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11', 'M12')
)

;
--RTIM
insert into maxim_rudnev.mark_retention

with

partners as
(
    select 
      trunc(comp.start_date,'dd') as start_date,
      trunc(comp.finish_date,'dd') as end_date,
      comp.collateral_id,
      comp.collateral_ver,
      comp.collateral_name, 
      listagg(distinct partner, ', ') within group(order by partner) as partner,
      listagg(distinct service_code, ', ') within group(order by service_code) as service_code
    from 
    pub_ds.D_RTIM_CAMPAIGN_RUN comp 
    left join
    (
        select 
          COLLATERAL_ID, 
          collateral_ver,
          case when d.pack_id is not null then d.VASP_SERV_ID 
               else BASIC_PROD_ID 
          end BASIC_PROD_ID
        from 
        pub_ds.D_RTIM_CAMPAIGN_PRODUCTS p 
        left join 
        dmitry_osinin.marker d
        on      p.BASIC_PROD_ID = d.PACK_ID
    ) p 
    on p.collateral_ver = comp.collateral_ver and p.collateral_id = comp.collateral_id
    left join 
    (
        select 
          s.start_date,
          s.end_date,
          trunc(VASP_SERV_ID) as BASIC_PROD_ID, 
          s.SERVICE_CODE,
          e.Partner
        from 
        pub_ds.D_vasp_service s 
        join 
        NW_DEV.IS_SERVICE_CODE e 
        on e.service_code=s.service_code
        union all
        select 
          start_date,
          end_date,
          trunc(SERVICE_CODE) as BASIC_PROD_ID,
          SERVICE_NAME as SERVICE_CODE,
          Partner  
        from 
        NW_DEV.IS_SERVICE_CODE 
        where ssource = 'BILLING'
    ) d 
    on p.BASIC_PROD_ID = d.BASIC_PROD_ID and trunc(comp.start_date,'dd') between d.start_date and d.end_date
  Group by 
        trunc(comp.start_date,'dd'),
        trunc(comp.finish_date,'dd'),
        comp.collateral_id,
        comp.collateral_ver,
        comp.collateral_name
)
,

Ret
as
(
Select    
    Month_offer_start_date as Campaign_Start_Month,
    p.start_date,
    p.end_date,
    ret.partner,
    service_code,
    clc.Collateral_id,
    clc.Collateral_ver,
    collateral_name,
    Response_Channel,
    trunc(Response_dttm, 'dd') as resp_day,
    clc.msisdn as clc_msisdn,
    ret.msisdn as ret_msisdn,
    startd,
    endd,
    LTV_LESS_VAT,
    Suspend_days,
    LT
from CVM_DS.F_RTIM_GEN_RESPONSE clc
left join partners p
on clc.collateral_id=p.collateral_id and clc.collateral_ver=p.collateral_ver
left join 
(Select 
  partner, 
  first_service_code,
  trunc(start_day,'dd') as startd,
  trunc(end_day, 'dd') as endd,
  msisdn,
  LTV_LESS_VAT,
  Suspend_days,
  LT
from rep_b2b.rkub_lifetime lft
) ret
on ret.msisdn=concat('7', clc.msisdn) and ret.partner=p.partner and trunc(clc.Response_dttm, 'dd') between ret.startd-1 and ret.startd+1 and p.service_code = ret.first_service_code
)


select *
from
(
Select 
    Ret0.Campaign_Start_Month,
    Ret0.start_date,
    Ret0.end_date,
    Ret0.partner,
    Ret0.service_code,
    null as run_id,
    Ret0.collateral_id,
    Ret0.collateral_ver,
    collateral_name,
    'RTIM' as Campaign_type,
    Ret0.response_channel,
    Case when M=0 then 'M0' when M=1 then 'M1' when M=2 then 'M2' when M=3 then 'M3' when M=4 then 'M4' when M=5 then 'M5' when M=6 then 'M6' when M=7 then 'M7' when M=8 then 'M8' when M=9 then 'M9' when M=10 then 'M10' when M=11 then 'M11' when M=12 then 'M12' end as M,
    R0 as OT3,
    Revenue,
    Suspend_days,
    Lifetime_days,
    paid,
    been_suspend,
    CASE When M=0 and ROUND((R0-Rp)/R0, 4) is null then 1 else ROUND((R0-Rp)/R0, 4) end as RetRate
from
(
Select 
    Campaign_Start_Month,
    partner,
    service_code,
    collateral_id,
    collateral_ver,
    response_channel,
    M,
sum(Rp) over (Partition by Campaign_Start_Month, partner, service_code, collateral_id, collateral_ver, response_channel order by M ) as Rp
from
     (
  Select
    Campaign_Start_Month,
    partner,
    service_code,
    collateral_id,
    collateral_ver,
    response_channel,
    FLOOR((endd-startd)/30) as M,
    count(distinct ret_msisdn) Rp
  from Ret
  Group by 
    Campaign_Start_Month,
    partner,
    service_code,
    collateral_id,
    collateral_ver,
    response_channel,
    FLOOR((endd-startd)/30)
    )
) RetN
left join 
(
Select
    Campaign_Start_Month,
    start_date,
    end_date,
    partner,
    service_code,
    collateral_id,
    collateral_ver,
    collateral_name,
    response_channel,
    SUM(Suspend_days) as Suspend_days,
    count(distinct case when Suspend_days>0 then clc_msisdn end) as been_suspend,
    SUM(LTV_LESS_VAT) as Revenue,
    SUM(LT) as Lifetime_days,
    count(distinct clc_msisdn) as R0,
    count(distinct case when LTV_LESS_VAT>0 then clc_msisdn end) as paid
  from Ret
  Group by 
    Campaign_Start_Month,
    start_date,
    end_date,
    partner,
    service_code,
    collateral_id,
    collateral_ver,
    collateral_name,
    response_channel
) Ret0
on      Ret0.Campaign_Start_Month=RetN.Campaign_Start_Month
    and Ret0.partner=RetN.partner 
    and Ret0.service_code=RetN.service_code
    and Ret0.collateral_ver=RetN.collateral_ver
    and Ret0.collateral_id=RetN.collateral_id
    and Ret0.response_channel=RetN.response_channel
Where M<=12
)
pivot
(
SUM(RetRate)
for M in ('M0', 'M1', 'M2', 'M3', 'M4', 'M5', 'M6', 'M7', 'M8', 'M9', 'M10', 'M11', 'M12')
)
;

commit
