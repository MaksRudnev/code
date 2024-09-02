/*drop table rep_b2b.marketing_clicks
;

create table rep_b2b.marketing_clicks 
(
Campaign_Start_Month DATE, 
Start_date DATE, 
Finish_date DATE, 
UC_JOB_ID NUMBER,
Run_id VARCHAR2(12),
COMMUNICATION_ID VARCHAR2(12),
Collateral_id VARCHAR2(12),
Collateral_ver VARCHAR2(12),
Collateral_name VARCHAR2(80),
COMMUNICATION_NAME VARCHAR2(100),
Campaign_type VARCHAR(10),
Channel_name VARCHAR2(100), 
lk_section VARCHAR2(100),
OT0 NUMBER,
OT1 NUMBER,
OT2 NUMBER,
OT2_2 NUMBER,
OT2_3 NUMBER,
OT2_4 NUMBER,
OT2_5 NUMBER,
OT2_6 NUMBER,
OT3 NUMBER,
Partner VARCHAR(100),
Service_code VARCHAR(1500),
basic_prod_id VARCHAR(3000)
)
;
*/
truncate table rep_b2b.marketing_clicks 
;

insert /*+ NO_STATEMENT_QUEUING PARALLEL(8) */ into rep_b2b.marketing_clicks 

With services as
    (
    select 
        s.start_date,
        s.end_date,
        to_char(VASP_SERV_ID) as BASIC_PROD_ID, 
        s.SERVICE_CODE,
        e.Partner
    from 
        pub_ds.D_vasp_service s 
    join 
        (
        Select distinct 
            service_code,
            partner
        from
            NW_DEV.IS_SERVICE_CODE
        ) e 
    on e.service_code=s.service_code
    union all
    select distinct
        start_date,
        end_date,
        SERVICE_CODE as BASIC_PROD_ID,
        SERVICE_CODE,
        Partner  
    from 
        NW_DEV.IS_SERVICE_CODE 
    where ssource = 'BILLING'
    )

SELECT /*+ NO_STATEMENT_QUEUING PARALLEL(8) */ 
    OTS.Campaign_Start_Month,
    Start_date,
    Finish_date,
    UC_JOB_ID,
    OTS.Run_id,
    COMMUNICATION_ID,
    OTS.Collateral_id,
    null as Collateral_ver,
    Collateral_name,
    COMMUNICATION_NAME,
    'CIM' as Campaign_type,
    Channel_name,
    lk_section,
    OT0,
    OT1, 
    OT2,
    OT2_2,
    OT2_3,
    OT2_4,
    OT2_5,
    OT2_6,
    OT3,
    Partner,
    Service_code,
    basic_prod_id
FROM
    (
    Select
        Month_run_date as Campaign_Start_Month,
        trunc(Start_date,'dd') as Start_date,
        trunc(Finish_date,'dd') as Finish_date,
        UC_JOB_ID,
        Run_Id,
        Collateral_id,
        Channel_name,
        lk_section,
        SUM(Unique_subs_OT0_cnt) as OT0,
        SUM(Unique_subs_OT1_cnt) as OT1,
        SUM(Unique_subs_OT2_cnt) as OT2,
        SUM(Unique_subs_OT2_2_cnt) as OT2_2,
        SUM(Unique_subs_OT2_3_cnt) as OT2_3,
        SUM(Unique_subs_OT2_4_cnt) as OT2_4,
        SUM(Unique_subs_OT2_5_cnt) as OT2_5,
        SUM(Unique_subs_OT2_6_cnt) as OT2_6,
        SUM(Unique_OT3_cnt) as OT3
    from CVM_DS.A_CIM_RESPONSE_V
    Where 1=1 
         and Month_run_date>='01.01.2022'
    Group by 
        Month_run_date,
        trunc(Start_date,'dd'),
        trunc(Finish_date,'dd'),
        UC_JOB_ID,
        Run_Id,
        Collateral_id,
        Channel_name,
        lk_section
    ) OTS
LEFT JOIN
    (
    select 
        comp.RUN_ID,
        comp.communication_id,
        comp.communication_name,
        comp.COLLATERAL_ID, 
        comp.collateral_name,
        listagg(distinct p.basic_prod_id, ', ') within group(order by p.basic_prod_id) as basic_prod_id,
        listagg(distinct partner, ', ') within group(order by partner) as partner,
        listagg(distinct service_code, ', ') within group(order by service_code) as service_code
    from 
         pub_ds.D_CIM_CAMPAIGN_RUN comp
    left join
         pub_ds.D_CIM_CAMPAIGN_PRODUCTS p
    on p.run_id = comp.run_id and p.collateral_id = comp.collateral_id
    left join 
        services d 
    on p.BASIC_PROD_ID = d.BASIC_PROD_ID and trunc(comp.start_date,'dd') >= d.start_date and trunc(comp.start_date,'dd') < d.end_date
    Group by 
        comp.RUN_ID,
        comp.communication_id,
        comp.communication_name,
        comp.COLLATERAL_ID, 
        comp.collateral_name
    ) names
on names.collateral_id=OTS.collateral_id and names.RUN_ID=OTS.RUN_ID   
UNION ALL
SELECT /*+ NO_STATEMENT_QUEUING PARALLEL(8) */
    OTS.Campaign_Start_Month,
    Start_date,
    Finish_date,
    UC_JOB_ID,
    null as Run_id,
    null as communication_id,
    OTS.Collateral_id,
    OTS.Collateral_ver,
    Collateral_name,
    null as communication_name,
    'RTIM' as campaigh_type,
    Channel_name,
    Lk_section,
    OT0,
    OT1, 
    OT2,
    OT2_2,
    OT2_3,
    OT2_4,
    OT2_5,
    OT2_6,
    OT3,
    Partner,
    Service_code,
    basic_prod_id
FROM
    (
    Select
        month_offer_start_date as Campaign_Start_Month,
        trunc(offer_start_date, 'dd') as Start_date,
        trunc(finish_date, 'dd') as Finish_date,
        UC_JOB_ID,
        collateral_id,
        collateral_ver,
        channel_name,
        lk_section,
        SUM(Unique_subs_OT0_cnt) as OT0,
        SUM(Unique_subs_OT1_cnt) as OT1,
        SUM(Unique_subs_OT2_cnt) as OT2,
        SUM(Unique_subs_OT2_2_cnt) as OT2_2,
        SUM(Unique_subs_OT2_3_cnt) as OT2_3,
        SUM(Unique_subs_OT2_4_cnt) as OT2_4,
        SUM(Unique_subs_OT2_5_cnt) as OT2_5,
        SUM(Unique_subs_OT2_6_cnt) as OT2_6,
        SUM(Unique_OT3_cnt) as OT3
    from CVM_DS.A_RTIM_RESPONSE_V
    Where Month_offer_start_date>='01.01.2022'
    Group by 
        month_offer_start_date,
        offer_start_date,
        finish_date,
        UC_JOB_ID,
        collateral_id,
        collateral_ver,
        channel_name,
        lk_section
    ) OTS
left join
    (
    select 
        comp.collateral_id,
        comp.collateral_ver, 
        comp.collateral_name,
        listagg(distinct p.basic_prod_id, ', ') within group(order by p.basic_prod_id) as basic_prod_id,
        listagg(distinct partner, ', ') within group(order by partner) as partner,
        listagg(distinct service_code, ', ') within group(order by service_code) as service_code
    from 
        pub_ds.D_RTIM_CAMPAIGN_RUN comp 
    left join
        pub_ds.D_RTIM_CAMPAIGN_PRODUCTS p 
    on p.collateral_ver = comp.collateral_ver and p.collateral_id = comp.collateral_id
    left join 
        services d 
    on p.BASIC_PROD_ID = d.BASIC_PROD_ID and trunc(comp.start_date,'dd') between d.start_date and d.end_date
    Group by 
        comp.collateral_id,
        comp.collateral_ver,
        comp.collateral_name
    ) p
on OTS.collateral_ver=p.collateral_ver and OTS.collateral_id=p.collateral_id
;

commit
;
