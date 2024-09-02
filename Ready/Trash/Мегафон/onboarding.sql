with 
  s as
  (
  select
      sk_subs_id, 
      service_code, 
      start_date,
      end_date,
      MGD
  from 
      rep_b2b.sc_sub_flow
  Where partner = 'яндекс'
  )
,
  control as
  (
  select 
     'Control' as collateral_name,
     'Control' as collateral_id,
     run_id,
     run_dttm,
     start_date,
     clc.sk_subs_id,
     clc.msisdn,
     case 
        when s.sk_subs_id is null then 1 else 0
     end as not_active_fail,
     case 
        when MGD = 1 then 1 else 0
     end as lifetime_fail,
     null as response_dttm
  from
     PUB_DS.D_CIM_CAMPAIGN_CUSTOMERS clc
  left join 
       s
  on s.sk_subs_id = clc.sk_subs_id and clc.run_dttm between s.start_date and s.end_date
  Where communication_id = '4000rlsb96nd' and disposition_type = 6
  )

select
/*+ parallel(8) */
    collateral_name, 
    case 
      when not_active_fail = 0 and lifetime_fail = 0 and activate_in_time = 3  and top = 1 then 1
      when not_active_fail = 0 and lifetime_fail = 0 and activate_in_time = 3  and top <> 1 then 0
      else -1
    -- 1 без ошибок и подключилс€, 0  без ошибок и подключилс€ благодар€ другой коммуникации, -1 fail
    end as activated_by_com, --rollup
    count(distinct sk_subs_id), --уник пользователей
    count(distinct case when not_active_fail = 1 or lifetime_fail = 1 or activate_in_time <> 3 then sk_subs_id end) as spam_or_trouble,
    count(distinct case when not_active_fail = 1 then sk_subs_id end) as not_active_fail,
    count(distinct case when lifetime_fail = 1 then sk_subs_id end) as lifetime_fail,
    count(distinct case when activate_in_time = 1 then sk_subs_id end) as activated_in_start,
    count(distinct case when activate_in_time = 2 then sk_subs_id end) as activated_earlier   
from 
    (
    select 
       collateral_name, 
       collateral_id,
       run_id,
       sk_subs_id,
       msisdn,
       start_date,
       run_dttm,
       response_dttm,
       activation_date,
       not_active_fail,
       lifetime_fail,
       case when trunc(start_date, 'dd') = trunc(activation_date, 'dd') then 1 
            when trunc(start_date, 'dd') > trunc(activation_date, 'dd') then 2
            when trunc(start_date, 'dd') < trunc(activation_date, 'dd') then 3
       --1 в тот же день, 2 ранее, 3 позже
       end as activate_in_time,
       row_number() over (partition by sk_subs_id, start_date order by(activation_date - COALESCE(response_dttm, run_dttm)) desc) as top
    from 
        (
        select  
           collateral_name, 
           collateral_id,
           run_id,
           sk_subs_id,
           OT.msisdn,
           start_date,
           run_dttm,
           response_dttm,
           min(activation_date) as activation_date,
           not_active_fail,
           lifetime_fail
        from 
            (
                select 
                    collateral_name,
                    clc.collateral_id,
                    clc.run_id,
                    clc.run_dttm,
                    start_date,
                    clc.sk_subs_id,
                    clc.msisdn,
                    case 
                      when s.sk_subs_id is null then 1 else 0
                    end as not_active_fail,
                    case 
                      when MGD = 1 then 1 else 0
                    end as lifetime_fail, 
                    max(response_dttm) as response_dttm
                from 
                       PUB_DS.D_CIM_CAMPAIGN_CUSTOMERS clc
                inner join
                    (
                    select distinct 
                        collateral_id, 
                        run_id, 
                        communication_name, 
                        collateral_name 
                    from PUB_DS.D_CIM_CAMPAIGN_RUN
                    Where --communication_name in  ('OES_CIM_яндексѕ_onboarding_2312') or 
                    collateral_name in  ('OES_яндексѕ+ W_20день_Push',
                    'OES_яндексѕ+ W_30день_Push',
                    'OES_яндексѕ+ W_4день_SMS',
                    'OES_яндексѕ+ W_11день_SMS')
                    ) dct
                on clc.collateral_id = dct.collateral_id and clc.run_id = dct.run_id
                left join 
                    s
                on s.sk_subs_id = clc.sk_subs_id and clc.run_dttm between s.start_date and s.end_date
                Where response_type is not null and clc.sk_subs_id not in (select sk_subs_id from control)
                Group by
                    collateral_name,
                    clc.collateral_id,
                    clc.run_id,
                    clc.run_dttm,
                    start_date,
                    clc.sk_subs_id,
                    clc.msisdn,
                    case 
                      when s.sk_subs_id is null then 1 else 0
                    end,
                    case 
                      when MGD = 1 then 1 else 0
                    end
            union all 
            select * from control
            ) OT
        left join
           (
           select distinct 
                  msisdn, 
                  activation_date 
           from rep_b2b.yandex_act_temp
           ) au
        on concat('7', OT.msisdn) = au.msisdn
        group by
           collateral_name, 
           collateral_id,
           run_id,
           sk_subs_id,
           OT.msisdn,
           start_date,
           run_dttm,
           response_dttm,
           not_active_fail,
           lifetime_fail
        )
    )
group by 
    collateral_name, 
    case 
      when not_active_fail = 0 and lifetime_fail = 0 and activate_in_time = 3  and top = 1 then 1
      when not_active_fail = 0 and lifetime_fail = 0 and activate_in_time = 3  and top <> 1 then 0
      else -1
    -- 1 без ошибок и подключилс€, 0  без ошибок и подключилс€ благодар€ другой коммуникации, -1 fail
    end
--Where qnt>1

