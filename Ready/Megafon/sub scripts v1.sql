--rep_b2b.channel_groups

/*Create table rep_b2b.sc_sub_flow
(
SK_SUBS_ID VARCHAR(15),
PARTNER VARCHAR(50),
PARTNER2 VARCHAR(50),
SERVICE_CODE VARCHAR(50),
VASP_SERV_ID VARCHAR(50),
ACTIVATE_CHANNEL VARCHAR(50), 
CVM_CHANNEL VARCHAR(50),
START_DATE DATE,
END_DATE DATE,
MGD NUMBER,
WMG NUMBER
)
;*/

--СОБИРАЕМ КАЖДУЮ ПОДПИСКУ АБОНЕНТА НА КАЖДОМ СЕРВИС-КОДЕ, ОПРЕДЕЛЯЕМ КАНАЛЫ, ВЫДЕЛЯЕМ ПРИЗНАКИ МИГРАЦИИ

truncate table rep_b2b.sc_sub_flow
;

insert /*+ parallel(10) */  into rep_b2b.sc_sub_flow

with evnt as
(
select
    sk_subs_id,
    service_code,
    vasp_serv_id,
    typename,
    event_dttm as subscribe_dttm, --обозвал заранее, соответствует названию при соотв тайпнейме
    CASE
        --cd_value8 = 'cp-supersubscriptions' WHAT???
        when service_code in (select service_code from nw_dev.is_service_code where ORGANIC = '100%')
             then 'Органика'
        when service_code like '%retail%' or (channel_name = 'kc' and login like 'retail%') 
             then 'Ритейл'
        when channel_name = 'kc' and login = 'cfnn2mm'
             then 'Колл центр (cfnn2mm)'
        when channel_name = 'kc' and login = 'stft1voice'
             then 'Колл центр (stft1voice)'   
        when channel_name = 'kc' --channel_name='call center' ??
             then 'Колл центр'
        when channel_name = 'cc_sms' and SERVICE_CODE  not like '%retail%' 
             then 'ЦО_смс'
        when channel_name = 'lk' --не уверен, что это именно "Для меня"
             then 'ЛК' --исправил вместо "Для меня" 
        when channel_name='ussd'
                  and ssc_short_number in 
                  ('208','213','214','218','232','238','239','240','242','243','245','247','248',
                  '251','253','261','262','266','267','273','274','280','305','315','411','441',
                  '259','563','521','31','106','455','481','278','703','803','805','254','230',
                  '125','230','338','980','701','632','182','270','125','302','340','190','401','320'
           ) then 'dstk' --забрал у Задорожной Алёны
        when vasp_cd like '%3dgame-center%'  or vasp_cd like '%hezzl%' --везде где cd_value = 'hezzl01' vasp_cd like '%hezzl%', но наоборот - нет
          or cd_value1 like '%revgames%' --если пусто, но есть в vasp_cd, то проходит по vasp_cd like '%3dgame-center%'
          or cd_value6 = 'newyear_quest'  
          or SK_VASP_CD_ID IN (3121,3071,3103,3047) 
          or cd_value2 = 'игра в млк (партнерство hezzle)'   
          or cd_value8 = 'game-center' 
          or vasp_cd like '%3dgame-center%' 
          or vasp_cd like '%revgames%'                          
             then 'Игра (ЛК)' --Игры чек на 08.12.2022 за весь 2022, позже должна появиться квантрера, её надо будет прописать
        when cd_value8 = 'po-mygame' --отсутств в 2022
             then 'Игра в Для меня'
        when cd_value8 = 'po-biggame' --отсутств в 2022
             then 'Большая игра'
        when cd_value8 = 'superoffer' 
             then 'Супер-оффер'
        when cd_value8 = 'po-carousel' 
             then 'Карусель'
        when cd_value8 = 'cp' or cd_value1 = '10' 
             then 'ЦП_(услуги)'
        when cd_value8 = 'cp-look' 
             then 'раздел «Посмотри»'
        when cd_value8 = 'cp-listen' 
             then 'раздел «Послушай»'
        when cd_value8 = 'cp-play' 
             then 'раздел «Поиграй»'
        when cd_value8 = 'cp-read' 
             then 'раздел «Почитай»' 
        when cd_value8 = 'cp-helpful' 
             then 'раздел «Полезное»' 
        when cd_value8 = 'cp-recommended' 
             then 'раздел «Рекомендуем»'
        when cd_value8 = 'cp-onlinestore' 
             then 'раздел «Интернет-магазин»'
        when cd_value8 = 'cp-promoblock' 
             then 'Промоблок-услуги'
        when cd_value8 = 'cp-supersubscriptions'
             then 'раздел «Суперподписки»'
        when cd_value8 = 'smart-protection' 
             then 'Умная защита'
        when cd_value8 = 'po-personaloffer' 
          or (cd_value1 = 'mlk' and cd_value5 = 'pers_offers') 
             then 'Персональный оффер' 
        when cd_value4 = 'megafontv%22' 
             then 'Migr' --много для start_migr_14 и start_migr_21
        when cd_value1 = '1' 
             then 'ЛК партнера'
        when cd_value1 = '2' 
             then 'ЛК'
        when cd_value1 = '3' 
          or VASP_TRANSACT_ID like 'rkn%' --???
             then 'РКН'
        when cd_value1 = '4' 
             then 'РКН 2.0 (Фишинговые сайты)'
        when cd_value1 = '5' 
             then 'МегаБаннер'
        when cd_value1 = '11'
             then 'IVR (NN)'
        when cd_value1 = '12' 
             then 'IVR (JA)'
        when cd_value1 in ('13', 'квинт')
             then 'IVR (KV)'
        when cd_value4 = 'elena-cvm' 
             then 'Елена'
        when cd_value4 like '%sms%' 
             then 'sms'
        when cd_value1 = '7' 
             then 'RTB'
        when cd_value1 = '8' 
             then '404'
        when cd_value1 = '9' 
             then 'ЦП_Web'
        when cd_value4 like 'rfc%' --перебивает OSA ниже, подробности там же
             then 'Технический оптаут'
        when cd_value1 = '14' 
             then 'OSA' --замечено cd_value4 like '%rfc%' для start_for_archive и yandexplus299_repriced, поэтому написал выше
        when cd_value1 = '15' 
             then 'MF TV' --мб объединить с 'Migr'?
        when cd_value1 = '16' 
             then 'OnlineZoneML'  
        when cd_value1 in ('17', 'лк', 'млк') --это убивается подробностями о разделе, которые выше, нужно выбрать или сделать два столбца
             then 'ЛК' 
        when vasp_cd = 'sms' 
             then 'sms' --для happysms15
        when cd_value1 = 'webcc'
             then 'webcc'
        when cd_value1 = 'ussd'
             then 'ussd'
        when cd_value1 = '6' 
          or cd_value1 = 'landing' 
          or cd_value8 = 'landing' 
             then 'Landing page'
        when cd_value1 = 'start'
             then 'start'
        when vasp_transact_id like 'arboost_landing%' 
             then 'arboost_landing'
        when vasp_transact_id like 'arboost_api%' 
             then 'arboost_api'
        when service_code like '%yandex%cpa%' then 'cpa_voice'
        else channel_name 
    end as activate_channel,
    lead(event_dttm) over (partition by  sk_subs_id, vasp_serv_id, service_code order by event_dttm) as unsubscribe_dttm
from rep_b2b.is_vasp_snap 
Where typename in ('subscribe', 'unsubscribe')
)

select 
    s_sub.sk_subs_id,
    partner,
    partner2,
    s_sub.service_code,
    vasp_serv_id,
    case 
      when (trunc(s_sub.start_date, 'mm')<'01.05.2022' or activate_channel = 'api') and VASP_CHANNEL is not null then VASP_CHANNEL
      when COALESCE(clc1.response_channel, clc2.response_channel) is not null and activate_channel = 'api' then COALESCE(clc1.response_channel, clc2.response_channel)
      else activate_channel 
    end as activate_channel,
    case 
      when COALESCE(clc1.response_channel, clc2.response_channel) is not null then COALESCE(clc1.response_channel, clc2.response_channel)
      else null
    end as CVM_CHANNEL,
    s_sub.start_date,
    s_sub.end_date,
    MGD,
    WMG
from
    (
    Select 
      sk_subs_id, 
      partner,
      partner2,
      det.service_code, 
      vasp_serv_id,  
      activate_channel,
      CASE 
        when abs((subscribe_dttm-(lag(unsubscribe_dttm) over (partition by partner, sk_subs_id order by unsubscribe_dttm)))*24*60*60)<=5 then 1 else null 
      end as MGD,
      Case
        when abs(((lead(subscribe_dttm) over (partition by partner, sk_subs_id order by unsubscribe_dttm))-unsubscribe_dttm)*24*60*60)<=5 then 1 else null 
      end as WMG,
      subscribe_dttm as start_date, 
      COALESCE(unsubscribe_dttm, to_date('31.12.2999', 'dd.mm.yyyy')) as end_date
    from
      evnt det
    left join
        (
        select distinct
          partner,
          partner2,
          service_code 
        from 
          nw_dev.is_service_code
        Where service_code is not null
        ) lst
    on det.service_code=lst.service_code
    Where typename = 'subscribe' --чтобы в lead(dttm) оказались unsubscribe
    and (subscribe_dttm<>unsubscribe_dttm or unsubscribe_dttm is null) --убираем дубли с events
    ) s_sub
left join
     (
    select distinct 
        creation_date,
        VASP_CHANNEL,
        sk_subs_id,
        service_code   
    from  
        PUB_DS.S_OMS_ORDERS_KHD k 
    join 
        rep_b2b.d_oms_channel c
    on k.channel = c.channel
    join 
        rep_b2b.d_oms_product p
    on p.PRODUCT_OFFERING_ID = k.PRODUCT_OFFERING_ID and p.partner in ('МегаФон','Apple')
    where ACTION = 'NEW' and  ORDER_STATE = 'COMPLETED'
    ) ch --тут 14 дублей, но пофиг
on  s_sub.sk_subs_id = ch.sk_subs_id and s_sub.service_code  = ch.service_code and abs(s_sub.start_date-trunc(ch.creation_date))<=0.0007 and trunc(s_sub.start_date, 'mm')<= '01.05.2022' --последнее условие под вопросом, мб уберу
left join
     cvm_ds.f_rtim_gen_response clc1
on s_sub.sk_subs_id = clc1.sk_subs_id and abs(s_sub.start_date-clc1.response_dttm)<=0.0007 and s_sub.vasp_serv_id = clc1.basic_prod_id --and s_sub.activate_channel = 'api'
left join 
     cvm_ds.f_cim_gen_response clc2
on s_sub.sk_subs_id = clc2.sk_subs_id and abs(s_sub.start_date-clc2.response_dttm)<=0.0007 and s_sub.vasp_serv_id = clc2.basic_prod_id --and s_sub.activate_channel = 'api'
;

commit
;

/*Create table rep_b2b.sub_flow
(
SK_SUBS_ID VARCHAR(15),
PARTNER VARCHAR(50),
PARTNER2 VARCHAR(50),
FIRST_SERVICE_CODE VARCHAR(50),
FIRST_VASP_SERV_ID VARCHAR(50),
ACTIVATE_CHANNEL VARCHAR(50),
CVM_CHANNEL VARCHAR(50),
START_DATE DATE,
END_DATE DATE
)
;
*/

--СОБИРАЕМ sub_flow (объединяем записи с признаками MGD и WMG в один lifetime)

truncate table rep_b2b.sub_flow
;

insert /*+ NO_STATEMENT_QUEUING PARALLEL(8) */  into rep_b2b.sub_flow

select 
  SK_SUBS_ID, 
  PARTNER,
  PARTNER2, 
  SERVICE_CODE as First_service_code,
  VASP_SERV_ID as First_vasp_serv_id,
  activate_channel,
  CVM_CHANNEL,
  START_DATE, 
  CASE WHEN END_DATE is null then to_date('31.12.2999', 'dd.mm.yyyy') else END_DATE end as END_DATE
from
(
    Select
      SK_SUBS_ID,
      PARTNER,
      PARTNER2,
      service_code,
      vasp_serv_id,
      activate_channel,
      CVM_CHANNEL,
      CASE 
        WHEN MGD is null and WMG = 1 then start_date 
        WHEN MGD is null and WMG is null then start_date
        ELSE null
      end as start_date,
      --также, как и в sc_flow, когда-нибудь поменять на partner2, пока для безопасности partner
      CASE 
        WHEN MGD = 1 and WMG is null then end_date --убирается start_date is not null затем
        WHEN MGD is null and WMG is null then end_date
        ELSE LEAD ( --else это MGD  is null и WMG = 1 или MGD = 1 and WMG = 1, что убираем фильтром
                  CASE 
                    WHEN MGD = 1 and WMG is null then end_date 
                    WHEN MGD is null and WMG is null then end_date
                    ELSE null 
                  end
                  ) over (partition by partner, sk_subs_id order by end_date) --поставил вместо order by start_date; коля лишний: COALESCE(end_date, '31.12.2999')
      end as end_date
    from
      rep_b2b.sc_sub_flow
    Where MGD<>WMG or (WMG is null or MGD is null)
)
Where start_date is not null
;

commit
;

/*Create table rep_b2b.aggr_sc_sub_flow_monthly
(
MNTH DATE,
SK_SUBS_ID VARCHAR(15),
PARTNER VARCHAR(50),
PARTNER2 VARCHAR(50),
SERVICE_CODE VARCHAR(50),
VASP_SERV_ID VARCHAR(50),
ACTIVATE_CHANNEL VARCHAR(50), 
CVM_CHANNEL VARCHAR(50),
START_DATE DATE,
END_DATE DATE,
MGD NUMBER,
WMG NUMBER,
REVENUE NUMBER,
FIRST_PAY DATE
)
;*/

--далее разворачиваем таблицу по месяцам, кидаем в неё выручку

truncate table rep_b2b.aggr_sc_sub_flow_monthly
;

insert /*+ parallel(10) */  into rep_b2b.aggr_sc_sub_flow_monthly

select 
    mnth,
    sk_subs_id,
    partner,
    partner2,
    service_code,
    vasp_serv_id,
    activate_channel,
    cvm_channel,
    start_date,
    end_date,
    mgd,
    wmg,
    SUM(cost_less_vat) as REVENUE,
    FIRST_PAY
from
    ( 
    select
           mnth,
           s.sk_subs_id,
           partner,
           partner2,
           s.service_code,
           s.vasp_serv_id,
           s.activate_channel,
           cvm_channel,
           start_date,
           end_date,
           mgd,
           wmg,
           cost_less_vat,
           MIN(event_dttm) over (partition by s.sk_subs_id, s.service_code, start_date) as FIRST_PAY 
    from 
           rep_b2b.sc_sub_flow s
    left join
    (
        select distinct 
               month_start_date as mnth 
        from pub_ds.d_calendar
        Where month_start_date<=trunc(sysdate, 'mm')
    ) c
      on c.mnth>=trunc(s.start_date, 'mm') and c.mnth<=s.end_date
    left join
    (
        select 
            sk_subs_id, 
            service_code, 
            event_dttm, 
            cost_less_vat 
        from rep_b2b.is_vasp_snap 
        Where typename in ('subscribe', 'prolong')
        and cost_less_vat>0
    ) rev
      on s.sk_subs_id = rev.sk_subs_id and s.service_code = rev.service_code and rev.event_dttm >= s.start_date and rev.event_dttm < s.end_date and trunc(rev.event_dttm, 'mm') = c.mnth
    )
Group by
       mnth,
       sk_subs_id,
       partner,
       partner2,
       service_code,
       vasp_serv_id,
       activate_channel,
       cvm_channel,
       start_date,
       end_date,
       mgd,
       wmg,
       FIRST_PAY
;

commit
;

/*
Create table rep_b2b.aggr_sub_flow_monthly
(
MNTH DATE,
SK_SUBS_ID VARCHAR(15),
PARTNER VARCHAR(50),
PARTNER2 VARCHAR(50),
FIRST_SERVICE_CODE VARCHAR(50),
FIRST_VASP_SERV_ID VARCHAR(50),
ACTIVATE_CHANNEL VARCHAR(50),
CVM_CHANNEL VARCHAR(50),
START_DATE DATE,
END_DATE DATE,
REVENUE NUMBER,
FIRST_PAY DATE
)
;
*/

--СОБИРАЕМ мес отчёт

truncate table rep_b2b.aggr_sub_flow_monthly
;

insert /*+ NO_STATEMENT_QUEUING PARALLEL(8) */  into rep_b2b.aggr_sub_flow_monthly

select
    mnth,
    sk_subs_id,
    partner,
    partner2,
    first_service_code,
    first_vasp_serv_id,
    activate_channel,
    cvm_channel,
    start_date,
    end_date,
    SUM(Revenue) as REVENUE,
    FIRST_PAY
from 
    (
    select
           c.mnth,
           fl.sk_subs_id,
           fl.partner,
           fl.partner2,
           first_service_code,
           first_vasp_serv_id,
           fl.activate_channel,
           fl.cvm_channel,
           fl.start_date,
           fl.end_date,
           Revenue,
           min(FIRST_PAY) over (partition by fl.sk_subs_id, fl.partner, fl.partner2, first_service_code, first_vasp_serv_id, fl.activate_channel, fl.cvm_channel, fl.start_date, fl.end_date) as FIRST_PAY  
    from 
           rep_b2b.sub_flow fl
    left join
        (
            select distinct 
                   month_start_date as mnth 
            from pub_ds.d_calendar
            Where month_start_date<=trunc(sysdate, 'mm')
        ) c
    on c.mnth>=trunc(fl.start_date, 'mm') and c.mnth<=fl.end_date
    left join 
         rep_b2b.aggr_sc_sub_flow_monthly r
    on  fl.sk_subs_id = r.sk_subs_id and fl.partner = r.partner 
        and c.mnth = r.mnth 
        and 
        (
        case 
           when r.mgd is null and r.wmg is null and r.start_date = fl.start_date and r.end_date = fl.end_date and r.service_code = fl.first_service_code then 1
           when r.mgd = 1 and r.wmg is null and r.start_date>fl.start_date and r.end_date = fl.end_date then 1
           when r.mgd = 1 and r.wmg = 1 and  r.start_date>fl.start_date and r.end_date < fl.end_date then 1
           when r.mgd is null and r.wmg = 1 and r.start_date = fl.start_date and r.end_date < fl.end_date then 1
           else null
        end
        ) = 1
    )
group by
    mnth,
    sk_subs_id,
    partner,
    partner2,
    first_service_code,
    first_vasp_serv_id,
    activate_channel,
    cvm_channel,
    start_date,
    end_date,
    FIRST_PAY
;

commit
;

