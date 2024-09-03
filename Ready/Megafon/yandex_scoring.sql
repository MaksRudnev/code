/*Create table rep_b2b.yandex_forth_wave 
(
SK_SUBS_ID NUMBER, 
SUBS_SUBS_ID NUMBER, 
FILIAL_ID NUMBER, 
BILLING_FILIAL_ID NUMBER,
MODEL_VER_ID NUMBER,
SCORE_DATE DATE,  
SCORE NUMBER(19,4) 
)
;

grant all on rep_b2b.yandex_forth_wave to PUB_DS, PUB_TMP, PUB_DEV
;*/

insert /*+ parallel(10) */  into rep_b2b.yandex_forth_wave  

with CTE as
(
Select 
     subs_subs_id,
     service_code,
     start_date,
     end_date
from
    (    
    Select 
        subs_subs_id, 
        service_code, 
        typename, 
        event_dttm as start_date, 
        lead(event_dttm) over (partition by subs_subs_id, service_code order by event_dttm asc, typename asc) as end_date
    from
         (
        select subs_subs_id, service_code, typename, event_dttm from pub_ds.f_vasp_events_v 
        Where 
            (
            service_code = 'yandexplus299_repriced'
            and typename in ('subscribe', 'unsubscribe')
            )
        )
    )
Where typename = 'subscribe'
)

Select
      r.sk_subs_id,
      old.subs_subs_id,
      r.filial_id,
      r.billing_filial_id,
      10628 MODEL_VER_ID, 
      TRUNC(SYSDATE)-1  SCORE_DATE, 
      1 score
from
    (
    select 
        to_number(subs_subs_id) as subs_subs_id, 
        end_date
    from rep_b2b.ash_bil_pack
    Where service_code in 
      (
      '19212',
      '19214',
      '19872',
      '20390',
      '20392',
      '20451'
      )
    union all
    select 
          subs_subs_id,
          event_dttm as end_date
    from pub_ds.f_vasp_events_v 
    Where typename = 'unsubscribe'
    and service_code in ('yandexplus_139_cpa', 'yandexplus_139_cpa_0')
    ) old
inner join
    (
    select 
        subs_subs_id, 
        start_date, 
        end_date
    from CTE
    Where service_code = 'yandexplus299_repriced'
    ) new
on new.subs_subs_id = old.subs_subs_id and abs(new.start_date-old.end_date)*24*60<=120
inner join
     (
     select 
         sk_subs_id,
         subs_subs_id, 
         billing_filial_id,
         service_code, 
         event_dttm,
         filial_id
     from pub_ds.f_vasp_events_v 
     where typename in ('subscribe', 'prolong') 
     and cost_vat = 299 
     and service_code = 'yandexplus299_repriced'
     ) r
on old.subs_subs_id = r.subs_subs_id and r.event_dttm between new.start_date and coalesce(new.end_date, to_date('31.12.2999', 'dd.mm.yyyy'))
group by  
      r.sk_subs_id,
      old.subs_subs_id,
      r.filial_id,
      r.billing_filial_id
having trunc(min(event_dttm), 'dd') = trunc(sysdate, 'dd') - 1
;

commit
;




