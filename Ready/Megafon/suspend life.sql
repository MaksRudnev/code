select /*+ NO_STATEMENT_QUEUING PARALLEL(8) */ 
service_code,
avg(suspend_end_dttm-suspend_start_dttm),
count(sk_subs_id)   
from 
    (
    select  
        sk_subs_id, 
        service_code, 
        case 
          when prev_typename<>'suspend' then event_dttm 
          else null
        end as suspend_start_dttm
        ,
        case 
          when lead_typename<>'suspend' then lead_event_dttm
          when lead_typename = 'suspend' and lead(lead_typename) over (partition by sk_subs_id, service_code order by event_dttm asc) <> 'suspend' then lead(lead_event_dttm) over (partition by sk_subs_id, service_code order by event_dttm asc)
          when lead(lead_typename) over (partition by sk_subs_id, service_code order by event_dttm asc) is null then to_date('31.12.2999', 'dd.mm.yyyy')  
          else null  
        end as suspend_end_dttm
    from     
        (
        select 
            sk_subs_id,
            service_code,
            event_dttm,
            lag(typename) over (partition by sk_subs_id, service_code order by event_dttm asc) as prev_typename,
            typename,
            lead(typename) over (partition by sk_subs_id, service_code order by event_dttm asc) as lead_typename,
            lead(event_dttm) over (partition by sk_subs_id, service_code order by event_dttm asc) as lead_event_dttm
            --last_value(typename) over (partition by msisdn, service_code order by event_dttm asc range between current row and unbounded following) as last_value
        from rep_b2b.is_vasp_snap
        Where typename in ('subscribe', 'prolong', 'suspend', 'unsubscribe')
        and service_code in ('litres_plus_m', 'litres_m', 'litres_vip_m')
        )
    Where 1=1
        and typename = 'suspend'
        and (prev_typename<>'suspend' or (lead_typename<>'suspend' or lead_typename is null)) 
    )
Where suspend_start_dttm is not null
and suspend_start_dttm>='01.12.2022' and suspend_end_dttm <= '28.02.2023'
group by service_code
