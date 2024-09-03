select /*+ NO_STATEMENT_QUEUING PARALLEL(8) */ 
        sk_subs_id,
        event_dttm,
        response_dttm,
        abs(event_dttm-response_dttm) as delta,
        f.collateral_id,
        f.collateral_ver,
        collateral_name,
        service_code,
        response_type,
        response_value   
from     
    (
    select  
        det.sk_subs_id,
        service_code,
        event_dttm,
        ots.collateral_id, 
        ots.collateral_ver,
        response_dttm,
        response_type,
        response_value,
        row_number() over (partition by det.sk_subs_id, vasp_serv_id, event_dttm order by abs(event_dttm-response_dttm) asc) as top
    from 
           --rep_b2b.sub_flow det --���� ����� �������� �� s_subscribes
        (
        select sk_subs_id, vasp_serv_id, service_code, event_dttm from rep_b2b.is_vasp_snap 
        Where typename = 'subscribe'
        and vasp_serv_id in ('500023632', '500022717', '500023576', '500022716', '500023712', '500023763')
        ) det
    inner join 
        (
        select 
          clc.collateral_id, 
          clc.collateral_ver, 
          basic_prod_id,
          response_dttm,
          response_type,
          response_value,
          sk_subs_id
        from  
            (
            select 
                collateral_id, 
                collateral_ver, 
                response_dttm, 
                sk_subs_id, 
                response_type, 
                response_value 
            from  PUB_STG.D_RTIM_CAMPAIGN_CUSTOMERS
            Where response_type is not null --���� �� �����
            ) clc
        inner join --��������� ������ ������� �� ��������� � ������� ����������
            (
            select distinct
                collateral_id, 
                collateral_ver, 
                basic_prod_id 
            from 
                PUB_STG.D_RTIM_CAMPAIGN_PRODUCTS
            Where basic_prod_id in ('500023632', '500022717', '500023576', '500022716', '500023712', '500023763')
            ) dct
        on clc.collateral_id = dct.collateral_id and clc.collateral_ver = dct.collateral_ver
        ) ots
    on abs(det.event_dttm-ots.response_dttm)<=0.5 --��� ��� � ��� ������� ���� 
    and ots.sk_subs_id=det.sk_subs_id and ots.basic_prod_id = det.vasp_serv_id
    ) f
    left join
    PUB_STG.D_RTIM_CAMPAIGN_RUN n
    on f.collateral_id = n.collateral_id and f.collateral_ver = n.collateral_ver 
Where top=1 
and collateral_name in 
    (
    'OSM_RTIM_���_MyPrivacy_7�_440�_nov_��', 
    'OES_RTIM_���_VKMusic', 
    'OES_RTIM_���_VKMusic_30�_169_nov', 
    'OES_RTIM_���_LitRes_14�_399_nov', 
    'OES_RTIM_���_LitRes_14�_399�_nov', 
    'OES_RTIM_���_LitRes_30�_399_nov', 
    'OES_RTIM_LitRes_299�_���_��', 
    'OES_RTIM_���_���_7�_149�_nov', 
    'OES_RTIM_���_���_react_39�_3�_149�_nov', 
    'OES_RTIM_���_ITHelper_Android_30�_449�_n'
    )

 
