sorryguys01

grant select on rep_b2b.gamecenter_inner_sells to vas_rep

truncate table rep_b2b.gamecenter_inner_sells

select count(*) from rep_b2b.game_pack_prcl_prof_id
/*
drop table rep_b2b.game_pack_prcl_prof_id

create table rep_b2b.game_pack_prcl_prof_id
(
pack_id number,
prcl_id number,
prof_id number,
partner VARCHAR(50),
DEF varchar(50),
navi_user varchar(50),
navi_date date
)*/

truncate table rep_b2b.gamecenter_inner_sells

Create table rep_b2b.gamecenter_inner_sells
(
CALL_DATE date,
subs_subs_id NUMBER,
sk_subs_id NUMBER,
msisdn NUMBER,
calls NUMBER,
revenue NUMBER,
PACK_ID NUMBER,
PRCL_ID NUMBER,
PROF_ID NUMBER,
partner VARCHAR(50),
name_prof_id varchar(50)
)
COLUMN STORE COMPRESS FOR QUERY HIGH NO ROW LEVEL LOCKING  NOLOGGING
PARTITION BY RANGE (call_date) INTERVAL (INTERVAL '1' DAY)
  SUBPARTITION BY LIST (PARTNER)
  SUBPARTITION TEMPLATE 
  ( 
      SUBPARTITION p1 VALUES ('kvantera01'),
      SUBPARTITION p2 VALUES ('hezzl02'),
      SUBPARTITION p3 VALUES ('old_games'),
      SUBPARTITION p4 VALUES ('hezzl01'),
      SUBPARTITION pxxx VALUES (DEFAULT)
  ) 
  (
  PARTITION p0 VALUES LESS THAN (DATE'2018-01-01')
  COLUMN STORE COMPRESS FOR QUERY HIGH NO ROW LEVEL LOCKING 
  )
  PARALLEL 8 

;

delete from rep_b2b.gamecenter_inner_sells
Where trunc(call_date, 'mm') = trunc(trunc(sysdate, 'dd')-1, 'mm')
;

commit
;

insert /*+ NO_STATEMENT_QUEUING PARALLEL(8) */ into rep_b2b.gamecenter_inner_sells

select
    call_date,
    subs_subs_id,
    sk_subs_id,
    to_number(concat('7', msisdn)) as msisdn,
    calls,
    revenue,
    pack_id,
    prcl_id,
    prof_id,
    partner,
    def as name_prof_id
from pub_ds.f_subs_calls_daily c
inner join
      rep_b2b.game_pack_prcl_prof_id s
on s.prcl_id = c.prcl_prcl_id and trunc(c.call_date, 'mm') = trunc(trunc(sysdate, 'dd')-1, 'mm')
;

commit
;

declare v_date date
;

begin
v_date:= date '2022-12-01'
; -- замени на последний отчЄтный мес€ц

while v_date <= date '2023-04-01' loop

insert /*+ NO_STATEMENT_QUEUING PARALLEL(8) */ into rep_b2b.gamecenter_inner_sells

select
    call_date,
    subs_subs_id,
    sk_subs_id,
    to_number(concat('7', msisdn)) as msisdn,
    calls,
    revenue,
    pack_id,
    prcl_id,
    prof_id,
    partner,
    def as name_prof_id
from pub_ds.f_subs_calls_daily c
inner join
      rep_b2b.game_pack_prcl_prof_id s
on s.prcl_id = c.prcl_prcl_id and c.call_date = v_date
;

commit
;
  
v_date:= v_date + 1
;

end loop
;

end
; 

declare v_date date
;

begin
v_date:= date '2021-04-01'
; -- замени на последний отчЄтный мес€ц

while v_date <= date '2023-03-01' loop

insert into rep_b2b.aggr_sub_flow_monthly_idx

select /*+ NO_STATEMENT_QUEUING PARALLEL(8) */ 
    1 as idx,
    'BILLING' as source,
    trunc(call_date, 'mm') as mnth,
    sk_subs_id,
    subs_subs_id,
    null as msisdn,
    '√еймцент' as Service,
    '√еймцент' as Service_zont,
    Partner,
    Partner as Partner2,
    NAME_PROF_ID as service_code,
    prcl_prcl_id as vasp_serv_id,
    '»гра (Ћ )' as activate_channel,
    null as CVM_channel,
    call_date as start_date,
    call_date as end_date,
    null as mgd,
    null as wmg,
    sum(revenue) as revenue,
    call_date as first_pay,
    call_date as f_first_pay,
    '»гра (Ћ )' as f_activate_channel,
    NAME_PROF_ID as f_service_code,
    prcl_prcl_id as f_vasp_serv_id,
    call_date as f_start_date,
    call_date as f_end_date,
    1 as sell,
    1 as T2P,
    1 as Paid_month,
    COALESCE(sk_subs_id, subs_subs_id) as abon_id
from 
    pub_ds.f_subs_calls_daily c
inner join
      rep_b2b.game_pack_prcl_prof_id s
on s.prcl_id = c.prcl_prcl_id and trunc(c.call_date, 'mm') = v_date
group by 
    trunc(call_date, 'mm'),
    sk_subs_id,
    subs_subs_id,
    Partner,
    NAME_PROF_ID,
    prcl_prcl_id,
    call_date
    
;

commit
;
  
v_date:= add_months(v_date, 1)
;

end loop
;

end
; 

create table rep_b2b.game_report
(
event_dttm date,
game_channel varchar(15),
service_code varchar(50),
subscribe_qnt number
)
COLUMN STORE COMPRESS FOR QUERY HIGH NO ROW LEVEL LOCKING  NOLOGGING
PARTITION BY RANGE (event_dttm) INTERVAL (INTERVAL '1' DAY)
  SUBPARTITION BY LIST (game_channel)
  SUBPARTITION TEMPLATE 
  ( 
      SUBPARTITION p1 VALUES ('revgames01'),
      SUBPARTITION p2 VALUES ('revgames02'),
      SUBPARTITION p3 VALUES ('hezzl01'),
      SUBPARTITION p4 VALUES ('hezzl02'),
      SUBPARTITION p5 VALUES ('paladin01'),
      SUBPARTITION p6 VALUES ('be hezzle'),
      SUBPARTITION p7 VALUES ('kvantera01'),
      SUBPARTITION p8 VALUES ('sorryguys01'),
      SUBPARTITION p9 VALUES ('other game'),
      SUBPARTITION pxxx VALUES (DEFAULT)
  ) 
  (
  PARTITION p0 VALUES LESS THAN (DATE'2018-01-01')
  COLUMN STORE COMPRESS FOR QUERY HIGH NO ROW LEVEL LOCKING 
  )
  PARALLEL 8
;

delete from rep_b2b.game_report
Where event_dttm>=trunc(sysdate, 'dd')-3
;
commit
;

insert /*+ NO_STATEMENT_QUEUING PARALLEL(8) */ into rep_b2b.game_report

select
    trunc(event_dttm, 'dd') as event_dttm,
    case 
      when vasp_cd like '%revgames01%' then 'revgames01'   
      when vasp_cd like '%revgames02%' then 'revgames02'
      when vasp_cd like '%hezzl01%' then 'hezzl01'   
      when vasp_cd like '%hezzl02%' then 'hezzl02'
      when vasp_cd like '%paladin01%' then 'paladin01'  
      when vasp_cd like '%be hezzle%' then 'be hezzle'
      when vasp_cd like '%kvantera01%' then 'kvantera01'
      when vasp_cd like '%sorryguys01%' then 'sorryguys01'
      else 'other game'
    end as game_channel,
    service_code,
    count(sk_subs_id) as subscribe_qnt 
from pub_ds.f_vasp_events_v
where 1=1 
and event_dttm>=trunc(sysdate, 'dd')-3
and
(
vasp_cd like '%3dgame-center%'  or vasp_cd like '%hezzl%' --везде где cd_value = 'hezzl01' vasp_cd like '%hezzl%', но наоборот - нет
          or cd_value1 like '%revgames%' --если пусто, но есть в vasp_cd, то проходит по vasp_cd like '%3dgame-center%'
          or cd_value6 = 'newyear_quest'  
          or SK_VASP_CD_ID IN (3121,3071,3103,3047) 
          or cd_value2 = 'игра в млк (партнерство hezzle)'   
          or cd_value8 = 'game-center' 
          or vasp_cd like '%3dgame-center%' 
          or vasp_cd like '%revgames%'                          
       or  cd_value8 = 'po-mygame' --отсутств в 2022
        or cd_value8 = 'po-biggame' --отсутств в 2022
)
and typename = 'subscribe'
and service_code in (select service_code from nw_dev.is_service_code)
group by
      trunc(event_dttm, 'dd'),
      case 
      when vasp_cd like '%revgames01%' then 'revgames01'   
      when vasp_cd like '%revgames02%' then 'revgames02'
      when vasp_cd like '%hezzl01%' then 'hezzl01'   
      when vasp_cd like '%hezzl02%' then 'hezzl02'
      when vasp_cd like '%paladin01%' then 'paladin01'  
      when vasp_cd like '%be hezzle%' then 'be hezzle'
      when vasp_cd like '%kvantera01%' then 'kvantera01'
      when vasp_cd like '%sorryguys01%' then 'sorryguys01'
      else 'other game'
      end,
      service_code
;

commit
;

