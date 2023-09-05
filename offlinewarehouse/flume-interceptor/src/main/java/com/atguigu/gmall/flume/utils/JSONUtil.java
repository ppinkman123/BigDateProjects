2. 需求：找出所有科目成绩都大于某一学科平均成绩的学生

select
    t2.uid
from(
    select
        t1.uid,
        if(t1.score>t1.avg_score,0,1) flag
    from(
        select
            uid,
            score,
            avg(score) over(partition by subject_id) avg_score
        from score_info
    )t1
)t2
group by uid
having sum(t2.flag)=0;


2. 需求：要求使用SQL统计出每个用户的月累计访问次数及累计访问次数（注意日期数据的格式是否能解析）

select
    userId,
    mn,
    mn_count,
    sum(mn_count) over(partition by userId order by mn) 
from( 
    select
        userId,
        mn,
        sum(visitCount) mn_count
    from(
        select
             userId,
             date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') mn,
             visitCount
        from action
    )t1
    group by userId,mn
)t2;


取每个店铺排名前3的

select
    t2.shop,
    t2.user_id,
    t2.user_num
from (
     select
         t1.shop,
         t1.user_id,
         t1.user_num,
         rank() over(partition by t1.shop order by t1.user_num desc ) rk
     from (
              select
                  shop,
                  user_id,
                  count(user_id) user_num
              from visit
              group by shop,user_id
    ) t1
) t2
where t2.rk <=3;


给出 2017年每个月的订单数、用户数、总成交金额。
select
    date_format(dt,'yyyy-MM') mn,
    count(order_id) order_num,
    count(distinct user_id) user_num,
    sum(amount) sum_amount
from order_tab
where year(dt) = '2017'
group by date_format(dt,'yyyy-MM');


给出2017年11月的新客数（指在11月才有第一笔订单）

select
    user_id,
    count(distinct user_id) user_num
from order_tab
group by user_id
having date_format(min(dt),'yyyy-MM')='2017-11';

统计所有用户和活跃用户的总数及平均年龄
活跃用户：指连续两天都有访问记录的用户
select 
    sum(user_total_count),
    sum(user_total_avg_age),
    sum(twice_count),
    sum(twice_count_avg_age)
from (
    select
        0 user_total_count,
        0 user_total_avg_age,
        count(*) twice_count,
        cast(sum(age)/count(*) as decimal(10,2)) twice_count_avg_age
    from(
        select
           user_id,
           min(age) age
        from(
            select
               user_id,
               min(age) age
            from(
                select
                    user_id,
                    age,
                    date_sub(dt,rk) flag
                from(
                    select
                       dt,
                       user_id,
                       min(age) age,
                       rank() over(partition by user_id order by dt) rk
                    from user_age
                    group by dt,user_id
                )t1
            )t2
            group by user_id,flag
            having count(*)>=2
        )t3
        group by user_id 
    )t4

    union all

    select
       count(*) user_total_count,
       cast((sum(age)/count(*)) as decimal(10,1)),
       0 twice_count,
       0 twice_count_avg_age
    from(
        select
            user_id,
            min(age) age 
        from user_age 
        group by user_id
    )t5
)t6;





set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd_trade_cart_add_inc partition (dt)
select
    id,
    user_id,
    sku_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    source_id,
    source_type,
    dic.dic_name,
    sku_num,
    date_format(create_time, 'yyyy-MM-dd')
from
(
    select
        data.id,
        data.user_id,
        data.sku_id,
        data.create_time,
        data.source_id,
        data.source_type,
        data.sku_num
    from ods_cart_info_inc
    where dt = '2020-06-14'
    and type = 'bootstrap-insert'
)ci
left join
(
    select
        dic_code,
        dic_name
    from ods_base_dic_full
    where dt='2020-06-14'
    and parent_code='24'
)dic
on ci.source_type=dic.dic_code;





with
sku as
(
    select
        id,
        price,
        sku_name,
        sku_desc,
        weight,
        is_sale,
        spu_id,
        category3_id,
        tm_id,
        create_time
    from ods_sku_info_full
    where dt='2020-06-14'
),
spu as
(
    select
        id,
        spu_name
    from ods_spu_info_full
    where dt='2020-06-14'
),
c3 as
(
    select
        id,
        name,
        category2_id
    from ods_base_category3_full
    where dt='2020-06-14'
),
c2 as
(
    select
        id,
        name,
        category1_id
    from ods_base_category2_full
    where dt='2020-06-14'
),
c1 as
(
    select
        id,
        name
    from ods_base_category1_full
    where dt='2020-06-14'
),
tm as
(
    select
        id,
        tm_name
    from ods_base_trademark_full
    where dt='2020-06-14'
),
attr as
(
    select
        sku_id,
        collect_set(named_struct('attr_id',attr_id,'value_id',value_id,'attr_name',attr_name,'value_name',value_name)) attrs
    from ods_sku_attr_value_full
    where dt='2020-06-14'
    group by sku_id
),
sale_attr as
(
    select
        sku_id,
        collect_set(named_struct('sale_attr_id',sale_attr_id,'sale_attr_value_id',sale_attr_value_id,'sale_attr_name',sale_attr_name,'sale_attr_value_name',sale_attr_value_name)) sale_attrs
    from ods_sku_sale_attr_value_full
    where dt='2020-06-14'
    group by sku_id
)
insert overwrite table dim_sku_full partition(dt='2020-06-14')
select
    sku.id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.is_sale,
    sku.spu_id,
    spu.spu_name,
    sku.category3_id,
    c3.name,
    c3.category2_id,
    c2.name,
    c2.category1_id,
    c1.name,
    sku.tm_id,
    tm.tm_name,
    attr.attrs,
    sale_attr.sale_attrs,
    sku.create_time
from sku
left join spu on sku.spu_id=spu.id
left join c3 on sku.category3_id=c3.id
left join c2 on c3.category2_id=c2.id
left join c1 on c2.category1_id=c1.id
left join tm on sku.tm_id=tm.id
left join attr on sku.id=attr.sku_id
left join sale_attr on sku.id=sale_attr.sku_id;
