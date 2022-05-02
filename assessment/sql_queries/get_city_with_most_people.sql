with city_agg as (select city,count(1) as count from dim_people group by city),
city_ranks as (select city,count,rank() over ( order by count desc) as city_rank from city_agg )
select * from city_ranks where city_rank = 1