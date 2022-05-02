with all_interests as 
(select name,lower(Interest1) as interest from dim_people where Interest1 is not null union all
select name,lower(Interest2) as interest from dim_people  where Interest2 is not null union all
select name,lower(Interest3) as interest from dim_people  where Interest3 is not null union all
select name,lower(Interest4) as interest from dim_people  where Interest4 is not null
),
interest_counts as (select interest,count(1) as count from all_interests group by interest),
interest_ranks as (select interest,count,rank() over ( order by count desc) as interest_rank from interest_counts )
select * from interest_ranks where interest_rank < 6 order by interest_rank asc