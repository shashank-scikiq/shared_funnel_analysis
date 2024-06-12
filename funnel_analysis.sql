
bap_id = Buyer app 
app = seller app 

-- 
select * from shared_transaction_funnel_analysis limit 10;

-- Count of Entries for each Transaction ID
select distinct transaction_id, count(1) from shared_transaction_funnel_analysis
group by transaction_id
order by count(1) desc;

-- Details about one transaction ID
select transaction_type, transaction_id, num_count, select_timestamp, provider_id from shared_transaction_funnel_analysis where transaction_id = '6cec61ca-0476-4d1b-8dda-17272589f5a1' order by select_timestamp;

-- Start timestamp for a transaction ID
select max(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ')), transaction_id
from shared_transaction_funnel_analysis where transaction_id = '6cec61ca-0476-4d1b-8dda-17272589f5a1'
group by transaction_id;

-- Data to Query for all transactions.
select transaction_type, transaction_id, num_count, select_timestamp, provider_id 
from shared_transaction_funnel_analysis 
order by select_timestamp;

-- Data to Query for all transactions (Reduced Columns)
select transaction_type, transaction_id
from shared_transaction_funnel_analysis 
order by transaction_id, select_timestamp;


select * from shared_transaction_funnel_analysis where transaction_id = '000471a5-ac92-42f8-9239-19575ed2e6d5' order by provider_id, select_timestamp;

select * from shared_transaction_funnel_analysis where bap_id = 'ondc.paytm.com' order by transaction_id limit 100;

select * from shared_transaction_funnel_analysis
where transaction_id in (
select transaction_id from (
select distinct transaction_id, count(1) 
	from shared_transaction_funnel_analysis
	group by transaction_id
	order by count(1) desc limit 1000
)) order by transaction_id, select_timestamp;


select transaction_id from (
select distinct transaction_id, count(1) 
	from shared_transaction_funnel_analysis
	group by transaction_id
	order by count(1) desc limit 1000
);


select * from (
select distinct transaction_id, count(1) 
	from shared_transaction_funnel_analysis
	group by transaction_id
	order by count(1) desc limit 1000
);

select distinct date(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ')) as date_vals
from shared_transaction_funnel_analysis
group by date(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ'))
order by date(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ'));


select max (date(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ'))), 
min (date(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ'))) from shared_transaction_funnel_analysis;

select date(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ')) as select_date,
count(distinct transaction_id)
from shared_transaction_funnel_analysis
group by date(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ'))
order by date(date_parse("select_timestamp", '%Y-%m-%dT%H:%i:%sZ'));


SELECT 
    date(date_parse(select_timestamp, '%Y-%m-%dT%H:%i:%sZ')) AS date_val,
    bap_id, bpp_id
FROM 
    shared_transaction_funnel_analysis
GROUP BY 
    date(date_parse(select_timestamp, '%Y-%m-%dT%H:%i:%sZ')),
    bap_id, bpp_id    
ORDER BY 
    date(date_parse(select_timestamp, '%Y-%m-%dT%H:%i:%sZ'));


SELECT 
    provider_id,provider_name, bpp_id, bap_id
FROM 
    shared_transaction_funnel_analysis
GROUP BY 
    provider_id,provider_name, bpp_id, bap_id
ORDER BY 
	provider_id, provider_name, bpp_id, bap_id;


SELECT 
    transaction_id,
    COUNT(DISTINCT provider_id) AS unique_provider_count
FROM 
    shared_transaction_funnel_analysis
GROUP BY 
    transaction_id
HAVING 
    COUNT(DISTINCT provider_id) > 1;
   
   
   
WITH transaction_provider_counts AS (
    SELECT
        transaction_id,
        provider_id,
        COUNT(*) AS provider_count
    FROM
        shared_transaction_funnel_analysis
    GROUP BY
        transaction_id,
        provider_id
),
transaction_unique_provider_counts AS (
    SELECT
        transaction_id,
        COUNT(DISTINCT provider_id) AS unique_provider_count
    FROM
        shared_transaction_funnel_analysis
    GROUP BY
        transaction_id
)
SELECT
    tpc.transaction_id,
    tpc.provider_id,
    tpc.provider_count
FROM
    transaction_provider_counts tpc
JOIN
    transaction_unique_provider_counts tupc
    ON tpc.transaction_id = tupc.transaction_id
WHERE
    tupc.unique_provider_count > 1
order by tupc.transaction_id, tpc.provider_id;



WITH transaction_type_counts AS (
    SELECT
        transaction_id,
        provider_id,
        transaction_type,
        COUNT(*) AS type_count
    FROM
        shared_transaction_funnel_analysis
    GROUP BY
        transaction_id,
        provider_id,
        transaction_type
)
SELECT
    transaction_id,
    provider_id,
    transaction_type,
    type_count
FROM
    transaction_type_counts
WHERE
    type_count > 1
order by type_count desc, transaction_id, provider_id;


select transaction_id, count(distinct provider_id) 
from shared_transaction_funnel_analysis
group by transaction_id
order by count(distinct provider_id) desc;


select * from shared_transaction_funnel_analysis where transaction_id = 'cbe32021-eaaf-480f-8f3e-333656d4e4b8' order by transaction_id;
