-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT 
  FORMAT_DATE("%Y%m", PARSE_DATE("%Y%m%d", date)) AS month,
  sum(totals.visits) visits,
  sum(totals.pageviews) pageviews,
  sum(totals.transactions) transactions,
  sum(totals.totalTransactionRevenue)/power(10,6) revenue 

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where _table_suffix between '20170101' and '20170331'
group  by month
order by month


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
select 
  trafficSource.source,
  sum(totals.visits) total_visits,
  sum(totals.bounces) total_no_of_bounces,
  sum(totals.bounces)/sum(totals.visits)*100
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`  
where left(date,6) = '201707'     
group by trafficSource.source
order by total_visits desc


-- Query 3: Revenue by traffic source by week, by month in June 2017
#standardSQL
with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month;



-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select
  format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) month,
  sum(totals.transactions)/count(distinct fullVisitorId) Avg_total_transactions_per_user
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`     
where left(date,6)='201707' and totals.transactions is not null 
group by month

-- Query 06: Average amount of money spent per session
#standardSQL
select
  format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) month,
  avg(totals.totalTransactionRevenue) avg_revenue_by_user_per_visit
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where left(date,6)='201707' and totals.transactions IS NOT NULL
group by month

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL
with cte as (
select
  distinct fullVisitorID
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  unnest(hits) hits,
  unnest(hits.product) product
where v2ProductName = "YouTube Men's Vintage Henley"
  and format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) = '201707'
  and productRevenue is not null
)

select 
  product_quantity.v2ProductName other_purchased_products,
  sum(productQuantity) quantity
from cte
join (  
  select fullVisitorId, v2ProductName, productQuantity 
  from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  unnest(hits) hits,
  unnest(hits.product) product
  where format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) = '201707'
  and productRevenue is not null
  and v2ProductName <> "YouTube Men's Vintage Henley"
  ) product_quantity 
  on cte.fullVisitorId=product_quantity.fullVisitorId
group by other_purchased_products
order by quantity desc


--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL      
with cte1 as (
select
  format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) month,
  count(productSKU) num_product_view
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  unnest(hits) hits,
  unnest(hits.product) product
where _table_suffix between '20170101' and '20170331'
  and eCommerceAction.action_type = '2'
group by month),
cte2 as (
select
  format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) month,
  count(productSKU) num_addtocart
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  unnest(hits) hits,
  unnest(hits.product) product
where _table_suffix between '20170101' and '20170331'
  and eCommerceAction.action_type = '3'
group by month),
cte3 as (
select
  format_date("%Y%m", PARSE_DATE("%Y%m%d", date)) month,
  count(productSKU) num_purchase
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  unnest(hits) hits,
  unnest(hits.product) product
where _table_suffix between '20170101' and '20170331'
  and eCommerceAction.action_type = '6'
group by month)

select
  cte1.month,
  num_product_view,
  num_addtocart,
  num_purchase,
  round(num_addtocart/num_product_view*100, 2) add_to_cart_rate,
  round(num_purchase/num_product_view*100, 2) purchase_rate
from cte1
join cte2 on cte1.month=cte2.month
join cte3 on cte1.month=cte3.month
order by cte1.month




                                                                ---GOOD---



