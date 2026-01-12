-- Analytics view: NPS and Credit Performance Correlation
-- Purpose: Understand how customer satisfaction relates to payment behavior
-- Use this for strategic insights and product improvements

with nps_facts as (
    select * from {{ ref('fct_nps_analysis') }}
),

-- Aggregate metrics by NPS category
nps_summary as (
    select
        nps_category,
        
        -- Count of responses
        count(*) as total_responses,
        
        -- Average NPS score
        round(avg(nps_score), 2) as avg_nps_score,
        
        -- Payment performance
        round(avg(payment_completion_rate), 2) as avg_payment_rate,
        avg(days_past_due) as avg_days_past_due,
        round(avg(arrears_amount), 2) as avg_arrears,
        
        -- Delinquency rate
        round(sum(case when is_delinquent then 1 else 0 end) * 100.0 / count(*), 2) as delinquency_rate_pct,
        
        -- Issue prevalence
        round(avg(total_issues_count), 2) as avg_issues_per_customer,
        round(sum(case when experienced_payment_delay then 1 else 0 end) * 100.0 / count(*), 2) as payment_delay_pct,
        round(sum(case when had_support_difficulty then 1 else 0 end) * 100.0 / count(*), 2) as support_issue_pct,
        round(sum(case when has_battery_issue then 1 else 0 end) * 100.0 / count(*), 2) as battery_issue_pct,
        
        -- Satisfaction rates
        round(sum(case when is_happy_with_device then 1 else 0 end) * 100.0 / count(*), 2) as device_satisfaction_pct,
        round(sum(case when is_happy_with_service then 1 else 0 end) * 100.0 / count(*), 2) as service_satisfaction_pct

    from nps_facts
    group by nps_category
),

-- Aggregate by customer segment
segment_summary as (
    select
        customer_segment,
        count(*) as segment_count,
        round(avg(nps_score), 2) as avg_nps,
        round(avg(payment_completion_rate), 2) as avg_payment_rate,
        round(sum(case when is_delinquent then 1 else 0 end) * 100.0 / count(*), 2) as delinquency_rate
    from nps_facts
    group by customer_segment
),

-- Product-level insights
product_summary as (
    select
        product_brand,
        price_tier,
        count(*) as response_count,
        round(avg(nps_score), 2) as avg_nps,
        round(avg(payment_completion_rate), 2) as avg_payment_rate,
        round(sum(case when has_battery_issue then 1 else 0 end) * 100.0 / count(*), 2) as battery_issue_rate
    from nps_facts
    group by product_brand, price_tier
    having count(*) >= 3  -- Only show products with enough responses
),

-- Demographics impact
demographic_summary as (
    select
        age_group,
        income_bracket,
        count(*) as response_count,
        round(avg(nps_score), 2) as avg_nps,
        round(avg(payment_completion_rate), 2) as avg_payment_rate,
        round(sum(case when is_delinquent then 1 else 0 end) * 100.0 / count(*), 2) as delinquency_rate
    from nps_facts
    group by age_group, income_bracket
)

-- Combine all summaries for reporting
select 
    'NPS Category' as analysis_type,
    nps_category as category,
    total_responses as count,
    avg_nps_score as metric_1,
    delinquency_rate_pct as metric_2,
    device_satisfaction_pct as metric_3
from nps_summary

union all

select 
    'Customer Segment' as analysis_type,
    customer_segment as category,
    segment_count as count,
    avg_nps as metric_1,
    delinquency_rate as metric_2,
    avg_payment_rate as metric_3
from segment_summary

union all

select 
    'Product-Price' as analysis_type,
    product_brand || ' - ' || price_tier as category,
    response_count as count,
    avg_nps as metric_1,
    battery_issue_rate as metric_2,
    avg_payment_rate as metric_3
from product_summary

order by analysis_type, metric_1 desc