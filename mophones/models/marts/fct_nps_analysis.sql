-- Fact table: NPS responses with loan context
-- Purpose: Link customer satisfaction to loan performance
-- One row per NPS response

with nps as (
    select * from {{ ref('stg_nps_responses') }}
),

loans as (
    select * from {{ ref('fct_loan_performance') }}
),

-- Join NPS with loan data
nps_with_context as (
    select
        -- NPS identifiers
        n.submission_id,
        n.respondent_id,
        n.loan_id,
        n.submitted_at,
        
        -- NPS metrics
        n.nps_score,
        n.nps_category,
        n.main_reason,
        n.improvement_suggestion,
        
        -- Satisfaction flags
        n.is_happy_with_device,
        n.is_happy_with_service,
        n.uses_app,
        n.preferred_contact_channel,
        
        -- Issues reported
        n.experienced_payment_delay,
        n.had_support_difficulty,
        n.has_battery_issue,
        n.total_issues_count,
        
        -- Loan performance at time of survey
        l.days_past_due,
        l.delinquency_bucket,
        l.is_delinquent,
        l.payment_status,
        l.risk_category,
        l.loan_status_summary,
        l.arrears_amount,
        l.payment_completion_rate,
        
        -- Product information
        l.product_brand,
        l.product_name,
        l.price_tier,
        
        -- Customer demographics
        l.gender,
        l.age_group,
        l.income_bracket,
        l.debt_to_income_ratio,
        
        -- Calculate response timing
        datediff(day, l.sale_date, n.submitted_at) as days_since_purchase,
        
        -- Segment analysis flags
        case
            when n.nps_category = 'Detractor' and l.is_delinquent then 'At-Risk Customer'
            when n.nps_category = 'Promoter' and not l.is_delinquent then 'Healthy Customer'
            when n.nps_category = 'Promoter' and l.is_delinquent then 'Recovery Opportunity'
            when n.nps_category = 'Detractor' and not l.is_delinquent then 'Service Issue'
            else 'Standard'
        end as customer_segment,
        
        -- Issue severity score (0-10 scale)
        (n.total_issues_count * 2) + 
        case when n.nps_category = 'Detractor' then 3 else 0 end +
        case when l.is_delinquent then 2 else 0 end as issue_severity_score

    from nps n
    left join loans l on n.loan_id = l.loan_id
)

select * from nps_with_context