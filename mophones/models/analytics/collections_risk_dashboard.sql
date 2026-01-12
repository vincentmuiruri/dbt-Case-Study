-- Analytics view: Collections priority dashboard
-- Purpose: Identify high-risk loans that need immediate attention
-- Use this for collections team prioritization

with loan_performance as (
    select * from {{ ref('fct_loan_performance') }}
),

nps_data as (
    select 
        loan_id,
        nps_category,
        nps_score,
        total_issues_count,
        customer_segment
    from {{ ref('fct_nps_analysis') }}
),

-- Combine loan performance with latest NPS
collections_priority as (
    select
        -- Loan identifiers
        l.loan_id,
        l.snapshot_date,
        
        -- Customer info
        l.gender,
        l.age_group,
        l.income_bracket,
        
        -- Financial metrics
        l.outstanding_balance,
        l.arrears_amount,
        l.total_paid,
        l.total_due_today,
        l.payment_completion_rate,
        
        -- Delinquency status
        l.days_past_due,
        l.delinquency_bucket,
        l.risk_category,
        l.loan_status_summary,
        
        -- Product details
        l.product_brand,
        l.price_tier,
        
        -- NPS context
        n.nps_category,
        n.nps_score,
        n.customer_segment,
        n.total_issues_count,
        
        -- Priority scoring (higher = more urgent)
        (
            -- Weight delinquency heavily
            case 
                when l.days_past_due > 120 then 50
                when l.days_past_due > 90 then 40
                when l.days_past_due > 60 then 30
                when l.days_past_due > 30 then 20
                else 0
            end +
            -- Add weight for arrears amount
            case
                when l.arrears_amount > 50000 then 20
                when l.arrears_amount > 30000 then 15
                when l.arrears_amount > 10000 then 10
                else 5
            end +
            -- Add weight for detractors
            case
                when n.nps_category = 'Detractor' then 15
                when n.nps_category is null then 5  -- No feedback is risky
                else 0
            end +
            -- Add weight for reported issues
            coalesce(n.total_issues_count * 3, 0)
        ) as collections_priority_score,
        
        -- Recommended action
        case
            when l.days_past_due > 120 then 'Legal Action'
            when l.days_past_due > 90 then 'Intensive Collections'
            when l.days_past_due > 60 then 'Standard Collections'
            when l.days_past_due > 30 and n.nps_category = 'Detractor' then 'Customer Care Escalation'
            when l.days_past_due > 30 then 'Early Collections'
            else 'Monitor'
        end as recommended_action,
        
        -- Expected recovery probability
        case
            when l.payment_completion_rate > 80 and n.nps_category != 'Detractor' then 'High'
            when l.payment_completion_rate > 50 then 'Medium'
            when l.days_past_due < 90 then 'Low'
            else 'Very Low'
        end as recovery_probability

    from loan_performance l
    left join nps_data n on l.loan_id = n.loan_id
    where l.loan_status_summary not in ('Closed - Paid', 'Closed - Written Off')  -- Only active loans
)

-- Final output sorted by priority
select *
from collections_priority
where days_past_due > 0  -- Only delinquent loans
order by collections_priority_score desc