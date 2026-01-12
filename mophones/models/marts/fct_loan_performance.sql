-- Fact table: Comprehensive loan performance metrics
-- Purpose: One row per loan with all performance indicators
-- This joins loans with sales and customer data

with loans as (
    select * from {{ ref('stg_loans') }}
),

sales as (
    select * from {{ ref('stg_sales') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

-- Join all loan-related data
loan_facts as (
    select
        -- Identifiers
        l.loan_id,
        l.origination_date,
        
        -- Loan details from staging
        s.sale_date,
        l.loan_amount as outstanding_balance,
        l.total_paid,
        l.total_due_today,
        l.arrears_amount,
        l.days_past_due,
        l.delinquency_bucket,
        l.payment_status,
        l.account_status_primary,
        l.account_status_secondary,
        l.is_delinquent,
        l.loan_age_days,
        
        -- Sales information
        s.cash_price,
        s.loan_price,
        s.financing_markup,
        s.markup_percentage,
        s.product_name,
        s.product_brand,
        s.price_tier,
        s.seller_name,
        s.seller_type,
        s.is_returned,
        
        -- Customer demographics
        c.gender,
        c.current_age,
        c.age_group,
        c.monthly_income,
        c.income_bracket,
        
        -- Calculate key metrics
        round(l.total_paid / nullif(l.total_due_today, 0) * 100, 2) as payment_completion_rate,
        
        round(l.arrears_amount / nullif(l.total_due_today, 0) * 100, 2) as arrears_percentage,
        
        -- Calculate debt-to-income ratio (monthly payment vs income)
        round(l.weekly_rate * 4 / nullif(c.monthly_income, 0) * 100, 2) as debt_to_income_ratio,
        
        -- Risk flags
        case 
            when l.days_past_due > 90 then 'High Risk'
            when l.days_past_due > 60 then 'Medium Risk'
            when l.days_past_due > 30 then 'Low Risk'
            else 'Current'
        end as risk_category,
        
        -- Loan status summary
        case
            when lower(l.account_status_primary) like '%paid off%' then 'Closed - Paid'
            when lower(l.account_status_primary) like '%write off%' then 'Closed - Written Off'
            when lower(l.account_status_primary) like '%default%' then 'Defaulted'
            when l.is_delinquent then 'Active - Delinquent'
            else 'Active - Current'
        end as loan_status_summary

    from loans l
    left join sales s on l.loan_id = s.loan_id
    left join customers c on l.loan_id = c.loan_id
)

select * from loan_facts