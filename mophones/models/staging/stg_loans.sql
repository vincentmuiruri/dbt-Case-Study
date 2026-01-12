{{
  config(
    materialized='view',
    tags=['staging', 'loans']
  )
}}

with source as (
    select * from {{ ref('raw_credit_data') }}
),

renamed as (
    select
        -- Primary key
        loan_id,
        
        -- -- Foreign keys
        -- customer_id,
        
        -- Loan details
        balance as loan_amount,
        weekly_rate as interest_rate,
        days_past_due, 
        sale_date as origination_date,
        
        -- Status
        lower(trim(balance_due_status)) as loan_status,
        
        -- Credit info
        account_status_l2,
        
        -- Metadata
        date as created_at,
        date as updated_at
        
    from source
),

validated as (
    select
        *,
        -- Data quality flags
        case 
            when loan_amount <= 0 then true
            when interest_rate < 0 or interest_rate > 1 then true
            when days_past_due < 0 then true
            when account_status_l2 is null then true
            else false
        end as has_data_quality_issues,
        
    from renamed
)

select * from validated
where not has_data_quality_issues