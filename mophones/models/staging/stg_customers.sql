-- Staging model: Clean customer demographic data
-- Purpose: Standardize customer information

with source_data as (
    select * from {{ ref('raw_customer_data') }}
),

cleaned as (
    select
        -- IDs
        loan_id,
        
        -- Demographics
        lower(trim(gender)) as gender,
        to_date(date_of_birth, 'YYYY-MM-DD') as date_of_birth,
        
        -- Calculate current age
        datediff(year, to_date(date_of_birth, 'YYYY-MM-DD'), current_date()) as current_age,
        
        -- Age group categorization
        case
            when datediff(year, to_date(date_of_birth, 'YYYY-MM-DD'), current_date()) < 25 then '18-24'
            when datediff(year, to_date(date_of_birth, 'YYYY-MM-DD'), current_date()) < 35 then '25-34'
            when datediff(year, to_date(date_of_birth, 'YYYY-MM-DD'), current_date()) < 45 then '35-44'
            when datediff(year, to_date(date_of_birth, 'YYYY-MM-DD'), current_date()) < 55 then '45-54'
            else '55+'
        end as age_group,
        
        -- Income
        monthly_income,
        
        -- Income bracket
        case
            when monthly_income < 30000 then 'Low'
            when monthly_income < 50000 then 'Medium'
            when monthly_income < 70000 then 'High'
            else 'Very High'
        end as income_bracket

    from source_data
)

select * from cleaned