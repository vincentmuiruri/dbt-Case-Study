-- Custom test: Ensure all delinquent loans appear in collections dashboard
-- This test FAILS if it returns any rows (rows = problems)

with delinquent_loans as (
    select loan_id
    from {{ ref('fct_loan_performance') }}
    where is_delinquent = true
      and loan_status_summary not like 'Closed%'
),

collections_dashboard as (
    select distinct loan_id
    from {{ ref('collections_risk_dashboard') }}
)

-- Find delinquent loans NOT in the dashboard
select 
    d.loan_id,
    'Delinquent loan missing from collections dashboard' as error_message
from delinquent_loans d
left join collections_dashboard c
    on d.loan_id = c.loan_id
where c.loan_id is null