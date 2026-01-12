-- Staging model: Clean and categorize NPS survey responses
-- Purpose: Standardize customer satisfaction data

with source_data as (
    select * from {{ ref('raw_nps_data') }}
),

cleaned as (
    select
        -- IDs
        submission_id,
        respondent_id,
        loan_id,
        
        -- Convert timestamp
        to_timestamp(submitted_at, 'YYYY-MM-DD HH24:MI') as submitted_at,
        
        -- NPS Score
        nps_score,
        
        -- Categorize NPS (Promoter/Passive/Detractor)
        -- Using variables from dbt_project.yml
        case
            when nps_score >= {{ var('nps_promoter_min') }} then 'Promoter'
            when nps_score >= {{ var('nps_passive_min') }} then 'Passive'
            when nps_score is not null then 'Detractor'
            else 'No Response'
        end as nps_category,
        
        -- Text feedback
        main_reason,
        improvement_suggestion,
        
        -- Service satisfaction flags (convert Yes/No to boolean)
        case when lower(happy_with_device) = 'yes' then true else false end as is_happy_with_device,
        case when lower(happy_with_service) = 'yes' then true else false end as is_happy_with_service,
        
        -- Pain points (issues reported)
        case when lower(payment_delay) = 'yes' then true else false end as experienced_payment_delay,
        case when lower(support_difficulty) = 'yes' then true else false end as had_support_difficulty,
        case when lower(battery_issue) = 'yes' then true else false end as has_battery_issue,
        
        -- App usage
        case when lower(uses_moapp) like '%satisfied%' then true else false end as uses_app,
        
        -- Preferred contact channel
        lower(trim(preferred_channel)) as preferred_contact_channel,
        
        -- Count total issues reported
        (case when lower(payment_delay) = 'yes' then 1 else 0 end +
         case when lower(support_difficulty) = 'yes' then 1 else 0 end +
         case when lower(battery_issue) = 'yes' then 1 else 0 end) as total_issues_count

    from source_data
    where nps_score is not null  -- Only keep responses with actual scores
)

select * from cleaned