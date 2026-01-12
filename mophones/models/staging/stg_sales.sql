-- Staging model: Clean sales transaction data
-- Purpose: Standardize product and sales information

with source_data as (
    select * from {{ ref('raw_sales_data') }}
),

cleaned as (
    select
        -- IDs
        sale_id,
        loan_id,
        
        -- Dates
        to_date(sale_date, 'YYYY-MM-DD') as sale_date,
        to_date(return_date, 'YYYY-MM-DD') as return_date,
        
        -- Sale characteristics
        case when lower(returned) = 'true' then true else false end as is_returned,
        lower(trim(sale_type)) as sale_type,
        
        -- Pricing
        cash_price,
        loan_price,
        loan_price - cash_price as financing_markup,
        round((loan_price - cash_price) / nullif(cash_price, 0) * 100, 2) as markup_percentage,
        
        -- Product details
        lower(trim(product_name)) as product_name,
        lower(trim(model)) as product_model,
        loan_term,
        
        -- Seller information
        lower(trim(seller)) as seller_name,
        lower(trim(seller_type)) as seller_type,
        
        -- Categorize product type
        case
            when lower(product_name) like '%iphone%' then 'Apple iPhone'
            when lower(product_name) like '%samsung%' then 'Samsung'
            else 'Other'
        end as product_brand,
        
        -- Price tier
        case
            when cash_price < 30000 then 'Budget'
            when cash_price < 60000 then 'Mid-Range'
            when cash_price < 100000 then 'Premium'
            else 'Luxury'
        end as price_tier

    from source_data
)

select * from cleaned