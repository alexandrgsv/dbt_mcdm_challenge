with
    all_data as (
        select 'bing' as source, sum(spend) as total_spend, sum(clicks) as total_clicks
        from {{ ref("src_ads_bing_all_data") }}
        union all
        select
            'facebook' as source, sum(spend) as total_spend, sum(clicks) as total_clicks
        from {{ ref("src_ads_creative_facebook_all_data") }}
        union all
        select
            'tiktok' as source, sum(spend) as total_spend, sum(clicks) as total_clicks
        from {{ ref("src_ads_tiktok_ads_all_data") }}
        union all
        select
            'twitter' as source, sum(spend) as total_spend, sum(clicks) as total_clicks
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
    )

select source, total_spend / total_clicks as cpc
from all_data
