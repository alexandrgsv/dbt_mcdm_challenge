with
    aggregated_data as (
        select
            'facebook' as source,
            sum(spend) as total_spend,
            sum(clicks) + sum(comments) + sum(shares) + sum(views) as total_engagements
        from {{ ref("src_ads_creative_facebook_all_data") }}
        union all
        select
            'twitter' as source,
            sum(spend) as total_spend,
            sum(engagements) as total_engagements
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
    )

select source, total_spend / total_engagements as cost_per_engage
from
    aggregated_data

    -- для Tiktok и Bing cost_per_enage не определена в эталонном дашборде.
    
