with
    all_impressions as (

        select 'bing' as source, sum(imps) as total_imps
        from {{ ref("src_ads_bing_all_data") }}
        union all

        select 'facebook' as source, sum(impressions) as total_imps
        from {{ ref("src_ads_creative_facebook_all_data") }}
        union all

        select 'tiktok' as source, sum(impressions) as total_imps
        from {{ ref("src_ads_tiktok_ads_all_data") }}
        union all

        select 'twitter' as source, sum(impressions) as total_imps
        from {{ ref("src_promoted_tweets_twitter_all_data") }}
    )

select source, total_imps
from all_impressions
