with
    all_data as (
        select 'bing' as source, sum(spend) as total_spend, sum(conv) as total_conv
        from {{ ref("src_ads_bing_all_data") }}
        union all
        select
            'facebook' as source, sum(spend) as total_spend, sum(purchase) as total_conv
        from {{ ref("src_ads_creative_facebook_all_data") }}
        union all
        select
            'tiktok' as source,
            sum(spend) as total_spend,
            sum(conversions) as total_conv
        from {{ ref("src_ads_tiktok_ads_all_data") }}
    )
select source, total_spend / total_conv as conv_cost
from
    all_data

    -- для Twitter conversion cost не определена в эталонном дашборде. 
    -- конверсией можно было бы считать переходы по URL-ссылкам, например
    -- но если смотреть на эталон, там null, поэтому нет расчета
    
