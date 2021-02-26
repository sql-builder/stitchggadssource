const tableName = `ads`

module.exports = (params) => {

    return publish(params.stagingTablePrefix + "google_ads", {
        ...params.defaultConfig
    }).query(ctx => `
with source as ( 
${common.filterStitchRelation(ctx, params, tableName, `id`)}
)
SELECT
    cast(id as string) as ad_id,
    cast(campaign_id as string) as ad_campaign_id,
    cast(account_id as string) as ad_account_id,
    name as ad_name,
    status as ad_status,
    cast(adset_id as string) as ad_group_id,
    bid_type as ad_bid_type,
    bid_amount as ad_bid_amount,
    cast(null as string) as ad_utm_parameters,
    lower(cast(null as string)) as ad_utm_campaign,
    lower(cast(null as string)) as ad_utm_content,
    lower(cast(null as string)) as ad_utm_medium,
    lower(cast(null as string)) as ad_utm_source,
    lower(cast(null as string)) as ad_utm_term,
    cast(null as string) as ad_type,
    cast(null as string) as ad_final_urls,
    cast(null as string) as ad_network_type,
    cast(null as string) as ad_criteria_type,
    'Google' as ad_network,
    _sdc_batched_at as row_loaded_on,
    current_timestamp() as updated_on,
    'STITCH' as source
FROM source
`)
}