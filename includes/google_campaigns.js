const tableName = `campaigns`

module.exports = (params) => {

    return publish(params.stagingTablePrefix + "google_campaigns", {
        ...params.defaultConfig
    }).query(ctx => `
with source as (
${common.filterStitchRelation(ctx, params, tableName, `id`)}
)
SELECT
    cast(id as string) as ad_campaign_id,
    cast(account_id as string) as ad_account_id,
    name as ad_campaign_name,
    buying_type as ad_campaign_buying_type,
    effective_status as ad_campaign_status,
    cast(null as date) as ad_campaign_end_date,
    date(start_time) as ad_campaign_start_date,
    'Google' as ad_network,
    _sdc_batched_at as row_loaded_on,
    current_timestamp() as updated_on,
    'STITCH' as source
FROM source
`)
}