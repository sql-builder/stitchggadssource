const crossDB = require("./crossDB");
const sql = require("@dataform/sql")();

const tableName = `ad_groups`

module.exports = (params) => {

    return publish(params.stagingTablePrefix + "google_ad_groups", {
        ...params.defaultConfig
    }).query(ctx => `
with source as (
${crossDB.filterStitch(ctx, params, tableName, `id`)}
)
SELECT
    cast(id as string) as ad_group_id,
    cast(campaignid as string) as ad_campaign_id,
    cast(null as string) as as_account_id,
    name as ad_group_name,
    cast(null as string) as ad_group_targeting,
    status as ad_group_status,
    cast(null as timestamp) as ad_group_create_ts,
    date(null) as ad_group_start_date,
    cast(null as date) as ad_group_end_date,
    ${crossDB.castFloat(null, global.dataform.projectConfig.warehouse)} as ad_group_budget,
    'Google' as ad_network,
    _sdc_batched_at as row_loaded_on,
    current_timestamp() as updated_on,
    'STITCH' as source
FROM source
`)
}