const crossDB = require("./crossDB");
const sql = require("@dataform/sql")();

const tableName = `campaigns`

module.exports = (params) => {

    return publish(params.stagingTablePrefix + "google_campaigns", {
        ...params.defaultConfig
    }).query(ctx => `
with source as (
${crossDB.filterStitch(ctx, params, tableName, `id`)} )
SELECT
    cast(id as string) as ad_campaign_id,
    cast(null as string) as ad_account_id,
    name as ad_campaign_name,
    cast(null as string) as ad_campaign_buying_type,
    status as ad_campaign_status,
    date(enddate) as ad_campaign_end_date,
    date(startdate) as ad_campaign_start_date,
    'Google' as ad_network,
    _sdc_batched_at as row_loaded_on,
    current_timestamp() as updated_on,
    'STITCH' as source
FROM source
`)
}