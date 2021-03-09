const googleAds = require("./includes/stitch_google_ads");
const googleAdsets = require("./includes/stitch_google_ad_groups");
const googleCampaigns = require("./includes/stitch_google_campaigns");
const googleAdPerformance = require("./includes/stitch_google_ad_performance");

module.exports = (params) => {

    params = {
        // set defaults for parameters
        databaseName: "",
        schemaName: "",
        tablePrefix: "",
        stagingTablePrefix: "",
        stagingSchema: "",
        ...params
    };

    let stitchGoogleAds, stitchGoogleAdsets, stitchGoogleCampaigns, stitchGoogleAdPerformance;

    stitchGoogleAds = declare({
        ...params.defaultConfig,
        database: params.databaseName,
        schema: params.schemaName,
        name: "ads"
    });

    stitchGoogleAdsets = declare({
        ...params.defaultConfig,
        database: params.databaseName,
        schema: params.schemaName,
        name: "ad_groups"
    });

    stitchGoogleCampaigns = declare({
        ...params.defaultConfig,
        database: params.databaseName,
        schema: params.schemaName,
        name: "campaigns"
    });

    stitchGoogleAdPerformance = declare({
        ...params.defaultConfig,
        database: params.databaseName,
        schema: params.schemaName,
        name: "AD_PERFORMANCE_REPORT"
    });

    // Publish and return datasets.
    let result = {
        stitchGoogleAds: googleAds(params),
        stitchGoogleAdsets: googleAdsets(params),
        stitchGoogleCampaigns: googleCampaigns(params),
        stitchGoogleAdPerformance: googleAdPerformance(params)
    };

    return result;
}
