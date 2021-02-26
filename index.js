const googleAds = require("./includes/google_ads");
const googleAdsets = require("./includes/google_ad_groups");
const googleCampaigns = require("./includes/google_campaigns");
const googleAdPerformance = require("./includes/google_ad_performance");

module.exports = (params) => {

    params = {
        // set defaults for parameters
        googleDatabase: "raw",
        googleSchema: "google_ads",
        tablePrefix: "",
        stagingTablePrefix: "",
        stagingSchema: "",
        ...params
    };

    let ads, adsets, campaigns, adPerformance;

    ads = declare({
        ...params.defaultConfig,
        database: params.googleDatabase,
        schema: params.googleSchema,
        name: "ads"
    });

    adsets = declare({
        ...params.defaultConfig,
        database: params.googleDatabase,
        schema: params.googleSchema,
        name: "adsets"
    });

    campaigns = declare({
        ...params.defaultConfig,
        database: params.googleDatabase,
        schema: params.googleSchema,
        name: "campaigns"
    });

    adPerformance = declare({
        ...params.defaultConfig,
        database: params.googleDatabase,
        schema: params.googleSchema,
        name: "ads_insights"
    });

    // Publish and return datasets.
    let result = {
        ads: googleAds(params),
        adsets: googleAdsets(params),
        campaigns: googleCampaigns(params),
        adPerformance: googleAdPerformance(params)
    };

    return result;
}
