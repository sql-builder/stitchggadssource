const google = require("..");

google({
    //
    // Define the database and schema of your Google data
    databaseName: "marketerhire-warehouse",
    schemaName: "mh_google_ads",
    // Optional prefix metadata if applicable
    stagingTablePrefix: "stg_",
    // Define the destination schema and table/view
    defaultConfig: {
        database: "marketerhire-warehouse",
        schema: "dataform_tests",
        tags: ["google"],
        type: "table"
    },
});