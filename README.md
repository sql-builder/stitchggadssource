# Google-ads

BETA package for transforming Google datasets managed by Stitch.


## Configure the package

Create a new JS file in your `definitions/` folder and create the Google-ads tables as in the following example.

By default, the package will look for source data in the `google_ads` schema in the `raw` database. Please set the source database and schema as required.

```js
const google = require("google-ads");

google({
    //
    // Define the database and schema of your Google data
    databaseName: "database_name",
    schemaName: "schema_name",
    // Optional prefix metadata if applicable
    stagingTablePrefix: "stg_",
    // Define the destination schema and table/view
    defaultConfig: {
        database: "database_name",
        schema: "schema_name",
        tags: ["google"],
        type: "table"
    },
});

```

## Supported warehouses:
 - BigQuery [Tested]
 - Snowflake [Untested]
 - Redshift [Untested]
