# Google-ads

BETA package for transforming Google datasets managed by Stitch.


## Configure the package

Create a new JS file in your `definitions/` folder and called `stitch_google_ads_source.js` create the Google-ads tables as in the following example.

By default, the package will look for source data in the `google_ads` schema in the `raw` database. Please set the source database and schema as required.

```js
const google = require("stitch-google-ads-source");

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

And in your package.json file add a line for Google ads source:
```json
{
    "dependencies": {
        "@dataform/core": "x.xx.x",
        "stitch-google-ads-source": "git+https://github.com/sql-builder/stitch-google-ads-source.git"
    }
}
```

## Supported warehouses:
 - BigQuery [Tested]
 - Snowflake [Untested]
 - Redshift [Untested]
