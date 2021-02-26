const google = require("../");

const models = google({
    //
    // Define the database and schema of your google data
    googleDatabase: "raw",
    googleSchema: "google_ads",
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