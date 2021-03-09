/* 
 * The function countryGroup() takes as input the name of the country code field and returns a CASE statement that maps country codes to country groups
 * You can learn more about functions on https://docs.dataform.co/guides/includes
 */

const sql = require("@dataform/sql")();

function typeString(warehouse) {
  return ({
    bigquery: "string",
    redshift: "varchar",
    postgres: "varchar",
    snowflake: "varchar",
    sqldatawarehouse: "string",
  })[warehouse || dataform.projectConfig.warehouse];
};
function countryGroup(countryCodeField) {
  return `case
            when lower(${countryCodeField}) in ('united states', 'canada') then 'NA'
            when lower(${countryCodeField}) in ('united kingdom', 'france', 'germany', 'italy', 'poland') then 'EU'
            when lower(${countryCodeField}) in ('australia') then lower(${countryCodeField})
            else 'Other countries'
            end`;
};

function filterStitch(ctx, params, tableName, uniqueColumn) {
  return `
SELECT
  *
  FROM
    (
      SELECT
      *,
      MAX(_sdc_batched_at) OVER (PARTITION BY ${uniqueColumn} ORDER BY _sdc_batched_at RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_sdc_batched_at
    FROM
      ${ctx.ref({ database: params.defaultConfig.databaseName, schema: params.defaultConfig.schemaName, name: `${tableName}` })}
  )
WHERE
_sdc_batched_at = max_sdc_batched_at
`;
};

function filterSegment(ctx, params, tableName, uniqueColumn) {
  return `
SELECT
  *
  FROM
    (
      SELECT
      *,
      MAX(uuid_ts) OVER (PARTITION BY ${uniqueColumn} ORDER BY uuid_ts RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_uuid_ts
    FROM
      ${ctx.ref({ database: params.defaultConfig.databaseName, schema: params.defaultConfig.schemaName, name: `${tableName}` })}
  )
WHERE
uuid_ts = max_uuid_ts
`;
};

function castFloat(value, warehouse) {
  return ({
    bigquery: ` cast(${value} as float64) `,
    redshift: ` cast(${value} as float) `,
    postgres: ` cast(${value} as float) `,
    snowflake: ` cast(${value} as decimal) `,
    sqldatawarehouse: ` cast(${value} as float) `,
  })[warehouse || dataform.projectConfig.warehouse];
};

function castInt(value, warehouse) {
  return ({
    bigquery: ` cast(${value} as int64) `,
    redshift: ` cast(${value} as int) `,
    postgres: ` cast(${value} as int) `,
    snowflake: ` cast(${value} as int) `,
    sqldatawarehouse: ` cast(${value} as int) `,
  })[warehouse || dataform.projectConfig.warehouse];
};

function safeDivide(dividend, divisor, warehouse) {
  return ({
    bigquery: ` safe_divide(${dividend},${divisor})`,
    redshift: ` ${dividend} /nullif(${divisor},0) `,
    postgres: ` ${dividend} /nullif(${divisor},0) `,
    snowflake: ` ${dividend} /nullif(${divisor},0) `,
    sqldatawarehouse: `  ${dividend} /nullif(${divisor},0) `,
  })[warehouse || dataform.projectConfig.warehouse];
};

module.exports = {
  countryGroup,
  filterStitch,
  filterSegment,
  safeDivide,
  castInt,
  castFloat
};

//
//Common functions to handle different warehouses
//

// declare({
//   schema: constants.DATASET,
//   name: utils.tableName('Ad'),
//   description: 'Ad meta information'
// })
// And within SQLX files ${ constants.DATASET } ”.

// const project_id = "project_id";
// const first_date = "'1970-01-01'";
// module.exports = {
//   project_id,
//   first_date
// };


// function timestampDiff(date_part, start_timestamp, end_timestamp, warehouse) {
//   return ({
//     bigquery: `timestamp_diff(${end_timestamp}, ${start_timestamp}, ${date_part})`,
//     redshift: `datediff(${date_part}, ${start_timestamp}, ${end_timestamp})`,
//     postgres: {
//       day: `date_part('day', ${end_timestamp} - ${start_timestamp})`,
//       hour: `24 * date_part('day', ${end_timestamp} - ${start_timestamp}) + date_part('hour', ${end_timestamp} - ${start_timestamp})`,
//       minute: `24 * date_part('day', ${end_timestamp} - ${start_timestamp}) + 60 * date_part('hour', ${end_timestamp} - ${start_timestamp}) + date_part('minute', ${end_timestamp} - ${start_timestamp})`,
//       second: `24 * date_part('day', ${end_timestamp} - ${start_timestamp}) + 60 * date_part('hour', ${end_timestamp} - ${start_timestamp}) + 60 * date_part('minute', ${end_timestamp} - ${start_timestamp}) + date_part('second', ${end_timestamp} - ${start_timestamp})`,
//       millisecond: `24 * date_part('day', ${end_timestamp} - ${start_timestamp}) + 60 * date_part('hour', ${end_timestamp} - ${start_timestamp}) + 60 * date_part('minute', ${end_timestamp} - ${start_timestamp}) + 1000 * date_part('second', ${end_timestamp} - ${start_timestamp}) + date_part('millisecond', ${end_timestamp} - ${start_timestamp})`
//     }[date_part.toLowerCase()],
//     snowflake: `datediff(${date_part}, ${start_timestamp}, ${end_timestamp})`,
//     sqldatawarehouse: `datediff(${date_part}, ${start_timestamp}, ${end_timestamp})`
//   })[warehouse || dataform.projectConfig.warehouse];
// // };

// function generateSurrogateKey(id_array, warehouse) {
//   return ({
//     bigquery: `cast(farm_fingerprint(concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)})) as ${typeString()})`,
//     redshift: `md5(concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)}))`,
//     postgres: `md5(concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)}))`,
//     snowflake: `md5(concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)}))`,
//     sqldatawarehouse: `hashbytes("md5", (concat(${id_array.map((id) => (`cast(${id} as ${typeString()})`)).join(`,`)})))`,
//   })[warehouse || dataform.projectConfig.warehouse];
// };

// function windowFunction({
//   func,
//   value,
//   ignore_nulls,
//   partition_fields,
//   order_fields,
//   frame_clause,
//   warehouse
// }) {
//   return ({
//     bigquery: `${func}(${value} ${ignore_nulls ? `ignore nulls` : ``}) over (partition by ${partition_fields} order by ${order_fields} ${frame_clause ? frame_clause : ``})`,
//     redshift: `${func}(${value} ${ignore_nulls ? `ignore nulls` : ``}) over (partition by ${partition_fields} order by ${order_fields} ${frame_clause ? frame_clause : `rows between unbounded preceding and unbounded following`})`,
//     postgres: `${func}(${value}) over (partition by ${partition_fields} order by ${ignore_nulls ? `case when ${value} is not null then 0 else 1 end asc` : ``} ${order_fields && ignore_nulls ? `,` : ``} ${order_fields} ${frame_clause ? frame_clause : `rows between unbounded preceding and unbounded following`})`,
//     snowflake: `${func}(${value} ${ignore_nulls ? `ignore nulls` : ``}) over (partition by ${partition_fields} order by ${order_fields} ${frame_clause ? frame_clause : ``})`,
//     sqldatawarehouse: `${func}(${value} ${ignore_nulls ? `ignore nulls` : ``}) over (partition by ${partition_fields} order by ${order_fields} ${frame_clause ? frame_clause : ``})`,
//   })[warehouse || dataform.projectConfig.warehouse];
// };
