import snowflake from "snowflake-sdk";

console.log(process.env);

function getPrefixedEnvParam(varName, defaultValue) {
  const envVariable = process.env[varName];

  if (envVariable) {
    return envVariable;
  }

  if (defaultValue) {
    return defaultValue;
  }

  throw `Missing env parameter '${envVariable}'`;
}
const snowflakeConfig = {
  account: getPrefixedEnvParam("account"),
  username: getPrefixedEnvParam("username"),
  password: getPrefixedEnvParam("password"),
  warehouse: getPrefixedEnvParam("warehouse"),
  role: getPrefixedEnvParam("role"),
  schema: getPrefixedEnvParam("schema"),
  database: getPrefixedEnvParam("database"),
};
// console.log('snowflakeConfig', snowflakeConfig)
var connection = snowflake.createConnection(snowflakeConfig);
let c;
connection.connect(async function (err, conn) {
  if (err) {
    console.error("Unable to connect: " + err.message);
    throw "Unable to connect: " + err.message;
  } else {
    console.log("Successfully connected to Snowflake.");
    c = conn;
  }
  function executeStatement(sql) {
    return new Promise((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        complete: function (err, stmt, rows) {
          if (err) {
            console.error(
              "Failed to execute statement due to the following error: " +
                err.message
            );
            reject(
              "Failed to execute statement due to the following error: " +
                err.message
            );
          } else {
            resolve();
          }
        },
      });
    });
  }
  await executeStatement(
    `
      CREATE TABLE IF NOT EXISTS snowflake_monitor(
        id text not null,
        table_name text not null,
        record_count BIGINT,
        is_valid boolean,
        stored_at timestamp default CURRENT_TIMESTAMP
      )
    `
  );
  await executeStatement(
    `
      ALTER TABLE snowflake_monitor
      ADD COLUMN if not exists reason text;
    `
  );
});

import express from "express";
const app = express();
const port = getPrefixedEnvParam("port", 3000);
console.log(
  `Starting at port=${port} with params=${JSON.stringify(snowflakeConfig)}`
);
const checkMap = {
  "contract-workspace": [
    "cw_key_dates",
    "cw_ld",
    "cw_projects",
    "cw_variation_orders",
  ],
  risk: ["risk_actions", "risk_csa", "risk_history", "risk_rbs", "risks", "risk_csa_history", "risk_csa_risks", "risk_linked_risks"],
  // "risk-views": [
  //   "risk_actions_view",
  //   "risk_csa_history",
  //   "risk_csa_risks",
  //   "risk_linked_risks",
  //   "risks_view",
  // ],
  rma: ["rma_csa_project"],
  "rma-views": ["rma_csa_risk_view"],
  "lets-agree": ["la_activities", "la_interfaces", "la_projects"],
  opentext: ["opentext_documents", "opentext_projects", "opentext_revisions"],
  primavera: ["primavera_project", "primavera_task"],
};

app.get("/check/:key", async (req, res) => {
  if (!c) {
    res.status(404).send("Conneciton to snowflake not set");
  }
  const key = req.params.key;
  if (!checkMap[key]) {
    res.status(404).send(`No such key=${key}!`);
    return;
  }

  const result = await validateKey(key);
  const isValid = result.reduce((acc, v) => acc && v.isValid, true);
  if (isValid) {
    res.status(200).send(result);
  } else {
    res.status(500).send(result);
  }
});

async function validateKey(key) {
  const tables = checkMap[key];
  const outcomes = await Promise.allSettled(
    tables.map((table) => validate(key, table))
  );
  const validationResults = outcomes.map((outcome) => outcome.value);
  await Promise.allSettled(validationResults.map(store));
  return validationResults;
}

function validate(key, table) {
  return new Promise((resolve, reject) => {
    const startTime = new Date();
    const validationResult = {
      key,
      table,
      isValid: false,
      count: false,
      queryTime: -1,
      lastSuccesfulCount: -1,
      expectedCount: -1,
      reason: null,
    };
    c.execute({
      sqlText: `SELECT count(*) result,
                      coalesce(
                        (SELECT record_count
                            FROM snowflake_monitor
                            WHERE id = '${key}' and table_name = '${table}' and is_valid = true
                            ORDER BY stored_at desc
                            LIMIT 1), 0) prev
                FROM ${table}`,
      complete: function (err, stmt, rows) {
        validationResult.queryTime = Math.round(new Date() - startTime);

        if (err) {
          console.error(
            "Failed to execute statement due to the following error: " +
              err.message,
            stmt.sqlText
          );
          validationResult.reason = `SQL_ERROR: ${stmt.sqlText}`;
          reject(validationResult);
        } else {
          validationResult.count = rows[0]["RESULT"];
          validationResult.lastSuccesfulCount = rows[0]["PREV"];
          validationResult.expectedCount = Math.floor(
            validationResult.lastSuccesfulCount * 0.9
          );
          validationResult.isValid =
            validationResult.count >= validationResult.expectedCount;
          // console.log(`${key}::${table}`, { validationResult });
          if (!validationResult.isValid) {
            validationResult.reason = `COUNT_BELOW_EXPECTED: Last=${validationResult.lastSuccesfulCount}, Expected=${validationResult.expectedCount}`;
          }
          resolve(validationResult);
        }
      },
    });
  });
}

function store(validationResult) {
  const {
    key,
    table,
    isValid,
    count,
    queryTime,
    lastSuccesfulCount,
    expectedCount,
    reason,
  } = validationResult;
  return new Promise((resolve, reject) => {
    console.log(
      "Storing result ",
      validationResult,
      `
    INSERT INTO snowflake_monitor(id,table_name,record_count,is_valid,reason)
    VALUES ('${key}','${table}',${count}, ${isValid}, ${
        reason ? `'${reason}'` : null
      })
    `
    );
    c.execute({
      sqlText: `
      INSERT INTO snowflake_monitor(id,table_name,record_count,is_valid,reason)
      VALUES ('${key}','${table}',${count}, ${isValid}, ${
        reason ? `'${reason}'` : null
      })
      `,
      complete: function (err, stmt, rows) {
        if (err) {
          console.error(
            "Failed to execute statement due to the following error: " +
              err.message,
            stmt.sqlText
          );
          reject();
        } else {
          resolve();
        }
      },
    });
  });
}

app.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
});
