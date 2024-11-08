import snowflake from "snowflake-sdk";
var connection = snowflake.createConnection({
  account: process.env.account,
  username: process.env.username,
  password: process.env.password,
  warehouse: process.env.warehouse,
  role: process.env.role,
  schema: process.env.schema,
  database: process.env.database,
});
let c;
connection.connect(function (err, conn) {
  if (err) {
    console.error("Unable to connect: " + err.message);
  } else {
    console.log("Successfully connected to Snowflake.");
    c = conn;
  }

  conn.execute({
    sqlText: `
      CREATE TABLE IF NOT EXISTS snowflake_monitor(
        id text not null,
        table_name text not null,
        record_count BIGINT,
        is_valid boolean,
        stored_at timestamp default CURRENT_TIMESTAMP
      )
    `,
    complete: function (err, stmt, rows) {
      if (err) {
        console.error(
          "Failed to execute statement due to the following error: " +
            err.message,
        );
      } else {
        console.log("Successfully executed statement: " + stmt.getSqlText());
      }
    },
  });
});

import express from "express";
const app = express();
const port = process.env.port ?? 3000;

const checkMap = {
  "contract-workspace": [
    "cw_key_dates",
    "cw_ld",
    "cw_projects",
    "cw_variation_orders",
  ],
  risk: ["risk_actions", "risk_csa", "risk_history", "risk_rbs", "risks"],
  "risk-views": [
    "risk_actions_view",
    "risk_csa_history",
    "risk_csa_risks",
    "risk_linked_risks",
    "risks_view",
  ],
  rma: ["rma_csa_project"],
  "rma-views": ["rma_csa_risk_view"],
  "lets-agree": ["la_activities", "la_interfaces", "la_projects"],
  opentext: ["opentext_documents", "opentext_projects", "opentext_revisions"],
  primavera: ["primavera_project", "primavera_task"],
};
console.log(Object.keys(checkMap));
app.get("/check/:key", async (req, res) => {
  const key = req.params.key;
  if (!checkMap[key]) {
    res.status(404).send("No such key!");
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
  console.log("Validating ", key);
  const tables = checkMap[key];
  const outcomes = await Promise.allSettled(
    tables.map((table) => validate(key, table)),
  );
  const validationResults = outcomes.map((outcome) => outcome.value);
  console.log("Validation done ", validationResults);
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
            stmt.sqlText,
          );
          reject(validationResult);
        } else {
          validationResult.count = rows[0]["RESULT"];
          validationResult.lastSuccesfulCount = rows[0]["PREV"];
          validationResult.expectedCount =
            validationResult.lastSuccesfulCount * 0.9;
          validationResult.isValid =
            validationResult.count >= validationResult.expectedCount;
          // console.log(`${key}::${table}`, { validationResult });
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
  } = validationResult;
  return new Promise((resolve, reject) => {
    console.log(
      "Storing result ",
      validationResult,
      `
    INSERT INTO snowflake_monitor(id,table_name,record_count,is_valid)
    VALUES ('${key}','${table}',${count}, ${isValid})
    `,
    );
    c.execute({
      sqlText: `
      INSERT INTO snowflake_monitor(id,table_name,record_count,is_valid)
      VALUES ('${key}','${table}',${count}, ${isValid})
      `,
      complete: function (err, stmt, rows) {
        if (err) {
          console.error(
            "Failed to execute statement due to the following error: " +
              err.message,
            stmt.sqlText,
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
