/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import groovy.sql.Sql

env = System.getenv()

db = [ url:env.jdbcurl, user:env.username, password:env.password, driver:'org.apache.hive.jdbc.HiveDriver']

sql = Sql.newInstance(db.url, db.user, db.password, db.driver)

println("Dropping avro_hive_table")
sql.execute """DROP TABLE IF EXISTS avro_hive_table"""

println("Creating avro_hive_table")
sql.execute """
CREATE EXTERNAL TABLE avro_hive_table
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 's3a://temp-bucket/transactions/'
TBLPROPERTIES (
    'avro.schema.literal'='{
                            "namespace": "transaction.avro",
                                     "type": "record",
                                     "name": "Transaction",
                                     "fields": [
                                         {"name": "InvoiceNo",     "type": "int"    },
                                         {"name": "StockCode",     "type": "string" },
                                         {"name": "Description",   "type": "string" },
                                         {"name": "Quantity",      "type": "int"    },
                                         {"name": "InvoiceDate",   "type": "long"   },
                                         {"name": "UnitPrice",     "type": "float"  },
                                         {"name": "CustomerID",    "type": "int"    },
                                         {"name": "Country",       "type": "string" },
                                         {"name": "LineNo",        "type": "int"    },
                                         {"name": "InvoiceTime",   "type": "string" },
                                         {"name": "StoreID",       "type": "int"    },
                                         {"name": "TransactionID", "type": "string" }
                                     ]
                            }'
    )
""".toString()

println("Selecting from avro_hive_table")
sql.eachRow("select T.* from avro_hive_table T LIMIT 10".toString()) { row ->
  println "$row"
}

