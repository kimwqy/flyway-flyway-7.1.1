/*
 * Copyright © Red Gate Software Ltd 2010-2020
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.flywaydb.core.internal.database.xugu;

import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.logging.Log;
import org.flywaydb.core.api.logging.LogFactory;
import org.flywaydb.core.internal.database.base.Schema;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.database.h2.H2Table;
import org.flywaydb.core.internal.database.hsqldb.HSQLDBTable;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.util.StringUtils;

import java.sql.SQLException;
import java.util.*;

import static org.flywaydb.core.internal.database.oracle.OracleSchema.ObjectType.*;

/**
 * Xugu implementation of Schema.
 */
public class XuguSchema extends Schema<XuguDatabase, XuguTable> {

    /**
     * Creates a new schema.
     *
     * @param jdbcTemplate The Jdbc Template for communicating with the DB.
     * @param database     The database-specific support.
     * @param name         The name of the schema.
     */
    public XuguSchema(JdbcTemplate jdbcTemplate, XuguDatabase database, String name) {
        super(jdbcTemplate, database, name);
    }

    @Override
    protected boolean doExists() throws SQLException {
        return jdbcTemplate.queryForInt("SELECT (SELECT 1 FROM all_schemas  WHERE schema_name=? LIMIT 1)", name) > 0;
    }

    @Override
    protected boolean doEmpty() throws SQLException {
        return allTables().length == 0;
    }

    @Override
    protected void doCreate() throws SQLException {
        jdbcTemplate.execute("CREATE SCHEMA " + database.quote(name)+" AUTHORIZATION "+ database.getCurrentUser());
    }

    @Override
    protected void doDrop() throws SQLException {
        jdbcTemplate.execute("DROP SCHEMA " + database.quote(name));
    }

    @Override
    protected void doClean() throws SQLException {
        for (Table table : allTables()) {
            table.drop();
        }
    }

    @Override
    protected XuguTable[] doAllTables() throws SQLException {
        List<String> tableNames = jdbcTemplate.queryForStringList(
                "SELECT TABLE_NAME FROM all_tables AT ,ALL_SCHEMAS ALS where  AT.SCHEMA_ID = ALS.SCHEMA_ID AND ALS.SCHEMA_NAME = ? ", name);

        XuguTable[] tables = new XuguTable[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
            tables[i] = new XuguTable(jdbcTemplate, database, this, tableNames.get(i));
        }
        return tables;
    }

    @Override
    public Table getTable(String tableName) {
        return new XuguTable(jdbcTemplate, database, this, tableName);
    }
}