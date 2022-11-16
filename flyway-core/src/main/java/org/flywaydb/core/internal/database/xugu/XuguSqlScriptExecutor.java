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

import org.flywaydb.core.internal.callback.CallbackExecutor;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.sqlscript.DefaultSqlScriptExecutor;

@SuppressWarnings("SqlResolve")
public class XuguSqlScriptExecutor extends DefaultSqlScriptExecutor {
    public XuguSqlScriptExecutor(JdbcTemplate jdbcTemplate,
                                 CallbackExecutor callbackExecutor, boolean undo, boolean batch, boolean outputQueryResults,
                                 StatementInterceptor statementInterceptor
    ) {
        super(jdbcTemplate, callbackExecutor, undo, batch, outputQueryResults, statementInterceptor
        );
    }












































































































































































































































































}