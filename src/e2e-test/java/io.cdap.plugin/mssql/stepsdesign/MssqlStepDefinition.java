/*
 * Copyright (c) 2023.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.mssql.stepsdesign;

import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.mssql.actions.MssqlActions;
import io.cdap.plugin.mssql.hooks.MssqlTestSetUpHooks;
import io.cdap.plugin.mssql.utils.BQValidation;
import io.cucumber.java.en.And;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;

/**
 * Contains Mssql replication test scenarios step definitions.
 */
public class MssqlStepDefinition implements CdfHelper {

  @Then("Select the source table if available")
  public void selectTable() {
    MssqlActions.selectTable();
  }

  @And("Run insert, update and delete CDC events on source table")
  public void executeCdcEvents() throws SQLException, ClassNotFoundException {
    MssqlActions.executeCdcEventsOnSourceTable();
  }

  @Then("Validate the values of records transferred to target BigQuery table is equal to the values from MsSQL " +
          "source Table")
  public void validateTheValuesOfRecordsTransferredToTargetBigQueryTableIsEqualToTheValuesFromMsSQLSourceTable()
          throws InterruptedException, IOException, SQLException, ClassNotFoundException, ParseException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("mssqlSourceTable"));
    BeforeActions.scenario.write("No of Records Transferred to BigQuery:" + targetBQRecordsCount);
    boolean recordsMatched = BQValidation.validateDBToBQRecordValues(MssqlTestSetUpHooks.schemaName,
            MssqlTestSetUpHooks.tableName, MssqlTestSetUpHooks.tableName);
    Assert.assertTrue("Value of records transferred to the target table should be equal to the value " +
            "of the records in the source table", recordsMatched);
  }
}
