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

package io.cdap.plugin.mssql.actions;

import io.cdap.e2e.utils.AssertionHelper;
import io.cdap.e2e.utils.ElementHelper;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.e2e.utils.SeleniumHelper;
import io.cdap.e2e.utils.WaitHelper;
import io.cdap.plugin.common.locators.Locators;
import io.cdap.plugin.mssql.utils.MssqlClient;

import java.sql.SQLException;

/**
 * Replication Mssql Actions.
 */
public class MssqlActions {
    public static String tableName = PluginPropertyUtils.pluginProp("mssqlSourceTable");
    public static String schemaName = PluginPropertyUtils.pluginProp("mssqlSchema");
    public static String datatypeValues = PluginPropertyUtils.pluginProp("mssqlDatatypeForInsertOperation");
    public static String deleteCondition = PluginPropertyUtils.pluginProp("mssqlDeleteRowCondition");
    public static String updateCondition = PluginPropertyUtils.pluginProp("mssqlUpdateRowCondition");
    public static String updatedValue = PluginPropertyUtils.pluginProp("mssqlUpdatedRow");

    static {
        SeleniumHelper.getPropertiesLocators(Locators.class);
    }

    public static void selectTable() {
        String table = schemaName + "." + tableName;
        WaitHelper.waitForElementToBeDisplayed(Locators.selectTable(table), 300);
        AssertionHelper.verifyElementDisplayed(Locators.selectTable(table));
        ElementHelper.clickOnElement(Locators.selectTable(table));
    }

    public static void executeCdcEventsOnSourceTable()
            throws SQLException, ClassNotFoundException {
        MssqlClient.insertRow(tableName, schemaName, datatypeValues);
        MssqlClient.updateRow(tableName, schemaName, updateCondition, updatedValue);
        MssqlClient.deleteRow(tableName, schemaName, deleteCondition);
    }
}
