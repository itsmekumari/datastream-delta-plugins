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

package io.cdap.plugin.mysql.locators;

import io.cdap.e2e.utils.SeleniumDriver;
import org.openqa.selenium.By;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.FindBy;
import org.openqa.selenium.support.How;

/**
 * MySQL Plugin Locators.
 */
public class MysqlLocators {

    public static WebElement selectTable(String tableName) {
        return SeleniumDriver.getDriver().findElement(By.xpath("//div[contains(text(),'" + tableName + "')]" +
                "/preceding-sibling::div/span"));
    }

    @FindBy(how = How.XPATH, using = "//div[@data-cy='log-viewer-row']//div[contains(text(),'ERROR')]")
    public static WebElement error;

}