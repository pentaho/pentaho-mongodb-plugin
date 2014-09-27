/*!
* Copyright 2010 - 2013 Pentaho Corporation.  All rights reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
*/

package org.pentaho.di.trans.steps.mongodbdelete;

/**
 * Enumeration for supported comparator for MongoDbDelete step.
 *
 * @author Maas Dianto (maas.dianto@gmail.com)
 */
public enum Comparator {

    EQUAL("=")
    , NOT_EQUAL("<>")
    , LESS_THAN("<")
    , LESS_THAN_EQUAL("<=")
    , GREATER_THAN(">")
    , GREATER_THAN_EQUAL(">=")
    , BETWEEN("BETWEEN")
    , IS_NULL("IS NULL")
    , IS_NOT_NULL("IS NOT NULL")
    ;

    private String value;

    private Comparator(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static String[] asLabel() {
        String[] lables = new String[Comparator.values().length];
        int index = 0;
        for (Comparator mo : Comparator.values()) {
            lables[index] = mo.value;
            index++;
        }
        return lables;
    }
}
