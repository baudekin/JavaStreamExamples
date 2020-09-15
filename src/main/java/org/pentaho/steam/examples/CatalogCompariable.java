/*
 * *****************************************************************************
 *
 *   Pentaho Data Integration
 *
 *   Copyright (C) 2002-2020 by Hitachi Vantara : http://www.pentaho.com
 *
 *  ******************************************************************************
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with
 *   the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *  *****************************************************************************
 */

package org.pentaho.steam.examples;

import org.json.JSONObject;

public class CatalogCompariable implements Comparable<CatalogCompariable> {

    private int pos;
    private JSONObject json;

    public int getPos() {
      return pos;
    }

    public void setPos( int pos ) {
      this.pos = pos;
    }

    public JSONObject getJson() {
      return json;
    }

    public void setJson( JSONObject json ) {
      this.json = json;
    }

    public CatalogCompariable( int pos, JSONObject json ) {
      this.pos = pos;
      this.json = json ;
    }

    @Override public int hashCode() {
      return pos;
    }

    @Override public boolean equals( Object obj ) {
      return ( obj instanceof org.pentaho.steam.examples.CatalogCompariable ) && ( ( (org.pentaho.steam.examples.CatalogCompariable ) obj ).pos == pos );
    }

    @Override public String toString() {
      return "Pos: " + pos + " Json: " + json.toString();
    }

    @Override public int compareTo( org.pentaho.steam.examples.CatalogCompariable c ) {
      return pos - c.getPos();
    }
}
