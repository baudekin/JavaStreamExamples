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

import org.apache.http.NameValuePair;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CatalogSplitIteratorTest {

  private CatalogSplitIterator catItr;

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
    catItr = new CatalogSplitIterator( "sam_admin",
      "wds",
      "172.20.43.169",
      32082,
      "http",
      25 );
  }

  @org.junit.jupiter.api.Test
  void doGet() {
    List<NameValuePair> parameters = new ArrayList<>();
    parameters.add( CatalogSplitIterator.createNamedValuedPair( "start", "1" ) );
    parameters.add( CatalogSplitIterator.createNamedValuedPair( "size", "25" ) );
    parameters.add( CatalogSplitIterator.createNamedValuedPair( "browse", "true" ) );
    JSONObject json = catItr.doGet( "/api/v2/virtualfolder", parameters );
    assertTrue( json.getInt("totalCount") > 0 );

    // Test zero parameters and returning of JSONArray
    parameters = new ArrayList<>();
    json = catItr.doGet( "/api/v2/datasource", parameters );
    JSONArray jsonArr = json.getJSONArray( "jsonArray" );
    assertTrue( jsonArr.length() > 0 );
  }

  @org.junit.jupiter.api.Test
  void getBrowsableObjects() {
    JSONObject json = catItr.getBrowsableObjects( "/api/v2/virtualfolder", 1, 5 );
    JSONArray virtualFolders = json.getJSONArray( "list" );
    int totalCount = json.getInt("totalCount");
    assertTrue( totalCount >= 16 );
    assertTrue( virtualFolders.length() == 5 );
    json = catItr.getBrowsableObjects( "/api/v2/virtualfolder", 6, 20 );
    virtualFolders = json.getJSONArray( "list" );
    assertTrue( virtualFolders.length() == 10 );
  }

  @org.junit.jupiter.api.Test
  void tryAdvance() {
  }

  @org.junit.jupiter.api.Test
  void trySplit() {
  }

  @org.junit.jupiter.api.Test
  void estimateSize() {
  }
}