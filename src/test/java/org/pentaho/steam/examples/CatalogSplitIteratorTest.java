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

import static org.junit.jupiter.api.Assertions.*;

class CatalogSplitIteratorTest {

  private CatalogSplitIterator catItr;

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
    catItr = new CatalogSplitIterator( "sam_admin",
      "wds",
      "http://172.20.43.169:32082/api/v2/login/",
      25 );
  }

  @org.junit.jupiter.api.Test
  void getLoginToken() {
    String token = catItr.getLoginToken();
    assertNotNull( token );
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