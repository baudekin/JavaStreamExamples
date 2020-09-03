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


import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.cookie.ClientCookie;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Spliterator;
import java.util.function.Consumer;

public class CatalogSplitIterator implements Spliterator<JSONObject> {

  final private String name;
  final private String password;
  final private String baseCatalogUrlStr;
  final private int pageSize;
  private BasicClientCookie securityToken;

  CatalogSplitIterator( String name, String password, String baseCatalogUrlStr, int pageSize ) {
    this.name = name;
    this.password = password;
    this.baseCatalogUrlStr = baseCatalogUrlStr;
    this.pageSize = pageSize;
  }

  protected String getLoginToken() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpPost request = new HttpPost( this.baseCatalogUrlStr );
    try {
      JSONObject json = new JSONObject();
      json.put( "username", name );
      json.put( "password", password );
      StringEntity params = new StringEntity( json.toString() );
      request.addHeader( "content-type", "application/json" );
      request.setEntity( params );
      HttpResponse result = null;
      result = httpClient.execute( request );
      String jsonStr = EntityUtils.toString( result.getEntity(), "UTF-8" );
      json = new JSONObject( jsonStr );
      Header[] cookies = result.getHeaders( "Set-Cookie" );
      HeaderElement[] elements = cookies[0].getElements();
      return elements[0].getValue();
    } catch ( IOException e ) {
      e.printStackTrace();
    } finally {
      try {
        httpClient.close();
      } catch ( IOException e ) {
        e.printStackTrace();
      }
    }
    return null;
  }

  protected BasicClientCookie createGetSessionCookie( String token ) {
    if ( securityToken == null ) {
      securityToken = new BasicClientCookie( "WDSessionId", getLoginToken() );
      // Set effective domain and path attributes
      securityToken.setDomain( "172.20.43.169" );
      securityToken.setPath( "/" );
      // Set attributes exactly as sent by the server
      securityToken.setAttribute( ClientCookie.PATH_ATTR, "/" );
      securityToken.setAttribute( ClientCookie.EXPIRES_ATTR, "Session" );
      securityToken.setAttribute( ClientCookie.SECURE_ATTR, "false" );
    }
    return securityToken;
  }

  protected JSONObject getPageOfResources( String virtualFolderId, int start, int size ) {
    HttpGet request = new HttpGet( this.baseCatalogUrlStr );



  }

  @Override public boolean tryAdvance( Consumer<? super JSONObject> action ) {
    return false;
  }

  @Override public Spliterator<JSONObject> trySplit() {
    return null;
  }

  @Override public long estimateSize() {
    return 0;
  }

  @Override public int characteristics() {
    return 0;
  }
}
