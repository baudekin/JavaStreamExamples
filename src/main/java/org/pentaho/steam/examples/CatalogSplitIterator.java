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
import org.apache.http.NameValuePair;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.cookie.ClientCookie;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class CatalogSplitIterator implements Spliterator<CatalogCompariable> {

  final private String name;
  final private String password;
  final private String host;
  final private int port;
  final private String scheme;
  final private int pageSize;
  final private int smallestSplitSize;
  private RequestConfig globalConfig;
  private RequestConfig localConfig;
  final private CookieStore cookieStore;

  private CatalogCompariable[] ccArr;
  private final AtomicInteger currentSplitPos = new AtomicInteger( 0 );
  private final int totalCount;


  private JSONObject[] createArr( JSONArray jsonArr ) {
    JSONObject[] jsonObjs = new JSONObject[ jsonArr.length() ];
    for ( int i=0; i<jsonArr.length(); i++ ) {
      jsonObjs[i] = jsonArr.getJSONObject( i );
    }
    return jsonObjs;
  }

  public CatalogSplitIterator( String name, String password, String host, int port, String scheme, int pageSize, int smallestSplitSize, CookieStore cookieStore, int totalCount, CatalogCompariable[] arr) {
    this.name = name;
    this.password = password;
    this.host = host;
    this.port = port;
    this.scheme = scheme;
    this.pageSize = pageSize;
    this.cookieStore = cookieStore;
    this.totalCount = totalCount;
    this.currentSplitPos.set( 0 );
    this.smallestSplitSize = smallestSplitSize;
    this.ccArr = arr;
  }

  public CatalogSplitIterator( String name, String password, String host, int port, String scheme, int pageSize, int smallestSplitSize ) {
    // TODO PUll query logic out by using existing builders
    this.name = name;
    this.password = password;
    this.host = host;
    this.port = port;
    this.scheme = scheme;
    this.pageSize = pageSize;
    this.currentSplitPos.set( 0 );
    this.smallestSplitSize = smallestSplitSize;
    setCookiePolicy();
    cookieStore = new BasicCookieStore();
    cookieStore.addCookie( createSessionCookie() );

    JSONObject json = this.getBrowsableObjects( "/api/v2/virtualfolder", 0, pageSize );
    totalCount = json.getInt("totalCount");
    ccArr = new CatalogCompariable[ totalCount ];
    // Load Objects
    loadJsonArray( json.getJSONArray( "list" ));
    loadObjects();
    reset();
  }

  public void reset() {
    currentSplitPos.set( 0 );
  }

  private void loadJsonArray( JSONArray jsonArray ) {
    for ( int i = 0; i< jsonArray.length(); i++ ) {
      int pos = currentSplitPos.getAndIncrement();
      ccArr[ pos ] = new CatalogCompariable( pos, jsonArray.getJSONObject( i ) );
    }
  }

  private void loadObjects() {
    // TODO Make this parallel in the future
    JSONObject json = this.getBrowsableObjects( "/api/v2/virtualfolder", currentSplitPos.get(), pageSize );
    loadJsonArray( json.getJSONArray( "list"  ) );
    // Terminate
    if ( currentSplitPos.get() < totalCount ) {
      loadObjects();
    } else {
      return;
    }
  }

  private void setCookiePolicy() {
    globalConfig = RequestConfig.custom()
      .setCookieSpec( CookieSpecs.DEFAULT )
      .build();
    localConfig = RequestConfig.copy( globalConfig )
      .setCookieSpec( CookieSpecs.STANDARD_STRICT )
      .build();
  }

  static public NameValuePair createNamedValuedPair( String name, String value ) {
    return new NameValuePair() {
      @Override public String getName() {
        return name;
      }

      @Override public String getValue() {
        return value;
      }
    };
  }

  protected JSONObject doGet( String path, List<NameValuePair> parameters ) {
    JSONObject json = null;
    // Set the pass the security Token stored in the cookieStore
    CloseableHttpClient httpClient = HttpClients.custom()
      .setDefaultCookieStore( cookieStore )
      .build();

    try {
      // Create the request string
      URI uri = new URIBuilder()
        .setScheme( scheme )
        .setHost( host )
        .setPort( port )
        .setPath( path )
        .setParameters( parameters )
        .build();

      // Create get request
      HttpGet request = new HttpGet( uri );
      request.addHeader( "content-type", "application/json" );
      HttpResponse result = httpClient.execute( request );
      String jsonStr = EntityUtils.toString( result.getEntity(), "UTF-8" );
      if ( jsonStr.startsWith( "[" ) ) {
        JSONArray jsonArr = new JSONArray( jsonStr );
        json = new JSONObject();
        json.put("jsonArray", jsonArr );
      } else {
        json = new JSONObject( jsonStr );
      }
    } catch ( IOException | URISyntaxException e ) {
      e.printStackTrace();
    } finally {
      try {
        httpClient.close();
      } catch ( IOException e ) {
        e.printStackTrace();
      }
    }
    return json;
  }

  JSONObject getBrowsableObjects( String path, int start, int size ) {
    List<NameValuePair> parameters = new ArrayList<>();
    parameters.add( CatalogSplitIterator.createNamedValuedPair( "start", Integer.toString( start ) ) );
    parameters.add( CatalogSplitIterator.createNamedValuedPair( "size", Integer.toString( size ) ) );
    parameters.add( CatalogSplitIterator.createNamedValuedPair( "browse", "true" ) );
    JSONObject json = doGet( path, parameters );
    return json;
  }

  private String getLoginToken() {
    CloseableHttpClient httpClient = HttpClients.createDefault();
    try {
    URI uri = new URIBuilder()
      .setScheme( scheme )
      .setHost( host )
      .setPort( port )
      .setPath( "/api/v2/login/" )
      .build();

    HttpPost request = new HttpPost( uri );
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
      HeaderElement[] elements = cookies[ 0 ].getElements();
      return elements[ 0 ].getValue();
    } catch ( IOException | URISyntaxException e ) {
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

  private BasicClientCookie createSessionCookie() {
    BasicClientCookie securityToken = new BasicClientCookie( "WDSessionId", getLoginToken() );
    // Set effective domain and path attributes
    securityToken.setDomain( host );
    securityToken.setPath( "/" );
    // Set attributes exactly as sent by the server
    securityToken.setAttribute( ClientCookie.PATH_ATTR, "/" );
    securityToken.setAttribute( ClientCookie.EXPIRES_ATTR, "Session" );
    securityToken.setAttribute( ClientCookie.SECURE_ATTR, "false" );
    return securityToken;
  }

  @Override public boolean tryAdvance( Consumer<? super CatalogCompariable> action ) {
    action.accept( ccArr[ currentSplitPos.getAndIncrement() ] );
    return currentSplitPos.get() < ccArr.length;
  }

  @Override public Spliterator<CatalogCompariable> trySplit() {
    int currentSize = ccArr.length - currentSplitPos.get();
    if ( currentSize  <  smallestSplitSize )  {
      return null;
    }

    // Split the array
    int splitPos = currentSize / 2 + currentSplitPos.intValue();
    Spliterator<CatalogCompariable> spliterator =
      new CatalogSplitIterator( name, password, host, port, scheme, pageSize, smallestSplitSize, cookieStore, totalCount, split( currentSplitPos.get(), splitPos ) );
    currentSplitPos.set( splitPos );
    return spliterator;
  }

  @Override public long estimateSize() {
    return ccArr.length - (long)currentSplitPos.get();
  }

  @Override public int characteristics() {
    return CONCURRENT | DISTINCT | ORDERED | SIZED | SUBSIZED;
  }

  public synchronized CatalogCompariable[] split(int begin, int end ) {
    CatalogCompariable[] arr = null;
    int len = end - begin;
    if ( len > 0 ) {
      arr = new CatalogCompariable[len];
      System.arraycopy( ccArr, begin, arr, 0, len );
    }
    return arr;
  }

}
