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

import org.junit.Assert;

import java.util.List;
import java.util.Spliterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

class SimpleSplitIteratorTest {

  private SimpleSplitIterator resources;
  private int size = 1000;

  @org.junit.jupiter.api.BeforeEach
  void setUp() {
    resources = new SimpleSplitIterator( size, 10 );
    for ( int i =0; i<size; i++) {
      resources.add( new Resource( i, "Resource_" + i ) );
    }
  }

  @org.junit.jupiter.api.Test
  void add() {
    SimpleSplitIterator ssi = new SimpleSplitIterator( 1, 10 );
    ssi.add( new Resource( 21, "Test" ) );
    Resource r = ssi.pop();
    Assert.assertEquals( 21, r.getId() );
    Assert.assertEquals( "Test", r.getName()  );
  }

  @org.junit.jupiter.api.Test
  void split() {
    Resource[] resArr =  resources.split( 0, 5 );
    Assert.assertEquals( 5, resArr.length );
    Assert.assertEquals( 0, resArr[0].getId() );
    Assert.assertEquals( 1, resArr[1].getId() );
    Assert.assertEquals( 2, resArr[2].getId() );
    Assert.assertEquals( 3, resArr[3].getId() );
    Assert.assertEquals( 4, resArr[4].getId() );
  }

  @org.junit.jupiter.api.Test
  void pop() {
    SimpleSplitIterator ssi = new SimpleSplitIterator( 1, 10 );
    ssi.add( new Resource( 21, "Test" ) );
    Resource r = ssi.pop();
    Assert.assertEquals( 21, r.getId() );
    Assert.assertEquals( "Test", r.getName()  );
    r = ssi.pop();
    Assert.assertNull( r );
  }

  @org.junit.jupiter.api.Test
  void tryAdvance() {
    int index = 0;
    // Append "-Action" to the name using an consuable action.
    while ( resources.tryAdvance( resourceAction -> resourceAction.setName( resourceAction.getName().concat( "-Action" ) ) ) ) {
      index++;
    }
    Assert.assertEquals( size - 1, index );
    resources.reset();
    Stream<Resource> resStream = StreamSupport.stream( resources, true );
    Assert.assertEquals( size, resStream.count() );
    resources.reset();
    resStream = StreamSupport.stream( resources, true );
    Assert.assertEquals( size, resStream.parallel().filter( r -> { return r.getName().endsWith("-Action"); } ).count() );
  }

  @org.junit.jupiter.api.Test
  void trySplit() {
    Spliterator<Resource> si = resources.trySplit();
    int expected = size / 2;
    Assert.assertEquals( expected, si.estimateSize() );
    si = si.trySplit();
    expected = expected / 2;
    Assert.assertEquals( expected, si.estimateSize() );
  }

  @org.junit.jupiter.api.Test
  void estimateSize() {
    Assert.assertEquals( size, resources.estimateSize() );
  }

  @org.junit.jupiter.api.Test
  void characteristics() {
    Assert.assertEquals( Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.CONCURRENT | Spliterator.SIZED | Spliterator.SUBSIZED, resources.characteristics() );
  }

  @org.junit.jupiter.api.Test
  void functionalStreamingTest() {
    // See if we can filter out the first 50 resources
    Stream<Resource> resStream = StreamSupport.stream( resources, true );
    Assert.assertEquals( 50, resStream.parallel().filter( r -> { return r.getId() < 50; } ).count() );

    // See if we can filter out the first 3 resources and return as a sorted collection.
    resources.reset();
    resStream = StreamSupport.stream( resources, true );
    List<Resource> resList = resStream.parallel().filter( r -> { return r.getId() < 3; } ).sorted().collect( Collectors.toList() );

    Assert.assertEquals( 0, resList.get(0).getId() );
    Assert.assertEquals( 1, resList.get(1).getId() );
    Assert.assertEquals( 2, resList.get(2).getId() );
  }
}