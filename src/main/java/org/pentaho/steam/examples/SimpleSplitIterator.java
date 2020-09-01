/*! ******************************************************************************
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

import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class SimpleSplitIterator implements Spliterator<Resource> {
  // To understand how the Spliterator works I choose an array instead of the collect so
  // I would have to really implement the Spliterator methods.
  private final Resource[] resourceArr;
  private final AtomicInteger index = new AtomicInteger(0);
  private final AtomicInteger currentSplitPos = new AtomicInteger( 0 );
  private final int smallestSplitSize;


  public SimpleSplitIterator( int size, int smallestSplitSize ) {
    resourceArr = new Resource[size];
    this.smallestSplitSize = smallestSplitSize;
  }

  public SimpleSplitIterator( Resource[] arr, int smallestSplitSize  ) {
    this( arr.length, smallestSplitSize );
    for ( Resource resource : arr ) {
      add( resource );
    }
  }

  public synchronized void add( Resource r ) {
    resourceArr[ index.getAndIncrement() ] = r;
  }

  public synchronized Resource[] split(int begin, int end ) {
    Resource[] resArr = null;
    int len = end - begin;
    if ( len > 0 ) {
      resArr = new Resource[len];
      System.arraycopy( resourceArr, begin, resArr, 0, len );
    }
    return resArr;
  }

  public synchronized Resource pop() {
    if ( ( index.get() > resourceArr.length ) || ( index.get() == 0 ) ) {
      return null;
    }
    return resourceArr[ index.decrementAndGet() ];
  }

  public synchronized void reset() {
    index.set( 0 );
    currentSplitPos.set( 0 );
  }

  /**
   * Purpose: To accept a lambda action and perform it on the current Split postion.
   * @param action - lambda
   * @return True is if more Resources left to process.
   */
  @Override public boolean tryAdvance( Consumer<? super Resource> action ) {
    action.accept( resourceArr[ currentSplitPos.getAndIncrement() ] );
    return currentSplitPos.get() < resourceArr.length;
  }

  /**
   * Purpose: To split the Resource Arr into sub arrays for parallel processing. The size of the split is limited
   * by the smallestSplitSize value.
   * @return Instance of Spliteterator containing the sub array.
   */
  @Override public Spliterator<Resource> trySplit() {
    int currentSize = resourceArr.length - currentSplitPos.get();
    if ( currentSize  <  smallestSplitSize )  {
      return null;
    }

    // Split the array
    int splitPos = currentSize / 2 + currentSplitPos.intValue();
    Spliterator<Resource> spliterator = new SimpleSplitIterator( split( currentSplitPos.get(), splitPos ), smallestSplitSize );
    currentSplitPos.set( splitPos );
    return spliterator;
  }

  /**
   * Purpose: To calculate the number of remaining resources to process.
   *
   * @return number of reaming resources to process.
   */
  @Override public long estimateSize() {
    return resourceArr.length - (long)currentSplitPos.get();
  }

  /**
   * Purpose: To return the behavior of this Iterator captured as a bit mask. The possible values are:
   *
   * SIZED – if it's capable of returning an exact number of elements with the estimateSize() method
   * SORTED – if it's iterating through a sorted source
   * SUBSIZED – if we split the instance using a trySplit() method and obtain Spliterators that are SIZED as well
   * CONCURRENT – if source can be safely modified concurrently
   * DISTINCT – if for each pair of encountered elements x, y, !x.equals(y)
   * IMMUTABLE – if elements held by source can't be structurally modified
   * NONNULL – if source holds nulls or not
   * ORDERED – if iterating over an ordered sequence
   *
   * @return
   */
  @Override public int characteristics() {
    return CONCURRENT | DISTINCT | ORDERED | SIZED | SUBSIZED;
  }


}
