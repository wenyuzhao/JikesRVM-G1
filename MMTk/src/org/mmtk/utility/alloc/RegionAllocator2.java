/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */

package org.mmtk.utility.alloc;

import org.mmtk.plan.g1.G1;
import org.mmtk.policy.Space;
import org.mmtk.policy.region.Card;
import org.mmtk.policy.region.CardOffsetTable;
import org.mmtk.policy.region.Region;
import org.mmtk.policy.region.RegionSpace;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;

/**
 *
 */
@Uninterruptible
public class RegionAllocator2 extends Allocator {

  static final int LOG_UNIT_SIZE = 9;
  static final int UNIT_SIZE = 1 << LOG_UNIT_SIZE;
  static final int MIN_TLAB_SIZE = 2 * 1024;
  static final int MAX_TLAB_SIZE = Region.BYTES_IN_REGION;

  protected final RegionSpace space;
  protected final int spaceDescriptor;
  private final int generation;
  private Address cursor = Address.zero();
  private Address limit = Address.zero();
  int refills = 0;
  int tlabSize = MIN_TLAB_SIZE;// + MAX_TLAB_SIZE) >> 1;
  private static final int REFILLS_PER_GC = 50;

  /**
   * Constructor.
   *
   * @param space The space to bump point into.
   * @param generation TODO
   */
  public RegionAllocator2(RegionSpace space, int generation) {
    this.space = space;
    this.spaceDescriptor = space.getDescriptor();
    this.generation = generation;
  }

  @Inline
  static int alignTLAB(int size) {
    size = size + (UNIT_SIZE - 1);
    return size & ~(UNIT_SIZE - 1);
  }

  public void adjustTLABSize() {
    float factor = (float) refills / ((float) REFILLS_PER_GC);
    tlabSize = (int) (((float) tlabSize) * factor);
    tlabSize = alignTLAB(tlabSize);
    if (tlabSize < MIN_TLAB_SIZE) {
      tlabSize = MIN_TLAB_SIZE;
    } else if (tlabSize > MAX_TLAB_SIZE) {
      tlabSize = MAX_TLAB_SIZE;
    }
    refills = 0;
  }

  public void reset() {
    retireTLAB();
    cursor = Address.zero();
    limit = Address.zero();
  }

  @Inline
  private static void initOffsets(final Address start, Address limit) {
    Address region = Region.of(start);
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(limit.LE(region.plus(Region.BYTES_IN_REGION)));
    Address cursor = start;
    while (cursor.LT(limit)) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(cursor.GE(region));
        VM.assertions._assert(Card.isAligned(cursor));
      }
      CardOffsetTable.set(region, cursor, start);
      cursor = cursor.plus(Card.BYTES_IN_CARD);
    }
  }

  @Inline
  private void retireTLAB() {
    if (!G1.ENABLE_REMEMBERED_SETS) return;
    if (cursor.isZero() || limit.isZero()) {
      return;
    }
    fillAlignmentGap(cursor, limit);
  }

  /*****************************************************************************
   *
   * Public interface
   */

  /**
   * Allocate space for a new object.  This is frequently executed code and
   * the coding is deliberately sensitive to the optimizing compiler.
   * After changing this, always check the IR/MC that is generated.
   *
   * @param bytes The number of bytes allocated
   * @param align The requested alignment
   * @param offset The offset from the alignment
   * @return The address of the first byte of the allocated region
   */
  @Inline
  public final Address alloc(int bytes, int align, int offset) {
    /* establish how much we need */
    Address start = alignAllocationNoFill(cursor, align, offset);
    Address end = start.plus(bytes);
    /* check whether we've exceeded the limit */
    if (end.GT(limit)) {
      return allocSlow(bytes, align, offset);
    }
    /* sufficient memory is available, so we can finish performing the allocation */
    if (G1.ENABLE_REMEMBERED_SETS) fillAlignmentGap(cursor, start);
    cursor = end;
    end.plus(128).prefetch();
    return start;
  }

  /**
   * External allocation slow path (called by superclass when slow path is
   * actually taken.  This is necessary (rather than a direct call
   * from the fast path) because of the possibility of a thread switch
   * and corresponding re-association of bump pointers to kernel
   * threads.
   *
   * @param bytes The number of bytes allocated
   * @param align The requested alignment
   * @param offset The offset from the alignment
   * @return The address of the first byte of the allocated region or
   * zero on failure
   */
  @Override
  @Inline
  protected final Address allocSlowOnce(int bytes, int align, int offset) {
    int size = bytes > tlabSize ? bytes : tlabSize;
    size = alignTLAB(size);
    Address tlab = space.allocTLAB(generation, size);
    if (tlab.isZero()) return tlab;
    refills += 1;
    retireTLAB();
    cursor = tlab;
    limit = cursor.plus(size);
    if (G1.ENABLE_REMEMBERED_SETS) initOffsets(cursor, limit);
    return alloc(bytes, align, offset);
  }

  /** @return the space associated with this squish allocator */
  @Override
  public final Space getSpace() {
    return space;
  }
}
