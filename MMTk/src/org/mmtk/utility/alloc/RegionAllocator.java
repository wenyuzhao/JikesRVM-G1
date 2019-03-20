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

import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.policy.RegionSpace;
import org.mmtk.utility.Atomic;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Conversions;
import org.mmtk.utility.Log;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;

import static org.mmtk.utility.Constants.ALIGNMENT_VALUE;
import static org.mmtk.utility.Constants.BYTES_IN_INT;

/**
 *
 */
@Uninterruptible
public class RegionAllocator extends Allocator {

  /****************************************************************************
   *
   * Instance variables
   */

  protected final RegionSpace space;
  protected final int spaceDescriptor;
  private final int allocationKind;
  private Address region = Address.zero();
  private Address cursor = Address.zero();
  private Address limit = Address.zero();

  /**
   * Constructor.
   *
   * @param space The space to bump point into.
   * @param allocationKind TODO
   */
  public RegionAllocator(RegionSpace space, int allocationKind) {
    this.space = space;
    this.spaceDescriptor = space.getDescriptor();
    this.allocationKind = allocationKind;
  }

  public void retireTLAB() {
    if (VM.VERIFY_ASSERTIONS) {
      VM.assertions._assert(!cursor.isZero());
      VM.assertions._assert(!limit.isZero());
      VM.assertions._assert(Region.allocated(region));
    }

    fillAlignmentGap(cursor, limit);

    final Address cursorSlot = Region.metaDataOf(region, Region.METADATA_CURSOR_OFFSET);
    Address oldLimit;
    final Address newLimit = cursor;
    do {
      oldLimit = cursorSlot.prepareAddress();
      if (oldLimit.GE(newLimit)) break;
    } while (cursorSlot.attempt(oldLimit, newLimit));

    region = Address.zero();
    cursor = Address.zero();
    limit = Address.zero();
  }

  /**
   * Reset the allocator. Note that this does not reset the space.
   */
  public void reset() {
    if (!cursor.isZero()) {
      retireTLAB();
    }
    if (refills != 0) {
      totalRefills.add(refills);
      refills = 0;
    }
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
//      if (!cursor.isZero()) retireTLAB(cursor);
//      reset();
      if (VM.activePlan.isMutator()) refills += 1;
      return allocSlowInline(bytes, align, offset);
    }
    /* sufficient memory is available, so we can finish performing the allocation */
    fillAlignmentGap(cursor, start);
    cursor = end;
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
  protected final Address allocSlowOnce(int bytes, int align, int offset) {
    if (!cursor.isZero()) {
      retireTLAB();
    }
    int newTlabSize = bytes < tlabSize ? tlabSize : bytes;
    Address ptr = space.allocTLAB(allocationKind, newTlabSize); // New tlab
    if (ptr.isZero()) {
      return ptr; // failed allocation --- we will need to GC
    }
    /* we have been given a clean block */
    cursor = ptr;
    region = Region.of(cursor);
    limit = ptr.plus(newTlabSize);
    return alloc(bytes, align, offset);
  }


  static Atomic.Int totalRefills = new Atomic.Int();
  int refills = 0;
  static int gc = 0;
  static int tlabSize = Region.MIN_TLAB_SIZE;

  @Inline
  public static void adjustTLABSize() {
    gc += 1;
    int refills = totalRefills.get();
    if (refills == 50) return;
    int s = tlabSize;
    s *= (refills / gc / 50);
    final int KB = Constants.BYTES_IN_KBYTE;

    if (s < 2 * KB) {
      s = 2 * KB;
    } else if (s < 4 * KB) {
      s = 4 * KB;
    } else if (s < 8 * KB) {
      s = 8 * KB;
    } else if (s < 16 * KB) {
      s = 16 * KB;
    } else if (s < 32 * KB) {
      s = 32 * KB;
    } else if (s < 64 * KB) {
      s = 64 * KB;
    } else if (s < 128 * KB) {
      s = 128 * KB;
    } else if (s < 256 * KB) {
      s = 256 * KB;
    } else if (s < 512 * KB) {
      s = 512 * KB;
    } else {
      s = 1024 * KB;
    }
//    tlabSize = Conversions.alignUp(Address.fromIntZeroExtend(refills), Constants.LOG_BYTES_IN_KBYTE).toInt();
//    tlabSize = Region.BYTES_IN_REGION / ((int) (Region.BYTES_IN_REGION / tlabSize));
    if (s < Region.MIN_TLAB_SIZE) s = Region.MIN_TLAB_SIZE;
    if (s > Region.MAX_TLAB_SIZE) s = Region.MAX_TLAB_SIZE;
    tlabSize = s;
//    Log.writeln("TLABSize: ", tlabSize);
//    Log.writeln("GC Per: ", tlabSize);
  }

  /** @return the space associated with this squish allocator */
  @Override
  public final Space getSpace() {
    return space;
  }
}
