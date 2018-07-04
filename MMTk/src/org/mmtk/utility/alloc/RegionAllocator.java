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
import org.mmtk.utility.Log;
import org.mmtk.vm.Assert;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;

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
  private final boolean copy;
  private Address cursor;
  private Address limit;

  /**
   * Constructor.
   *
   * @param space The space to bump point into.
   * @param copy TODO
   */
  public RegionAllocator(RegionSpace space, boolean copy) {
    this.space = space;
    this.copy = copy;
    reset();
  }

  /**
   * Reset the allocator. Note that this does not reset the space.
   */
  public void reset() {
    cursor = Address.zero();
    limit = Address.zero();
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
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(bytes > 0, "Trying to allocate negative bytes");
    /* establish how much we need */
    Address start = alignAllocationNoFill(cursor, align, offset);
    Address end = start.plus(bytes);
    /* check whether we've exceeded the limit */
    if (end.GT(limit)) {
        return allocSlowInline(bytes, align, offset);
    }
    /* sufficient memory is available, so we can finish performing the allocation */
    fillAlignmentGap(cursor, start);
    cursor = end;
    if (Space.isInSpace(this.space.getDescriptor(), start)) {
      Region.setCursor(Region.of(start), cursor);
    }
    if (VM.VERIFY_ASSERTIONS) {
      if (!Region.allocated(Region.of(start))) {
        Log.writeln("cursor: ", cursor);
        Log.writeln("start: ", start);
        Log.writeln("limit: ", limit);
      }
      VM.assertions._assert(Region.allocated(Region.of(start)));
    }
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
    Address ptr = space.getSpace(copy); // New block

    if (ptr.isZero()) {
      return ptr; // failed allocation --- we will need to GC
    }
    /* we have been given a clean block */
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.isAligned(ptr));
    cursor = ptr;
    limit = ptr.plus(Region.BYTES_IN_BLOCK);
    //if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(Region.allocated(ptr));
    return alloc(bytes, align, offset);
  }

  /** @return the space associated with this squish allocator */
  @Override
  public final Space getSpace() {
    return space;
  }
}
