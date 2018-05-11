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

import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.Space;
import org.mmtk.policy.MarkBlockSpace;
import org.mmtk.utility.Constants;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.AddressArray;

/**
 *
 */
@Uninterruptible
public class MarkBlockAllocator extends Allocator {

  /****************************************************************************
   *
   * Instance variables
   */

  protected final MarkBlockSpace space;
  private final boolean copy;
  private Address cursor;
  private Address limit;

  /**
   * Constructor.
   *
   * @param space The space to bump point into.
   * @param copy TODO
   */
  public MarkBlockAllocator(MarkBlockSpace space, boolean copy) {
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
      MarkBlock.setCursor(MarkBlock.of(start), cursor);
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
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(MarkBlock.isAligned(ptr));
    cursor = ptr;
    limit = ptr.plus(MarkBlock.BYTES_IN_BLOCK);
    return alloc(bytes, align, offset);
  }

  /** @return the space associated with this squish allocator */
  @Override
  public final Space getSpace() {
    return space;
  }
}
