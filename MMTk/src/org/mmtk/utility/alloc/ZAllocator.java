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

import org.mmtk.policy.Space;
import org.mmtk.policy.zgc.ZObjectHeader;
import org.mmtk.policy.zgc.ZPage;
import org.mmtk.policy.zgc.ZSpace;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Extent;
import org.vmmagic.unboxed.Word;

//import static org.mmtk.policy.immix.ImmixConstants.*;

/**
 *
 */
@Uninterruptible
public class ZAllocator extends Allocator {

  /****************************************************************************
   *
   * Instance variables
   */

  /** space this allocator is associated with */
  protected final ZSpace space;
  //private final boolean hot;
  private final boolean copy;

  /** bump pointer */
  private Address cursor;
  /** limit for bump pointer */
  private Address limit;

  /**
   * Constructor.
   *
   * @param space The space to bump point into.
   * @param copy TODO
   */
  public ZAllocator(ZSpace space, boolean copy) {
    this.space = space;
    //this.hot = hot;
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
    VM.assertions._assert(bytes <= ZPage.USEABLE_BYTES, "Trying to allocate " + bytes + " bytes");
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
    Address ptr = space.getSpace(copy);

    if (ptr.isZero()) {
      return ptr; // failed allocation --- we will need to GC
    }
    /* we have been given a clean block */
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(ZPage.isAligned(ptr));
    //lineUseCount = LINES_IN_BLOCK;
    cursor = ptr;
    limit = ptr.plus(ZPage.USEABLE_BYTES);
    return alloc(bytes, align, offset);
  }

  /*private void zeroBlock(Address block) {
    // FIXME: efficiency check here!
    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(block.toWord().and(Word.fromIntSignExtend(ZBlock.BYTES - 1)).isZero());
    VM.memory.zero(false, block, Extent.fromIntZeroExtend(ZPage.BYTES));
  }*/

  /** @return the space associated with this squish allocator */
  @Override
  public final Space getSpace() {
    return space;
  }
}
