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
package org.mmtk.plan.concurrent.markcopy;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.MarkBlock;
import org.mmtk.policy.MarkBlockSpace;
import org.mmtk.policy.Space;
import org.mmtk.utility.Constants;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.EmbeddedMetaData;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;

/**
 * This class implements the core functionality for a transitive
 * closure over the heap graph.
 */
@Uninterruptible
public class MarkCopyRelocationTraceLocal extends TraceLocal {

  public MarkCopyRelocationTraceLocal(Trace trace) {
    super(MarkCopy.SCAN_RELOCATE, trace);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(MarkCopy.MC, object))
      return MarkCopy.markBlockSpace.isLive(object);
    return super.isLive(object);
  }

  private static Lock relocationSetSelectionLock = VM.newLock("relocation-set-selection-lock");
  private static boolean relocationSetSelected = false;

  //@Override
  public void prepare() {
    super.prepare();
    blocksReleased = false;
    // Only 1 collector thread can perform relocation set selection
    relocationSetSelectionLock.acquire();
    if (relocationSetSelected) {
      relocationSetSelectionLock.release();
      return;
    }
    relocationSetSelected = true;
    relocationSetSelectionLock.release();


    if (VM.VERIFY_ASSERTIONS) {
      Log.write("Memory: ", VM.activePlan.global().getPagesUsed());
      Log.writeln(" / ", VM.activePlan.global().getTotalPages());
      Log.writeln("BLOCK SIZE ", MarkBlock.count());
    }

    MarkCopy.markBlockSpace.selectRelocationBlocks();
  }

  private static Lock blocksReleaseLock = VM.newLock("blocks-release-lock");
  private static boolean blocksReleased = false;


  //@Override
  public void release() {
    super.release();

    relocationSetSelected = false;
    // Only 1 collector thread can performs relocation set selection
    blocksReleaseLock.acquire();
    if (blocksReleased) {
      blocksReleaseLock.release();
      return;
    }
    blocksReleased = true;
    blocksReleaseLock.release();

    //lock.acquire();
    int visitedPages = 0;
    MarkBlockSpace space = MarkCopy.markBlockSpace;
    Address region = space.firstBlock();
    while (region != null) {
      Address nextRegion = space.nextBlock(region);
      if (MarkBlock.relocationRequired(region)) {
        if (VM.VERIFY_ASSERTIONS) {
          Log.write("#Block ", region);
          Log.write(": ", MarkBlock.usedSize(region));
          Log.write("/", MarkBlock.BYTES_IN_BLOCK);
          Log.writeln(" released");
        }
        MarkCopy.markBlockSpace.release(region);
      } else {
        visitedPages++;
        MarkBlock.setUsedSize(region, 0);
        if (VM.VERIFY_ASSERTIONS) {
          Log.write("#Block ", region);
          Log.write(": ", MarkBlock.usedSize(region));
          Log.writeln("/", MarkBlock.BYTES_IN_BLOCK);
        }
      }
      region = nextRegion;
    }
    //lock.release();
    if (VM.VERIFY_ASSERTIONS) {
      Log.write("Memory: ", VM.activePlan.global().getPagesReserved());
      Log.write(" / ", VM.activePlan.global().getTotalPages());
      Log.writeln(", ", MarkCopy.markBlockSpace.availablePhysicalPages());
    }
  }


  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(MarkCopy.MC, object))
      return MarkCopy.markBlockSpace.traceRelocateObject(this, object, MarkCopy.ALLOC_MC);
    return super.traceObject(object);
  }

  /**
   * Will this object move from this point on, during the current trace ?
   *
   * @param object The object to query.
   * @return True if the object will not move.
   */
  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (Space.isInSpace(MarkCopy.MC, object)) {
      return !MarkBlock.relocationRequired(MarkBlock.of(VM.objectModel.objectStartRef(object)));
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
