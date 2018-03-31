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
package org.mmtk.plan.markcopy;

import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.Space;
import org.mmtk.policy.MarkBlock;
import org.mmtk.utility.Log;
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
public class MarkCopyMarkTraceLocal extends TraceLocal {

  public MarkCopyMarkTraceLocal(Trace trace) {
    super(MarkCopy.SCAN_MARK, trace);
  }

  /****************************************************************************
   *
   * Externally visible Object processing and tracing
   */

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

  @Override
  public void prepare() {
    super.prepare();
    MarkCopyRelocationTraceLocal.lock.acquire();
    int count = 0;

    Address region = MarkBlock.firstBlock();
    while (region != null) {
      if (VM.VERIFY_ASSERTIONS) {
        VM.assertions._assert(MarkBlock.isValidBlock(region));
        VM.assertions._assert(MarkBlock.allocated(region));
      }
      count++;

      MarkBlock.setUsedSize(region, 0);
      MarkBlock.setRelocationState(region, false);

      region = MarkBlock.nextBlock(region);
    }
    if (count != MarkBlock.count()) {
        Log.write("Invalid iteration: ", count);
        Log.writeln(" / ", MarkBlock.count());
    }
    VM.assertions._assert(count == MarkBlock.count());

    MarkCopyRelocationTraceLocal.lock.release();
  }

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
    if (Space.isInSpace(MarkCopy.MC, object))
      return MarkCopy.markBlockSpace.traceMarkObject(this, object);
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
      return true;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }
}
