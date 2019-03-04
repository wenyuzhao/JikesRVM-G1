package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Trace;
import org.mmtk.policy.*;
import org.mmtk.utility.Log;
import org.mmtk.utility.alloc.BlockAllocator;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class G1NurseryTraceLocal extends G1EvacuationTraceLocal {

  public G1NurseryTraceLocal(Trace trace) {
    super(G1.SCAN_NURSERY, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (Space.isInSpace(G1.G1, object)) {
      if (!Region.allocated(Region.of(object))) return false;
      return G1.regionSpace.isLive(object);
    }
    return super.isLive(object);
  }

  @Uninterruptible
  static class EvacuationAccumulator extends RegionSpace.EvacuationAccumulator {
    long totalObjectSize = 0;
    long totalTime = 0;

    @Inline
    public void reset() {
      totalObjectSize = 0;
      totalTime = 0;
    }

    @Inline
    public void flush() {
//      PauseTimePredictor.updateObjectEvacuationTime(totalObjectSize, totalTime);
    }

    @Override
    @Inline
    public void updateObjectEvacuationTime(long size, long time) {
//      totalObjectSize += size;
//      totalTime += time;
      PauseTimePredictor.updateObjectEvacuationTime(size, time);
    }
  }

  public EvacuationAccumulator evacuationAccumulator = new EvacuationAccumulator();

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;

    if (Space.isInSpace(G1.G1, object)) {
      int allocator = Region.kind(Region.of(object)) == Region.EDEN ? G1.ALLOC_SURVIVOR : G1.ALLOC_OLD;
      if (!Region.allocated(Region.of(object))) return object;
      return G1.regionSpace.traceEvacuateObject(this, object, allocator, evacuationAccumulator);
    } else {
      return super.traceObject(object);
    }
  }

  public final RemSet.Processor processor = new RemSet.Processor(this, G1.regionSpace, true);
  @Inline
  public void processRemSets() {
    processor.processRemSets(G1.relocationSet, false, true, G1.regionSpace, PauseTimePredictor.remSetCardScanningTimer);
  }
}
