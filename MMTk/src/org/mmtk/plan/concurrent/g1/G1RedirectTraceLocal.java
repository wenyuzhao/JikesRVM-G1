package org.mmtk.plan.concurrent.g1;

import org.mmtk.plan.Plan;
import org.mmtk.plan.Trace;
import org.mmtk.plan.TraceLocal;
import org.mmtk.policy.*;
import org.mmtk.utility.ForwardingWord;
import org.mmtk.utility.Log;
import org.mmtk.vm.Lock;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

@Uninterruptible
public class G1RedirectTraceLocal extends TraceLocal {
  RemSet.Processor processor = new RemSet.Processor(this, G1.regionSpace);

  public G1RedirectTraceLocal(Trace trace) {
    super(G1.SCAN_REDIRECT, trace);
  }

  @Override
  public boolean isLive(ObjectReference object) {
    if (object.isNull()) return false;
    if (global().nurseryGC()) {
//      if (Space.isInSpace(G1.G1, object)) {
//        return G1.regionSpace.isLive(object);
//      }
//      return super.isLive(object);
//      if (Space.isInSpace(G1.G1, object)) {
//        Word s = VM.objectModel.readAvailableBitsWord(object);
//        if (s.and(Word.fromIntZeroExtend(3)).toInt() == 1) {
//          return false;
//        }
//      }

      if (Space.isInSpace(G1.G1, object) && Region.relocationRequired(Region.of(object))) {

        return ForwardingWord.isForwardedOrBeingForwarded(object);
//        return G1.regionSpace.isLive(object);
      }
      return true;
    } else {
      if (Space.isInSpace(G1.G1, object)) {
//        if (!Region.relocationRequired(Region.of(object))) return true;
        return G1.regionSpace.isLive(object);
      }
      return super.isLive(object);
    }
  }

  @Inline
  @Override
  public ObjectReference traceObject(ObjectReference object, boolean root) {
    ObjectReference newObject = super.traceObject(object, root);

    if (!object.isNull() && root) {
      this.processNode(newObject);
    }

    return newObject;
  }

  @Inline
  public void processEdge(ObjectReference source, Address slot) {
//    if (VM.VERIFY_ASSERTIONS) {
//      ObjectReference ref = slot.loadObjectReference();
//
////      if (global().nurseryGC() && (ref.isNull() || !Space.isMappedObject(ref) || (Space.isInSpace(G1.G1, ref) && !Region.allocated(Region.of(ref))))) {
////        return;
////      }
//
////      if (global().nurseryGC() && G1.regionSpace.contains(ref) && G1.regionSpace.contains(source)) {
////        if (Region.of(ref).NE(Region.of(source)) && !Region.relocationRequired(Region.of(ref)) && !RemSet.contains(Region.of(ref), Region.Card.of(source))) {
////          return;
////        }
////      }
//
//      if (!VM.debugging.validRef(ref)) {
//        Log.writeln();
//        Log.write(Space.getSpaceForObject(source).getName(), source);
//        Log.write(".", slot);
//        Log.write(" -> ", ref);
//        Log.write("Space: ");
//        Log.writeln(Space.getSpaceForObject(ref).getName());
//        if (Space.isInSpace(G1.G1, ref)) {
//          if (RemSet.contains(Region.of(ref), Region.Card.of(source))) {
//            Log.writeln("Remembers card");
//          } else {
//            Log.writeln("Not remembers card");
//          }
//        }
//      }
//      VM.assertions._assert(VM.debugging.validRef(ref));
//    }

    ObjectReference oldRef = slot.loadObjectReference();
//    if (!oldRef.isNull() && Space.isInSpace(G1.G1, oldRef) && (VM.objectModel.readAvailableByte(oldRef) & 3) == 1) return;

    super.processEdge(source, slot);

    ObjectReference ref = slot.loadObjectReference();

//    if (ref.toAddress().NE(oldRef.toAddress())) {
//      Log log = VM.activePlan.mutator().getLog();
//      log.write(source);
//      log.write(".", slot);
//      log.write(" ", oldRef);
//      log.writeln(" ~> ", ref);
//    }

//

    if (!ref.isNull() && Space.isInSpace(G1.G1, ref)) {
      Address block = Region.of(ref);
      if (block.NE(Region.of(source))) {
        Address card = Region.Card.of(source);
        Region.Card.updateCardMeta(source);
//        Log log = VM.activePlan.mutator().getLog();
//        log.write("Add card ", source);
//        log.write(".", slot);
//        log.writeln(" -> ", ref);
        RemSet.addCard(block, card);
      }
    }


//    if (VM.VERIFY_ASSERTIONS) {
//      ObjectReference obj = ref;
//      if (G1.regionSpace.contains(obj) && !Space.isInSpace(G1.VM_SPACE, source) && Region.of(source).NE(Region.of(obj))) {
//        Address region = Region.of(obj);
//        Address card = Region.Card.of(source);
//        if (!RemSet.contains(region, card)) {
//          lock.acquire();
//          Log log = VM.activePlan.mutator().getLog();
//          log.write(Space.getSpaceForObject(source).getName());
//          log.write(" ", source);
//          log.write(" in card ", card);
//          log.writeln(" is not in remset of region ", region);
//          VM.assertions.fail("");
////          lock.release();
//        }
//      }
//    }
  }

  static Lock lock = VM.newLock("dfgjkstheuirgfdnvfbhi");

  boolean nurseryTraceFinalizables = false;

  @Override
  @Inline
  public ObjectReference traceObject(ObjectReference object) {
    if (object.isNull()) return object;
//    if (!Space.isMappedObject(object)) return object;
//    if (Space.isInSpace(G1.G1, object) && (VM.objectModel.readAvailableByte(object) & 3) == 1) return  object;


//    if (VM.VERIFY_ASSERTIONS) {
//      if (!VM.debugging.validRef(object)) {
//        Log.write("Space: ");
//        Log.writeln(Space.getSpaceForObject(object).getName());
//      }
//      VM.assertions._assert(VM.debugging.validRef(object));
//      if (Space.isInSpace(G1.G1, object)) {
//        VM.assertions._assert(G1.regionSpace.contains(object));
//        if ((VM.objectModel.readAvailableByte(object) & 3) == 1) VM.objectModel.dumpObject(object);
//        VM.assertions._assert((VM.objectModel.readAvailableByte(object) & 3) != 1);
//      }
//    }
    Region.Card.updateCardMeta(object);

//    VM.assertions._assert(s == 1 || s == 0);

    ObjectReference newObject = object;

    if (global().nurseryGC()) {

//      if (nurseryTraceFinalizables) {
//        if (Space.isInSpace(G1.G1, object)) {
//          newObject = G1.regionSpace.traceEvacuateObject(this, object, G1.ALLOC_RS, PauseTimePredictor.evacuationTimer);
//          if (newObject.toAddress().NE(object.toAddress()))
//            Region.Card.updateCardMeta(newObject);
//        } else {
////          newObject = super.traceObject(object);
//        }
//      } else {
        if (G1.regionSpace.contains(object) && Region.relocationRequired(Region.of(object))) {
          newObject = G1.regionSpace.traceEvacuateObject(this, object, G1.ALLOC_RS, PauseTimePredictor.evacuationTimer);
          if (newObject.toAddress().NE(object.toAddress()))
            Region.Card.updateCardMeta(newObject);
        }
//      }

//      if (Space.isInSpace(G1.G1, object)) {
//        newObject = G1.regionSpace.traceEvacuateObject(this, object, G1.ALLOC_RS, PauseTimePredictor.evacuationTimer);
//        if (newObject.toAddress().NE(object.toAddress()))
//          Region.Card.updateCardMeta(newObject);
//      } else {
//        newObject = super.traceObject(object);
//      }

      //if (nurseryTraceFinalizables) {
//        if (Space.isInSpace(G1.G1, object)) {
//          newObject = G1.regionSpace.traceEvacuateObject(this, object, G1.ALLOC_RS, PauseTimePredictor.evacuationTimer);
//          if (newObject.toAddress().NE(object.toAddress()))
//            Region.Card.updateCardMeta(newObject);
//        } else {
//          if (Space.isInSpace(Plan.VM_SPACE, object))
//            newObject = Plan.vmSpace.traceObject(this, object);
//          else if (Space.isInSpace(Plan.IMMORTAL, object))
//            newObject = Plan.immortalSpace.traceObject(this, object);
//          else if (Space.isInSpace(Plan.LOS, object))
//            newObject = Plan.loSpace.traceObject(this, object);
//          else if (Space.isInSpace(Plan.NON_MOVING, object))
//            newObject = Plan.nonMovingSpace.traceObject(this, object);
//          else if (Plan.USE_CODE_SPACE && Space.isInSpace(Plan.SMALL_CODE, object))
//            newObject = Plan.smallCodeSpace.traceObject(this, object);
//          else if (Plan.USE_CODE_SPACE && Space.isInSpace(Plan.LARGE_CODE, object))
//            newObject = Plan.largeCodeSpace.traceObject(this, object);
//        }
//      } else {
//        if (G1.regionSpace.contains(object) && Region.relocationRequired(Region.of(object))) {
//          newObject = G1.regionSpace.traceEvacuateObject(this, object, G1.ALLOC_RS, PauseTimePredictor.evacuationTimer);
//          if (newObject.toAddress().NE(object.toAddress()))
//            Region.Card.updateCardMeta(newObject);
//        }
//      }


      //if (isLive(object)) return object;
//      if (Space.isInSpace(G1.G1, object)) {
//        newObject = G1.regionSpace.traceEvacuateObject(this, object, G1.ALLOC_RS, PauseTimePredictor.evacuationTimer);
//        if (newObject.toAddress().NE(object.toAddress()))
//          Region.Card.updateCardMeta(newObject);
//      } else {
//        newObject = super.traceObject(object);
//      }
    } else {
      if (Space.isInSpace(G1.G1, object)) {
        newObject = G1.regionSpace.traceEvacuateObject(this, object, G1.ALLOC_RS, PauseTimePredictor.evacuationTimer);
        if (newObject.toAddress().NE(object.toAddress()))
          Region.Card.updateCardMeta(newObject);
      } else {
        newObject = super.traceObject(object);
      }
    }
//    if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(VM.debugging.validRef(newObject));
    return newObject;
  }

  @Override
  public boolean willNotMoveInCurrentCollection(ObjectReference object) {
    if (G1.regionSpace.contains(object) && Region.relocationRequired(Region.of(object))) {
      return false;
    } else {
      return super.willNotMoveInCurrentCollection(object);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference getForwardedReference(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    if (global().nurseryGC()) processNode(rtn);
    return rtn;
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference retainReferent(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    if (global().nurseryGC()) processNode(rtn);
    return rtn;
  }

  /**
   * {@inheritDoc}
   */
  @Inline
  @Override
  public ObjectReference retainForFinalize(ObjectReference object) {
    ObjectReference rtn = traceObject(object);
    if (global().nurseryGC()) processNode(rtn);
    return rtn;
  }


  @Inline
  public void processRemSets() {
    processor.processRemSets(G1.relocationSet, false, G1.regionSpace, PauseTimePredictor.remSetCardScanningTimer);
  }

  @Inline
  private static G1 global() {
    return (G1) VM.activePlan.global();
  }
}
