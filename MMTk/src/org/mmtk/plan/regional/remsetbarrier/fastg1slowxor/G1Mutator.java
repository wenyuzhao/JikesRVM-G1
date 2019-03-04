package org.mmtk.plan.regional.remsetbarrier.fastg1slowxor;

import org.mmtk.policy.CardTable;
import org.mmtk.policy.Region;
import org.mmtk.policy.Space;
import org.mmtk.vm.VM;
import org.vmmagic.pragma.Inline;
import org.vmmagic.pragma.NoInline;
import org.vmmagic.pragma.Uninterruptible;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.ObjectReference;
import org.vmmagic.unboxed.Word;

import static org.mmtk.utility.Constants.LOG_BYTES_IN_ADDRESS;

@Uninterruptible
public class G1Mutator extends org.mmtk.plan.regional.remsetbarrier.G1Mutator {
    @NoInline
    public void markAndEnqueueCard(ObjectReference src, Address slot, ObjectReference ref) {
        Word x = VM.objectModel.refToAddress(src).toWord();
        Word y = VM.objectModel.refToAddress(ref).toWord();
        Word tmp = x.xor(y).rshl(Region.LOG_BYTES_IN_REGION);
        if (!tmp.isZero()) {
            Address card = Region.Card.of(src);
            if (CardTable.attemptToMarkCard(card, true)) {
                remSetLogBuffer().plus(remSetLogBufferCursor << LOG_BYTES_IN_ADDRESS).store(card);
                remSetLogBufferCursor += 1;
                if (remSetLogBufferCursor >= REMSET_LOG_BUFFER_SIZE) {
                    enqueueCurrentRSBuffer(true, true);
                }
            }
        }
    }

    @Inline
    @Override
    public void checkCrossRegionPointer(ObjectReference src, Address slot, ObjectReference ref) {
        if (!ref.isNull() && Space.isInSpace(G1.RS, ref)) {
            markAndEnqueueCard(src, slot, ref);
        }
    }
}
