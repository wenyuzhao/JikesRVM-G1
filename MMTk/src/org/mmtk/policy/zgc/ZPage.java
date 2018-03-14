package org.mmtk.policy.zgc;

import com.sun.org.apache.xerces.internal.impl.dv.xs.AbstractDateTimeDV;
import org.mmtk.utility.deque.AddressDeque;
import org.mmtk.vm.VM;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Word;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.mmtk.utility.Constants.*;

public class ZPage {
    public static final int BYTES = BYTES_IN_PAGE;
    public static final int PAGES = 1;
    public static final int USEABLE_BYTES = BYTES - 13; // exclude metadata

    public static final int NEXT_PAGE_POINTER_OFFSET = BYTES - 4;
    public static final int PREV_PAGE_POINTER_OFFSET = BYTES - 8;
    public static final int ALIVE_BYTES_OFFSET = BYTES - 12;
    public static final int RELOCATION_MARK_OFFSET = BYTES - 13;


    public static final Word PAGE_MASK = Word.fromIntZeroExtend(BYTES - 1);

    public static int pagesReservedForCopying = -1;
    public static ZFreeList fromPages = new ZFreeList();

    public static Address of(final Address ptr) {
        return align(ptr);
    }

    public static Address align(final Address ptr) {
        return ptr.toWord().and(PAGE_MASK.not()).toAddress();
    }

    public static boolean isAligned(final Address address) {
        return address.EQ(align(address));
    }

    public static void setRelocationState(Address zPage, boolean relocation) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && isAligned(zPage));
        zPage.plus(RELOCATION_MARK_OFFSET).store((byte) (relocation ? 1 : 0));
    }

    public static boolean relocationRequired(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && isAligned(zPage));
        return zPage.plus(RELOCATION_MARK_OFFSET).loadByte() > 0;
    }

    public static void setUsedSize(Address zPage, int bytes) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && isAligned(zPage));
        zPage.plus(ALIVE_BYTES_OFFSET).store(bytes);
    }

    public static int usedSize(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && isAligned(zPage));
        return zPage.plus(ALIVE_BYTES_OFFSET).loadInt();
    }
}
