package org.mmtk.policy.zgc;

import com.sun.org.apache.xerces.internal.impl.dv.xs.AbstractDateTimeDV;
import org.mmtk.vm.VM;
import org.vmmagic.unboxed.Address;
import org.vmmagic.unboxed.Word;

import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.mmtk.utility.Constants.*;

public class ZPage {
    public static final int BYTES = BYTES_IN_PAGE;
    public static final int USEABLE_BYTES = BYTES - 13; // exclude metadata

    public static final int NEXT_PAGE_POINTER_OFFSET = BYTES - 4;
    public static final int PREV_PAGE_POINTER_OFFSET = BYTES - 8;
    public static final int ALIVE_BYTES_OFFSET = BYTES - 12;
    public static final int RELOCATION_MARK_OFFSET = BYTES - 13;


    public static final Word PAGE_MASK = Word.fromIntZeroExtend(BYTES - 1);

    private static Address head = Address.zero();
    private static Address tail = Address.zero();
    private static int size = 0;

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

    public static Address next(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && isAligned(zPage));
        return zPage.plus(NEXT_PAGE_POINTER_OFFSET).loadAddress();
    }
    public static Address prev(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && isAligned(zPage));
        return zPage.plus(PREV_PAGE_POINTER_OFFSET).loadAddress();
    }

    public static void link(Address prevPage, Address nextPage) {
        if (!prevPage.isZero()) {
            prevPage.plus(NEXT_PAGE_POINTER_OFFSET).store(nextPage);
        }
        if (!nextPage.isZero()) {
            nextPage.plus(PREV_PAGE_POINTER_OFFSET).store(prevPage);
        }
    }

    public static void unlink(Address prevPage, Address nextPage) {
        if (VM.VERIFY_ASSERTIONS) {
            VM.assertions._assert(isAligned(prevPage) && isAligned(nextPage));
            VM.assertions._assert(prevPage.isZero() || next(prevPage).EQ(nextPage));
            VM.assertions._assert(nextPage.isZero() || prev(nextPage).EQ(prevPage));
        }
        if (!nextPage.isZero()) nextPage.plus(PREV_PAGE_POINTER_OFFSET).store(Address.zero());
        if (!prevPage.isZero()) prevPage.plus(NEXT_PAGE_POINTER_OFFSET).store(Address.zero());
    }

    public static void reset() {
        head = Address.zero();
        tail = Address.zero();
        size = 0;
    }

    public static Address pop() {
        if (tail.isZero()) return Address.zero();
        Address rtn = tail;
        tail = prev(tail);
        unlink(tail, rtn);
        if (tail.isZero()) head = Address.zero();
        size -= rtn.isZero() ? 0 : 1;
        return rtn;
    }

    public static void push(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && isAligned(zPage));
        if (tail.isZero()) {
            head = tail = zPage;
            return;
        }
        size += 1;
        link(tail, zPage);
    }

    public static void remove(Address zPage) {
        if (VM.VERIFY_ASSERTIONS) VM.assertions._assert(!zPage.isZero() && isAligned(zPage));
        Address prevPage = prev(zPage), nextPage = next(zPage);
        unlink(prevPage, zPage);
        unlink(zPage, nextPage);
        link(prevPage, nextPage);
        if (prevPage.isZero()) head = Address.zero();
        if (nextPage.isZero()) tail = Address.zero();
        size -= 1;
    }

    public static int allocatedZPages() {
        return size;
    }

    public static void forEach(Consumer<Address> f) {
        for (Address zPage = head; !zPage.isZero(); zPage = next(zPage))
            f.accept(zPage);
    }



}
