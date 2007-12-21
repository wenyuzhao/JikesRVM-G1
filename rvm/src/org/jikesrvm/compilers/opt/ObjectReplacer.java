/*
 *  This file is part of the Jikes RVM project (http://jikesrvm.org).
 *
 *  This file is licensed to You under the Common Public License (CPL);
 *  You may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *      http://www.opensource.org/licenses/cpl1.0.php
 *
 *  See the COPYRIGHT.txt file distributed with this work for information
 *  regarding copyright ownership.
 */
package org.jikesrvm.compilers.opt;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.jikesrvm.VM;
import org.jikesrvm.classloader.VM_Class;
import org.jikesrvm.classloader.VM_Field;
import org.jikesrvm.classloader.VM_FieldReference;
import org.jikesrvm.classloader.VM_TypeReference;
import org.jikesrvm.compilers.opt.ir.Empty;
import org.jikesrvm.compilers.opt.ir.GetField;
import org.jikesrvm.compilers.opt.ir.Move;
import org.jikesrvm.compilers.opt.ir.New;
import org.jikesrvm.compilers.opt.ir.IR;
import org.jikesrvm.compilers.opt.ir.IRTools;
import org.jikesrvm.compilers.opt.ir.Instruction;
import org.jikesrvm.compilers.opt.ir.Operand;
import org.jikesrvm.compilers.opt.ir.Operator;
import static org.jikesrvm.compilers.opt.ir.Operators.BOOLEAN_CMP_ADDR_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.BOOLEAN_CMP_INT_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.CHECKCAST_NOTNULL_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.CHECKCAST_UNRESOLVED_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.CHECKCAST_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.GETFIELD_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.GET_OBJ_TIB_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.INSTANCEOF_NOTNULL_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.INSTANCEOF_UNRESOLVED_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.INSTANCEOF_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.LONG_STORE_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.MONITORENTER_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.MONITOREXIT_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.MUST_IMPLEMENT_INTERFACE_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.NULL_CHECK_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.PUTFIELD_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.READ_CEILING;
import static org.jikesrvm.compilers.opt.ir.Operators.REF_IFCMP_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.REF_MOVE_opcode;
import static org.jikesrvm.compilers.opt.ir.Operators.WRITE_FLOOR;
import org.jikesrvm.compilers.opt.ir.Register;
import org.jikesrvm.compilers.opt.ir.RegisterOperand;
import org.jikesrvm.compilers.opt.ir.PutField;

/**
 * Class that performs scalar replacement of aggregates for non-array
 * objects
 */
public final class ObjectReplacer implements AggregateReplacer {
  static final boolean DEBUG = false;

  /**
   * Return an object representing this transformation for a given
   * allocation site
   *
   * @param inst the allocation site
   * @param ir
   * @return the object, or null if illegal
   */
  public static ObjectReplacer getReplacer(Instruction inst, IR ir) {
    Register r = New.getResult(inst).getRegister();
    // TODO :handle these cases
    if (containsUnsupportedUse(ir, r)) {
      return null;
    }
    VM_Class klass = New.getType(inst).getVMType().asClass();
    return new ObjectReplacer(r, klass, ir);
  }

  /**
   * Perform the transformation
   */
  public void transform() {
    // store the object's fields in a ArrayList
    ArrayList<VM_Field> fields = getFieldsAsArrayList(klass);
    // create a scalar for each field. initialize the scalar to
    // default values before the object's def
    RegisterOperand[] scalars = new RegisterOperand[fields.size()];
    RegisterOperand def = reg.defList;
    Instruction defI = def.instruction;
    for (int i = 0; i < fields.size(); i++) {
      VM_Field f = fields.get(i);
      Operand defaultValue = IRTools.getDefaultOperand(f.getType());
      scalars[i] = IRTools.moveIntoRegister(ir.regpool, defI, defaultValue);
      scalars[i].setType(f.getType());
    }
    transform2(this.reg, defI, scalars, fields, null);
  }

  private void transform2(Register reg, Instruction defI, RegisterOperand[] scalars, ArrayList<VM_Field> fields, Set<Register> visited) {
    // now remove the def
    if (DEBUG) {
      System.out.println("Removing " + defI);
    }
    DefUse.removeInstructionAndUpdateDU(defI);
    // now handle the uses
    for (RegisterOperand use = reg.useList; use != null; use = use.getNext()) {
      scalarReplace(use, scalars, fields, visited);
    }
  }

  /**
   * type of the object
   */
  private final VM_Class klass;
  /**
   * the IR
   */
  private final IR ir;
  /**
   * the register holding the object reference
   */
  private final Register reg;

  /**
   * Returns a ArrayList<VM_Field>, holding the fields of the object
   * @param klass the type of the object
   */
  private static ArrayList<VM_Field> getFieldsAsArrayList(VM_Class klass) {
    ArrayList<VM_Field> v = new ArrayList<VM_Field>();
    for (VM_Field field : klass.getInstanceFields()) {
      v.add(field);
    }
    return v;
  }

  /**
   * @param r the register holding the object reference
   * @param _klass the type of the object to replace
   * @param i the IR
   */
  private ObjectReplacer(Register r, VM_Class _klass, IR i) {
    reg = r;
    klass = _klass;
    ir = i;
  }

  /**
   * Replace a given use of a object with its scalar equivalent
   *
   * @param use the use to replace
   * @param scalars an array of scalar register operands to replace
   *                  the object's fields with
   */
  private void scalarReplace(RegisterOperand use, RegisterOperand[] scalars, ArrayList<VM_Field> fields, Set<Register> visited) {
    Instruction inst = use.instruction;
    try{
      switch (inst.getOpcode()) {
      case PUTFIELD_opcode: {
        VM_FieldReference fr = PutField.getLocation(inst).getFieldRef();
        if (VM.VerifyAssertions) VM._assert(fr.isResolved());
        VM_Field f = fr.peekResolvedField();
        int index = fields.indexOf(f);
        VM_TypeReference type = scalars[index].getType();
        Operator moveOp = IRTools.getMoveOp(type);
        Instruction i = Move.create(moveOp, scalars[index].copyRO(), PutField.getClearValue(inst));
        inst.insertBefore(i);
        DefUse.removeInstructionAndUpdateDU(inst);
        DefUse.updateDUForNewInstruction(i);
      }
      break;
      case GETFIELD_opcode: {
        VM_FieldReference fr = GetField.getLocation(inst).getFieldRef();
        if (VM.VerifyAssertions) VM._assert(fr.isResolved());
        VM_Field f = fr.peekResolvedField();
        int index = fields.indexOf(f);
        VM_TypeReference type = scalars[index].getType();
        Operator moveOp = IRTools.getMoveOp(type);
        Instruction i = Move.create(moveOp, GetField.getClearResult(inst), scalars[index].copyRO());
        inst.insertBefore(i);
        DefUse.removeInstructionAndUpdateDU(inst);
        DefUse.updateDUForNewInstruction(i);
      }
      break;
      case MONITORENTER_opcode:
        if (ir.options.NO_CACHE_FLUSH) {
          DefUse.removeInstructionAndUpdateDU(inst);
        } else {
          inst.insertBefore(Empty.create(READ_CEILING));
          DefUse.removeInstructionAndUpdateDU(inst);
        }
        break;
      case MONITOREXIT_opcode:
        if (ir.options.NO_CACHE_FLUSH) {
          DefUse.removeInstructionAndUpdateDU(inst);
        } else {
          inst.insertBefore(Empty.create(WRITE_FLOOR));
          DefUse.removeInstructionAndUpdateDU(inst);
        }
        break;
      case NULL_CHECK_opcode:
        // (SJF) TODO: Why wasn't this caught by BC2IR for
        //      java.lang.Double.<init> (Ljava/lang/String;)V ?
        DefUse.removeInstructionAndUpdateDU(inst);
        break;
      case REF_MOVE_opcode:
        if (visited == null) {
          visited = new HashSet<Register>();
        }
        Register copy = Move.getResult(use.instruction).getRegister();
        if(!visited.contains(copy)) {
          visited.add(copy);
          transform2(copy, inst, scalars, fields, visited);
        }
        break;
      default:
        throw new OptimizingCompilerException("ObjectReplacer: unexpected use " + inst);
      }
    } catch (Exception e) {
      OptimizingCompilerException oe = new OptimizingCompilerException("Error handling use ("+ use +") of: "+ inst);
      oe.initCause(e);
      throw oe;
    }
  }

  /**
   * Some cases we don't handle yet. TODO: handle them.
   */
  private static boolean containsUnsupportedUse(IR ir, Register reg) {
    for (RegisterOperand use = reg.useList; use != null; use = use.getNext()) {
      switch (use.instruction.getOpcode()) {
        case CHECKCAST_opcode:
        case CHECKCAST_UNRESOLVED_opcode:
        case MUST_IMPLEMENT_INTERFACE_opcode:
        case CHECKCAST_NOTNULL_opcode:
        case GET_OBJ_TIB_opcode:
        case INSTANCEOF_opcode:
        case INSTANCEOF_NOTNULL_opcode:
        case INSTANCEOF_UNRESOLVED_opcode:
        case REF_IFCMP_opcode:
        case BOOLEAN_CMP_INT_opcode:
        case BOOLEAN_CMP_ADDR_opcode:
        case LONG_STORE_opcode:
          return true;
      }
    }
    return false;
  }
}


