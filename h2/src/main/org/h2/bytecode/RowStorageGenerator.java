package org.h2.bytecode;

import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import jdk.internal.org.objectweb.asm.ClassVisitor;
import jdk.internal.org.objectweb.asm.ClassWriter;
import jdk.internal.org.objectweb.asm.Label;
import jdk.internal.org.objectweb.asm.MethodVisitor;
import org.h2.value.Value;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static jdk.internal.org.objectweb.asm.Opcodes.*;

/**
 * Class RowStorageGenerator.
 * <UL>
 * <LI> 4/10/17 8:59 PM initial creation
 * </UL>
 *
 * @author <a href='mailto:andrei.tokar@gmail.com'>Andrei Tokar</a>
 */
public final class RowStorageGenerator {

    private static final String FLD_NAME_INDEXES = "INDEXES";
    private static final String FLD_NAME_BITMASK = "_bits_";
    private static final String DESC_INDEXES = "[I";

    public enum ValueType {
//        BOOLEAN(Value.BOOLEAN, "Z", 1, "getValueBoolean", "getBoolean"),
//        BYTE(Value.BYTE,       "B", 1, "getValueByte", "getByte"),
//        SHORT(Value.SHORT,     "S", 2, "getValueShort", "getShort"),
        INT(Value.INT,         "I", 4, IRETURN, "Int") {
            @Override
            public void visitCompareTo(MethodVisitor mv, String className) {
                mv.visitMethodInsn(INVOKESTATIC, "java/lang/Integer", "compare", "(II)I", false);
            }
        },

        LONG(Value.LONG,       "J", 8, LRETURN, "Long") {
            @Override
            public void visitCompareTo(MethodVisitor mv, String className) {
                mv.visitMethodInsn(INVOKESTATIC, "java/lang/Long", "compare", "(JJ)I", false);
            }
        },

        DOUBLE(Value.DOUBLE,   "D", 8, DRETURN, "Double") {
            @Override
            public void visitCompareTo(MethodVisitor mv, String className) {
                mv.visitMethodInsn(INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            }
        },

        FLOAT(Value.FLOAT,     "F", 4, FRETURN, "Float") {
            @Override
            public void visitCompareTo(MethodVisitor mv, String className) {
                mv.visitMethodInsn(INVOKESTATIC, "java/lang/Float", "compare", "(FF)I", false);
            }
        },

        DECIMAL(Value.DECIMAL, "Ljava/math/BigDecimal;", Constants.MEMORY_OBJECT + Constants.MEMORY_POINTER,
                "Decimal", ARETURN, "d") {
            @Override
            public void visitGetMemory(MethodVisitor mv, String className, int indx) {
                mv.visitVarInsn(ALOAD, 0);
                mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), "Ljava/math/BigDecimal;");
                mv.visitMethodInsn(INVOKESTATIC, className, "getMemory", "(Ljava/math/BigDecimal;)I", false);
                mv.visitInsn(IADD);
            }
        },

        STRING_FIXED(Value.STRING_FIXED, "[B", Constants.MEMORY_OBJECT, "Bytes", ARETURN, "t") {
            @Override
            public void visitGetMemory(MethodVisitor mv, String className, int indx) {
                mv.visitVarInsn(ALOAD, 0);
                mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), "[B");
                mv.visitMethodInsn(INVOKESTATIC, className, "getMemory", "([B)I", false);
                mv.visitInsn(IADD);
            }
        },
        STRING(Value.STRING,   "Ljava/lang/String;", Constants.MEMORY_OBJECT + Constants.MEMORY_POINTER,
                "String", ARETURN, "T") {
            @Override
            public void visitGetMemory(MethodVisitor mv, String className, int indx) {
                mv.visitVarInsn(ALOAD, 0);
                mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), "Ljava/lang/String;");
                mv.visitMethodInsn(INVOKESTATIC, className, "getMemory", "(Ljava/lang/String;)I", false);
                mv.visitInsn(IADD);
            }
        },
        DEFAULT(Value.UNKNOWN, "Lorg/h2/value/Value;", Constants.MEMORY_OBJECT, "Value", ARETURN, "v") {
            @Override
            public void visitGetMemory(MethodVisitor mv, String className, int indx) {
                mv.visitVarInsn(ALOAD, 0);
                mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), "Lorg/h2/value/Value;");
                mv.visitMethodInsn(INVOKESTATIC, className, "getMemory", "(Lorg/h2/value/Value;)I", false);
                mv.visitInsn(IADD);
            }
        }
        ;

        private static final Map<Integer, ValueType> map = new HashMap<>();
        static {
            for (ValueType valueType : ValueType.values()) {
                map.put(valueType.type, valueType);
            }
        }

        private final int    type;
        private final String descriptor;
        private final int    memory;
        private final int    returnInstruction;
        private final String rawAccessor;
        private final String nameSuffix;


        public static ValueType get(int type) {
            ValueType valueType = map.get(type);
            if(valueType == null) {
                valueType = DEFAULT;
            }
            return valueType;
        }

        ValueType(int type, String descriptor, int memory, int returnInstruction, String rawAccessor) {
            this(type, descriptor, memory, rawAccessor, returnInstruction, null);
        }

        ValueType(int type, String descriptor, int memory, String rawAccessor, int returnInstruction, String nameSuffix) {
            this.type = type;
            this.descriptor = descriptor;
            this.memory = memory;
            this.returnInstruction = returnInstruction;
            this.rawAccessor = rawAccessor;
            this.nameSuffix = nameSuffix;
        }

        public String getClassNameSuffix() {
            return nameSuffix == null ? descriptor : nameSuffix;
        }

        private String getConvertToName() {
            return "to" + rawAccessor;
        }

        private String getRawAccessorName() {
            return "get" + rawAccessor;
        }

        public int getReturnInstruction() {
            return returnInstruction;
        }

        protected String getFieldName(int indx) {
            return "field_" + indx;
        }

        public int getMemory() {
            return memory;
        }

        public boolean requireNullabilityBit() {
            return nameSuffix == null;
        }

        public void visitFieldCreation(ClassVisitor cw, int indx) {
            cw.visitField(ACC_PRIVATE, getFieldName(indx), descriptor, null, null).visitEnd();
        }

        public void visitGetter(MethodVisitor mv, String className, int indx) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), descriptor);
            // convert from raw type to Value
            if(this != ValueType.DEFAULT) {
                mv.visitMethodInsn(INVOKESTATIC, className, "convertFrom", "(" + descriptor + ")Lorg/h2/value/Value;", false);
            }
            mv.visitInsn(ARETURN);
        }

        public void visitSetter(MethodVisitor mv, String className, int indx) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitVarInsn(ALOAD, 2);
            // convert from Value to raw type
            if(this != ValueType.DEFAULT) {
                mv.visitMethodInsn(INVOKESTATIC, className, getConvertToName(), "(Lorg/h2/value/Value;)" + descriptor, false);
            }
            mv.visitFieldInsn(PUTFIELD, className, getFieldName(indx), descriptor);
            mv.visitInsn(RETURN);
        }

        public void visitGetMemory(MethodVisitor mv, String className, int indx) {}

        public void visitCompareTo(MethodVisitor mv, String className) {
            mv.visitVarInsn(ALOAD, 2);
            mv.visitMethodInsn(INVOKESTATIC, className, "compare", "(" + descriptor + descriptor + "Lorg/h2/value/CompareMode;)I", false);
        }
    }

    public enum BitMaskType {
        BYTE(8, "B"),
        SHORT(16, "S"),
        INT(32, "I"),
        LONG(64, "L") {
            @Override
            public void visitGet(MethodVisitor mv, int bitIndex) {
                mv.visitIntInsn(BIPUSH, bitIndex);
                mv.visitInsn(LUSHR);
                mv.visitInsn(L2I);
                mv.visitInsn(ICONST_1);
                mv.visitInsn(IAND);
            }

            public void visitSet(MethodVisitor mv, String className, int bitIndex, boolean isNullify) {
                mv.visitVarInsn(ALOAD, 0); // this
                mv.visitVarInsn(ALOAD, 0); // this
                mv.visitFieldInsn(GETFIELD, className, FLD_NAME_BITMASK, "L");
                mv.visitInsn(LCONST_1);
                mv.visitIntInsn(BIPUSH, bitIndex);
                mv.visitInsn(LSHL);
                if (isNullify) {
                    mv.visitInsn(LOR);
                } else {
                    mv.visitInsn(ICONST_M1);
                    mv.visitInsn(I2L);
                    mv.visitInsn(LXOR);
                    mv.visitInsn(LAND);
                }
                mv.visitFieldInsn(GETFIELD, className, FLD_NAME_BITMASK, "L");
            }
        },
        BITMASK(Integer.MAX_VALUE, "[I") {
            @Override
            public void visitGet(MethodVisitor mv, int bitIndex) {
                mv.visitIntInsn(BIPUSH, bitIndex);
                mv.visitInsn(DUP_X1);
                mv.visitIntInsn(BIPUSH, 5);
                mv.visitInsn(IUSHR);
                mv.visitInsn(IALOAD);
                mv.visitInsn(SWAP);
                mv.visitIntInsn(BIPUSH, 31);
                mv.visitInsn(IAND);
                mv.visitInsn(IUSHR);
//                mv.visitInsn(L2I);
                mv.visitInsn(ICONST_1);
                mv.visitInsn(IAND);
            }

            public void visitSet(MethodVisitor mv, String className, int bitIndex, boolean isNullify) {
                mv.visitVarInsn(ALOAD, 0); // this
                mv.visitFieldInsn(GETFIELD, className, FLD_NAME_BITMASK, "[I");
                mv.visitInsn(DUP);  // array reference
                mv.visitIntInsn(BIPUSH, bitIndex);
                mv.visitIntInsn(BIPUSH, 5);
                mv.visitInsn(IUSHR);

                mv.visitInsn(DUP2);
                mv.visitInsn(IALOAD);

                mv.visitInsn(LCONST_1);
                mv.visitIntInsn(BIPUSH, bitIndex);
                mv.visitIntInsn(BIPUSH, 31);
                mv.visitInsn(IAND);
                mv.visitInsn(ISHL);
                if (isNullify) {
                    mv.visitInsn(IOR);
                } else {
                    mv.visitInsn(ICONST_M1);
                    mv.visitInsn(IXOR);
                    mv.visitInsn(IAND);
                }
                mv.visitInsn(IASTORE);
            }
        };

        private final int    capacity;
        private final String typeDescriptor;

        public static BitMaskType getInstanceFor(int size) {
            for (BitMaskType bitMaskType : BitMaskType.values()) {
                if(bitMaskType.capacity >= size) {
                    return bitMaskType;
                }
            }
            throw new IllegalArgumentException("Illegal size: " + size);
        }

        BitMaskType(int capacity, String typeDescriptor) {
            this.capacity = capacity;
            this.typeDescriptor = typeDescriptor;
        }

        public void visitFieldCreation(ClassVisitor cv) {
            cv.visitField(ACC_PRIVATE, FLD_NAME_BITMASK, typeDescriptor, null, null).visitEnd();
        }

        // stack has raw field value
        public void visitGet(MethodVisitor mv, int bitIndex) {
            if(bitIndex != 0) {
                mv.visitIntInsn(BIPUSH, bitIndex);
                mv.visitInsn(IUSHR);
            }
            mv.visitInsn(ICONST_1);
            mv.visitInsn(IAND);
        }

        public void visitSet(MethodVisitor mv, String className, int bitIndex, boolean isNullify) {
            mv.visitVarInsn(ALOAD, 0); // this
            mv.visitVarInsn(ALOAD, 0); // this
            mv.visitFieldInsn(GETFIELD, className, FLD_NAME_BITMASK, typeDescriptor);
            mv.visitInsn(ICONST_1);
            mv.visitIntInsn(BIPUSH, bitIndex);
            mv.visitInsn(ISHL);
            if (isNullify) {
                mv.visitInsn(IOR);
            } else {
                mv.visitInsn(ICONST_M1);
                mv.visitInsn(IXOR);
                mv.visitInsn(IAND);
            }
            mv.visitFieldInsn(PUTFIELD, className, FLD_NAME_BITMASK, typeDescriptor);
        }
    }


    private static final String ROOT_CLASS_NAME_SLASHED = "org/h2/bytecode/RowStorage";


    public static Class<? extends RowStorage> generateStorageClass(int valueTypes[], int indexes[]) {
        String className = getClassName(valueTypes, indexes);
        Class<?> clazz;
        try {
            clazz = Class.forName(className, true, DynamicClassLoader.INSTANCE);
        } catch (ClassNotFoundException e) {
            byte[] classBytes = generateClassDefinition(valueTypes, indexes, className);

            OutputStream outputStream = null;
            try {
                outputStream = FileUtils.newOutputStream("generated/" + className.replace('.', '/') + ".class", false);
                outputStream.write(classBytes);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                if(outputStream != null) try { outputStream.close(); } catch (IOException ignore) {/**/}
            }
            clazz = DynamicClassLoader.INSTANCE.defineClass(className, classBytes);
        }
        //noinspection unchecked
        return (Class<? extends RowStorage>)clazz;
    }

    public static String getClassName(int valueTypes[], int indexes[]) {
        StringBuilder sb = new StringBuilder(80);
        sb.append("org.h2.bytecode.RowStorage");
        for (int type : valueTypes) {
            ValueType valueType = ValueType.get(type);
            sb.append(valueType.getClassNameSuffix());
        }
        if (indexes != null) {
            for (int index : indexes) {
                sb.append('_').append(index);
            }
        }
        return sb.toString();
    }

    public static byte[] generateClassDefinition(int valueTypes[], int indexes[], String className) {
        className = className.replace('.', '/');
        int fieldCount = valueTypes.length;
        boolean full = indexes == null;

        Set<Integer> set = null;
        if (!full) {
            set = new HashSet<>();
            for (int index : indexes) {
                set.add(index);
            }
        }

//        String parentClassName = full ? ROOT_CLASS_NAME_SLASHED : getClassName(valueTypes, null);
        String parentClassName = ROOT_CLASS_NAME_SLASHED;

        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
//        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS);

        cw.visit(V1_7, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, className, null, parentClassName, null);

        MethodVisitor mv;

        if(indexes != null) {
            generateGetIndexes(cw, className, indexes);
        }
        generateConstructor(cw, parentClassName);
/*
        mv = cw.visitMethod(ACC_PUBLIC, "<init>", "([Lorg/h2/value/Value;)V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(ALOAD, 1);
        mv.visitMethodInsn(INVOKESPECIAL, parentClassName, "<init>", "([Lorg/h2/value/Value;)V", false);
        mv.visitInsn(RETURN);
        mv.visitMaxs(2, 2);
        mv.visitEnd();
*/
        int nullabilityBitCount = 0;
        for (int i = 0; i < fieldCount; i++) {
            int type = valueTypes[i];
            if (set == null || set.contains(i)) {
                ValueType valueType = ValueType.get(type);
                if(valueType.requireNullabilityBit()) {
                    ++nullabilityBitCount;
                }
            }
        }
        BitMaskType bitMaskType = BitMaskType.getInstanceFor(nullabilityBitCount);
        bitMaskType.visitFieldCreation(cw);



        MethodVisitor gmv = cw.visitMethod(ACC_PUBLIC, "get", "(I)Lorg/h2/value/Value;", null, null);
        gmv.visitCode();
        gmv.visitVarInsn(ILOAD, 1);
        Label[] getLabels = createSwitchLabels(fieldCount);
        Label defaultGetLabel = new Label();
        gmv.visitTableSwitchInsn(0, fieldCount - 1, defaultGetLabel, getLabels);


        MethodVisitor smv = cw.visitMethod(ACC_PUBLIC, "set", "(ILorg/h2/value/Value;)V", null, null);
        smv.visitCode();
        smv.visitVarInsn(ILOAD, 1);
        Label[] setLabels = createSwitchLabels(fieldCount);
        Label defaultSetLabel = new Label();
        smv.visitTableSwitchInsn(0, fieldCount - 1, defaultSetLabel, setLabels);

        int memory = Constants.MEMORY_OBJECT;
        for (int i = 0; i < fieldCount; i++) {
            int type = valueTypes[i];
            if (set == null || set.contains(i)) {
                ValueType valueType = ValueType.get(type);
                memory += valueType.getMemory();

                valueType.visitFieldCreation(cw, i);

                gmv.visitLabel(getLabels[i]);
                gmv.visitFrame(F_SAME, 0, null, 0, null);
                valueType.visitGetter(gmv, className, i);

                smv.visitLabel(setLabels[i]);
                smv.visitFrame(F_SAME, 0, null, 0, null);
                valueType.visitSetter(smv, className, i);
            }
        }

        if (set != null) {
            for (int i = 0; i < fieldCount; i++) {
                if (!set.contains(i)) {
                    gmv.visitLabel(getLabels[i]);
                    smv.visitLabel(setLabels[i]);
                }
            }
            gmv.visitFrame(F_SAME, 0, null, 0, null);
            gmv.visitMethodInsn(INVOKESTATIC, ROOT_CLASS_NAME_SLASHED, "getNullValue", "()Lorg/h2/value/Value;", false);
            gmv.visitInsn(ARETURN);
        }

        gmv.visitLabel(defaultGetLabel);
        gmv.visitFrame(F_SAME, 0, null, 0, null);
        gmv.visitVarInsn(ALOAD, 0);
        gmv.visitVarInsn(ILOAD, 1);
        gmv.visitMethodInsn(INVOKESPECIAL, ROOT_CLASS_NAME_SLASHED, "get", "(I)Lorg/h2/value/Value;", false);
        gmv.visitInsn(ARETURN);
        gmv.visitMaxs(2, 2);
        gmv.visitEnd();

        smv.visitLabel(defaultSetLabel);
        smv.visitFrame(F_SAME, 0, null, 0, null);
        smv.visitVarInsn(ALOAD, 0);
        smv.visitVarInsn(ILOAD, 1);
        smv.visitVarInsn(ALOAD, 2);
        smv.visitMethodInsn(INVOKESPECIAL, ROOT_CLASS_NAME_SLASHED, "set", "(ILorg/h2/value/Value;)V", false);
        smv.visitInsn(RETURN);
        smv.visitMaxs(3, 3);
        smv.visitEnd();


        mv = cw.visitMethod(ACC_PUBLIC, "getColumnCount", "()I", null, null);
        mv.visitCode();
        mv.visitIntInsn(SIPUSH, fieldCount);
        mv.visitInsn(IRETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();


        MethodVisitor getMemoryVisitor = cw.visitMethod(ACC_PUBLIC, "getMemory", "()I", null, null);
        getMemoryVisitor.visitCode();
        getMemoryVisitor.visitIntInsn(SIPUSH, memory);

        for (int i = 0; i < fieldCount; i++) {
            if (set == null || set.contains(i)) {
                ValueType valueType = ValueType.get(valueTypes[i]);
                valueType.visitGetMemory(getMemoryVisitor, className, i);
            }
        }

        getMemoryVisitor.visitInsn(IRETURN);
        getMemoryVisitor.visitMaxs(2, 1);
        getMemoryVisitor.visitEnd();


        MethodVisitor compareToVisitor = cw.visitMethod(ACC_PUBLIC, "compareToSecure",
                                "(L" + ROOT_CLASS_NAME_SLASHED + ";Lorg/h2/value/CompareMode;)I", null, null);
        compareToVisitor.visitCode();

        compareToVisitor.visitVarInsn(ALOAD, 1);
        compareToVisitor.visitTypeInsn(CHECKCAST, className);
        compareToVisitor.visitVarInsn(ASTORE, 3);

        Label retLabel = new Label();

        boolean cont = false;
        if (full) {
            indexes = new int[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                indexes[i] = i;
            }
        }

        for (int i : indexes) {
            if (cont) {
                compareToVisitor.visitVarInsn(ILOAD, 4);
                compareToVisitor.visitJumpInsn(IFNE, retLabel);
            }
            cont = true;

            ValueType valueType = ValueType.get(valueTypes[i]);

            compareToVisitor.visitVarInsn(ALOAD, 0);
            compareToVisitor.visitFieldInsn(GETFIELD, className, valueType.getFieldName(i), valueType.descriptor);
            compareToVisitor.visitVarInsn(ALOAD, 3);
            compareToVisitor.visitFieldInsn(GETFIELD, className, valueType.getFieldName(i), valueType.descriptor);

            valueType.visitCompareTo(compareToVisitor, className);

            compareToVisitor.visitVarInsn(ISTORE, 4);
        }

        compareToVisitor.visitLabel(retLabel);
        compareToVisitor.visitFrame(F_APPEND, 2, new Object[]{ className, INTEGER }, 0, null);

        compareToVisitor.visitVarInsn(ILOAD, 4);

        compareToVisitor.visitInsn(IRETURN);
        compareToVisitor.visitMaxs(4, 5);
        compareToVisitor.visitEnd();

        generateIsNull(cw, valueTypes, indexes, className, bitMaskType);
        generateNullify(cw, valueTypes, indexes, className, bitMaskType, false);
        generateNullify(cw, valueTypes, indexes, className, bitMaskType, true);
        generateCompareToSecure(cw, valueTypes, indexes, className);
        generateCopyFrom(cw, valueTypes, indexes, className);
        generateRawAccessor(cw, valueTypes, indexes, className);

        cw.visitEnd();

        return cw.toByteArray();
    }

    private static void generateConstructor(ClassWriter cw, String parentClassName) {
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, parentClassName, "<init>", "()V", false);
        mv.visitInsn(RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();
    }

    private static void generateGetIndexes(ClassWriter cw, String className, int[] indexes) {
        cw.visitField(ACC_PRIVATE|ACC_STATIC, FLD_NAME_INDEXES, DESC_INDEXES, null, null).visitEnd();

        MethodVisitor mv = cw.visitMethod(ACC_PROTECTED|ACC_STATIC, "<clinit>", "()V", null, null);
        mv.visitCode();
        if (indexes == null) {
            mv.visitInsn(ACONST_NULL);
        } else {
            mv.visitIntInsn(SIPUSH, indexes.length);
            mv.visitVarInsn(NEWARRAY, T_INT);
            for (int i = 0; i < indexes.length; i++) {
                int index = indexes[i];
                mv.visitInsn(DUP);
                mv.visitIntInsn(SIPUSH, i);
                mv.visitIntInsn(SIPUSH, index);
                mv.visitInsn(IASTORE);
            }
        }
        mv.visitFieldInsn(PUTSTATIC, className, FLD_NAME_INDEXES, DESC_INDEXES);
        mv.visitInsn(RETURN);
        mv.visitMaxs(4, 1);
        mv.visitEnd();

        mv = cw.visitMethod(ACC_PROTECTED, "getIndexes", "()[I", null, null);
        mv.visitCode();
        mv.visitFieldInsn(GETSTATIC, className, FLD_NAME_INDEXES, DESC_INDEXES);
        mv.visitInsn(ARETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();
    }

    private static void generateIsNull(ClassWriter cv, int[] valueTypes, int[] indexes, String className,
                                       BitMaskType bitMaskType) {
        int fieldCount = valueTypes.length;
        MethodVisitor mv = cv.visitMethod(ACC_PUBLIC, "isNull", "(I)Z", null, null);
        mv.visitCode();

        mv.visitVarInsn(ILOAD, 1); // indx
        Label[] labels = createSwitchLabels(fieldCount);
        Label defaultSetLabel = new Label();
        mv.visitTableSwitchInsn(0, fieldCount - 1, defaultSetLabel, labels);

        int bitIndex = 0;
        for (int i : indexes) {
            int type = valueTypes[i];
            ValueType valueType = ValueType.get(type);
            Label label = labels[i];
            labels[i] = null;
            mv.visitLabel(label);
            mv.visitFrame(F_SAME, 0, null, 0, null);
            if (valueType.requireNullabilityBit()) {
                mv.visitVarInsn(ALOAD, 0); // this
                mv.visitFieldInsn(GETFIELD, className, FLD_NAME_BITMASK, bitMaskType.typeDescriptor);
                bitMaskType.visitGet(mv, bitIndex);
                mv.visitInsn(IRETURN);
                ++bitIndex;
            } else {
                mv.visitVarInsn(ALOAD, 0);  // this
                mv.visitFieldInsn(GETFIELD, className, valueType.getFieldName(i), valueType.descriptor);
                mv.visitMethodInsn(INVOKESTATIC, className, "isNull", "(" + valueType.descriptor + ")Z", false);
                mv.visitInsn(IRETURN);
            }
        }

        boolean isSparse = false;
        for (Label label : labels) {
            if (label != null) {
                mv.visitLabel(label);
                isSparse = true;
            }
        }
        if (isSparse) {
            mv.visitFrame(F_SAME, 0, null, 0, null);
            mv.visitInsn(ICONST_1);
            mv.visitInsn(IRETURN);
        }

        mv.visitLabel(defaultSetLabel);
        mv.visitFrame(F_SAME, 0, null, 0, null);
        mv.visitVarInsn(ALOAD, 0);  // this
        mv.visitVarInsn(ILOAD, 1);  // index
        mv.visitMethodInsn(INVOKESPECIAL, ROOT_CLASS_NAME_SLASHED, "isNull", "(I)Z", false);
        mv.visitInsn(IRETURN);

        mv.visitMaxs(0,0);
        mv.visitEnd();
    }

    private static void generateNullify(ClassWriter cv, int[] valueTypes, int[] indexes, String className,
                                        BitMaskType bitMaskType, boolean isNullify) {
        int fieldCount = valueTypes.length;
        MethodVisitor mv = cv.visitMethod(ACC_PROTECTED, isNullify ? "nullify" : "clearNull", "(I)V", null, null);
        mv.visitCode();

        mv.visitVarInsn(ILOAD, 1); // indx
        Label[] labels = createSwitchLabels(fieldCount);
        Label defaultSetLabel = new Label();
        mv.visitTableSwitchInsn(0, fieldCount - 1, defaultSetLabel, labels);

        int bitIndex = 0;
        for (int i : indexes) {
            int type = valueTypes[i];
            ValueType valueType = ValueType.get(type);
            if (valueType.requireNullabilityBit()) {
                Label label = labels[i];
                labels[i] = null;
                mv.visitLabel(label);
                mv.visitFrame(F_SAME, 0, null, 0, null);
//                mv.visitVarInsn(ALOAD, 0); // this
//                mv.visitFieldInsn(GETFIELD, className, FLD_NAME_BITMASK, bitMaskType.typeDescriptor);
                bitMaskType.visitSet(mv, className, bitIndex, isNullify);
                mv.visitInsn(RETURN);
                ++bitIndex;
            }
        }

        for (Label label : labels) {
            if (label != null) {
                mv.visitLabel(label);
            }
        }
        mv.visitLabel(defaultSetLabel);
        mv.visitFrame(F_SAME, 0, null, 0, null);
        mv.visitInsn(RETURN);

        mv.visitMaxs(0,0);
        mv.visitEnd();
    }

    //
    // public long getLong(int index) tec.
    //
    private static void generateRawAccessor(ClassWriter cw, int[] valueTypes, int[] indexes, String className) {
        int fieldCount = valueTypes.length;
        for (ValueType valueType : ValueType.values()) {
            if(valueType != ValueType.DEFAULT) {
                boolean inUse = false;
                for (int i : indexes) {
                    int type = valueTypes[i];
                    if (ValueType.get(type) == valueType) {
                        inUse = true;
                    }
                }
                if(inUse) {
                    MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, valueType.getRawAccessorName(), "(I)" + valueType.descriptor, null, null);
                    mv.visitCode();

                    mv.visitVarInsn(ILOAD, 1);  // index
                    Label[] labels = createSwitchLabels(fieldCount);
                    Label defaultSetLabel = new Label();
                    mv.visitTableSwitchInsn(0, fieldCount - 1, defaultSetLabel, labels);

                    for (int i : indexes) {
                        int type = valueTypes[i];
                        if (ValueType.get(type) == valueType) {
                            Label label = labels[i];
                            labels[i] = null;

                            mv.visitLabel(label);
                            mv.visitFrame(F_SAME, 0, null, 0, null);
                            mv.visitVarInsn(ALOAD, 0);  // this
                            mv.visitFieldInsn(GETFIELD, className, valueType.getFieldName(i), valueType.descriptor);
                            mv.visitInsn(valueType.getReturnInstruction());
                        }
                    }
                    for (Label label : labels) {
                        if (label != null) {
                            mv.visitLabel(label);
                        }
                    }

                    mv.visitLabel(defaultSetLabel);
                    mv.visitFrame(F_SAME, 0, null, 0, null);
                    mv.visitVarInsn(ALOAD, 0);  // this
                    mv.visitVarInsn(ILOAD, 1);  // index
                    mv.visitMethodInsn(INVOKESPECIAL, ROOT_CLASS_NAME_SLASHED, valueType.getRawAccessorName(), "(I)" + valueType.descriptor, false);
                    mv.visitInsn(valueType.getReturnInstruction());
                    mv.visitMaxs(4, 4);
                    mv.visitEnd();
                }
            }
        }
    }


    //
    // protected int compareToSecure(RowStorage other, CompareMode mode, int index)
    //
    private static void generateCompareToSecure(ClassWriter cw, int[] valueTypes, int[] indexes, String className) {
        int fieldCount = valueTypes.length;
        MethodVisitor mv = cw.visitMethod(ACC_PROTECTED, "compareToSecure",
                                "(L" + ROOT_CLASS_NAME_SLASHED + ";Lorg/h2/value/CompareMode;I)I", null, null);
        mv.visitCode();

        mv.visitVarInsn(ILOAD, 3);  // index
        Label[] labels = createSwitchLabels(fieldCount);
        Label defaultSetLabel = new Label();
        mv.visitTableSwitchInsn(0, fieldCount - 1, defaultSetLabel, labels);

        for (int i : indexes) {
            Label label = labels[i];
            labels[i] = null;
            int type = valueTypes[i];
            ValueType valueType = ValueType.get(type);

            mv.visitLabel(label);
            mv.visitFrame(F_SAME, 0, null, 0, null);
            mv.visitVarInsn(ALOAD, 0);  // this
            mv.visitFieldInsn(GETFIELD, className, valueType.getFieldName(i), valueType.descriptor);
            mv.visitVarInsn(ALOAD, 1);  // other
            mv.visitIntInsn(SIPUSH, i);
            mv.visitMethodInsn(INVOKEVIRTUAL, ROOT_CLASS_NAME_SLASHED, valueType.getRawAccessorName(), "(I)"+valueType.descriptor, false);
//            mv.visitTypeInsn(CHECKCAST, className); // (this.class) other
//            mv.visitFieldInsn(GETFIELD, className, valueType.getFieldName(i), valueType.descriptor);
            valueType.visitCompareTo(mv, className);
            mv.visitInsn(IRETURN);
        }

        for (Label label : labels) {
            if (label != null) {
                mv.visitLabel(label);
            }
        }

        mv.visitLabel(defaultSetLabel);
        mv.visitFrame(F_SAME, 0, null, 0, null);
        mv.visitInsn(ICONST_0);
        mv.visitInsn(IRETURN);
        mv.visitMaxs(4, 4);
        mv.visitEnd();
    }


    //
    // protected void copyFrom(RowStorage other, int index) {
    //
    private static void generateCopyFrom(ClassWriter cw, int[] valueTypes, int[] indexes, String className) {
        int fieldCount = valueTypes.length;
        MethodVisitor mv = cw.visitMethod(0, "copyFrom",
                                "(L" + ROOT_CLASS_NAME_SLASHED + ";I)V", null, null);
        mv.visitCode();

        mv.visitVarInsn(ILOAD, 2);  // index
        Label[] labels = createSwitchLabels(fieldCount);
        Label defaultSetLabel = new Label();
        mv.visitTableSwitchInsn(0, fieldCount - 1, defaultSetLabel, labels);

        for (int i : indexes) {
            Label label = labels[i];
            labels[i] = null;
            int type = valueTypes[i];
            ValueType valueType = ValueType.get(type);

            mv.visitLabel(label);
            mv.visitFrame(F_SAME, 0, null, 0, null);
            mv.visitVarInsn(ALOAD, 0);  // this
            mv.visitVarInsn(ALOAD, 1);  // other
            mv.visitIntInsn(SIPUSH, i);
            mv.visitMethodInsn(INVOKEVIRTUAL, ROOT_CLASS_NAME_SLASHED, valueType.getRawAccessorName(), "(I)"+valueType.descriptor, false);
            mv.visitFieldInsn(PUTFIELD, className, valueType.getFieldName(i), valueType.descriptor);
            mv.visitInsn(RETURN);
        }

        for (Label label : labels) {
            if (label != null) {
                mv.visitLabel(label);
            }
        }

        mv.visitLabel(defaultSetLabel);
        mv.visitFrame(F_SAME, 0, null, 0, null);
        mv.visitInsn(RETURN);
        mv.visitMaxs(3, 3);
        mv.visitEnd();
    }

    private static Label[] createSwitchLabels(int fieldCount) {
        Label[] labels = new Label[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            labels[i] = new Label();
        }
        return labels;
    }

    public static class DynamicClassLoader extends ClassLoader {
        private static final DynamicClassLoader INSTANCE = new DynamicClassLoader(RowStorage.class.getClassLoader());
        private DynamicClassLoader(ClassLoader parent) {
            super(parent);
        }

        public Class<?> defineClass(String className, byte b[]) {
            return defineClass(className, b, 0, b.length);
        }
    }
}
