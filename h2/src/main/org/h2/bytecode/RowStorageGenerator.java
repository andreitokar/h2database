package org.h2.bytecode;

import org.h2.engine.Constants;
import org.h2.store.fs.FileUtils;
import org.h2.value.*;
import jdk.internal.org.objectweb.asm.ClassVisitor;
import jdk.internal.org.objectweb.asm.ClassWriter;
import jdk.internal.org.objectweb.asm.Label;
import jdk.internal.org.objectweb.asm.MethodVisitor;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

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


    public enum ValueType {
//        BOOLEAN(Value.BOOLEAN, "Z", 1, "getValueBoolean", "getBoolean"),
//        BYTE(Value.BYTE,       "B", 1, "getValueByte", "getByte"),
//        SHORT(Value.SHORT,     "S", 2, "getValueShort", "getShort"),
        INT(Value.INT,         "I", 4, "getValueInt", "getInt") {
            @Override
            public void visitCompareTo(MethodVisitor mv, String className, int indx) {
                mv.visitMethodInsn(INVOKESTATIC, "java/lang/Integer", "compare", "(II)I", false);
            }
        },
        LONG(Value.LONG,       "J", 8, "getValueLong", "getLong") {
            @Override
            public void visitCompareTo(MethodVisitor mv, String className, int indx) {
                mv.visitMethodInsn(INVOKESTATIC, "java/lang/Long", "compare", "(JJ)I", false);
            }
        },
        DOUBLE(Value.DOUBLE,   "D", 8, "getValueDouble", "getDouble") {
            @Override
            public void visitCompareTo(MethodVisitor mv, String className, int indx) {
                mv.visitMethodInsn(INVOKESTATIC, "java/lang/Double", "compare", "(DD)I", false);
            }
        },
        FLOAT(Value.FLOAT,     "F", 4, "getValueFloat", "getFloat") {
            @Override
            public void visitCompareTo(MethodVisitor mv, String className, int indx) {
                mv.visitMethodInsn(INVOKESTATIC, "java/lang/Float", "compare", "(FF)I", false);
            }
        },
        DECIMAL(Value.DECIMAL, "Ljava/math/BigDecimal;", Constants.MEMORY_OBJECT + Constants.MEMORY_POINTER, "getValueDecimal", "getDecimal", "d") {
            @Override
            public void visitGetMemory(MethodVisitor mv, String className, int indx) {
                mv.visitVarInsn(ALOAD, 0);
                mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), "Ljava/math/BigDecimal;");
                mv.visitMethodInsn(INVOKESTATIC, className, "getMemory", "(Ljava/math/BigDecimal;)I", false);
                mv.visitInsn(IADD);
            }
        },
        STRING_FIXED(Value.STRING_FIXED, "[B", Constants.MEMORY_OBJECT, "getValueBytes", "getBytes", "t") {
            @Override
            public void visitGetMemory(MethodVisitor mv, String className, int indx) {
                mv.visitVarInsn(ALOAD, 0);
                mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), "[B");
                mv.visitMethodInsn(INVOKESTATIC, className, "getMemory", "([B)I", false);
                mv.visitInsn(IADD);
            }
        },
        STRING(Value.STRING,   "Ljava/lang/String;", Constants.MEMORY_OBJECT + Constants.MEMORY_POINTER, "getValueString", "getString", "T") {
            @Override
            public void visitGetMemory(MethodVisitor mv, String className, int indx) {
                mv.visitVarInsn(ALOAD, 0);
                mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), "Ljava/lang/String;");
                mv.visitMethodInsn(INVOKESTATIC, className, "getMemory", "(Ljava/lang/String;)I", false);
                mv.visitInsn(IADD);
            }
        },
        DEFAULT(Value.UNKNOWN, "Lorg/h2/value/Value;", Constants.MEMORY_OBJECT, "getValue", "getValue", "v") {
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
        private final String accessor;
        private final String rawAccessor;
        private final String nameSuffix;


        public static ValueType get(int type) {
            ValueType valueType = map.get(type);
            if(valueType == null) {
                valueType = DEFAULT;
            }
            return valueType;
        }

        ValueType(int type, String descriptor, int memory, String accessor, String rawAccessor) {
            this(type, descriptor, memory, accessor, rawAccessor, null);
        }

        ValueType(int type, String descriptor, int memory, String accessor, String rawAccessor, String nameSuffix) {
            this.type = type;
            this.descriptor = descriptor;
            this.memory = memory;
            this.accessor = accessor;
            this.rawAccessor = rawAccessor;
            this.nameSuffix = nameSuffix;
        }

        public String getClassNameSuffix() {
            return nameSuffix == null ? descriptor : nameSuffix;
        }

        private String getAccessorName() {
            return accessor == null ? "getValue" + descriptor : accessor;
        }

        private String getRawAccessorName() {
            return rawAccessor == null ? "get" + descriptor : rawAccessor;
        }

        protected String getFieldName(int indx) {
            return "field_" + indx;
        }

        public int getMemory() {
            return memory;
        }

        public void visitFieldCreation(ClassVisitor cw, int indx) {
            cw.visitField(ACC_PRIVATE, getFieldName(indx), descriptor, null, null).visitEnd();
        }

        public void visitGetter(MethodVisitor mv, String className, int indx) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitFieldInsn(GETFIELD, className, getFieldName(indx), descriptor);
            if(this != ValueType.DEFAULT) {
                mv.visitMethodInsn(INVOKESTATIC, className, getAccessorName(), "(" + descriptor + ")Lorg/h2/value/Value;", false);
            }
            mv.visitInsn(ARETURN);
        }

        public void visitSetter(MethodVisitor mv, String className, int indx) {
            mv.visitVarInsn(ALOAD, 0);
            mv.visitVarInsn(ALOAD, 2);
            if(this != ValueType.DEFAULT) {
                mv.visitMethodInsn(INVOKESTATIC, className, getRawAccessorName(), "(Lorg/h2/value/Value;)" + descriptor, false);
            }
            mv.visitFieldInsn(PUTFIELD, className, getFieldName(indx), descriptor);
            mv.visitInsn(RETURN);
        }

        public void visitGetMemory(MethodVisitor mv, String className, int indx) {}

        public void visitCompareTo(MethodVisitor mv, String className, int indx) {
            mv.visitVarInsn(ALOAD, 2);
            mv.visitMethodInsn(INVOKESTATIC, className, "compare", "(" + descriptor + descriptor + "Lorg/h2/value/CompareMode;)I", false);
        }
    }

    public static void main(String[] args) {
        int[] valueTypes = { Value.INT, Value.STRING, Value.UNKNOWN, Value.LONG, Value.DOUBLE };
//*
        String className = getClassName(valueTypes);
        byte classBytes[] = generateStorageBytes(valueTypes, className);
        className = className.substring(className.lastIndexOf('.') + 1);
        try (java.io.FileOutputStream out = new java.io.FileOutputStream(new java.io.File(className + ".class"))) {
            out.write(classBytes);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
//*/
        Class<? extends RowStorage> cls = generateStorageClass(valueTypes);
        try {
            Constructor<? extends RowStorage> constructor = cls.getConstructor();
            Value[] initargs = {
                    ValueInt.get(3),
                    ValueString.get("Hello"),
                    ValueDate.fromMillis(System.currentTimeMillis()),
                    ValueLong.get(77),
                    ValueDouble.get(3.62)
            };
            RowStorage row = constructor.newInstance();
            row.setValues(initargs);
            row.setKey(12345);
            System.out.println(row);

            RowStorage rowTwo = row.clone();
            rowTwo.setValue(0, ValueInt.get(5));
            rowTwo.setValue(1, ValueString.get("World"));
            rowTwo.setValue(2, ValueDate.parse("2001-09-11"));
            rowTwo.setValue(3, ValueLong.get(999));
            rowTwo.setValue(4, ValueDouble.get(4.12));
            System.out.println(rowTwo);
            System.out.println("Comparison result:" + row.compare(rowTwo, null));
            System.out.println("Comparison result:" + rowTwo.compare(row, null));
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            e.printStackTrace();
        }
    }

    public static Class<? extends RowStorage> generateStorageClass(int valueTypes[]) {
        String className = getClassName(valueTypes);
        Class<?> clazz;
        try {
            clazz = Class.forName(className, true, DynamicClassLoader.INSTANCE);
        } catch (ClassNotFoundException e) {
            byte[] classBytes = generateStorageBytes(valueTypes, className);

            OutputStream outputStream = null;
            try {
                outputStream = FileUtils.newOutputStream("generated/" + className.replace('.', '/') + ".class", false);
                outputStream.write(classBytes);
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                if(outputStream != null) try { outputStream.close(); } catch (IOException ignore) {/**/}
            }
//            className = className.replace('.','/');
            clazz = DynamicClassLoader.INSTANCE.defineClass(className, classBytes);
        }
        //noinspection unchecked
        return (Class<? extends RowStorage>)clazz;
    }

    private static String getClassName(int[] valueTypes) {
        StringBuilder sb = new StringBuilder(80);
        sb.append("org.h2.bytecode.RowStorage");
        for (int type : valueTypes) {
            ValueType valueType = ValueType.get(type);
            sb.append(valueType.getClassNameSuffix());
        }
        return sb.toString();
    }

    private static byte[] generateStorageBytes(int valueTypes[], String className) {
        className = className.replace('.', '/');
        int fieldCount = valueTypes.length;

//        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES/* | ClassWriter.COMPUTE_MAXS*/);
        ClassWriter cw = new ClassWriter(0);

        cw.visit(V1_7, ACC_PUBLIC | ACC_FINAL | ACC_SUPER, className, null, "org/h2/bytecode/RowStorage", null);

        MethodVisitor mv;

        mv = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitMethodInsn(INVOKESPECIAL, "org/h2/bytecode/RowStorage", "<init>", "()V", false);
        mv.visitInsn(RETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();

/*
        mv = cw.visitMethod(ACC_PUBLIC, "<init>", "([Lorg/h2/value/Value;)V", null, null);
        mv.visitCode();
        mv.visitVarInsn(ALOAD, 0);
        mv.visitVarInsn(ALOAD, 1);
        mv.visitMethodInsn(INVOKESPECIAL, "org/h2/bytecode/RowStorage", "<init>", "([Lorg/h2/value/Value;)V", false);
        mv.visitInsn(RETURN);
        mv.visitMaxs(2, 2);
        mv.visitEnd();
*/


        MethodVisitor gmv = cw.visitMethod(ACC_PUBLIC, "getValue", "(I)Lorg/h2/value/Value;", null, null);
        gmv.visitCode();
        gmv.visitVarInsn(ILOAD, 1);
        Label[] getLabels = createSwitchLabels(fieldCount);
        Label defaultGetLabel = new Label();
        gmv.visitTableSwitchInsn(0, fieldCount - 1, defaultGetLabel, getLabels);


        MethodVisitor smv = cw.visitMethod(ACC_PUBLIC, "setValue", "(ILorg/h2/value/Value;)V", null, null);
        smv.visitCode();
        smv.visitVarInsn(ILOAD, 1);
        Label[] setLabels = createSwitchLabels(fieldCount);
        Label defaultSetLabel = new Label();
        smv.visitTableSwitchInsn(0, fieldCount - 1, defaultSetLabel, setLabels);


        int memory = Constants.MEMORY_OBJECT;
        for (int i = 0; i < fieldCount; i++) {
            int type = valueTypes[i];
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

        gmv.visitLabel(defaultGetLabel);
        gmv.visitFrame(F_SAME, 0, null, 0, null);
        gmv.visitVarInsn(ALOAD, 0);
        gmv.visitVarInsn(ILOAD, 1);
        gmv.visitMethodInsn(INVOKESPECIAL, "org/h2/bytecode/RowStorage", "getValue", "(I)Lorg/h2/value/Value;", false);
        gmv.visitInsn(ARETURN);
        gmv.visitMaxs(2, 2);
        gmv.visitEnd();

        smv.visitLabel(defaultSetLabel);
        smv.visitFrame(F_SAME, 0, null, 0, null);
        smv.visitVarInsn(ALOAD, 0);
        smv.visitVarInsn(ILOAD, 1);
        smv.visitVarInsn(ALOAD, 2);
        smv.visitMethodInsn(INVOKESPECIAL, "org/h2/bytecode/RowStorage", "setValue", "(ILorg/h2/value/Value;)V", false);
        smv.visitInsn(RETURN);
        smv.visitMaxs(3, 3);
        smv.visitEnd();


        mv = cw.visitMethod(ACC_PUBLIC, "getColumnCount", "()I", null, null);
        mv.visitCode();
        mv.visitIntInsn(BIPUSH, fieldCount);
        mv.visitInsn(IRETURN);
        mv.visitMaxs(1, 1);
        mv.visitEnd();

        
        MethodVisitor getMemoryVisitor = cw.visitMethod(ACC_PUBLIC, "getMemory", "()I", null, null);
        getMemoryVisitor.visitCode();
        getMemoryVisitor.visitIntInsn(SIPUSH, memory);

        for (int indx = 0; indx < fieldCount; indx++) {
            ValueType valueType = ValueType.get(valueTypes[indx]);
            valueType.visitGetMemory(getMemoryVisitor, className, indx);
        }

        getMemoryVisitor.visitInsn(IRETURN);
        getMemoryVisitor.visitMaxs(2, 1);
        getMemoryVisitor.visitEnd();


        MethodVisitor compareToVisitor = cw.visitMethod(ACC_PUBLIC, "compareToSecure", "(Lorg/h2/bytecode/RowStorage;Lorg/h2/value/CompareMode;)I", null, null);
        compareToVisitor.visitCode();

        compareToVisitor.visitVarInsn(ALOAD, 1);
        compareToVisitor.visitTypeInsn(CHECKCAST, className);
        compareToVisitor.visitVarInsn(ASTORE, 3);
//        compareToVisitor.visitInsn(ICONST_0);
//        compareToVisitor.visitVarInsn(ISTORE, 4);

        Label retLabel = new Label();

        for (int indx = 0; indx < fieldCount; indx++) {
            if(indx != 0) {
                compareToVisitor.visitVarInsn(ILOAD, 4);
                compareToVisitor.visitJumpInsn(IFNE, retLabel);
            }

            ValueType valueType = ValueType.get(valueTypes[indx]);

            compareToVisitor.visitVarInsn(ALOAD, 0);
            compareToVisitor.visitFieldInsn(GETFIELD, className, valueType.getFieldName(indx), valueType.descriptor);
            compareToVisitor.visitVarInsn(ALOAD, 3);
            compareToVisitor.visitFieldInsn(GETFIELD, className, valueType.getFieldName(indx), valueType.descriptor);

            valueType.visitCompareTo(compareToVisitor, className, indx);

            compareToVisitor.visitVarInsn(ISTORE, 4);
        }

        compareToVisitor.visitLabel(retLabel);
        compareToVisitor.visitFrame(F_APPEND, 2, new Object[]{ className, INTEGER }, 0, null);

        compareToVisitor.visitVarInsn(ILOAD, 4);

        compareToVisitor.visitInsn(IRETURN);
        compareToVisitor.visitMaxs(5, 5);
        compareToVisitor.visitEnd();


        cw.visitEnd();

        return cw.toByteArray();
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
