/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.table;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.h2.constraint.Constraint;
import org.h2.engine.SessionLocal;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.index.MetaIndex;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.schema.Schema;
import org.h2.util.StringUtils;
import org.h2.value.TypeInfo;
import org.h2.value.Value;
import org.h2.value.ValueNull;
import org.h2.value.ValueVarchar;
import org.h2.value.ValueVarcharIgnoreCase;

/**
 * This class is responsible to build the database meta data pseudo tables.
 */
public abstract class MetaTable extends Table {

    /**
     * The approximate number of rows of a meta table.
     */
    public static final long ROW_COUNT_APPROXIMATION = 1000;

    /**
     * The table type.
     */
    protected final int type;

    /**
     * The indexed column.
     */
    protected int indexColumn;

    /**
     * The index for this table.
     */
    protected MetaIndex metaIndex;

    private MetaIndex scanIndex;
    private final ArrayList<Index> indexes = new ArrayList<>(2);

    /**
     * Create a new metadata table.
     *
     * @param schema the schema
     * @param id the object id
     * @param type the meta table type
     */
    protected MetaTable(Schema schema, int id, int type) {
        // tableName will be set later
        super(schema, id, null, true, true);
        this.type = type;
    }

    @Override
    protected void setColumns(Column[] columns) {
        super.setColumns(columns);
        scanIndex = new MetaIndex(this, IndexColumn.wrap(columns), true);
        indexes.clear();
        indexes.add(scanIndex);
        if (metaIndex != null) {
            indexes.add(metaIndex);
        }
    }

    protected final void setMetaTableName(String upperName) {
        setObjectName(database.sysIdentifier(upperName));
    }

    /**
     * Creates a column with the specified name and character string data type.
     *
     * @param name
     *            the uppercase column name
     * @return the column
     */
    final Column column(String name) {
        return new Column(database.sysIdentifier(name),
                database.getSettings().caseInsensitiveIdentifiers ? TypeInfo.TYPE_VARCHAR_IGNORECASE
                        : TypeInfo.TYPE_VARCHAR);
    }

    /**
     * Creates a column with the specified name and data type.
     *
     * @param name
     *            the uppercase column name
     * @param type
     *            the data type
     * @return the column
     */
    protected final Column column(String name, TypeInfo type) {
        return new Column(database.sysIdentifier(name), type);
    }

    @Override
    public final String getCreateSQL() {
        return null;
    }

    @Override
    public final Index addIndex(SessionLocal session, String indexName, int indexId, IndexColumn[] cols,
            int uniqueColumnCount, IndexType indexType, boolean create, String indexComment) {
        throw DbException.getUnsupportedException("META");
    }

    /**
     * If needed, convert the identifier to lower case.
     *
     * @param s the identifier to convert
     * @return the converted identifier
     */
    protected final String identifier(String s) {
        if (database.getSettings().databaseToLower) {
            s = s == null ? null : StringUtils.toLowerEnglish(s);
        }
        return s;
    }

    /**
     * Checks index conditions.
     *
     * @param session the session
     * @param value the value
     * @param indexFrom the lower bound of value, or {@code null}
     * @param indexTo the higher bound of value, or {@code null}
     * @return whether row should be included into result
     */
    protected final boolean checkIndex(SessionLocal session, String value, Value indexFrom, Value indexTo) {
        if (value == null || (indexFrom == null && indexTo == null)) {
            return true;
        }
        Value v;
        if (database.getSettings().caseInsensitiveIdentifiers) {
            v = ValueVarcharIgnoreCase.get(value);
        } else {
            v = ValueVarchar.get(value);
        }
        if (indexFrom != null && session.compare(v, indexFrom) < 0) {
            return false;
        }
        if (indexTo != null && session.compare(v, indexTo) > 0) {
            return false;
        }
        return true;
    }

    /**
     * Get all tables of this database, including local temporary tables for the
     * session.
     *
     * @param session
     *            the session
     * @param indexFrom
     *            first value or {@code null}
     * @param indexTo
     *            last value or {@code null}
     * @return the stream of tables
     */
    protected final Stream<Table> getAllTables(SessionLocal session, Value indexFrom, Value indexTo) {
        if (indexFrom != null && indexFrom.equals(indexTo)) {
            String tableName = indexFrom.getString();
            if (tableName == null) {
                return Stream.empty();
            }
            return Stream
                    .concat(database.getAllSchemas().stream()
                            .map(schema -> schema.getTableOrViewByName(session, tableName)),
                            Stream.ofNullable(session.findLocalTempTable(tableName)))
                    .filter(Objects::nonNull);
        } else {
            return Stream
                    .concat(database.getAllSchemas().stream()
                            .flatMap(schema -> schema.getAllTablesAndViews(session).stream()),
                            session.getLocalTempTables().stream())
                    .filter(table -> checkIndex(session, table.getName(), indexFrom, indexTo));
        }
    }

    /**
     * Get all constraints of this database, including constraints of local
     * temporary tables for the session.
     *
     * @param session
     *            the session
     * @return the stream of constraints
     */
    protected final Stream<Constraint> getAllConstraints(SessionLocal session) {
        return Stream.concat(database.getAllSchemas().stream().flatMap(schema -> schema.getAllConstraints().stream()),
                session.getLocalTempTableConstraints().values().stream());
    }

    /**
     * Generate the data for the given metadata table using the given first and
     * last row filters.
     *
     * @param session the session
     * @param first the first row to return
     * @param last the last row to return
     * @return the generated rows
     */
    public abstract ArrayList<Row> generateRows(SessionLocal session, SearchRow first, SearchRow last);

    @Override
    public boolean isInsertable() {
        return false;
    }

    @Override
    public final void removeRow(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void addRow(SessionLocal session, Row row) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void removeChildrenAndResources(SessionLocal session) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void close(SessionLocal session) {
        // nothing to do
    }

    /**
     * Add a row to a list.
     *
     * @param session the session
     * @param rows the original row list
     * @param stringsOrValues the values, or strings
     */
    protected final void add(SessionLocal session, ArrayList<Row> rows, Object... stringsOrValues) {
        Value[] values = new Value[stringsOrValues.length];
        for (int i = 0; i < stringsOrValues.length; i++) {
            Object s = stringsOrValues[i];
            Value v = s == null ? ValueNull.INSTANCE : s instanceof String ? ValueVarchar.get((String) s) : (Value) s;
            values[i] = columns[i].convert(session, v);
        }
        rows.add(Row.get(values, 1, rows.size()));
    }

    @Override
    public final void checkRename() {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final void checkSupportAlter() {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public final long truncate(SessionLocal session) {
        throw DbException.getUnsupportedException("META");
    }

    @Override
    public long getRowCount(SessionLocal session) {
        throw DbException.getInternalError(toString());
    }

    @Override
    public boolean canGetRowCount(SessionLocal session) {
        return false;
    }

    @Override
    public final boolean canDrop() {
        return false;
    }

    @Override
    public final TableType getTableType() {
        return TableType.SYSTEM_TABLE;
    }

    @Override
    public final Index getScanIndex(SessionLocal session) {
        return scanIndex;
    }

    @Override
    public final List<Index> getIndexes() {
        return indexes;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return ROW_COUNT_APPROXIMATION;
    }

    @Override
    public final boolean isDeterministic() {
        return true;
    }

    @Override
    public final boolean canReference() {
        return false;
    }

}
