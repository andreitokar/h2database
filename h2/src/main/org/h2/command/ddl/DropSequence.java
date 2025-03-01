/*
 * Copyright 2004-2025 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.schema.Schema;
import org.h2.schema.Sequence;

/**
 * This class represents the statement
 * DROP SEQUENCE
 */
public class DropSequence extends SchemaOwnerCommand {

    private String sequenceName;
    private boolean ifExists;

    public DropSequence(SessionLocal session, Schema schema) {
        super(session, schema);
    }

    public void setIfExists(boolean b) {
        ifExists = b;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    @Override
    long update(Schema schema) {
        Sequence sequence = schema.findSequence(sequenceName);
        if (sequence == null) {
            if (!ifExists) {
                throw DbException.get(ErrorCode.SEQUENCE_NOT_FOUND_1, sequenceName);
            }
        } else {
            if (sequence.getBelongsToTable()) {
                throw DbException.get(ErrorCode.SEQUENCE_BELONGS_TO_A_TABLE_1, sequenceName);
            }
            getDatabase().removeSchemaObject(session, sequence);
        }
        return 0;
    }

    @Override
    public int getType() {
        return CommandInterface.DROP_SEQUENCE;
    }

}
