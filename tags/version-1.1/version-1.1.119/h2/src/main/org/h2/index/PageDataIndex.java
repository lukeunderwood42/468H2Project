/*
 * Copyright 2004-2009 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import org.h2.engine.Constants;
import org.h2.engine.Session;
import org.h2.log.UndoLogRecord;
import org.h2.message.Message;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.PageStore;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableData;
import org.h2.util.MathUtils;
import org.h2.util.New;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;

/**
 * The scan index allows to access a row by key. It can be used to iterate over
 * all rows of a table. Each regular table has one such object, even if no
 * primary key or indexes are defined.
 */
public class PageDataIndex extends PageIndex implements RowIndex {

    private PageStore store;
    private TableData tableData;
    private long lastKey;
    private long rowCount;
    private HashSet<Row> delta;
    private int rowCountDiff;
    private HashMap<Integer, Integer> sessionRowCount;
    private int mainIndexColumn = -1;
    private SQLException fastDuplicateKeyException;
    private int memorySizePerPage;

    public PageDataIndex(TableData table, int id, IndexColumn[] columns, IndexType indexType, int headPos, Session session) throws SQLException {
        initBaseIndex(table, id, table.getName() + "_TABLE_SCAN", columns, indexType);
        // trace.setLevel(TraceSystem.DEBUG);
        if (database.isMultiVersion()) {
            sessionRowCount = New.hashMap();
            isMultiVersion = true;
        }
        tableData = table;
        this.store = database.getPageStore();
        store.addIndex(this);
        if (!database.isPersistent()) {
            throw Message.throwInternalError(table.getName());
        }
        if (headPos == Index.EMPTY_HEAD) {
            // new table
            rootPageId = store.allocatePage();
            store.addMeta(this, session);
            PageDataLeaf root = PageDataLeaf.create(this, rootPageId, PageData.ROOT);
            store.updateRecord(root, true, root.data);
        } else {
            rootPageId = store.getRootPageId(id);
            PageData root = getPage(rootPageId, 0);
            lastKey = root.getLastKey();
            rowCount = root.getRowCount();
            // could have been created before, but never committed
            if (!database.isReadOnly()) {
                // TODO check if really required
                store.updateRecord(root, false, null);
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("opened " + getName() + " rows:" + rowCount);
        }
        table.setRowCount(rowCount);
        fastDuplicateKeyException = super.getDuplicateKeyException();
        // estimate the memory usage as follows:
        // the less column, the more memory is required,
        // because the more rows fit on a page
        memorySizePerPage = store.getPageSize();
        int estimatedRowsPerPage =  store.getPageSize() / ((1 + columns.length) * 8);
        memorySizePerPage += estimatedRowsPerPage * 64;
    }

    public SQLException getDuplicateKeyException() {
        return fastDuplicateKeyException;
    }

    public void add(Session session, Row row) throws SQLException {
        boolean retry = false;
        if (mainIndexColumn != -1) {
            row.setKey(row.getValue(mainIndexColumn).getLong());
        } else {
            if (row.getKey() == 0) {
                row.setKey((int) ++lastKey);
                retry = true;
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("add table:" + table.getId() + " " + row);
        }
        if (tableData.getContainsLargeObject()) {
            for (int i = 0; i < row.getColumnCount(); i++) {
                Value v = row.getValue(i);
                Value v2 = v.link(database, getId());
                if (v2.isLinked()) {
                    session.unlinkAtCommitStop(v2);
                }
                if (v != v2) {
                    row.setValue(i, v2);
                }
            }
        }
        // when using auto-generated values, it's possible that multiple
        // tries are required (specially if there was originally a primary key)
        long add = 0;
        while (true) {
            try {
                addTry(session, row);
                break;
            } catch (SQLException e) {
                if (e != fastDuplicateKeyException) {
                    throw e;
                }
                if (!retry) {
                    throw super.getDuplicateKeyException();
                }
                if (add == 0) {
                    // in the first re-try add a small random number,
                    // to avoid collisions after a re-start
                    row.setKey((long) (row.getKey() + Math.random() * 10000));
                } else {
                    row.setKey(row.getKey() + add);
                }
                add++;
            }
        }
        lastKey = Math.max(lastKey, row.getKey() + 1);
    }

    private void addTry(Session session, Row row) throws SQLException {
        while (true) {
            PageData root = getPage(rootPageId, 0);
            int splitPoint = root.addRowTry(row);
            if (splitPoint == -1) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("split " + splitPoint);
            }
            long pivot = splitPoint == 0 ? row.getKey() : root.getKey(splitPoint - 1);
            PageData page1 = root;
            PageData page2 = root.split(splitPoint);
            int rootPageId = root.getPos();
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(rootPageId);
            page2.setParentPageId(rootPageId);
            PageDataNode newRoot = PageDataNode.create(this, rootPageId, PageData.ROOT);
            newRoot.init(page1, pivot, page2);
            store.updateRecord(page1, true, page1.data);
            store.updateRecord(page2, true, page2.data);
            store.updateRecord(newRoot, true, null);
            root = newRoot;
        }
        row.setDeleted(false);
        if (database.isMultiVersion()) {
            if (delta == null) {
                delta = New.hashSet();
            }
            boolean wasDeleted = delta.remove(row);
            if (!wasDeleted) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), 1);
        }
        invalidateRowCount();
        rowCount++;
        store.logAddOrRemoveRow(session, tableData.getId(), row, true);
    }

    /**
     * Read an overflow page page.
     *
     * @param id the page id
     * @return the page
     */
    PageDataOverflow getPageOverflow(int id) throws SQLException {
        return (PageDataOverflow) store.getPage(id);
    }

    /**
     * Read the given page.
     *
     * @param id the page id
     * @param parent the parent, or -1 if unknown
     * @return the page
     */
    PageData getPage(int id, int parent) throws SQLException {
        PageData p = (PageData) store.getPage(id);
        if (p == null) {
            PageDataLeaf empty = PageDataLeaf.create(this, id, parent);
            return empty;
        }
        if (p.index.rootPageId != rootPageId) {
            throw Message.throwInternalError("Wrong index: " + p.index.getName() + ":" + p.index.rootPageId + " " + getName() + ":" + rootPageId);
        }
        if (parent != -1) {
            if (p.getParentPageId() != parent) {
                throw Message.throwInternalError(p + " parent " + p.getParentPageId() + " expected " + parent);
            }
        }
        return p;
    }

    public boolean canGetFirstOrLast() {
        return false;
    }

    /**
     * Get the key from the row
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @return the key
     */
    long getLong(SearchRow row, long ifEmpty) throws SQLException {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null || v == ValueNull.INSTANCE) {
            return ifEmpty;
        }
        return v.getLong();
    }

    public Cursor find(Session session, SearchRow first, SearchRow last) throws SQLException {
        // ignore first and last
        PageData root = getPage(rootPageId, 0);
        return root.find(session, Long.MIN_VALUE, Long.MAX_VALUE, isMultiVersion);
    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @param multiVersion if mvcc should be used
     * @return the cursor
     */
    Cursor find(Session session, long first, long last, boolean multiVersion) throws SQLException {
        PageData root = getPage(rootPageId, 0);
        return root.find(session, first, last, multiVersion);
    }

    public Cursor findFirstOrLast(Session session, boolean first) {
        throw Message.throwInternalError();
    }

    long getLastKey() throws SQLException {
        PageData root = getPage(rootPageId, 0);
        return root.getLastKey();
    }

    public double getCost(Session session, int[] masks) {
        long cost = 10 * (tableData.getRowCountApproximation() + Constants.COST_ROW_OFFSET);
        return cost;
    }

    public boolean needRebuild() {
        return false;
    }

    public void remove(Session session, Row row) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("remove " + row.getKey());
        }
        if (tableData.getContainsLargeObject()) {
            for (int i = 0; i < row.getColumnCount(); i++) {
                Value v = row.getValue(i);
                if (v.isLinked()) {
                    session.unlinkAtCommit((ValueLob) v);
                }
            }
        }
        if (rowCount == 1) {
            removeAllRows();
        } else {
            long key = row.getKey();
            PageData root = getPage(rootPageId, 0);
            root.remove(key);
            invalidateRowCount();
            rowCount--;
        }
        if (database.isMultiVersion()) {
            // if storage is null, the delete flag is not yet set
            row.setDeleted(true);
            if (delta == null) {
                delta = New.hashSet();
            }
            boolean wasAdded = delta.remove(row);
            if (!wasAdded) {
                delta.add(row);
            }
            incrementRowCount(session.getId(), -1);
        }
        store.logAddOrRemoveRow(session, tableData.getId(), row, false);
    }

    private void invalidateRowCount() throws SQLException {
        PageData root = getPage(rootPageId, 0);
        root.setRowCountStored(PageData.UNKNOWN_ROWCOUNT);
    }

    public void remove(Session session) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("remove");
        }
        removeAllRows();
        store.freePage(rootPageId, false, null);
        store.removeMeta(this, session);
    }

    public void truncate(Session session) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("truncate");
        }
        store.logTruncate(session, tableData.getId());
        removeAllRows();
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            ValueLob.removeAllForTable(database, table.getId());
        }
        if (database.isMultiVersion()) {
            sessionRowCount.clear();
        }
        tableData.setRowCount(0);
    }

    private void removeAllRows() throws SQLException {
        PageData root = getPage(rootPageId, 0);
        root.freeChildren();
        root = PageDataLeaf.create(this, rootPageId, PageData.ROOT);
        store.removeRecord(rootPageId);
        store.updateRecord(root, true, null);
        rowCount = 0;
        lastKey = 0;
    }

    public void checkRename() throws SQLException {
        throw Message.getUnsupportedException("PAGE");
    }

    public Row getRow(Session session, long key) throws SQLException {
        return getRow(key);
    }

    /**
     * Get the row with the given key.
     *
     * @param key the key
     * @return the row
     */
    public Row getRow(long key) throws SQLException {
        PageData root = getPage(rootPageId, 0);
        return root.getRow(key);
    }

    PageStore getPageStore() {
        return store;
    }

    /**
     * Read a row from the data page at the given position.
     *
     * @param data the data page
     * @param columnCount the number of columns
     * @return the row
     */
    Row readRow(Data data, int columnCount) throws SQLException {
        Value[] values = new Value[columnCount];
        for (int i = 0; i < columnCount; i++) {
            values[i] = data.readValue();
        }
        return tableData.createRow(values);
    }

    public long getRowCountApproximation() {
        return rowCount;
    }

    public long getRowCount(Session session) {
        if (database.isMultiVersion()) {
            Integer i = sessionRowCount.get(session.getId());
            long count = i == null ? 0 : i.intValue();
            count += rowCount;
            count -= rowCountDiff;
            return count;
        }
        return rowCount;
    }

    public String getCreateSQL() {
        return null;
    }

    public int getColumnIndex(Column col) {
        if (col.getColumnId() == mainIndexColumn) {
            return 0;
        }
        return -1;
    }

    public void close(Session session) throws SQLException {
        if (trace.isDebugEnabled()) {
            trace.debug("close");
        }
        if (delta != null) {
            delta.clear();
        }
        rowCountDiff = 0;
        if (sessionRowCount != null) {
            sessionRowCount.clear();
        }
        // can not close the index because it might get used afterwards,
        // for example after running recovery
        PageData root = getPage(rootPageId, 0);
        root.setRowCountStored(MathUtils.convertLongToInt(rowCount));
    }

    Iterator<Row> getDelta() {
        if (delta == null) {
            List<Row> e = Collections.emptyList();
            return e.iterator();
        }
        return delta.iterator();
    }

    private void incrementRowCount(int sessionId, int count) {
        if (database.isMultiVersion()) {
            Integer id = sessionId;
            Integer c = sessionRowCount.get(id);
            int current = c == null ? 0 : c.intValue();
            sessionRowCount.put(id, current + count);
            rowCountDiff += count;
        }
    }

    public void commit(int operation, Row row) {
        if (database.isMultiVersion()) {
            if (delta != null) {
                delta.remove(row);
            }
            incrementRowCount(row.getSessionId(), operation == UndoLogRecord.DELETE ? 1 : -1);
        }
    }

    /**
     * The root page has changed.
     *
     * @param session the session
     * @param newPos the new position
     */
    void setRootPageId(Session session, int newPos) throws SQLException {
        store.removeMeta(this, session);
        this.rootPageId = newPos;
        store.addMeta(this, session);
        store.addIndex(this);
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    int getMemorySizePerPage() {
        return memorySizePerPage;
    }

}