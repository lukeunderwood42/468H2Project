/*
 * Copyright 2004-2013 H2 Group. Multiple-Licensed under the H2 License,
 * Version 1.0, and under the Eclipse Public License, Version 1.0
 * (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.android;

import org.h2.result.ResultInterface;
import android.content.ContentResolver;
import android.database.AbstractWindowedCursor;
import android.database.CharArrayBuffer;
import android.database.ContentObserver;
import android.database.CursorWindow;
import android.database.DataSetObserver;
import android.net.Uri;
import android.os.Bundle;

/**
 * A cursor implementation.
 */
public class H2Cursor extends AbstractWindowedCursor {

    private H2Database database;
    private ResultInterface result;

    H2Cursor(H2Database db, H2CursorDriver driver, String editTable,
            H2Query query) {
        this.database = db;
        // TODO
    }

    H2Cursor(ResultInterface result) {
        this.result = result;
    }

    public void close() {
        result.close();
    }

    public void deactivate() {
        // TODO
    }

    public int getColumnIndex(String columnName) {
        return 0;
    }

    public String[] getColumnNames() {
        return null;
    }

    public int getCount() {
        return result.getRowCount();
    }

    /**
     * Get the database that created this cursor.
     *
     * @return the database
     */
    public H2Database getDatabase() {
        return database;
    }

    /**
     * The cursor moved to a new position.
     *
     * @param oldPosition the previous position
     * @param newPosition the new position
     * @return TODO
     */
    public boolean onMove(int oldPosition, int newPosition) {
        return false;
    }

    public void registerDataSetObserver(DataSetObserver observer) {
        // TODO
    }

    public boolean requery() {
        return false;
    }

    /**
     * Set the parameter values.
     *
     * @param selectionArgs the parameter values
     */
    public void setSelectionArguments(String[] selectionArgs) {
        // TODO
    }

    /**
     * TODO
     *
     * @param window the window
     */
    public void setWindow(CursorWindow window) {
        // TODO
    }

    public boolean move(int offset) {
        if (offset == 1) {
            return result.next();
        }
        throw H2Database.unsupported();
    }

    public void copyStringToBuffer(int columnIndex, CharArrayBuffer buffer) {
        // TODO

    }

    public byte[] getBlob(int columnIndex) {
        // TODO
        return null;
    }

    public int getColumnCount() {
        // TODO
        return 0;
    }

    public int getColumnIndexOrThrow(String columnName) {
        // TODO
        return 0;
    }

    public String getColumnName(int columnIndex) {
        // TODO
        return null;
    }

    public double getDouble(int columnIndex) {
        // TODO
        return 0;
    }

    public Bundle getExtras() {
        // TODO
        return null;
    }

    public float getFloat(int columnIndex) {
        // TODO
        return 0;
    }

    public int getInt(int columnIndex) {
        return result.currentRow()[columnIndex].getInt();
    }

    public long getLong(int columnIndex) {
        return result.currentRow()[columnIndex].getLong();
    }

    public int getPosition() {
        // TODO
        return 0;
    }

    public short getShort(int columnIndex) {
        // TODO
        return 0;
    }

    public String getString(int columnIndex) {
        return result.currentRow()[columnIndex].getString();
    }

    public boolean getWantsAllOnMoveCalls() {
        // TODO
        return false;
    }

    public boolean isAfterLast() {
        // TODO
        return false;
    }

    public boolean isBeforeFirst() {
        // TODO
        return false;
    }

    public boolean isClosed() {
        // TODO
        return false;
    }

    public boolean isFirst() {
        // TODO
        return false;
    }

    public boolean isLast() {
        // TODO
        return false;
    }

    public boolean isNull(int columnIndex) {
        // TODO
        return false;
    }

    public boolean moveToFirst() {
        // TODO
        return false;
    }

    public boolean moveToLast() {
        // TODO
        return false;
    }

    public boolean moveToNext() {
        // TODO
        return false;
    }

    public boolean moveToPosition(int position) {
        // TODO
        return false;
    }

    public boolean moveToPrevious() {
        // TODO
        return false;
    }

    public void registerContentObserver(ContentObserver observer) {
        // TODO

    }

    public Bundle respond(Bundle extras) {
        // TODO
        return null;
    }

    public void setNotificationUri(ContentResolver cr, Uri uri) {
        // TODO

    }

    public void unregisterContentObserver(ContentObserver observer) {
        // TODO

    }

    public void unregisterDataSetObserver(DataSetObserver observer) {
        // TODO

    }

}
