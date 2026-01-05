package com.arcdb.jdbc

import com.google.gson.JsonArray
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import java.io.InputStream
import java.io.Reader
import java.math.BigDecimal
import java.net.URL
import java.sql.*
import java.util.Calendar

class ArcResultSet(
    val statement: ArcStatement,
    json: JsonObject
) : ResultSet {
    private val columns: List<String>
    private val rows: JsonArray
    private var currentRowIndex = -1
    private var currentRow: JsonArray? = null
    var _isClosed = false

    init {
        val colsJson = json.getAsJsonArray("columns")
        columns = List(colsJson.size()) { i -> colsJson.get(i).asString }
        rows = json.getAsJsonArray("rows")
    }

    override fun next(): Boolean {
        currentRowIndex++
        if (currentRowIndex < rows.size()) {
            val rowElement = rows.get(currentRowIndex)
            currentRow = if (rowElement.isJsonArray) {
                 rowElement.asJsonArray
            } else if (rowElement.isJsonObject && rowElement.asJsonObject.has("values")) {
                 rowElement.asJsonObject.get("values").asJsonArray
            } else {
                 throw SQLException("Row format unknown: $rowElement")
            }
            return true
        }
        currentRow = null
        return false
    }

    private fun getJsonValue(columnIndex: Int): JsonElement {
        if (currentRow == null) throw SQLException("No current row")
        if (columnIndex < 1 || columnIndex > columns.size) throw SQLException("Column index out of bounds")
        return currentRow!!.get(columnIndex - 1)
    }

    private fun unwrap(v: JsonElement): JsonElement {
        if (v.isJsonObject && v.asJsonObject.size() == 1) {
            val entry = v.asJsonObject.entrySet().first()
            return entry.value
        }
        return v
    }

    override fun getString(columnIndex: Int): String? {
        val v = unwrap(getJsonValue(columnIndex))
        if (v.isJsonNull) return null
        return if (v.isJsonPrimitive) v.asString else v.toString()
    }

    override fun getInt(columnIndex: Int): Int {
        val v = unwrap(getJsonValue(columnIndex))
        if (v.isJsonNull) return 0
        return v.asInt
    }
    
    override fun getLong(columnIndex: Int): Long {
         val v = unwrap(getJsonValue(columnIndex))
         if (v.isJsonNull) return 0L
         return v.asLong
    }

    override fun getDouble(columnIndex: Int): Double {
        val v = unwrap(getJsonValue(columnIndex))
        if (v.isJsonNull) return 0.0
        return v.asDouble
    }

    override fun getBoolean(columnIndex: Int): Boolean {
        val v = unwrap(getJsonValue(columnIndex))
        if (v.isJsonNull) return false
        return v.asBoolean
    }

    override fun getByte(columnIndex: Int): Byte {
        val v = unwrap(getJsonValue(columnIndex))
        if (v.isJsonNull) return 0
        return v.asByte
    }

    override fun getShort(columnIndex: Int): Short {
        val v = unwrap(getJsonValue(columnIndex))
        if (v.isJsonNull) return 0
        return v.asShort
    }

    override fun getFloat(columnIndex: Int): Float {
        val v = unwrap(getJsonValue(columnIndex))
        if (v.isJsonNull) return 0.0f
        return v.asFloat
    }

    override fun getString(columnLabel: String): String? = getString(findColumn(columnLabel))
    override fun getInt(columnLabel: String): Int = getInt(findColumn(columnLabel))
    override fun getLong(columnLabel: String): Long = getLong(findColumn(columnLabel))
    override fun getDouble(columnLabel: String): Double = getDouble(findColumn(columnLabel))
    override fun getBoolean(columnLabel: String): Boolean = getBoolean(findColumn(columnLabel))
    override fun getByte(columnLabel: String): Byte = getByte(findColumn(columnLabel))
    override fun getShort(columnLabel: String): Short = getShort(findColumn(columnLabel))
    override fun getFloat(columnLabel: String): Float = getFloat(findColumn(columnLabel))

    override fun findColumn(columnLabel: String): Int {
        val idx = columns.indexOf(columnLabel)
        if (idx == -1) throw SQLException("Column not found: $columnLabel")
        return idx + 1
    }

    override fun close() { _isClosed = true }
    override fun isClosed() = _isClosed

    // Metadata
    override fun getMetaData(): ResultSetMetaData = throw SQLFeatureNotSupportedException()

    // Navigation (Stubbed)
    override fun isBeforeFirst(): Boolean = currentRowIndex == -1
    override fun isAfterLast(): Boolean = currentRowIndex >= rows.size()
    override fun isFirst(): Boolean = currentRowIndex == 0
    override fun isLast(): Boolean = currentRowIndex == rows.size() - 1
    override fun beforeFirst() { currentRowIndex = -1; currentRow = null }
    override fun afterLast() { currentRowIndex = rows.size(); currentRow = null }
    override fun first(): Boolean = throw SQLFeatureNotSupportedException()
    override fun last(): Boolean = throw SQLFeatureNotSupportedException()
    override fun getRow(): Int = currentRowIndex + 1
    override fun absolute(row: Int): Boolean = throw SQLFeatureNotSupportedException()
    override fun relative(rows: Int): Boolean = throw SQLFeatureNotSupportedException()
    override fun previous(): Boolean = throw SQLFeatureNotSupportedException()

    // Stubbed Getters - Fixed Signatures
    override fun getObject(columnIndex: Int): Any? = getString(columnIndex)
    override fun getObject(columnLabel: String): Any? = getObject(findColumn(columnLabel))
    override fun getObject(columnIndex: Int, map: Map<String, Class<*>>): Any? = throw SQLFeatureNotSupportedException()
    override fun getObject(columnLabel: String, map: Map<String, Class<*>>): Any? = throw SQLFeatureNotSupportedException()
    override fun <T : Any?> getObject(columnIndex: Int, type: Class<T>): T = throw SQLFeatureNotSupportedException()
    override fun <T : Any?> getObject(columnLabel: String, type: Class<T>): T = throw SQLFeatureNotSupportedException()

    override fun getBigDecimal(columnIndex: Int, scale: Int): BigDecimal = throw SQLFeatureNotSupportedException()
    override fun getBigDecimal(columnLabel: String, scale: Int): BigDecimal = throw SQLFeatureNotSupportedException()
    override fun getBigDecimal(columnIndex: Int): BigDecimal = throw SQLFeatureNotSupportedException()
    override fun getBigDecimal(columnLabel: String): BigDecimal = throw SQLFeatureNotSupportedException()
    override fun getBytes(columnIndex: Int): ByteArray = throw SQLFeatureNotSupportedException()
    override fun getBytes(columnLabel: String): ByteArray = throw SQLFeatureNotSupportedException()
    override fun getDate(columnIndex: Int): Date = throw SQLFeatureNotSupportedException()
    override fun getDate(columnLabel: String): Date = throw SQLFeatureNotSupportedException()
    override fun getDate(columnIndex: Int, cal: Calendar?): Date = throw SQLFeatureNotSupportedException()
    override fun getDate(columnLabel: String, cal: Calendar?): Date = throw SQLFeatureNotSupportedException()
    override fun getTime(columnIndex: Int): Time = throw SQLFeatureNotSupportedException()
    override fun getTime(columnLabel: String): Time = throw SQLFeatureNotSupportedException()
    override fun getTime(columnIndex: Int, cal: Calendar?): Time = throw SQLFeatureNotSupportedException()
    override fun getTime(columnLabel: String, cal: Calendar?): Time = throw SQLFeatureNotSupportedException()
    override fun getTimestamp(columnIndex: Int): Timestamp = throw SQLFeatureNotSupportedException()
    override fun getTimestamp(columnLabel: String): Timestamp = throw SQLFeatureNotSupportedException()
    override fun getTimestamp(columnIndex: Int, cal: Calendar?): Timestamp = throw SQLFeatureNotSupportedException()
    override fun getTimestamp(columnLabel: String, cal: Calendar?): Timestamp = throw SQLFeatureNotSupportedException()
    
    // Streams
    override fun getAsciiStream(columnIndex: Int): InputStream = throw SQLFeatureNotSupportedException()
    override fun getAsciiStream(columnLabel: String): InputStream = throw SQLFeatureNotSupportedException()
    override fun getUnicodeStream(columnIndex: Int): InputStream = throw SQLFeatureNotSupportedException()
    override fun getUnicodeStream(columnLabel: String): InputStream = throw SQLFeatureNotSupportedException()
    override fun getBinaryStream(columnIndex: Int): InputStream = throw SQLFeatureNotSupportedException()
    override fun getBinaryStream(columnLabel: String): InputStream = throw SQLFeatureNotSupportedException()
    override fun getCharacterStream(columnIndex: Int): Reader = throw SQLFeatureNotSupportedException()
    override fun getCharacterStream(columnLabel: String): Reader = throw SQLFeatureNotSupportedException()
    
    override fun getRef(columnIndex: Int): Ref = throw SQLFeatureNotSupportedException()
    override fun getRef(columnLabel: String): Ref = throw SQLFeatureNotSupportedException()
    override fun getBlob(columnIndex: Int): Blob = throw SQLFeatureNotSupportedException()
    override fun getBlob(columnLabel: String): Blob = throw SQLFeatureNotSupportedException()
    override fun getClob(columnIndex: Int): Clob = throw SQLFeatureNotSupportedException()
    override fun getClob(columnLabel: String): Clob = throw SQLFeatureNotSupportedException()
    override fun getArray(columnIndex: Int): java.sql.Array = throw SQLFeatureNotSupportedException()
    override fun getArray(columnLabel: String): java.sql.Array = throw SQLFeatureNotSupportedException()
    override fun getURL(columnIndex: Int): URL = throw SQLFeatureNotSupportedException()
    override fun getURL(columnLabel: String): URL = throw SQLFeatureNotSupportedException()
    override fun getNClob(columnIndex: Int): NClob = throw SQLFeatureNotSupportedException()
    override fun getNClob(columnLabel: String): NClob = throw SQLFeatureNotSupportedException()
    override fun getSQLXML(columnIndex: Int): SQLXML = throw SQLFeatureNotSupportedException()
    override fun getSQLXML(columnLabel: String): SQLXML = throw SQLFeatureNotSupportedException()
    override fun getNString(columnIndex: Int): String = throw SQLFeatureNotSupportedException()
    override fun getNString(columnLabel: String): String = throw SQLFeatureNotSupportedException()
    override fun getNCharacterStream(columnIndex: Int): Reader = throw SQLFeatureNotSupportedException()
    override fun getNCharacterStream(columnLabel: String): Reader = throw SQLFeatureNotSupportedException()
    override fun getRowId(columnIndex: Int): RowId = throw SQLFeatureNotSupportedException()
    override fun getRowId(columnLabel: String): RowId = throw SQLFeatureNotSupportedException()

    // Updates (Stubbed) - Fixed Long signatures
    override fun rowUpdated(): Boolean = false
    override fun rowInserted(): Boolean = false
    override fun rowDeleted(): Boolean = false
    override fun insertRow() {}
    override fun updateRow() {}
    override fun deleteRow() {}
    override fun refreshRow() {}
    override fun cancelRowUpdates() {}
    override fun moveToInsertRow() {}
    override fun moveToCurrentRow() {}

    override fun updateNull(columnIndex: Int) {}
    override fun updateBoolean(columnIndex: Int, x: Boolean) {}
    override fun updateByte(columnIndex: Int, x: Byte) {}
    override fun updateShort(columnIndex: Int, x: Short) {}
    override fun updateInt(columnIndex: Int, x: Int) {}
    override fun updateLong(columnIndex: Int, x: Long) {}
    override fun updateFloat(columnIndex: Int, x: Float) {}
    override fun updateDouble(columnIndex: Int, x: Double) {}
    override fun updateBigDecimal(columnIndex: Int, x: BigDecimal?) {}
    override fun updateString(columnIndex: Int, x: String?) {}
    override fun updateBytes(columnIndex: Int, x: ByteArray?) {}
    override fun updateDate(columnIndex: Int, x: Date?) {}
    override fun updateTime(columnIndex: Int, x: Time?) {}
    override fun updateTimestamp(columnIndex: Int, x: Timestamp?) {}
    
    // update streams with Long length
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Int) {}
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?, length: Long) {}
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Int) {}
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?, length: Long) {}
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Int) {}
    override fun updateCharacterStream(columnIndex: Int, x: Reader?, length: Long) {}
    
    override fun updateObject(columnIndex: Int, x: Any?, scaleOrLength: Int) {}
    override fun updateObject(columnIndex: Int, x: Any?) {}
    override fun updateNull(columnLabel: String) {}
    override fun updateBoolean(columnLabel: String, x: Boolean) {}
    override fun updateByte(columnLabel: String, x: Byte) {}
    override fun updateShort(columnLabel: String, x: Short) {}
    override fun updateInt(columnLabel: String, x: Int) {}
    override fun updateLong(columnLabel: String, x: Long) {}
    override fun updateFloat(columnLabel: String, x: Float) {}
    override fun updateDouble(columnLabel: String, x: Double) {}
    override fun updateBigDecimal(columnLabel: String, x: BigDecimal?) {}
    override fun updateString(columnLabel: String, x: String?) {}
    override fun updateBytes(columnLabel: String, x: ByteArray?) {}
    override fun updateDate(columnLabel: String, x: Date?) {}
    override fun updateTime(columnLabel: String, x: Time?) {}
    override fun updateTimestamp(columnLabel: String, x: Timestamp?) {}
    
    // update streams with Long length for columnLabel
    override fun updateAsciiStream(columnLabel: String, x: InputStream?, length: Int) {}
    override fun updateAsciiStream(columnLabel: String, x: InputStream?, length: Long) {}
    override fun updateBinaryStream(columnLabel: String, x: InputStream?, length: Int) {}
    override fun updateBinaryStream(columnLabel: String, x: InputStream?, length: Long) {}
    override fun updateCharacterStream(columnLabel: String, x: Reader?, length: Int) {}
    override fun updateCharacterStream(columnLabel: String, x: Reader?, length: Long) {}

    override fun updateObject(columnLabel: String, x: Any?, scaleOrLength: Int) {}
    override fun updateObject(columnLabel: String, x: Any?) {}
    
    override fun updateNString(columnIndex: Int, nString: String?) {}
    override fun updateNString(columnLabel: String, nString: String?) {}
    override fun updateNClob(columnIndex: Int, nClob: NClob?) {}
    override fun updateNClob(columnLabel: String, nClob: NClob?) {}
    override fun updateNClob(columnIndex: Int, reader: Reader?, length: Long) {}
    override fun updateNClob(columnLabel: String, reader: Reader?, length: Long) {}
    override fun updateNClob(columnIndex: Int, reader: Reader?) {}
    override fun updateNClob(columnLabel: String, reader: Reader?) {}
    
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?, length: Long) {}
    override fun updateNCharacterStream(columnLabel: String, reader: Reader?, length: Long) {}
    override fun updateNCharacterStream(columnIndex: Int, x: Reader?) {}
    override fun updateNCharacterStream(columnLabel: String, reader: Reader?) {}
    override fun updateAsciiStream(columnIndex: Int, x: InputStream?) {}
    override fun updateBinaryStream(columnIndex: Int, x: InputStream?) {}
    override fun updateCharacterStream(columnIndex: Int, x: Reader?) {}
    override fun updateAsciiStream(columnLabel: String, x: InputStream?) {}
    override fun updateBinaryStream(columnLabel: String, x: InputStream?) {}
    override fun updateCharacterStream(columnLabel: String, x: Reader?) {}
    override fun updateBlob(columnIndex: Int, x: Blob?) {}
    override fun updateBlob(columnLabel: String, x: Blob?) {}
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?, length: Long) {}
    override fun updateBlob(columnLabel: String, inputStream: InputStream?, length: Long) {}
    override fun updateBlob(columnIndex: Int, inputStream: InputStream?) {}
    override fun updateBlob(columnLabel: String, inputStream: InputStream?) {}
    override fun updateClob(columnIndex: Int, x: Clob?) {}
    override fun updateClob(columnLabel: String, x: Clob?) {}
    override fun updateClob(columnIndex: Int, reader: Reader?, length: Long) {}
    override fun updateClob(columnLabel: String, reader: Reader?, length: Long) {}
    override fun updateClob(columnIndex: Int, reader: Reader?) {}
    override fun updateClob(columnLabel: String, reader: Reader?) {}
    override fun updateSQLXML(columnIndex: Int, xmlObject: SQLXML?) {}
    override fun updateSQLXML(columnLabel: String, xmlObject: SQLXML?) {}
    override fun updateRef(columnIndex: Int, x: Ref?) {}
    override fun updateRef(columnLabel: String, x: Ref?) {}
    override fun updateArray(columnIndex: Int, x: java.sql.Array?) {}
    override fun updateArray(columnLabel: String, x: java.sql.Array?) {}
    override fun updateRowId(columnIndex: Int, x: RowId?) {}
    override fun updateRowId(columnLabel: String, x: RowId?) {}

    override fun wasNull(): Boolean = currentRow?.get(currentRowIndex)?.isJsonNull ?: false
    override fun getStatement(): Statement = statement
    override fun getWarnings(): SQLWarning? = null
    override fun clearWarnings() {}
    override fun getCursorName(): String = ""
    override fun getHoldability(): Int = ResultSet.HOLD_CURSORS_OVER_COMMIT
    override fun setFetchDirection(direction: Int) {}
    override fun getFetchDirection(): Int = ResultSet.FETCH_FORWARD
    override fun setFetchSize(rows: Int) {}
    override fun getFetchSize(): Int = 0
    override fun getType(): Int = ResultSet.TYPE_FORWARD_ONLY
    override fun getConcurrency(): Int = ResultSet.CONCUR_READ_ONLY

    override fun <T : Any?> unwrap(iface: Class<T>?): T = throw SQLException("Not implemented")
    override fun isWrapperFor(iface: Class<*>?): Boolean = false
}
