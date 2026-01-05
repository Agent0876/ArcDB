package com.arcdb.jdbc

import java.sql.*
import java.util.Properties
import java.util.logging.Logger

class ArcDriver : Driver {
    companion object {
        init {
            try {
                DriverManager.registerDriver(ArcDriver())
            } catch (e: SQLException) {
                e.printStackTrace()
            }
        }
    }

    override fun connect(url: String, info: Properties?): Connection? {
        if (!acceptsURL(url)) return null
        
        // Parsing logic: jdbc:arcdb://host:port/db
        val cleanUrl = url.removePrefix("jdbc:arcdb://")
        val parts = cleanUrl.split("/", limit = 2)
        val hostPort = parts[0]
        val database = if (parts.size > 1) parts[1] else ""
        
        val hp = hostPort.split(":")
        val host = hp[0]
        val port = if (hp.size > 1) hp[1].toInt() else 7171

        return ArcConnection(host, port, database)
    }

    override fun acceptsURL(url: String): Boolean {
        return url.startsWith("jdbc:arcdb://")
    }

    override fun getPropertyInfo(url: String?, info: Properties?): Array<DriverPropertyInfo> {
        return emptyArray()
    }

    override fun getMajorVersion(): Int = 1
    override fun getMinorVersion(): Int = 0
    override fun jdbcCompliant(): Boolean = false
    override fun getParentLogger(): Logger = Logger.getLogger("com.arcdb.jdbc")
}
