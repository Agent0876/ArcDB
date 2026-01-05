package com.arcdb.jdbc

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.sql.DriverManager

class JdbcTest {
    @Test
    fun testEndToEnd() {
        // Register driver
        Class.forName("com.arcdb.jdbc.ArcDriver")

        // Connect
        val conn = DriverManager.getConnection("jdbc:arcdb://localhost:7171/testdb")
        assertNotNull(conn)

        val stmt = conn.createStatement()

        // Create Table
        stmt.execute("CREATE TABLE IF NOT EXISTS jdbc_users (id INT, name VARCHAR(50));")
        
        // Insert
        val count = stmt.executeUpdate("INSERT INTO jdbc_users VALUES (1, 'Kotlin'), (2, 'Java');")
        assertTrue(count >= 2)

        // Select
        val rs = stmt.executeQuery("SELECT * FROM jdbc_users WHERE id = 1;")
        assertTrue(rs.next())
        assertEquals(1, rs.getInt("id"))
        assertEquals("Kotlin", rs.getString("name"))
        assertFalse(rs.next())

        // Clean up
        stmt.execute("DROP TABLE jdbc_users;")
        conn.close()
    }
}
