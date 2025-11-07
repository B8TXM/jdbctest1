package jm.task.core.jdbc.util;

import java.sql.*;


public class Util {

    public static final String DRIVER = "com.mysql.cj.jdbc.Driver";
    public static final String URL = "jdbc:mysql://localhost:3306/jdbctest";
    public static final String USER = "root";
    public static final String PASSWORD = "root";

    private Util() {

    }

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Class.forName(DRIVER);
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
}
    /*public static Connection getConnection(){
        Connection conn = null;
        try {
            Class.forName(DRIVER);
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            System.out.println("Connected to database successfully");
        } catch (ClassNotFoundException | SQLException e) {
            System.out.println("Connection Failed! Check output console");
        }
        return conn;
    }*/

