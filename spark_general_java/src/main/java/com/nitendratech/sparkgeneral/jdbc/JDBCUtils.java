package com.nitendratech.sparkgeneral.jdbc;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;



/*
 * Created by @author nitendratech on 5/24/20
 */

/**
 * Uses the java.sql native Connection
 */
public class JDBCUtils {

    /**
     * "jdbc:oracle:thin:username/password@//hostname:portnumber/service_name"
     */
    private static String jdbcUrl ="jdbc:oracle:thin:ot/admin123@//localhost:1521/xe";
    private static String user ="ot";
    private static String pass="admin123";



    static {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");


        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.out.println("Failed to Load the Database Driver");

        }
    }


    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(jdbcUrl,user,pass);
    }

    public static void closeConnections(Statement stmt, Connection conn){

        try {
            if (stmt!= null) stmt.close();
        } catch(SQLException e){
            e.printStackTrace();
        } finally{
            try {
                if(conn !=null) conn.close();
            } catch(SQLException e){
                e.printStackTrace();
            }
        }
    }

    public static void main(String args[]) throws SQLException {

        // Testing the code

        Connection connection = JDBCUtils.getConnection();

        System.out.println(connection.isReadOnly());

        System.out.println(connection.getSchema());

    }

}