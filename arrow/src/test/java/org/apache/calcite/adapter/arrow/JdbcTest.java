package org.apache.calcite.adapter.arrow;

import java.sql.*;

/**
 * Created by masayuki on 2017/12/26.
 */
public class JdbcTest {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Class.forName("org.apache.calcite.jdbc.Driver");
        try(Connection conn = DriverManager.getConnection("jdbc:calcite:model=arrow/target/classes/samples/model.json", "admin", "admin")) {
            PreparedStatement pstmt = conn.prepareStatement("select N_NATIONKEY, N_NAME from NATIONSSF WHERE N_REGIONKEY=?");
            pstmt.setLong(1, 1L);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                System.out.println(String.format("N_NATIONKEY: %d, N_NAME: %s", rs.getLong("N_NATIONKEY"), rs.getString("N_NAME")));
            }
        }
    }
}
