import java.sql.DriverManager;
import java.sql.SQLException;

public class ij {
    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        DriverManager.getConnection("");
    }
}
