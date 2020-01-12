import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConexionBDSqlite {
	
	//Constantes
	private static final String DRIVER_SQLITE = "org.sqlite.JDBC";
	private static final String URL_BD = "jdbc:sqlite:";
	
	//Atributos
	private static Connection conexion = null;
	
	////////////////////////Constructor//////////////////////////////
	private ConexionBDSqlite(String nombreBD) throws SQLException, ClassNotFoundException {
		
		if (!nombreBD.endsWith(".db")) {
			nombreBD = nombreBD+".db";
		}
		
		Class.forName(DRIVER_SQLITE);
		conexion = DriverManager.getConnection(URL_BD+nombreBD);
		
		conexion.setAutoCommit(false);
		
	}
	
	/////////////////////////Método público de conexión//////////////////////////////////
	public static Connection getConnection(String nombreBD) throws ClassNotFoundException, SQLException {
		if (conexion == null) {
			new ConexionBDSqlite(nombreBD);
		}
		
		return conexion;
	}
	
	/////////////Métodos commit y rollback////////////////
	public static void hacerCommit() throws SQLException {
		if (conexion != null) {
			conexion.commit();
		}
	}
	
	public static void hacerRollBack() {
		if (conexion != null) {
			try {
				conexion.rollback();
			} catch (SQLException e) {
				System.out.println("Error grave al hacer rollback");
				e.printStackTrace();
			}
		}
	}
	
	//////////////////Cerrar conexión/////////////////////////
	public static void cerrarConexion() throws SQLException {
		if (conexion != null) {
			conexion.close();
		}
	}
}
