import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConexionBDMySQL {
	
	//Constantes
	private static final String USUARIO = "root";
	private static final String CONTRASENNA = "";
	private static final String URL_BD = "jdbc:mysql://localhost/";
	
	//Atributos
	private static Connection conexion = null;
	
	////////////////////////Constructor//////////////////////////////
	private ConexionBDMySQL(String nombreBD) throws SQLException, ClassNotFoundException {
		
		conexion = DriverManager.getConnection(URL_BD+nombreBD, USUARIO, CONTRASENNA);
		conexion.setAutoCommit(false);
		
	}
	
	/////////////////////////Método público de conexión//////////////////////////////////
	public static Connection getConnection(String nombreBD) throws ClassNotFoundException, SQLException {
		if (conexion == null) {
			new ConexionBDMySQL(nombreBD);
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
