import java.io.File;
import java.sql.SQLException;
import java.util.Scanner;

public class Principal {

	private static Scanner teclado = new Scanner(System.in);
	
	public static void main(String[] args) {
		String rutaSQLITE, nombreMYSQL;
		MigrarDAO dao = null;
		File bd;
		
		try {			
			///////////////Pedir datos////////////////
			System.out.println("Ruta de la BD SQLITE:");
			rutaSQLITE = teclado.nextLine();
			
			//Comprobación de SQLITE
			if (rutaSQLITE.endsWith(".db")) {
				bd = new File(rutaSQLITE);
			} else {
				bd = new File(rutaSQLITE+".db");
			}
			
			if (!bd.exists()) {
				throw new SQLException("Error en el login de Sqlite");
			}
			
			System.out.println("Nombre de la BD MYSQL:");
			nombreMYSQL = teclado.nextLine();
			//////////////////////////////////////////

			dao = MigrarDAO.getDao(rutaSQLITE, nombreMYSQL);
				
			dao.realizarMigracion(rutaSQLITE);
			
			System.out.println("Proceso realizado con éxito...");
			
			//Commit y cierre de conexión//
			ConexionBDMySQL.hacerCommit();
			ConexionBDSqlite.hacerCommit();
			ConexionBDMySQL.cerrarConexion();
			ConexionBDSqlite.cerrarConexion();
			///////////////////////////////
			
		} catch (SQLException e) {
			ConexionBDMySQL.hacerRollBack();
			ConexionBDSqlite.hacerRollBack();
			System.err.println(e.getMessage());
		} catch (Exception e) {
			ConexionBDMySQL.hacerRollBack();
			ConexionBDSqlite.hacerRollBack();
			e.printStackTrace();
		}

	}

}
