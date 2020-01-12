import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MigrarDAO {
		
	//Atributos
	private static Connection conexionMysql = null;
	private static Connection conexionSqlite = null;
	private static MigrarDAO dao = null;
	private static final String[][] conversionDatos = {
		{"UNSIGNED BIG INT", "BIGINT"},
		{"INT2", "INT"},
		{"INT8", "INT"},
		{"CHARACTER", "CHAR"},
		{"VARYING CHARACTER", "CHAR"},
		{"NCHAR", "CHAR"},
		{"NATIVE CHARACTER", "VARCHAR"},
		{"NVARCHAR", "VARCHAR"},
		{"CLOB", "VARCHAR"}
	};
	
	//Constructor
	private MigrarDAO(String nombreMysql, String nombreSqlite) throws ClassNotFoundException, SQLException {
		try {
			conexionMysql = ConexionBDMySQL.getConnection(nombreMysql);
			conexionSqlite = ConexionBDSqlite.getConnection(nombreSqlite);
		} catch (SQLException e) {
			throw new SQLException("Error en el login de MySQL"); 
		}
		
	}
	
	//Métodos propios
	public static MigrarDAO getDao(String nombreSqlite, String nombreMysql) throws ClassNotFoundException, SQLException {
		if (dao == null) {
			dao = new MigrarDAO(nombreMysql, nombreSqlite);
		}
		
		return dao;
	}
	
	/**
	 * Método principal que realiza la migración de la base de datos.
	 * Recorre tres veces las tablas de la base de datos que vamos a traspasar, para crear tablas e insertarlas (mismo
	 * bucle while), añadir las claves primarias y añadir las claves ajenas.
	 * @param nombreBDSqlite
	 * @throws SQLException
	 */
	public void realizarMigracion(String nombreBDSqlite) throws SQLException {
		DatabaseMetaData dbmd = conexionSqlite.getMetaData();
		
		String[] tipos = {"TABLE"}; //Para que no salgan las tablas del sistema
		
		try (ResultSet resultCrearEInsertar = dbmd.getTables(nombreBDSqlite, "PUBLIC", null, tipos);
				ResultSet resultPrimarias = dbmd.getTables(nombreBDSqlite, "PUBLIC", null, tipos);
				ResultSet resultAjenas = dbmd.getTables(nombreBDSqlite, "PUBLIC", null, tipos);) {
			
			String nombreTabla;
			
			//Crear tablas e insertar datos en ellas
			while(resultCrearEInsertar.next()) {
				nombreTabla = resultCrearEInsertar.getString("TABLE_NAME");
				
				crearTablasEInsertarDatos(dbmd, nombreTabla, nombreBDSqlite);
			}
							
			//Añadir claves primarias
			while (resultPrimarias.next()) {
				nombreTabla = resultPrimarias.getString("TABLE_NAME");
					
				annadirClavesPrimarias(dbmd, nombreTabla, nombreBDSqlite);
			}
								
			//Añadir claves ajenas
			while (resultAjenas.next()) {
				nombreTabla = resultAjenas.getString("TABLE_NAME");
				
				annadirClavesExternas(dbmd, nombreTabla, nombreBDSqlite);
			}
			
		}
				
	}
	
	/**
	 * Método que recorre las columnas de cada tabla y mira su nombre y tipo, para añadirlos
	 * a un StringBuilder que servirá luego para crear esa tabla en la nueva base de datos. 
	 * Tiene en cuenta que ese dato sea propio de Sqlite y que el campo admita nulos o no. También llama
	 * a un método para añadir los datos.
	 * @param dbmd: Metadatos.
	 * @param nombreTabla:  Nombre de la tabla por la que vamos.
	 * @param nombreBD: Nombre de la base de datos SQLITE.
	 * @throws SQLException
	 */
	public void crearTablasEInsertarDatos(DatabaseMetaData dbmd, String nombreTabla, String nombreBD) throws SQLException, NullPointerException {		
		StringBuilder sb = new StringBuilder();

		try (ResultSet columnas = dbmd.getColumns(nombreBD, "PUBLIC", nombreTabla, null);) {
			
			while (columnas.next()) {
				String tipoMySql;
				
				String nombreCol = columnas.getString("COLUMN_NAME"); //Nombre columna
				sb.append(nombreCol+" ");
				
				String tipoCol = columnas.getString("TYPE_NAME"); //Tipo columna
				tipoMySql = conversionAMySql(tipoCol); //Si el dato es propio de Sqlite lo convertimos a su equivalente en MySQL
				
				comprobarSiEsNulo(sb, columnas, tipoMySql);
			}

			//Borramos última coma
			sb.deleteCharAt(sb.length() -1);

			crearTabla(nombreTabla, sb);
			insertarDatos(dbmd, nombreTabla, nombreBD);
			
		}
		
	}

	//////Los dos siguientes métodos son para afinar la creación de la tabla, sabiendo si el campo admite nulos o no
	/////y si el tipo de dato es propio de MySQL o no ///////
	
	/**
	 * Método que comprueba si una columna admite o no nulos, según el resultado forma la sentencia.
	 * @param sb
	 * @param columnas
	 * @param tipoMySql
	 * @throws SQLException
	 */
	private void comprobarSiEsNulo(StringBuilder sb, ResultSet columnas, String tipoMySql) throws SQLException {
		String admiteNulos;

		admiteNulos = columnas.getString("IS_NULLABLE");
		
		if (admiteNulos.equalsIgnoreCase("N")) {
			sb.append(tipoMySql+" NOT NULL,");
		} else {
			sb.append(tipoMySql+",");
		}
	}

	/**
	 * Si el tipo de dato es propio de SQlite, lo convierte a un tipo equivalente que exista en MySQL.
	 * @param tipoCol
	 * @return
	 */
	private String conversionAMySql(String tipoCol) {
		String tipoMySql = tipoCol;
		boolean encontrado = false;
		int i = 0;
		
		while (i < conversionDatos.length && !encontrado) {
			if (tipoCol.toUpperCase().contains(conversionDatos[i][0])) {
				tipoMySql = tipoCol.replaceAll(conversionDatos[i][0], conversionDatos[i][1]);
				encontrado = true;
			}
			
			i++;
		}
		
		return tipoMySql;
		
	}
	
	/////Los dos siguientes métodos son los que contienen las sentencias para crear las tablas e insertar los datos//////

	/**
	 * Método sencillo que crea una tabla pasándole su nombre y lo que va a contener
	 * @param nombreTabla
	 * @param sb
	 * @throws SQLException
	 */
	public void crearTabla (String nombreTabla, StringBuilder sb) throws SQLException {
		
		try (Statement sentencia = conexionMysql.createStatement();) {
			
			System.out.println("CREATE TABLE IF NOT EXISTS "+nombreTabla+" ("+sb.toString()+");");
			sentencia.executeUpdate("CREATE TABLE IF NOT EXISTS "+nombreTabla+" ("+sb.toString()+");"); 
		} catch (SQLException e) {
			throw new SQLException("Error al crear la tabla "+nombreTabla);
		}
		
	}
	
	
	/**
	 * Inserta los diferentes datos que había en la base de datos en sus respectivas columnas
	 * @param dbmd
	 * @param nombreTabla
	 * @param nombreBD
	 * @throws SQLException
	 */
	public void insertarDatos(DatabaseMetaData dbmd, String nombreTabla, String nombreBD) throws SQLException {
		
		try (Statement sentenciaSelect = conexionSqlite.createStatement();
				Statement sentenciaInsercion = conexionMysql.createStatement();
				ResultSet resultadoQuery = sentenciaSelect.executeQuery("select * from "+nombreTabla);) {
			
			ResultSet columnas = null;
			
			//Cada una de las columnas de un registro en la tabla
			columnas = dbmd.getColumns(nombreBD, "PUBLIC", nombreTabla, null);
			
			StringBuilder sb = new StringBuilder();

			while(resultadoQuery.next()) { //Recorremos cada registro de la tabla
				while (columnas.next()) { //En cada registro recorremos sus columnas
					formacionDeSentenciaActualizacion(resultadoQuery, columnas, sb);
				}
				
				sb.deleteCharAt(sb.length()-1);
				System.out.println("INSERT INTO "+nombreTabla+" VALUES ("+sb.toString()+");");
				sentenciaInsercion.executeUpdate("INSERT INTO "+nombreTabla+" VALUES ("+sb.toString()+");");
				//Reinicia los valores
				sb.delete(0, sb.length()); //Limpia el StringBuilder
				columnas.close(); //Pone la lista de columnas en la primera, para el siguiente registro
				columnas = dbmd.getColumns(nombreBD, "PUBLIC", nombreTabla, null);
			}
			
			columnas.close();
		} catch (SQLException e) {
			throw new SQLException("Error de inserción en la tabla "+nombreTabla);
		}

	}

	/**
	 * Método que en cada columna devuelve el valor de la misma y lo añade a un StringBuilder, controlando
	 * que los campos sean o no null para poner la sentencia correctamente en cada caso. 
	 * @param resultadoQuery
	 * @param columnas
	 * @param sb
	 * @throws SQLException
	 */
	private void formacionDeSentenciaActualizacion(ResultSet resultadoQuery, ResultSet columnas, StringBuilder sb)
			throws SQLException {
		String nombreCol = columnas.getString("COLUMN_NAME");
		String valor;
		
		if (resultadoQuery.getObject(nombreCol) != null) {
			valor = resultadoQuery.getObject(nombreCol).toString();
			valor = valor.replaceAll("'", ""); //Por si algún valor tiene comillas simples, evita fallos
			
			sb.append("'"+valor+"',");
		} else {
			valor = "null"; //Para que no dé fallo en el caso de devolver null
			
			sb.append(valor+",");
		}
	}
	
	////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Recorre las claves primarias de cada tabla y las añade a la base de datos MySQL.
	 * @param dbmd
	 * @param nombreTabla
	 * @param nombreBDSqlite
	 * @throws SQLException
	 */
	public void annadirClavesPrimarias(DatabaseMetaData dbmd, String nombreTabla, String nombreBDSqlite) throws SQLException {
		
		StringBuilder sbPrimarys = new StringBuilder();
		
		try (ResultSet primarys = dbmd.getPrimaryKeys(nombreBDSqlite, null, nombreTabla);
				Statement sentenciaPrimary = conexionMysql.createStatement();){
			
			while (primarys.next()) {
				sbPrimarys.append(primarys.getString("COLUMN_NAME")+",");
			}
			//Borra última coma
			sbPrimarys.deleteCharAt(sbPrimarys.length() -1);
			//Sentencia
			System.out.println("ALTER TABLE "+nombreTabla+" ADD PRIMARY KEY ("+sbPrimarys.toString()+");");		
			sentenciaPrimary.executeUpdate("ALTER TABLE "+nombreTabla+" ADD PRIMARY KEY ("+sbPrimarys.toString()+");");
		} catch (SQLException e) {
			throw new SQLException("Error al añadir clave primaria en la tabla "+nombreTabla);
		}
	}

	////////////////////////////////////////////////////////////////////////////
	
	/**
	 * Añade las claves ajenas de cada tabla.
	 * @param dbmd
	 * @param nombreTabla
	 * @param nombreBDSqlite
	 * @throws SQLException
	 */
	public void annadirClavesExternas(DatabaseMetaData dbmd, String nombreTabla, String nombreBDSqlite) throws SQLException {
		
		try (Statement sentenciaForeign = conexionMysql.createStatement();
				ResultSet clavesInternas = dbmd.getImportedKeys(null, "PUBLIC", nombreTabla);) {
			
			while (clavesInternas.next()) {			
				String columnaFK = clavesInternas.getString("FKCOLUMN_NAME"); //Campo foráneo de la tabla
				String tablaPK = clavesInternas.getString("PKTABLE_NAME"); //Tabla a la que se referencia 
				String columnaPK = clavesInternas.getString("PKCOLUMN_NAME"); //Columna a la que se referencia
				
				//Sentencia
				System.out.println("ALTER TABLE "+nombreTabla+" ADD FOREIGN KEY ("+columnaFK+") REFERENCES "+tablaPK+"("+columnaPK+");");				
				sentenciaForeign.executeUpdate("ALTER TABLE "+nombreTabla+" ADD FOREIGN KEY ("+columnaFK+") REFERENCES "+tablaPK+"("+columnaPK+");");
			} 
		
		} catch (SQLException e) {
			throw new SQLException("Error al añadir clave ajena a la tabla "+nombreTabla);
		}
			
	}
}
