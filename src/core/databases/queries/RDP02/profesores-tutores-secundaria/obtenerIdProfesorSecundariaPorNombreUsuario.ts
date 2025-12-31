import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Obtiene el ID de un profesor de secundaria por su nombre de usuario
 * SOLO CONSULTA EN RDP02
 */
export async function obtenerIdProfesorSecundariaPorNombreUsuario(
  nombreUsuario: string
): Promise<string | null> {
  try {
    const sql = `
      SELECT "Id_Profesor_Secundaria" 
      FROM "T_Profesores_Secundaria"
      WHERE "Nombre_Usuario" = $1
      LIMIT 1
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, [nombreUsuario]);

    if (result.rows.length === 0) {
      console.log(`⚠️  Profesor Secundaria no encontrado: ${nombreUsuario}`);
      return null;
    }

    const idProfesor = result.rows[0].Id_Profesor_Secundaria;
    console.log(`✅ ID Profesor Secundaria obtenido: ${idProfesor}`);

    return idProfesor;
  } catch (error) {
    console.error(`❌ Error al obtener ID de Profesor Secundaria ${nombreUsuario}:`, error);
    throw error;
  }
}