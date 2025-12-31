import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Obtiene el ID de un auxiliar por su nombre de usuario
 * SOLO CONSULTA EN RDP02
 */
export async function obtenerIdAuxiliarPorNombreUsuario(
  nombreUsuario: string
): Promise<string | null> {
  try {
    const sql = `
      SELECT "Id_Auxiliar" 
      FROM "T_Auxiliares"
      WHERE "Nombre_Usuario" = $1
      LIMIT 1
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, [nombreUsuario]);

    if (result.rows.length === 0) {
      console.log(`⚠️  Auxiliar no encontrado: ${nombreUsuario}`);
      return null;
    }

    const idAuxiliar = result.rows[0].Id_Auxiliar;
    console.log(`✅ ID Auxiliar obtenido: ${idAuxiliar}`);

    return idAuxiliar;
  } catch (error) {
    console.error(`❌ Error al obtener ID de Auxiliar ${nombreUsuario}:`, error);
    throw error;
  }
}