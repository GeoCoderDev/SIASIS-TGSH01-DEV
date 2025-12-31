
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Obtiene el Identificador Nacional (DNI) de un directivo por su nombre de usuario
 */
export async function obtenerIdDirectivoPorNombreUsuario(
  nombreUsuario: string
): Promise<string | null> {
  try {
    const sql = `
      SELECT "Identificador_Nacional" 
      FROM "T_Directivos"
      WHERE "Nombre_Usuario" = $1
      LIMIT 1
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, [nombreUsuario]);

    if (result.rows.length === 0) {
      console.log(`⚠️  Directivo no encontrado: ${nombreUsuario}`);
      return null;
    }

    const idDirectivo = result.rows[0].Identificador_Nacional;
    console.log(`✅ ID Directivo obtenido: ${idDirectivo}`);

    return idDirectivo;
  } catch (error) {
    console.error(
      `❌ Error al obtener ID de Directivo ${nombreUsuario}:`,
      error
    );
    throw error;
  }
}
