
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Obtiene el ID del personal administrativo por su nombre de usuario
 */
export async function obtenerIdPersonalAdministrativoPorNombreUsuario(
  nombreUsuario: string
): Promise<string | null> {
  try {
    const sql = `
      SELECT "Id_Personal_Administrativo" 
      FROM "T_Personal_Administrativo"
      WHERE "Nombre_Usuario" = $1
      LIMIT 1
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, [nombreUsuario]);

    if (result.rows.length === 0) {
      console.log(`⚠️  Personal Administrativo no encontrado: ${nombreUsuario}`);
      return null;
    }

    const idPersonal = result.rows[0].Id_Personal_Administrativo;
    console.log(`✅ ID Personal Administrativo obtenido: ${idPersonal}`);

    return idPersonal;
  } catch (error) {
    console.error(`❌ Error al obtener ID de Personal Administrativo ${nombreUsuario}:`, error);
    throw error;
  }
}