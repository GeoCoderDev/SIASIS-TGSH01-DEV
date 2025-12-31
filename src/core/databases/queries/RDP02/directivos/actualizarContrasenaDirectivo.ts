import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Actualiza la contraseña de un directivo
 */
export async function actualizarContrasenaDirectivo(
  nombreUsuario: string,
  contrasenaEncriptada: string
): Promise<boolean> {
  try {
    const sql = `
      UPDATE "T_Directivos"
      SET "Contraseña" = $1
      WHERE "Nombre_Usuario" = $2
    `;

    await RDP02_DB_INSTANCES.query(sql, [contrasenaEncriptada, nombreUsuario]);

    console.log(`✅ Contraseña actualizada en RDP02 para Directivo: ${nombreUsuario}`);
    return true;
  } catch (error) {
    console.error(`❌ Error al actualizar contraseña de Directivo ${nombreUsuario}:`, error);
    throw error;
  }
}