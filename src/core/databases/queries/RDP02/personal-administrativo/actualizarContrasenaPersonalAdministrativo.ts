import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Actualiza la contraseña del personal administrativo
 */
export async function actualizarContrasenaPersonalAdministrativo(
  nombreUsuario: string,
  contrasenaEncriptada: string
): Promise<boolean> {
  try {
    const sql = `
      UPDATE "T_Personal_Administrativo"
      SET "Contraseña" = $1
      WHERE "Nombre_Usuario" = $2
    `;

    await RDP02_DB_INSTANCES.query(sql, [contrasenaEncriptada, nombreUsuario]);

    console.log(`✅ Contraseña actualizada en RDP02 para Personal Administrativo: ${nombreUsuario}`);
    return true;
  } catch (error) {
    console.error(`❌ Error al actualizar contraseña de Personal Administrativo ${nombreUsuario}:`, error);
    throw error;
  }
}