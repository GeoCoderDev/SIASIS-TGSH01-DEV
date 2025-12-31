import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Actualiza la contraseña de un auxiliar en RDP02
 * AUXILIARES ESTÁN EN RDP02 Y RDP03
 */
export async function actualizarContrasenaAuxiliarRDP02(
  nombreUsuario: string,
  contrasenaEncriptada: string
): Promise<boolean> {
  try {
    const sql = `
      UPDATE "T_Auxiliares"
      SET "Contraseña" = $1
      WHERE "Nombre_Usuario" = $2
    `;

    await RDP02_DB_INSTANCES.query(sql, [contrasenaEncriptada, nombreUsuario]);

    console.log(
      `✅ Contraseña actualizada en RDP02 para Auxiliar: ${nombreUsuario}`
    );
    return true;
  } catch (error) {
    console.error(
      `❌ Error al actualizar contraseña de Auxiliar ${nombreUsuario} en RDP02:`,
      error
    );
    throw error;
  }
}
