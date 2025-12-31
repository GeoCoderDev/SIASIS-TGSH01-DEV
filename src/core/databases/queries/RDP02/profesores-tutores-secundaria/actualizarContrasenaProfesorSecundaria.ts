import RDP02_DB_INSTANCES from "../../../connectors/postgres";

/**
 * Actualiza la contraseña de un profesor de secundaria en RDP02
 */
export async function actualizarContrasenaProfesorSecundariaRDP02(
  nombreUsuario: string,
  contrasenaEncriptada: string
): Promise<boolean> {
  try {
    const sql = `
      UPDATE "T_Profesores_Secundaria"
      SET "Contraseña" = $1
      WHERE "Nombre_Usuario" = $2
    `;

    await RDP02_DB_INSTANCES.query(sql, [contrasenaEncriptada, nombreUsuario]);

    console.log(`✅ Contraseña actualizada en RDP02 para Profesor Secundaria: ${nombreUsuario}`);
    return true;
  } catch (error) {
    console.error(`❌ Error al actualizar contraseña de Profesor Secundaria ${nombreUsuario}:`, error);
    throw error;
  }
}