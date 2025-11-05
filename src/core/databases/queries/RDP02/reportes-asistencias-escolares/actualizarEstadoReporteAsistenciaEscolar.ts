
import RDP02_DB_INSTANCES from "../../../connectors/postgres";
import { EstadoReporteAsistenciaEscolar } from "../../../../../interfaces/shared/ReporteAsistenciaEscolar";

export async function actualizarEstadoReporteAsistenciaEscolar(
  combinacionParametros: string,
  nuevoEstado: EstadoReporteAsistenciaEscolar,
  googleDriveId?: string | null
): Promise<boolean> {
  try {
    const sql = `
      UPDATE "T_Reportes_Asistencia_Escolar"
      SET "Estado_Reporte" = $1,
          "Datos_Google_Drive_Id" = $2
      WHERE "Combinacion_Parametros_Reporte" = $3
    `;

    await RDP02_DB_INSTANCES.query(sql, [
      nuevoEstado,
      googleDriveId,
      combinacionParametros,
    ]);

    console.log(
      `✅ Estado del reporte actualizado a: ${nuevoEstado} (${combinacionParametros})`
    );
    return true;
  } catch (error) {
    console.error("❌ Error al actualizar estado del reporte:", error);
    throw error;
  }
}
