
import { T_Reportes_Asistencia_Escolar } from "@prisma/client";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function registrarReporteAsistenciaEscolar(
  reporte: Omit<T_Reportes_Asistencia_Escolar, never>
): Promise<boolean> {
  try {
    const sql = `
      INSERT INTO "T_Reportes_Asistencia_Escolar" (
        "Combinacion_Parametros_Reporte",
        "Estado_Reporte",
        "Datos_Google_Drive_Id",
        "Fecha_Generacion",
        "Rol_Usuario",
        "Id_Usuario"
      ) VALUES ($1, $2, $3, $4, $5, $6)
    `;

    await RDP02_DB_INSTANCES.query(sql, [
      reporte.Combinacion_Parametros_Reporte,
      reporte.Estado_Reporte,
      reporte.Datos_Google_Drive_Id,
      reporte.Fecha_Generacion,
      reporte.Rol_Usuario,
      reporte.Id_Usuario,
    ]);

    console.log(
      `✅ Reporte registrado: ${reporte.Combinacion_Parametros_Reporte}`
    );
    return true;
  } catch (error) {
    console.error("❌ Error al registrar reporte:", error);
    throw error;
  }
}
