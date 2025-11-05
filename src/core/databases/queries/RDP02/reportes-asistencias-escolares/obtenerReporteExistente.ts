import { T_Reportes_Asistencia_Escolar } from "@prisma/client";
import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export async function obtenerReporteExistente(
  combinacionParametros: string
): Promise<T_Reportes_Asistencia_Escolar | null> {
  try {
    const sql = `
      SELECT 
        "Combinacion_Parametros_Reporte",
        "Estado_Reporte",
        "Datos_Google_Drive_Id",
        "Fecha_Generacion",
        "Rol_Usuario",
        "Id_Usuario"
      FROM "T_Reportes_Asistencia_Escolar"
      WHERE "Combinacion_Parametros_Reporte" = $1
      LIMIT 1
    `;

    const result = await RDP02_DB_INSTANCES.query(sql, [combinacionParametros]);

    if (result.rows.length === 0) {
      return null;
    }

    return result.rows[0] as T_Reportes_Asistencia_Escolar;
  } catch (error) {
    console.error("❌ Error al obtener reporte existente:", error);
    throw error;
  }
}
