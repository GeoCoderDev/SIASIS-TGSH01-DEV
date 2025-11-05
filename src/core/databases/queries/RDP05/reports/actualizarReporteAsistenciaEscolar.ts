// src/core/databases/queries/RDP05/reportes/actualizarReporteEnRedis.ts
import { T_Reportes_Asistencia_Escolar } from "@prisma/client";
import {
  GruposIntanciasDeRedis,
  redisClient,
} from "../../../../../config/Redis/RedisClient";
import { TIEMPO_EXPIRACION_REPORTES_ASISTENCIAS_ESCOLARES_SEGUNDOS_CACHE_REDIS } from "../../../../../constants/REPORTES_ASISTENCIA";

export async function actualizarReporteAsistenciaEscolarEnRedis(
  reporte: T_Reportes_Asistencia_Escolar
): Promise<boolean> {
  try {
    // Usar la instancia de Redis para reportes
    // Asumiendo que existe un TipoAsistencia para reportes, si no, ajustar
    const redisClientInstance = redisClient(
      GruposIntanciasDeRedis.ParaReportesDeAsistenciasEscolares
    );

    // Guardar en Redis con expiración
    await redisClientInstance.set(
      reporte.Combinacion_Parametros_Reporte,
      JSON.stringify(reporte),
      TIEMPO_EXPIRACION_REPORTES_ASISTENCIAS_ESCOLARES_SEGUNDOS_CACHE_REDIS
    );

    console.log(
      `✅ Reporte actualizado en Redis: ${reporte.Combinacion_Parametros_Reporte} - Estado: ${reporte.Estado_Reporte}`
    );
    return true;
  } catch (error) {
    console.error("❌ Error al actualizar reporte en Redis:", error);
    return false;
  }
}
