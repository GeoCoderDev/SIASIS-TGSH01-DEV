import { TipoAsistencia } from "../../../../interfaces/shared/AsistenciaRequests";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { redisClient } from "../../../../config/Redis/RedisClient";
import {
  JobsEnEjecucionListasAsistenciasEscolaresHoy,
  NOMBRE_CLAVE_JOBS_EN_EJECUCION_LISTAS_ASISTENCIAS_ESCOLARES_HOY,
} from "../../../../interfaces/shared/Asistencia/ListasAsistenciasEscolaresHoy";

/**
 * Obtiene el objeto de Jobs en ejecución desde Redis con resiliencia total
 */
async function obtenerObjetoJobsAsistenciaEscolaresEnEjecucion(
  nivel: NivelEducativo
): Promise<JobsEnEjecucionListasAsistenciasEscolaresHoy> {
  try {
    console.log(`📡 Obteniendo objeto de Jobs desde Redis para ${nivel}...`);

    // Determinar instancia de Redis según el nivel
    const tipoAsistencia =
      nivel === NivelEducativo.PRIMARIA
        ? TipoAsistencia.ParaEstudiantesPrimaria
        : TipoAsistencia.ParaEstudiantesSecundaria;

    const redisClientInstance = redisClient(tipoAsistencia);

    // Intentar obtener el objeto desde Redis
    const objetoRedis = await redisClientInstance.get(
      NOMBRE_CLAVE_JOBS_EN_EJECUCION_LISTAS_ASISTENCIAS_ESCOLARES_HOY
    );

    if (!objetoRedis) {
      console.log(
        "ℹ️ No se encontró objeto de Jobs en Redis, creando uno nuevo"
      );
      return {}; // Objeto vacío inicial
    }

    // Validar que sea un array y convertir a objeto
    if (Array.isArray(objetoRedis) && objetoRedis.length > 0) {
      try {
        const objetoParsed = JSON.parse(objetoRedis[0]);
        console.log(
          `✅ Objeto de Jobs obtenido desde Redis: ${JSON.stringify(
            objetoParsed
          ).substring(0, 100)}...`
        );
        return objetoParsed;
      } catch (parseError) {
        console.warn(
          "⚠️ Error parseando objeto de Jobs desde Redis, iniciando con objeto vacío"
        );
        return {};
      }
    }

    console.log("ℹ️ Formato inválido en Redis, iniciando con objeto vacío");
    return {};
  } catch (error) {
    console.error("❌ Error obteniendo objeto de Jobs desde Redis:", error);
    console.log("🔄 Retornando objeto vacío para mantener resiliencia");
    return {};
  }
}

/**
 * Actualiza el objeto de Jobs en Redis
 */
async function actualizarObjetoJobsAsistenciaEscolaresEnRedis(
  nivel: NivelEducativo,
  objetoJobs: JobsEnEjecucionListasAsistenciasEscolaresHoy
): Promise<void> {
  try {
    console.log(`💾 Actualizando objeto de Jobs en Redis para ${nivel}...`);

    // Determinar instancia de Redis según el nivel
    const tipoAsistencia =
      nivel === NivelEducativo.PRIMARIA
        ? TipoAsistencia.ParaEstudiantesPrimaria
        : TipoAsistencia.ParaEstudiantesSecundaria;

    const redisClientInstance = redisClient(tipoAsistencia);

    // Guardar objeto en Redis como array con un elemento (formato estándar)
    const valorParaRedis = [JSON.stringify(objetoJobs)];

    // Establecer sin expiración ya que es un objeto de configuración permanente
    await redisClientInstance.set(
      NOMBRE_CLAVE_JOBS_EN_EJECUCION_LISTAS_ASISTENCIAS_ESCOLARES_HOY,
      valorParaRedis
    );

    console.log("✅ Objeto de Jobs actualizado en Redis correctamente");
  } catch (error) {
    console.error("❌ Error actualizando objeto de Jobs en Redis:", error);
    // No lanzar error para mantener resiliencia
  }
}

/**
 * Marca un job como en ejecución (sobrescribe con true)
 */
export async function marcarJobAsistenciaEscolaresEnEjecucion(
  nivel: NivelEducativo,
  grado: number
): Promise<void> {
  try {
    console.log(`🚀 Marcando job como EN EJECUCIÓN: ${nivel} grado ${grado}`);

    // Obtener objeto actual de jobs
    const objetoJobs = await obtenerObjetoJobsAsistenciaEscolaresEnEjecucion(
      nivel
    );

    // Asegurar que existe la estructura del nivel
    if (!objetoJobs[nivel]) {
      objetoJobs[nivel] = {};
    }

    // Marcar como en ejecución (siempre true, sobrescribiendo)
    objetoJobs[nivel]![grado] = true;

    // Guardar objeto actualizado en Redis
    await actualizarObjetoJobsAsistenciaEscolaresEnRedis(nivel, objetoJobs);

    console.log(`✅ Job marcado como EN EJECUCIÓN: ${nivel} grado ${grado}`);
  } catch (error) {
    console.error("❌ Error marcando job como en ejecución:", error);
    // No lanzar error para permitir que el proceso continúe
  }
}

/**
 * Marca un job como terminado (pone en false)
 */
export async function marcarJobAsistenciaEscolaresTerminado(
  nivel: NivelEducativo,
  grado: number
): Promise<void> {
  try {
    console.log(`🏁 Marcando job como TERMINADO: ${nivel} grado ${grado}`);

    // Obtener objeto actual de jobs
    const objetoJobs = await obtenerObjetoJobsAsistenciaEscolaresEnEjecucion(
      nivel
    );

    // Verificar que existe la estructura y el job específico
    if (objetoJobs[nivel] && objetoJobs[nivel]![grado] !== undefined) {
      // Marcar como terminado (false)
      objetoJobs[nivel]![grado] = false;

      // Guardar objeto actualizado en Redis
      await actualizarObjetoJobsAsistenciaEscolaresEnRedis(nivel, objetoJobs);

      console.log(`✅ Job marcado como TERMINADO: ${nivel} grado ${grado}`);
    } else {
      console.log(
        `ℹ️ Job no encontrado para marcar como terminado: ${nivel} grado ${grado}`
      );
    }
  } catch (error) {
    console.error("❌ Error marcando job como terminado:", error);
    // No lanzar error para permitir que el proceso continúe
  }
}
