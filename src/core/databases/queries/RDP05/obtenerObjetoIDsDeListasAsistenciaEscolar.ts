import { TipoAsistencia } from "../../../../interfaces/shared/AsistenciaRequests";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { redisClient } from "../../../../config/Redis/RedisClient";

import {
  GoogleDriveIDsListasAsistenciasEscolaresHoy,
  ListaAsistenciasEscolaresHoy,
  NOMBRE_CLAVE_GOOGLE_DRIVE_IDs_LISTAS_ASISTENCIAS_ESCOLARES_HOY,
} from "../../../../interfaces/shared/Asistencia/ListasAsistenciasEscolaresHoy";
import { updateJsonFileInDrive } from "../../../external/google/drive/updateJsonInDrive";
import { uploadJsonToDrive } from "../../../external/google/drive/uploadJsonToDrive";
import { deleteFileFromDrive } from "../../../external/google/drive/deleteFileFromDrive";

const CARPETA_LISTAS_ASISTENCIAS_ESCOLARES_HOY =
  "Listas de Asistencias Escolares de Hoy";

interface ResultadoActualizacion {
  exito: boolean;
  googleDriveId: string | null;
  accionRealizada: "creado" | "actualizado" | "sin_cambios";
  error?: string;
}

/**
 * Obtiene el objeto de IDs de Google Drive desde Redis con resiliencia total
 */
async function obtenerObjetoIDsListasAsistenciaEscolarHoy(
  nivel: NivelEducativo
): Promise<GoogleDriveIDsListasAsistenciasEscolaresHoy> {
  try {
    console.log(`📡 Obteniendo objeto de IDs desde Redis para ${nivel}...`);

    // Determinar instancia de Redis según el nivel
    const tipoAsistencia =
      nivel === NivelEducativo.PRIMARIA
        ? TipoAsistencia.ParaEstudiantesPrimaria
        : TipoAsistencia.ParaEstudiantesSecundaria;

    const redisClientInstance = redisClient(tipoAsistencia);

    // Intentar obtener el objeto desde Redis
    const objetoRedis = await redisClientInstance.get(
      NOMBRE_CLAVE_GOOGLE_DRIVE_IDs_LISTAS_ASISTENCIAS_ESCOLARES_HOY
    );

    if (!objetoRedis) {
      console.log(
        "ℹ️ No se encontró objeto de IDs en Redis, creando uno nuevo"
      );
      return {}; // Objeto vacío inicial
    }

    // Validar que sea un array y convertir a objeto
    if (Array.isArray(objetoRedis) && objetoRedis.length > 0) {
      try {
        const objetoParsed = JSON.parse(objetoRedis[0]);
        console.log(
          `✅ Objeto de IDs obtenido desde Redis: ${JSON.stringify(
            objetoParsed
          ).substring(0, 100)}...`
        );
        return objetoParsed;
      } catch (parseError) {
        console.warn(
          "⚠️ Error parseando objeto de IDs desde Redis, iniciando con objeto vacío"
        );
        return {};
      }
    }

    console.log("ℹ️ Formato inválido en Redis, iniciando con objeto vacío");
    return {};
  } catch (error) {
    console.error("❌ Error obteniendo objeto de IDs desde Redis:", error);
    console.log("🔄 Retornando objeto vacío para mantener resiliencia");
    return {};
  }
}

/**
 * Actualiza el objeto de IDs en Redis
 */
async function actualizarObjetoIDsEnRedis(
  nivel: NivelEducativo,
  objetoIDs: GoogleDriveIDsListasAsistenciasEscolaresHoy
): Promise<void> {
  try {
    console.log(`💾 Actualizando objeto de IDs en Redis para ${nivel}...`);

    // Determinar instancia de Redis según el nivel
    const tipoAsistencia =
      nivel === NivelEducativo.PRIMARIA
        ? TipoAsistencia.ParaEstudiantesPrimaria
        : TipoAsistencia.ParaEstudiantesSecundaria;

    const redisClientInstance = redisClient(tipoAsistencia);

    // Guardar objeto en Redis como array con un elemento (formato estándar)
    const valorParaRedis = [JSON.stringify(objetoIDs)];

    // Establecer sin expiración ya que es un objeto de configuración permanente
    await redisClientInstance.set(
      NOMBRE_CLAVE_GOOGLE_DRIVE_IDs_LISTAS_ASISTENCIAS_ESCOLARES_HOY,
      valorParaRedis
    );

    console.log("✅ Objeto de IDs actualizado en Redis correctamente");
  } catch (error) {
    console.error("❌ Error actualizando objeto de IDs en Redis:", error);
    // No lanzar error para mantener resiliencia
  }
}

/**
 * Función principal para obtener y actualizar IDs de Google Drive
 */
export async function obtenerYActualizarObjetoIDsGoogleDriveDeListasDeAsistenciaHoy(
  nombreArchivo: string,
  contenidoArchivo: ListaAsistenciasEscolaresHoy,
  nivel: NivelEducativo,
  grado: number
): Promise<ResultadoActualizacion> {
  try {
    console.log(
      `🔄 Iniciando proceso de actualización para archivo: ${nombreArchivo}`
    );

    // PASO 1: Obtener objeto de IDs desde Redis
    const objetoIDs = await obtenerObjetoIDsListasAsistenciaEscolarHoy(nivel);

    // PASO 2: Verificar si ya existe un ID para esta combinación nivel/grado
    const idExistente = objetoIDs[nivel]?.[grado];

    if (idExistente) {
      console.log(`📁 ID existente encontrado: ${idExistente}`);

      // PASO 3A: Intentar actualizar archivo existente
      try {
        const resultadoActualizacion = await updateJsonFileInDrive(
          idExistente,
          contenidoArchivo
        );

        if (resultadoActualizacion.exito) {
          console.log("✅ Archivo existente actualizado correctamente");
          return {
            exito: true,
            googleDriveId: idExistente,
            accionRealizada: "actualizado",
          };
        } else {
          console.warn(
            `⚠️ No se pudo actualizar archivo existente: ${resultadoActualizacion.error}`
          );
          console.log(
            "🔄 Procediendo a crear nuevo archivo y limpiar el anterior..."
          );
          // Continuar para crear nuevo archivo
        }
      } catch (actualizacionError) {
        console.warn(
          "⚠️ Error actualizando archivo existente:",
          actualizacionError
        );
        console.log(
          "🔄 Procediendo a crear nuevo archivo y limpiar el anterior..."
        );
        // Continuar para crear nuevo archivo
      }
    }

    // PASO 3B: Crear nuevo archivo en Google Drive
    console.log("📤 Creando nuevo archivo en Google Drive...");

    try {
      // Usar el nombre de archivo que viene como parámetro + extensión JSON
      const nombreArchivoConExtension = `${nombreArchivo}.json`;

      console.log(`📁 Carpeta: ${CARPETA_LISTAS_ASISTENCIAS_ESCOLARES_HOY}`);
      console.log(`📄 Archivo: ${nombreArchivoConExtension}`);

      const resultadoSubida = await uploadJsonToDrive(
        contenidoArchivo,
        CARPETA_LISTAS_ASISTENCIAS_ESCOLARES_HOY,
        nombreArchivoConExtension
      );

      console.log("✅ Archivo subido exitosamente a Google Drive");
      console.log(`🆔 Nuevo ID: ${resultadoSubida.id}`);

      // PASO 3C: Eliminar archivo anterior si existía (limpieza)
      if (idExistente) {
        console.log(`🗑️ Eliminando archivo anterior con ID: ${idExistente}`);
        const eliminacionExitosa = await deleteFileFromDrive(idExistente);

        if (eliminacionExitosa) {
          console.log("✅ Archivo anterior eliminado correctamente");
        } else {
          console.warn(
            "⚠️ No se pudo eliminar el archivo anterior, pero continuando..."
          );
        }
      }

      // PASO 4: Actualizar objeto de IDs en Redis
      console.log("🔄 Actualizando objeto de IDs con nuevo archivo...");

      // Asegurar que existe la estructura del nivel
      if (!objetoIDs[nivel]) {
        objetoIDs[nivel] = {};
      }

      // Actualizar el ID para este grado
      objetoIDs[nivel]![grado] = resultadoSubida.id;

      // Guardar objeto actualizado en Redis
      await actualizarObjetoIDsEnRedis(nivel, objetoIDs);

      console.log(
        `✅ Proceso completado: archivo ${
          idExistente ? "reemplazado" : "creado"
        }`
      );

      return {
        exito: true,
        googleDriveId: resultadoSubida.id,
        accionRealizada: "creado",
      };
    } catch (subirError) {
      console.error("❌ Error al subir archivo a Google Drive:", subirError);
      return {
        exito: false,
        googleDriveId: null,
        accionRealizada: "sin_cambios",
        error:
          subirError instanceof Error ? subirError.message : String(subirError),
      };
    }
  } catch (error) {
    console.error(
      "❌ Error en proceso de actualización de Google Drive:",
      error
    );
    return {
      exito: false,
      googleDriveId: null,
      accionRealizada: "sin_cambios",
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
