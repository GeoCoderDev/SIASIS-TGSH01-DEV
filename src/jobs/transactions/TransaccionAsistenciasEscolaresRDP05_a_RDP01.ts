import { closeClient } from "../../core/databases/connectors/mongodb";
import { closePool } from "../../core/databases/connectors/postgres";
import { verificarDiaEvento } from "../../core/databases/queries/RDP02/eventos/verificarDiaEvento";
import { obtenerFechasActuales } from "../../core/utils/dates/obtenerFechasActuales";
import { RolesSistema } from "../../interfaces/shared/RolesSistema";
import { NivelEducativo } from "../../interfaces/shared/NivelEducativo";
import {
  GradosPrimaria,
  GradosSecundaria,
} from "../../constants/GRADOS_POR_NIVEL_EDUCATIVO";
import { NOMBRES_ARCHIVOS_ASISTENCIAS_ESCOLARES_HOY } from "../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { obtenerAsistenciasEscolaresDelDiaActual } from "../../core/databases/queries/RDP05/obtenerAsistenciasEscolaresDelDiaActual";
import { ListaAsistenciasEscolaresHoy } from "../../interfaces/shared/Asistencia/ListasAsistenciasEscolaresHoy";
import { obtenerYActualizarObjetoIDsGoogleDriveDeListasDeAsistenciaHoy } from "../../core/databases/queries/RDP05/obtenerObjetoIDsDeListasAsistenciaEscolar";
import { marcarJobAsistenciaEscolaresEnEjecucion, marcarJobAsistenciaEscolaresTerminado } from "../../core/databases/queries/RDP05/ObtejoJobsDeListasDeAsistenciasEscolaresHoyEnEjecucion";

/**
 * Función principal del script
 */
async function main() {
  let nivel: NivelEducativo;
  let grado: number;

  try {
    // Obtener argumentos de línea de comandos
    const args = process.argv.slice(2);

    if (args.length < 2) {
      console.error("❌ Error: Se requieren 2 parámetros: nivel y grado");
      console.error("Uso: npm run script -- <nivel> <grado>");
      console.error("Ejemplo: npm run script -- P 3");
      console.error("Ejemplo: npm run script -- S 2");
      process.exit(1);
    }

    const [nivelParam, gradoParam] = args;

    // Validar nivel
    if (nivelParam.toUpperCase() === NivelEducativo.PRIMARIA) {
      nivel = NivelEducativo.PRIMARIA;
    } else if (nivelParam.toUpperCase() === NivelEducativo.SECUNDARIA) {
      nivel = NivelEducativo.SECUNDARIA;
    } else {
      console.error(
        "❌ Error: Nivel debe ser 'P' (Primaria) o 'S' (Secundaria)"
      );
      process.exit(1);
    }

    // Validar grado
    grado = parseInt(gradoParam, 10);
    if (isNaN(grado)) {
      console.error("❌ Error: Grado debe ser un número");
      process.exit(1);
    }

    // Validar rango de grados según el nivel
    if (nivel === NivelEducativo.PRIMARIA) {
      if (!Object.values(GradosPrimaria).includes(grado as GradosPrimaria)) {
        console.error(
          "❌ Error: Para primaria, el grado debe estar entre 1 y 6"
        );
        process.exit(1);
      }
    } else {
      if (
        !Object.values(GradosSecundaria).includes(grado as GradosSecundaria)
      ) {
        console.error(
          "❌ Error: Para secundaria, el grado debe estar entre 1 y 5"
        );
        process.exit(1);
      }
    }

    console.log(
      `🚀 Iniciando actualización de lista de asistencias para ${nivel} grado ${grado}...`
    );

    // ✅ PASO 0: Marcar job como en ejecución
    console.log("\n🔒 === PASO 0: Marcando job como EN EJECUCIÓN ===");
    await marcarJobAsistenciaEscolaresEnEjecucion(nivel, grado);

    // Definir roles a bloquear
    const rolesABloquear = [
      RolesSistema.Directivo,
      RolesSistema.Auxiliar,
      RolesSistema.ProfesorPrimaria,
      RolesSistema.ProfesorSecundaria,
      RolesSistema.Tutor,
      RolesSistema.Responsable,
    ];

    // Obtener fecha actual
    const { fechaLocalPeru } = await obtenerFechasActuales();
    console.log(
      `📅 Procesando asistencias para: ${
        fechaLocalPeru.toISOString().split("T")[0]
      }`
    );

    // Verificar si es día de evento
    const esDiaEvento = await verificarDiaEvento(fechaLocalPeru);
    if (esDiaEvento) {
      console.log(
        "🎉 Es día de evento, pero continuando con la actualización de listas"
      );
    }

    // FASE 1: Obtener asistencias del día actual desde Redis
    console.log(
      `\n🔄 === FASE 1: Obteniendo asistencias de ${nivel} grado ${grado} ===`
    );

    const asistenciasDelDia = await obtenerAsistenciasEscolaresDelDiaActual(
      nivel,
      grado,
      fechaLocalPeru
    );
    console.log(
      `✅ Se obtuvieron ${
        Object.keys(asistenciasDelDia).length
      } registros de asistencia`
    );

    // FASE 2: Construir objeto de lista de asistencias
    console.log("\n📋 === FASE 2: Construyendo lista de asistencias ===");

    const listaAsistencias: ListaAsistenciasEscolaresHoy = {
      AsistenciasEscolaresDeHoy: asistenciasDelDia,
      Fecha_Actualizacion: fechaLocalPeru.toISOString(),
    };

    console.log(
      `📊 Lista construida con ${
        Object.keys(asistenciasDelDia).length
      } estudiantes`
    );

    // FASE 3: Obtener nombre del archivo y actualizar Google Drive
    console.log("\n💾 === FASE 3: Actualizando archivo en Google Drive ===");

    const nombreArchivo =
      NOMBRES_ARCHIVOS_ASISTENCIAS_ESCOLARES_HOY[nivel][grado];
    console.log(`📁 Nombre del archivo: ${nombreArchivo}`);

    const resultado =
      await obtenerYActualizarObjetoIDsGoogleDriveDeListasDeAsistenciaHoy(
        nombreArchivo,
        listaAsistencias,
        nivel,
        grado
      );

    if (resultado.exito) {
      console.log(`✅ Archivo actualizado exitosamente`);
      console.log(`🆔 ID de Google Drive: ${resultado.googleDriveId}`);
      console.log(`📝 Acción realizada: ${resultado.accionRealizada}`);
    } else {
      console.error(`❌ Error al actualizar archivo: ${resultado.error}`);
      throw new Error(`Error actualizando Google Drive: ${resultado.error}`);
    }

    console.log("\n🎉 Proceso completado exitosamente");
  } catch (error) {
    console.error("❌ Error en el procesamiento:", error);
    process.exit(1);
  } finally {
    try {
      // ✅ PASO FINAL: Marcar job como terminado (SIEMPRE se ejecuta)
      if (nivel! !== undefined && grado! !== undefined) {
        console.log("\n🔓 === PASO FINAL: Marcando job como TERMINADO ===");
        await marcarJobAsistenciaEscolaresTerminado(nivel, grado);
      }

      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando proceso...");
    } catch (closeError) {
      console.error("❌ Error al cerrar conexiones:", closeError);
    }
    process.exit(0);
  }
}

// Ejecutar el script
main();
