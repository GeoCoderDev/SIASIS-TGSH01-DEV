import { closeClient } from "../../core/databases/connectors/mongodb";
import { closePool } from "../../core/databases/connectors/postgres";

import {
  EstadoReporteAsistenciaEscolar,
  ReporteAsistenciaEscolarPorDias,
  ReporteAsistenciaEscolarPorMeses,
  TipoReporteAsistenciaEscolar,
} from "../../interfaces/shared/ReporteAsistenciaEscolar";
import { T_Reportes_Asistencia_Escolar } from "@prisma/client";
import { uploadJsonToDrive } from "../../core/external/google/drive/uploadJsonToDrive";
import { NivelEducativo } from "../../interfaces/shared/NivelEducativo";
import { EstadosAsistenciaEscolar } from "../../interfaces/shared/EstadosAsistenciaEstudiantes";
import { ModoRegistro } from "../../interfaces/shared/ModoRegistroPersonal";
import { obtenerReporteExistente } from "../../core/databases/queries/RDP02/reportes-asistencias-escolares/obtenerReporteExistente";
import { actualizarReporteAsistenciaEscolarEnRedis } from "../../core/databases/queries/RDP05/reports/actualizarReporteAsistenciaEscolar";
import { registrarReporteAsistenciaEscolar } from "../../core/databases/queries/RDP02/reportes-asistencias-escolares/registrarReporteAsistenciaEscolar";
import decodificarCombinacionParametrosParaReporteEscolar from "../../core/utils/helpers/decoders/reportes-asistencia-escolares/decodificarCombinacionParametrosParaReporteEscolar";
import { actualizarEstadoReporteAsistenciaEscolar } from "../../core/databases/queries/RDP02/reportes-asistencias-escolares/actualizarEstadoReporteAsistenciaEscolar";
import { obtenerConfiguracionesToleranciasTardanza } from "../../core/databases/queries/RDP02/ajustes-generales/obtenerConfiguracionesToleranciasTardanza";
import { obtenerDatosEstudiantesYAulasDesdeGoogleDrive } from "../../core/databases/queries/RDP01/obtenerDatosEstudiantesYAulasDesdeGoogleDrive";
import { obtenerAsistenciasEstudiantesPorRango } from "../../core/databases/queries/RDP03/asistencias-escolares/obtenerAsistenciasEstudiantesPorRango";
import { getDiasDisponiblesPorMes } from "../../core/utils/helpers/getters/getDiasDisponiblesPorMes";

interface PayloadReporte extends Omit<T_Reportes_Asistencia_Escolar, never> {}

/**
 * Función principal del script
 */
async function main() {
  let payloadOriginal: PayloadReporte | null = null;

  try {
    // ============================================================
    // PASO 1: Validar argumentos de entrada
    // ============================================================
    console.log("🚀 Iniciando generación de reporte de asistencia escolar...");

    const args = process.argv.slice(2);
    if (args.length < 1) {
      console.error("❌ Error: Se requiere el payload del reporte como JSON");
      console.error(
        'Uso: npm run script -- \'{"Combinacion_Parametros_Reporte":"D3A6BP4A",...}\''
      );
      process.exit(1);
    }

    let payload: PayloadReporte;
    try {
      payload = JSON.parse(args[0]);
      payloadOriginal = payload; // Guardar para uso en catch
    } catch (error) {
      console.error("❌ Error: El payload no es un JSON válido");
      process.exit(1);
    }

    console.log("📋 Payload recibido:", payload);

    // ============================================================
    // PASO 2: Verificar si ya existe en PostgreSQL (RDP02)
    // ============================================================
    console.log(
      "\n🔍 === PASO 2: Verificando si el reporte ya existe en PostgreSQL ==="
    );

    const reporteExistente = await obtenerReporteExistente(
      payload.Combinacion_Parametros_Reporte
    );

    if (reporteExistente) {
      console.log(
        `✅ Reporte encontrado en PostgreSQL con estado: ${reporteExistente.Estado_Reporte}`
      );

      // Actualizar Redis con los datos existentes de PostgreSQL
      await actualizarReporteAsistenciaEscolarEnRedis(reporteExistente);

      console.log(
        "✅ Redis actualizado con datos existentes de PostgreSQL. Finalizando proceso."
      );
      process.exit(0);
    }

    console.log(
      "ℹ️  Reporte no existe en PostgreSQL, procediendo con la creación"
    );

    // ============================================================
    // PASO 3: Registrar el reporte en PostgreSQL con estado PENDIENTE
    // ============================================================
    console.log("\n📝 === PASO 3: Registrando reporte en PostgreSQL ===");

    await registrarReporteAsistenciaEscolar(payload);

    // ============================================================
    // PASO 4: Decodificar parámetros del reporte
    // ============================================================
    console.log("\n🔍 === PASO 4: Decodificando parámetros del reporte ===");

    const parametrosDecodificados =
      decodificarCombinacionParametrosParaReporteEscolar(
        payload.Combinacion_Parametros_Reporte
      );

    if (!parametrosDecodificados) {
      console.error(
        "❌ Error: No se pudieron decodificar los parámetros del reporte"
      );

      // Actualizar estado a ERROR en PostgreSQL
      await actualizarEstadoReporteAsistenciaEscolar(
        payload.Combinacion_Parametros_Reporte,
        EstadoReporteAsistenciaEscolar.ERROR
      );

      // Actualizar Redis con estado ERROR
      const reporteError: T_Reportes_Asistencia_Escolar = {
        ...payload,
        Estado_Reporte: EstadoReporteAsistenciaEscolar.ERROR,
      };
      await actualizarReporteAsistenciaEscolarEnRedis(reporteError);

      process.exit(1);
    }

    console.log("✅ Parámetros decodificados:", parametrosDecodificados);

    const { tipoReporte, rangoTiempo, aulasSeleccionadas } =
      parametrosDecodificados;

    // ============================================================
    // PASO 5: Obtener configuraciones de tolerancia
    // ============================================================
    console.log(
      "\n⚙️ === PASO 5: Obteniendo configuraciones de tolerancia ==="
    );

    const tolerancias = await obtenerConfiguracionesToleranciasTardanza();
    const toleranciaSegundos =
      aulasSeleccionadas.Nivel === NivelEducativo.PRIMARIA
        ? tolerancias.toleranciaTardanzaMinutosPrimaria * 60
        : tolerancias.toleranciaTardanzaMinutosSecundaria * 60;

    console.log(
      `✅ Tolerancia para ${aulasSeleccionadas.Nivel}: ${
        toleranciaSegundos / 60
      } minutos`
    );

    // ============================================================
    // PASO 6: Obtener datos de estudiantes y aulas desde Google Drive
    // ============================================================
    console.log(
      "\n📂 === PASO 6: Obteniendo datos de estudiantes y aulas desde Google Drive ==="
    );

    const { estudiantes: estudiantesMap, aulas: aulasMap } =
      await obtenerDatosEstudiantesYAulasDesdeGoogleDrive(
        aulasSeleccionadas.Nivel
      );

    console.log(
      `✅ Datos obtenidos: ${estudiantesMap.size} estudiantes, ${aulasMap.size} aulas`
    );

    // Filtrar aulas según los parámetros del reporte
    const aulasFiltradas = Array.from(aulasMap.values()).filter((aula) => {
      if (
        aulasSeleccionadas.Grado !== "T" &&
        aula.Grado !== aulasSeleccionadas.Grado
      ) {
        return false;
      }

      if (
        aulasSeleccionadas.Seccion !== "T" &&
        aula.Seccion !== aulasSeleccionadas.Seccion
      ) {
        return false;
      }

      return true;
    });

    console.log(`✅ ${aulasFiltradas.length} aulas coinciden con los filtros`);

    // ============================================================
    // PASO 7: Obtener asistencias desde MongoDB
    // ============================================================
    console.log("\n💾 === PASO 7: Obteniendo asistencias desde MongoDB ===");

    const gradosAConsultar =
      aulasSeleccionadas.Grado === "T"
        ? aulasSeleccionadas.Nivel === NivelEducativo.PRIMARIA
          ? [1, 2, 3, 4, 5, 6]
          : [1, 2, 3, 4, 5]
        : [aulasSeleccionadas.Grado];

    const mesesAConsultar: number[] = [];
    for (let mes = rangoTiempo.DesdeMes; mes <= rangoTiempo.HastaMes; mes++) {
      mesesAConsultar.push(mes);
    }

    console.log(`📊 Consultando grados: ${gradosAConsultar.join(", ")}`);
    console.log(`📊 Consultando meses: ${mesesAConsultar.join(", ")}`);

    const todasLasAsistencias = [];
    for (const grado of gradosAConsultar) {
      const asistencias = await obtenerAsistenciasEstudiantesPorRango(
        aulasSeleccionadas.Nivel,
        grado as number,
        mesesAConsultar
      );
      todasLasAsistencias.push(...asistencias);
    }

    console.log(
      `✅ ${todasLasAsistencias.length} registros de asistencia obtenidos`
    );

    // ============================================================
    // PASO 8: Procesar asistencias y generar reporte
    // ============================================================
    console.log(
      "\n📊 === PASO 8: Procesando asistencias y generando reporte ==="
    );

    let reporteGenerado:
      | ReporteAsistenciaEscolarPorDias
      | ReporteAsistenciaEscolarPorMeses;

    if (tipoReporte === TipoReporteAsistenciaEscolar.POR_DIA) {
      reporteGenerado = generarReportePorDias(
        todasLasAsistencias,
        aulasFiltradas,
        estudiantesMap,
        rangoTiempo,
        toleranciaSegundos,
        aulasSeleccionadas.Nivel
      );
    } else {
      reporteGenerado = generarReportePorMeses(
        todasLasAsistencias,
        aulasFiltradas,
        estudiantesMap,
        rangoTiempo,
        toleranciaSegundos
      );
    }

    console.log("✅ Reporte generado exitosamente");
    console.log(
      `   - ${Object.keys(reporteGenerado).length} aulas en el reporte`
    );

    // ============================================================
    // PASO 9: Subir reporte a Google Drive
    // ============================================================
    console.log("\n☁️ === PASO 9: Subiendo reporte a Google Drive ===");

    const nombreArchivo = `Reporte_${
      payload.Combinacion_Parametros_Reporte
    }_${Date.now()}.json`;
    const resultadoSubida = await uploadJsonToDrive(
      reporteGenerado,
      "Reportes",
      nombreArchivo
    );

    console.log(`✅ Reporte subido a Google Drive`);
    console.log(`   - ID: ${resultadoSubida.id}`);
    console.log(`   - Nombre: ${nombreArchivo}`);

    // ============================================================
    // PASO 10: Actualizar estado en PostgreSQL a DISPONIBLE
    // ============================================================
    console.log(
      "\n✅ === PASO 10: Actualizando estado en PostgreSQL a DISPONIBLE ==="
    );

    await actualizarEstadoReporteAsistenciaEscolar(
      payload.Combinacion_Parametros_Reporte,
      EstadoReporteAsistenciaEscolar.DISPONIBLE,
      resultadoSubida.id
    );

    // ============================================================
    // PASO 11: Actualizar Redis con el reporte completo
    // ============================================================
    console.log(
      "\n💾 === PASO 11: Actualizando Redis con reporte completo ==="
    );

    const reporteCompleto: T_Reportes_Asistencia_Escolar = {
      ...payload,
      Estado_Reporte: EstadoReporteAsistenciaEscolar.DISPONIBLE,
      Datos_Google_Drive_Id: resultadoSubida.id,
    };

    await actualizarReporteAsistenciaEscolarEnRedis(reporteCompleto);

    console.log("\n🎉 Proceso completado exitosamente");
  } catch (error) {
    console.error("❌ Error en el procesamiento:", error);

    // Intentar actualizar el estado a ERROR tanto en PostgreSQL como en Redis
    try {
      if (payloadOriginal) {
        console.log("\n⚠️ Actualizando estado a ERROR...");

        // Actualizar PostgreSQL
        await actualizarEstadoReporteAsistenciaEscolar(
          payloadOriginal.Combinacion_Parametros_Reporte,
          EstadoReporteAsistenciaEscolar.ERROR
        );

        // Actualizar Redis
        const reporteError: T_Reportes_Asistencia_Escolar = {
          ...payloadOriginal,
          Estado_Reporte: EstadoReporteAsistenciaEscolar.ERROR,
        };
        await actualizarReporteAsistenciaEscolarEnRedis(reporteError);

        console.log("✅ Estado actualizado a ERROR en PostgreSQL y Redis");
      }
    } catch (updateError) {
      console.error("❌ No se pudo actualizar el estado a ERROR:", updateError);
    }

    process.exit(1);
  } finally {
    try {
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando proceso...");
    } catch (closeError) {
      console.error("❌ Error al cerrar conexiones:", closeError);
    }
    process.exit(0);
  }
}

// ============================================================
// FUNCIONES AUXILIARES
// ============================================================

function generarReportePorDias(
  asistencias: any[],
  aulas: any[],
  estudiantesMap: Map<string, any>,
  rangoTiempo: any,
  toleranciaSegundos: number,
  nivelEducativo: NivelEducativo
): ReporteAsistenciaEscolarPorDias {
  const reporte: ReporteAsistenciaEscolarPorDias = {};

  // Obtener fecha actual para getDiasDisponiblesPorMes
  const ahora = new Date();
  const diaActual = ahora.getDate();
  const horaActual = ahora.getHours();

  // Agrupar estudiantes por aula
  const estudiantesPorAula = new Map<string, Set<string>>();
  for (const [idEstudiante, estudiante] of estudiantesMap) {
    if (!estudiantesPorAula.has(estudiante.Id_Aula)) {
      estudiantesPorAula.set(estudiante.Id_Aula, new Set());
    }
    estudiantesPorAula.get(estudiante.Id_Aula)!.add(idEstudiante);
  }

  // Crear estructura del reporte por aula
  for (const aula of aulas) {
    const totalEstudiantes = estudiantesPorAula.get(aula.Id_Aula)?.size || 0;

    reporte[aula.Id_Aula] = {
      Total_Estudiante: totalEstudiantes,
      ConteoEstadosAsistencia: {},
    };

    // Inicializar contadores para cada mes y sus días hábiles
    for (let mes = rangoTiempo.DesdeMes; mes <= rangoTiempo.HastaMes; mes++) {
      reporte[aula.Id_Aula].ConteoEstadosAsistencia[mes] = {};

      // Obtener días disponibles del mes
      const diasDisponibles = getDiasDisponiblesPorMes(
        mes,
        diaActual,
        horaActual,
        nivelEducativo
      );

      // Inicializar cada día hábil con contadores en 0
      for (const { numeroDiaDelMes } of diasDisponibles) {
        const dia = numeroDiaDelMes;

        // Validar rango de días específico si aplica
        let incluirDia = true;

        if (rangoTiempo.DesdeDia !== null && mes === rangoTiempo.DesdeMes) {
          if (dia < rangoTiempo.DesdeDia) {
            incluirDia = false;
          }
        }

        if (rangoTiempo.HastaDia !== null && mes === rangoTiempo.HastaMes) {
          if (dia > rangoTiempo.HastaDia) {
            incluirDia = false;
          }
        }

        if (incluirDia) {
          reporte[aula.Id_Aula].ConteoEstadosAsistencia[mes][dia] = {
            [EstadosAsistenciaEscolar.Temprano]: 0,
            [EstadosAsistenciaEscolar.Tarde]: 0,
            [EstadosAsistenciaEscolar.Falta]: 0,
          };
        }
      }
    }
  }

  // Procesar cada registro de asistencia
  for (const registro of asistencias) {
    const estudiante = estudiantesMap.get(registro.Id_Estudiante);
    if (!estudiante) continue;

    const idAula = estudiante.Id_Aula;
    if (!reporte[idAula]) continue;

    const asistenciasMensuales = JSON.parse(registro.Asistencias_Mensuales);
    const mes = registro.Mes;

    // Verificar que el mes esté en el reporte
    if (!reporte[idAula].ConteoEstadosAsistencia[mes]) continue;

    // Procesar cada día del mes
    for (const [diaStr, asistenciaDia] of Object.entries(
      asistenciasMensuales
    )) {
      const dia = parseInt(diaStr, 10);

      // Verificar que el día esté inicializado en el reporte
      if (!reporte[idAula].ConteoEstadosAsistencia[mes][dia]) {
        continue;
      }

      // Determinar el estado de asistencia
      const entrada = (asistenciaDia as any)[ModoRegistro.Entrada];

      if (!entrada || entrada.DesfaseSegundos === null) {
        reporte[idAula].ConteoEstadosAsistencia[mes][dia][
          EstadosAsistenciaEscolar.Falta
        ]++;
      } else if (entrada.DesfaseSegundos > toleranciaSegundos) {
        reporte[idAula].ConteoEstadosAsistencia[mes][dia][
          EstadosAsistenciaEscolar.Tarde
        ]++;
      } else {
        reporte[idAula].ConteoEstadosAsistencia[mes][dia][
          EstadosAsistenciaEscolar.Temprano
        ]++;
      }
    }
  }

  return reporte;
}

function generarReportePorMeses(
  asistencias: any[],
  aulas: any[],
  estudiantesMap: Map<string, any>,
  rangoTiempo: any,
  toleranciaSegundos: number
): ReporteAsistenciaEscolarPorMeses {
  const reporte: ReporteAsistenciaEscolarPorMeses = {};

  // Agrupar estudiantes por aula
  const estudiantesPorAula = new Map<string, Set<string>>();
  for (const [idEstudiante, estudiante] of estudiantesMap) {
    if (!estudiantesPorAula.has(estudiante.Id_Aula)) {
      estudiantesPorAula.set(estudiante.Id_Aula, new Set());
    }
    estudiantesPorAula.get(estudiante.Id_Aula)!.add(idEstudiante);
  }

  // Crear estructura del reporte por aula
  for (const aula of aulas) {
    const totalEstudiantes = estudiantesPorAula.get(aula.Id_Aula)?.size || 0;

    reporte[aula.Id_Aula] = {
      Total_Estudiante: totalEstudiantes,
      ConteoEstadosAsistencia: {},
    };

    // Inicializar contadores para cada mes
    for (let mes = rangoTiempo.DesdeMes; mes <= rangoTiempo.HastaMes; mes++) {
      reporte[aula.Id_Aula].ConteoEstadosAsistencia[mes] = {
        [EstadosAsistenciaEscolar.Temprano]: 0,
        [EstadosAsistenciaEscolar.Tarde]: 0,
        [EstadosAsistenciaEscolar.Falta]: 0,
      };
    }
  }

  // Procesar cada registro de asistencia
  for (const registro of asistencias) {
    const estudiante = estudiantesMap.get(registro.Id_Estudiante);
    if (!estudiante) continue;

    const idAula = estudiante.Id_Aula;
    if (!reporte[idAula]) continue;

    const asistenciasMensuales = JSON.parse(registro.Asistencias_Mensuales);
    const mes = registro.Mes;

    // Procesar cada día del mes
    for (const [diaStr, asistenciaDia] of Object.entries(
      asistenciasMensuales
    )) {
      const dia = parseInt(diaStr, 10);

      // Validar rango de días si aplica
      if (rangoTiempo.DesdeDia !== null && mes === rangoTiempo.DesdeMes) {
        if (dia < rangoTiempo.DesdeDia) {
          continue;
        }
      }

      if (rangoTiempo.HastaDia !== null && mes === rangoTiempo.HastaMes) {
        if (dia > rangoTiempo.HastaDia) {
          continue;
        }
      }

      // Determinar el estado de asistencia
      const entrada = (asistenciaDia as any)[ModoRegistro.Entrada];

      if (!entrada || entrada.DesfaseSegundos === null) {
        reporte[idAula].ConteoEstadosAsistencia[mes][
          EstadosAsistenciaEscolar.Falta
        ]++;
      } else if (entrada.DesfaseSegundos > toleranciaSegundos) {
        reporte[idAula].ConteoEstadosAsistencia[mes][
          EstadosAsistenciaEscolar.Tarde
        ]++;
      } else {
        reporte[idAula].ConteoEstadosAsistencia[mes][
          EstadosAsistenciaEscolar.Temprano
        ]++;
      }
    }
  }

  return reporte;
}

// Ejecutar el script
main();
