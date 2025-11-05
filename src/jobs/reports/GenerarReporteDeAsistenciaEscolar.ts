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
import decodificarCombinacionParametrosParaReporteEscolar from "../../core/utils/helpers/decoders/reportes-asistencia-escolares/decodificarCombinacionParametrosParaReporteEscolar";
import { registrarReporteAsistenciaEscolar } from "../../core/databases/queries/RDP02/reportes-asistencias-escolares/registrarReporteAsistenciaEscolar";
import { actualizarEstadoReporteAsistenciaEscolar } from "../../core/databases/queries/RDP02/reportes-asistencias-escolares/actualizarEstadoReporteAsistenciaEscolar";
import { obtenerConfiguracionesToleranciasTardanza } from "../../core/databases/queries/RDP02/ajustes-generales/obtenerConfiguracionesToleranciasTardanza";
import { obtenerAsistenciasEstudiantesPorRango } from "../../core/databases/queries/RDP03/asistencias-escolares/obtenerAsistenciasEstudiantesPorRango";
import { obtenerDatosEstudiantesYAulasDesdeGoogleDrive } from "../../core/databases/queries/RDP01/obtenerDatosEstudiantesYAulasDesdeGoogleDrive";

/**
 * Función principal del script
 */
async function main() {
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

    let payload: T_Reportes_Asistencia_Escolar;
    try {
      payload = JSON.parse(args[0]);
    } catch (error) {
      console.error("❌ Error: El payload no es un JSON válido");
      process.exit(1);
    }

    console.log("📋 Payload recibido:", payload);

    // ============================================================
    // PASO 2: Registrar el reporte en PostgreSQL con estado PENDIENTE
    // ============================================================
    console.log("\n📝 === PASO 2: Registrando reporte en PostgreSQL ===");

    await registrarReporteAsistenciaEscolar(payload);

    // ============================================================
    // PASO 3: Decodificar parámetros del reporte
    // ============================================================
    console.log("\n🔍 === PASO 3: Decodificando parámetros del reporte ===");

    const parametrosDecodificados =
      decodificarCombinacionParametrosParaReporteEscolar(
        payload.Combinacion_Parametros_Reporte
      );

    if (!parametrosDecodificados) {
      console.error(
        "❌ Error: No se pudieron decodificar los parámetros del reporte"
      );
      await actualizarEstadoReporteAsistenciaEscolar(
        payload.Combinacion_Parametros_Reporte,
        EstadoReporteAsistenciaEscolar.ERROR
      );
      process.exit(1);
    }

    console.log("✅ Parámetros decodificados:", parametrosDecodificados);

    const { tipoReporte, rangoTiempo, aulasSeleccionadas } =
      parametrosDecodificados;

    // ============================================================
    // PASO 4: Obtener configuraciones de tolerancia
    // ============================================================
    console.log(
      "\n⚙️ === PASO 4: Obteniendo configuraciones de tolerancia ==="
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
    // PASO 5: Obtener datos de estudiantes y aulas desde Google Drive
    // ============================================================
    console.log(
      "\n📂 === PASO 5: Obteniendo datos de estudiantes y aulas desde Google Drive ==="
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
      // Filtrar por grado
      if (
        aulasSeleccionadas.Grado !== "T" &&
        aula.Grado !== aulasSeleccionadas.Grado
      ) {
        return false;
      }

      // Filtrar por sección
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
    // PASO 6: Obtener asistencias desde MongoDB
    // ============================================================
    console.log("\n💾 === PASO 6: Obteniendo asistencias desde MongoDB ===");

    // Determinar qué grados consultar
    const gradosAConsultar =
      aulasSeleccionadas.Grado === "T"
        ? aulasSeleccionadas.Nivel === NivelEducativo.PRIMARIA
          ? [1, 2, 3, 4, 5, 6]
          : [1, 2, 3, 4, 5]
        : [aulasSeleccionadas.Grado];

    // Determinar qué meses consultar
    const mesesAConsultar: number[] = [];
    for (let mes = rangoTiempo.DesdeMes; mes <= rangoTiempo.HastaMes; mes++) {
      mesesAConsultar.push(mes);
    }

    console.log(`📊 Consultando grados: ${gradosAConsultar.join(", ")}`);
    console.log(`📊 Consultando meses: ${mesesAConsultar.join(", ")}`);

    // Obtener todas las asistencias necesarias
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
    // PASO 7: Procesar asistencias y generar reporte
    // ============================================================
    console.log(
      "\n📊 === PASO 7: Procesando asistencias y generando reporte ==="
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
        toleranciaSegundos
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
    // PASO 8: Subir reporte a Google Drive
    // ============================================================
    console.log("\n☁️ === PASO 8: Subiendo reporte a Google Drive ===");

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
    // PASO 9: Actualizar estado del reporte a DISPONIBLE
    // ============================================================
    console.log(
      "\n✅ === PASO 9: Actualizando estado del reporte a DISPONIBLE ==="
    );

    await actualizarEstadoReporteAsistenciaEscolar(
      payload.Combinacion_Parametros_Reporte,
      EstadoReporteAsistenciaEscolar.DISPONIBLE,
      resultadoSubida.id
    );

    console.log("\n🎉 Proceso completado exitosamente");
  } catch (error) {
    console.error("❌ Error en el procesamiento:", error);

    // Intentar actualizar el estado a ERROR si es posible
    try {
      const args = process.argv.slice(2);
      if (args.length > 0) {
        const payload = JSON.parse(args[0]);
        await actualizarEstadoReporteAsistenciaEscolar(
          payload.Combinacion_Parametros_Reporte,
          EstadoReporteAsistenciaEscolar.ERROR
        );
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
  toleranciaSegundos: number
): ReporteAsistenciaEscolarPorDias {
  const reporte: ReporteAsistenciaEscolarPorDias = {};

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
      reporte[aula.Id_Aula].ConteoEstadosAsistencia[mes] = {};
    }
  }

  // Procesar cada registro de asistencia
  for (const registro of asistencias) {
    const estudiante = estudiantesMap.get(registro.Id_Estudiante);
    if (!estudiante) continue;

    const idAula = estudiante.Id_Aula;
    if (!reporte[idAula]) continue;

    const asistenciasMensuales = JSON.parse(registro.Asistencias_Mensuales);

    // Procesar cada día del mes
    for (const [diaStr, asistenciaDia] of Object.entries(
      asistenciasMensuales
    )) {
      const dia = parseInt(diaStr, 10);
      const mes = registro.Mes;

      // Validar rango de días si aplica
      if (rangoTiempo.DesdeDia !== null && rangoTiempo.HastaDia !== null) {
        if (mes === rangoTiempo.DesdeMes && dia < rangoTiempo.DesdeDia) {
          continue;
        }
        if (mes === rangoTiempo.HastaMes && dia > rangoTiempo.HastaDia) {
          continue;
        }
      }

      // Inicializar contadores del día si no existen
      if (!reporte[idAula].ConteoEstadosAsistencia[mes][dia]) {
        reporte[idAula].ConteoEstadosAsistencia[mes][dia] = {
          [EstadosAsistenciaEscolar.Temprano]: 0,
          [EstadosAsistenciaEscolar.Tarde]: 0,
          [EstadosAsistenciaEscolar.Falta]: 0,
        };
      }

      // Determinar el estado de asistencia
      const entrada = (asistenciaDia as any)[ModoRegistro.Entrada];

      if (!entrada || entrada.DesfaseSegundos === null) {
        // Falta
        reporte[idAula].ConteoEstadosAsistencia[mes][dia][
          EstadosAsistenciaEscolar.Falta
        ]++;
      } else if (entrada.DesfaseSegundos > toleranciaSegundos) {
        // Tardanza (superó la tolerancia)
        reporte[idAula].ConteoEstadosAsistencia[mes][dia][
          EstadosAsistenciaEscolar.Tarde
        ]++;
      } else {
        // Temprano o puntual (dentro de la tolerancia)
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
      if (rangoTiempo.DesdeDia !== null && rangoTiempo.HastaDia !== null) {
        if (mes === rangoTiempo.DesdeMes && dia < rangoTiempo.DesdeDia) {
          continue;
        }
        if (mes === rangoTiempo.HastaMes && dia > rangoTiempo.HastaDia) {
          continue;
        }
      }

      // Determinar el estado de asistencia
      const entrada = (asistenciaDia as any)[ModoRegistro.Entrada];

      if (!entrada || entrada.DesfaseSegundos === null) {
        // Falta
        reporte[idAula].ConteoEstadosAsistencia[mes][
          EstadosAsistenciaEscolar.Falta
        ]++;
      } else if (entrada.DesfaseSegundos > toleranciaSegundos) {
        // Tardanza (superó la tolerancia)
        reporte[idAula].ConteoEstadosAsistencia[mes][
          EstadosAsistenciaEscolar.Tarde
        ]++;
      } else {
        // Temprano o puntual (dentro de la tolerancia)
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
