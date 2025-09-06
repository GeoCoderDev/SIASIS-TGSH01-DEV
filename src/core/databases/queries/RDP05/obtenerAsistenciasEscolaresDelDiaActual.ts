import { TipoAsistencia } from "../../../../interfaces/shared/AsistenciaRequests";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { ModoRegistro } from "../../../../interfaces/shared/ModoRegistroPersonal";

import { redisClient } from "../../../../config/Redis/RedisClient";
import {
  AsistenciaEscolarDeUnDia,
  DetalleAsistenciaEscolar,
} from "../../../../interfaces/shared/AsistenciasEscolares";

/**
 * Obtiene todas las asistencias del día actual desde Redis filtradas por nivel y grado
 */
export async function obtenerAsistenciasEscolaresDelDiaActual(
  nivel: NivelEducativo,
  grado: number,
  fechaActualizacion: Date
): Promise<Record<string, AsistenciaEscolarDeUnDia>> {
  try {
    console.log(
      `🔍 Obteniendo asistencias de ${nivel} grado ${grado} desde Redis...`
    );

    // Determinar el tipo de asistencia según el nivel
    const tipoAsistencia =
      nivel === NivelEducativo.PRIMARIA
        ? TipoAsistencia.ParaEstudiantesPrimaria
        : TipoAsistencia.ParaEstudiantesSecundaria;

    const redisClientInstance = redisClient(tipoAsistencia);
    const todasLasClaves = await redisClientInstance.keys("*");

    console.log(
      `🔑 Total de claves encontradas en Redis: ${todasLasClaves.length}`
    );

    // Filtrar claves que correspondan al día, nivel y grado específicos
    const fechaStr = fechaActualizacion.toISOString().split("T")[0]; // YYYY-MM-DD
    const nivelCode = nivel === NivelEducativo.PRIMARIA ? "P" : "S";

    const clavesFiltradas = todasLasClaves.filter((clave) => {
      // Formato esperado: 2025-08-29:E:E:S:1:A:77742971
      const partes = clave.split(":");

      if (partes.length !== 7) return false;

      const [
        fecha,
        modoRegistro,
        actor,
        nivelClave,
        gradoClave,
        seccion,
        idEstudiante,
      ] = partes;

      return (
        fecha === fechaStr &&
        actor === "E" &&
        nivelClave === nivelCode &&
        parseInt(gradoClave, 10) === grado
      );
    });

    console.log(
      `🎯 Claves filtradas para ${nivel} grado ${grado}: ${clavesFiltradas.length}`
    );

    if (clavesFiltradas.length === 0) {
      console.log("ℹ️ No se encontraron asistencias para este nivel y grado");
      return {};
    }

    // Procesar las claves filtradas para construir el objeto de asistencias
    const asistenciasPorEstudiante: Record<string, AsistenciaEscolarDeUnDia> =
      {};

    for (const clave of clavesFiltradas) {
      try {
        const partes = clave.split(":");
        const [
          fecha,
          modoRegistro,
          actor,
          nivelClave,
          gradoClave,
          seccion,
          idEstudiante,
        ] = partes;

        // Obtener valor desde Redis
        const valor = await redisClientInstance.get(clave);
        if (!valor || !Array.isArray(valor) || valor.length === 0) {
          console.warn(`⚠️ Valor inválido para clave ${clave}: ${valor}`);
          continue;
        }

        const desfaseSegundos = parseInt(valor[0], 10);
        if (isNaN(desfaseSegundos)) {
          console.warn(`⚠️ Desfase inválido para clave ${clave}: ${valor[0]}`);
          continue;
        }

        // Crear detalle de asistencia
        const detalleAsistencia: DetalleAsistenciaEscolar = {
          DesfaseSegundos: desfaseSegundos,
        };

        // ✅ CAMBIO: Inicializar asistencia del estudiante si no existe (SIN propiedades null)
        if (!asistenciasPorEstudiante[idEstudiante]) {
          asistenciasPorEstudiante[idEstudiante] = {
            [ModoRegistro.Entrada]: null,
          };
        }

        // ✅ CAMBIO: Solo asignar las propiedades que realmente existen
        if (modoRegistro === ModoRegistro.Entrada) {
          asistenciasPorEstudiante[idEstudiante][ModoRegistro.Entrada] =
            detalleAsistencia;
        } else if (modoRegistro === ModoRegistro.Salida) {
          asistenciasPorEstudiante[idEstudiante][ModoRegistro.Salida] =
            detalleAsistencia;
        }
      } catch (error) {
        console.error(`❌ Error procesando clave ${clave}:`, error);
      }
    }

    console.log(
      `✅ Procesadas asistencias de ${
        Object.keys(asistenciasPorEstudiante).length
      } estudiantes`
    );

    // Mostrar resumen por tipo de registro
    let contadorEntradas = 0;
    let contadorSalidas = 0;

    Object.values(asistenciasPorEstudiante).forEach((asistencia) => {
      if (asistencia[ModoRegistro.Entrada]) contadorEntradas++;
      if (asistencia[ModoRegistro.Salida]) contadorSalidas++;
    });

    console.log(
      `📊 Resumen: ${contadorEntradas} entradas, ${contadorSalidas} salidas`
    );

    return asistenciasPorEstudiante;
  } catch (error) {
    console.error(
      "❌ Error obteniendo asistencias del día actual desde Redis:",
      error
    );
    // En caso de error, retornar objeto vacío para mantener resiliencia
    return {};
  }
}
