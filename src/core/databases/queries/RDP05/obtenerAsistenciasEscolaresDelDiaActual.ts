import { TipoAsistencia } from "../../../../interfaces/shared/AsistenciaRequests";
import { NivelEducativo } from "../../../../interfaces/shared/NivelEducativo";
import { ModoRegistro } from "../../../../interfaces/shared/ModoRegistroPersonal";

import { redisClient } from "../../../../config/Redis/RedisClient";
import {
  AsistenciaEscolarDeUnDia,
  DetalleAsistenciaEscolar,
} from "../../../../interfaces/shared/AsistenciasEscolares";

/**
 * Obtiene todas las asistencias del día actual desde Redis filtradas por nivel, grado y opcionalmente por sección
 */
export async function obtenerAsistenciasEscolaresDelDiaActual(
  nivel: NivelEducativo,
  grado: number,
  fechaLocalPeru: Date,
  seccionEspecifica?: string
): Promise<Record<string, Record<string, AsistenciaEscolarDeUnDia>>> {
  try {
    console.log(
      `🔍 Obteniendo asistencias de ${nivel} grado ${grado}${
        seccionEspecifica
          ? ` sección ${seccionEspecifica}`
          : " (todas las secciones)"
      } desde Redis...`
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

    // Filtrar claves que correspondan al día, nivel, grado y sección específicos
    const fechaStr = fechaLocalPeru.toISOString().split("T")[0]; // YYYY-MM-DD
    const nivelCode = nivel === NivelEducativo.PRIMARIA ? "P" : "S";

    const clavesFiltradas = todasLasClaves.filter((clave) => {
      // Formato esperado: fecha:ModoRegistro:Actor:Nivel:Grado:Seccion:IdEstudiante
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

      const cumpleFiltrosBasicos =
        fecha === fechaStr &&
        actor === "E" &&
        nivelClave === nivelCode &&
        parseInt(gradoClave, 10) === grado;

      // Si se especifica una sección, filtrar por ella; si no, incluir todas
      const cumpleFiltroSeccion = seccionEspecifica
        ? seccion === seccionEspecifica
        : true;

      return cumpleFiltrosBasicos && cumpleFiltroSeccion;
    });

    console.log(
      `🎯 Claves filtradas para ${nivel} grado ${grado}${
        seccionEspecifica ? ` sección ${seccionEspecifica}` : ""
      }: ${clavesFiltradas.length}`
    );

    if (clavesFiltradas.length === 0) {
      console.log(
        "ℹ️ No se encontraron asistencias para los filtros especificados"
      );
      return {};
    }

    // Procesar las claves filtradas para construir el objeto de asistencias agrupado por sección
    const asistenciasPorSeccion: Record<
      string,
      Record<string, AsistenciaEscolarDeUnDia>
    > = {};

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

        // ✅ Inicializar sección si no existe
        if (!asistenciasPorSeccion[seccion]) {
          asistenciasPorSeccion[seccion] = {};
        }

        // ✅ Inicializar asistencia del estudiante en la sección si no existe
        if (!asistenciasPorSeccion[seccion][idEstudiante]) {
          asistenciasPorSeccion[seccion][idEstudiante] =
            {} as AsistenciaEscolarDeUnDia;
        }

        // ✅ Asignar según el modo de registro (acumulando entradas y salidas)
        if (modoRegistro === ModoRegistro.Entrada) {
          asistenciasPorSeccion[seccion][idEstudiante][ModoRegistro.Entrada] =
            detalleAsistencia;
        } else if (modoRegistro === ModoRegistro.Salida) {
          asistenciasPorSeccion[seccion][idEstudiante][ModoRegistro.Salida] =
            detalleAsistencia;
        }
      } catch (error) {
        console.error(`❌ Error procesando clave ${clave}:`, error);
      }
    }

    console.log(
      `✅ Procesadas asistencias de ${
        Object.keys(asistenciasPorSeccion).length
      } secciones`
    );

    // Mostrar resumen por sección
    let totalEstudiantes = 0;
    let totalEntradas = 0;
    let totalSalidas = 0;

    Object.entries(asistenciasPorSeccion).forEach(([seccion, estudiantes]) => {
      const numEstudiantes = Object.keys(estudiantes).length;
      totalEstudiantes += numEstudiantes;

      let entradasSeccion = 0;
      let salidasSeccion = 0;

      Object.values(estudiantes).forEach((asistencia) => {
        if (asistencia[ModoRegistro.Entrada]) entradasSeccion++;
        if (asistencia[ModoRegistro.Salida]) salidasSeccion++;
      });

      totalEntradas += entradasSeccion;
      totalSalidas += salidasSeccion;

      console.log(
        `📊 Sección ${seccion}: ${numEstudiantes} estudiantes, ${entradasSeccion} entradas, ${salidasSeccion} salidas`
      );
    });

    console.log(
      `🎯 TOTAL: ${totalEstudiantes} estudiantes, ${totalEntradas} entradas, ${totalSalidas} salidas`
    );

    return asistenciasPorSeccion;
  } catch (error) {
    console.error(
      "❌ Error obteniendo asistencias del día actual desde Redis:",
      error
    );
    // En caso de error, retornar objeto vacío para mantener resiliencia
    return {};
  }
}
