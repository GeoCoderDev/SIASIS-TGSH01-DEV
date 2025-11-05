import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import {
  AulasSeleccionadasParaReporteAsistenciaEscolar,
  RangoTiempoReporteAsistenciasEscolares,
  TipoReporteAsistenciaEscolar,
} from "../../../../../interfaces/shared/ReporteAsistenciaEscolar";
import { decodificarCaracterANumero } from "../decodificarCaracterANumero";

export interface ParametrosDecodificadosCombinacionReporteEscolar {
  tipoReporte: TipoReporteAsistenciaEscolar;
  rangoTiempo: RangoTiempoReporteAsistenciasEscolares;
  aulasSeleccionadas: AulasSeleccionadasParaReporteAsistenciaEscolar;
}

/**
 * Decodifica una cadena de combinación de parámetros para reporte escolar
 * @param combinacionCodificada - String codificado que representa los parámetros del reporte
 * @returns Objeto con los parámetros decodificados o false si la cadena no es válida
 */
const decodificarCombinacionParametrosParaReporteEscolar = (
  combinacionCodificada: string
): ParametrosDecodificadosCombinacionReporteEscolar | false => {
  try {
    // Validar que la cadena no esté vacía
    if (!combinacionCodificada || combinacionCodificada.length === 0) {
      return false;
    }

    // Extraer el tipo de reporte (primer carácter)
    const tipoReporte =
      combinacionCodificada[0] as TipoReporteAsistenciaEscolar;

    // Validar que el tipo de reporte sea válido
    if (!Object.values(TipoReporteAsistenciaEscolar).includes(tipoReporte)) {
      return false;
    }

    let posicion = 1; // Empezamos después del tipo de reporte

    if (tipoReporte === TipoReporteAsistenciaEscolar.POR_DIA) {
      // Formato: D + MesDesde(1) + DiaDesde(1) + MesHasta(1) + DiaHasta(1) + Nivel(1) + Grado(1) + Seccion(1)
      // Longitud mínima esperada: 8 caracteres
      if (combinacionCodificada.length < 8) {
        return false;
      }

      // Decodificar MesDesde (posición 1)
      const mesDesdeCodificado = combinacionCodificada[posicion];
      const mesDesde = decodificarCaracterANumero(mesDesdeCodificado);
      if (
        mesDesde === null ||
        (mesDesde as number) < 1 ||
        (mesDesde as number) > 12
      ) {
        return false;
      }
      posicion++;

      // Decodificar DiaDesde (posición 2)
      const diaDesdeCodificado = combinacionCodificada[posicion];
      const diaDesde = decodificarCaracterANumero(diaDesdeCodificado);
      if (diaDesde === null || diaDesde < 1 || diaDesde > 31) {
        return false;
      }
      posicion++;

      // Decodificar MesHasta (posición 3)
      const mesHastaCodificado = combinacionCodificada[posicion];
      const mesHasta = decodificarCaracterANumero(mesHastaCodificado);
      if (mesHasta === null || mesHasta < 1 || mesHasta > 12) {
        return false;
      }
      posicion++;

      // Decodificar DiaHasta (posición 4)
      const diaHastaCodificado = combinacionCodificada[posicion];
      const diaHasta = decodificarCaracterANumero(diaHastaCodificado);
      if (diaHasta === null || diaHasta < 1 || diaHasta > 31) {
        return false;
      }
      posicion++;

      // Extraer Nivel (posición 5) - 1 carácter
      const nivel = combinacionCodificada[posicion];
      if (
        !nivel ||
        !Object.values(NivelEducativo).includes(nivel as NivelEducativo)
      ) {
        return false;
      }
      posicion++;

      // Extraer Grado (posición 6) - 1 carácter (puede ser número 1-6 o "T" para todos)
      const gradoStr = combinacionCodificada[posicion];
      let grado: number | string;

      if (gradoStr === "T") {
        grado = "T";
      } else {
        const gradoNum = parseInt(gradoStr, 10);
        if (isNaN(gradoNum) || gradoNum < 1 || gradoNum > 6) {
          return false;
        }
        grado = gradoNum;
      }
      posicion++;

      // Extraer Sección (posición 7) - 1 carácter (letra A-Z o "T" para todas)
      const seccion = combinacionCodificada[posicion];
      if (!seccion || (!/^[A-Z]$/.test(seccion) && seccion !== "T")) {
        return false;
      }

      return {
        tipoReporte,
        rangoTiempo: {
          DesdeMes: mesDesde as number,
          DesdeDia: diaDesde,
          HastaMes: mesHasta as number,
          HastaDia: diaHasta,
        },
        aulasSeleccionadas: {
          Nivel: nivel as NivelEducativo,
          Grado: grado as number | "T",
          Seccion: seccion,
        },
      };
    } else {
      // TipoReporteAsistenciaEscolar.POR_MES
      // Formato: M + MesDesde(1) + MesHasta(1) + Nivel(1) + Grado(1) + Seccion(1)
      // Longitud mínima esperada: 6 caracteres
      if (combinacionCodificada.length < 6) {
        return false;
      }

      // Decodificar MesDesde (posición 1)
      const mesDesdeCodificado = combinacionCodificada[posicion];
      const mesDesde = decodificarCaracterANumero(mesDesdeCodificado);
      if (mesDesde === null || mesDesde < 1 || mesDesde > 12) {
        return false;
      }
      posicion++;

      // Decodificar MesHasta (posición 2)
      const mesHastaCodificado = combinacionCodificada[posicion];
      const mesHasta = decodificarCaracterANumero(mesHastaCodificado);
      if (mesHasta === null || mesHasta < 1 || mesHasta > 12) {
        return false;
      }
      posicion++;

      // Extraer Nivel (posición 3) - 1 carácter
      const nivel = combinacionCodificada[posicion];
      if (
        !nivel ||
        !Object.values(NivelEducativo).includes(nivel as NivelEducativo)
      ) {
        return false;
      }
      posicion++;

      // Extraer Grado (posición 4) - 1 carácter (puede ser número 1-6 o "T" para todos)
      const gradoStr = combinacionCodificada[posicion];
      let grado: number | string;

      if (gradoStr === "T") {
        grado = "T";
      } else {
        const gradoNum = parseInt(gradoStr, 10);
        if (isNaN(gradoNum) || gradoNum < 1 || gradoNum > 6) {
          return false;
        }
        grado = gradoNum;
      }
      posicion++;

      // Extraer Sección (posición 5) - 1 carácter (letra A-Z o "T" para todas)
      const seccion = combinacionCodificada[posicion];
      if (!seccion || (!/^[A-Z]$/.test(seccion) && seccion !== "T")) {
        return false;
      }

      return {
        tipoReporte,
        rangoTiempo: {
          DesdeMes: mesDesde,
          DesdeDia: null,
          HastaMes: mesHasta,
          HastaDia: null,
        },
        aulasSeleccionadas: {
          Nivel: nivel as NivelEducativo,
          Grado: grado as number | "T",
          Seccion: seccion,
        },
      };
    }
  } catch (error) {
    console.error("Error al decodificar combinación de parámetros:", error);
    return false;
  }
};

export default decodificarCombinacionParametrosParaReporteEscolar;
