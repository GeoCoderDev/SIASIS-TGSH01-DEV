import { T_Reportes_Asistencia_Escolar } from "@prisma/client";
import { NivelEducativo } from "./NivelEducativo";
import { EstadosAsistenciaEscolar } from "./EstadosAsistenciaEstudiantes";

export enum TipoReporteAsistenciaEscolar {
  POR_DIA = "D",
  POR_MES = "M",
}

export enum EstadoReporteAsistenciaEscolar {
  PENDIENTE = "P",
  DISPONIBLE = "D",
  ERROR = "E",
}

export interface RangoTiempoReporteAsistenciasEscolares {
  DesdeMes: number;
  DesdeDia: number | null;

  HastaMes: number;
  HastaDia: number | null;
}

export interface AulasSeleccionadasParaReporteAsistenciaEscolar {
  Nivel: NivelEducativo;
  Grado: number | "T" | "";
  Seccion: `${string}` | "T" | "";
}

export type ReporteAsistenciaEscolarAnonimo = Pick<
  T_Reportes_Asistencia_Escolar,
  | "Combinacion_Parametros_Reporte"
  | "Datos_Google_Drive_Id"
  | "Estado_Reporte"
  | "Fecha_Generacion"
>;

export interface ConteoEstadosAsistenciaEscolarPorAula {
  [EstadosAsistenciaEscolar.Temprano]: number;
  [EstadosAsistenciaEscolar.Tarde]: number;
  [EstadosAsistenciaEscolar.Falta]: number;
}

//{Id_Aula : { Mes: { Dia: } }}
export type ReporteAsistenciaEscolarPorDias = Record<
  number,
  {
    Total_Estudiante: number;
    ConteoEstadosAsistencia: Record<
      number,
      Record<number, ConteoEstadosAsistenciaEscolarPorAula>
    >;
  }
>;

//{Id_Aula : { Mes: { ..... } }}
export type ReporteAsistenciaEscolarPorMeses = Record<
  number,
  {
    Total_Estudiante: number;
    ConteoEstadosAsistencia: Record<
      number,
      ConteoEstadosAsistenciaEscolarPorAula
    >;
  }
>;
