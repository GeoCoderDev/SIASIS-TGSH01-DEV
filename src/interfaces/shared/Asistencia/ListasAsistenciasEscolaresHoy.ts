import { AsistenciaEscolarDeUnDia } from "../AsistenciasEscolares";
import { NivelEducativo } from "../NivelEducativo";

export interface ListaAsistenciasEscolaresHoy {
  // Seccion: {Id_Estudiante : Asistencia De Hoy}
  AsistenciasEscolaresDeHoy: Record<
    string,
    Record<string, AsistenciaEscolarDeUnDia>
  >;
  Fecha_Actualizacion: string;
}

export const NOMBRE_CLAVE_GOOGLE_DRIVE_IDs_LISTAS_ASISTENCIAS_ESCOLARES_HOY =
  "Google_Drive_IDs_Listas_Asistencias_Escolares_Hoy";

export interface GoogleDriveIDsListasAsistenciasEscolaresHoy {
  // GRADO : ID DE GOOGLE DRIVE
  [NivelEducativo.PRIMARIA]?: Record<number, string>;
  [NivelEducativo.SECUNDARIA]?: Record<number, string>;
}

export const NOMBRE_CLAVE_JOBS_EN_EJECUCION_LISTAS_ASISTENCIAS_ESCOLARES_HOY =
  "Jobs_En_Ejecucion_Listas_Asistencias_Escolares_Hoy";

export interface JobsEnEjecucionListasAsistenciasEscolaresHoy {
  [NivelEducativo.PRIMARIA]?: Record<number, boolean>;
  [NivelEducativo.SECUNDARIA]?: Record<number, boolean>;
}
