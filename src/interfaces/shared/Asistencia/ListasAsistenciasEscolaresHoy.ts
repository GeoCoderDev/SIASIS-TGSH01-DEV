import { AsistenciaEscolarDeUnDia } from "../AsistenciasEscolares";

export interface ListaAsistenciasEscolaresHoy {
                                        //Id_Estudiante : Asistencia De Hoy
  ListasDeAsistenciasEscolaresHoy: Record<string, AsistenciaEscolarDeUnDia>;
  Fecha_Actualizacion: string;
}
