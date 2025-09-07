import { NOMBRE_ARCHIVO_LISTA_ESTUDIANTES } from "../../../constants/NOMBRE_ARCHIVOS_SISTEMA";
import { NivelEducativo } from "../NivelEducativo";

export interface ReporteActualizacionDeListasEstudiantes {
  EstadoDeListasDeEstudiantes: Record<NOMBRE_ARCHIVO_LISTA_ESTUDIANTES, Date>;
  Fecha_Actualizacion: Date;
}


