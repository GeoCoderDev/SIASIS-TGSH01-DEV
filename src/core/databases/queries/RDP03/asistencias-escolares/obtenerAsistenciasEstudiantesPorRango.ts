import { NivelEducativo } from "../../../../../interfaces/shared/NivelEducativo";
import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import { RDP03_Nombres_Tablas } from "../../../../../interfaces/shared/RDP03/RDP03_Tablas";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

export interface AsistenciaEstudianteMensual {
  Id_Estudiante: string;
  Mes: number;
  Asistencias_Mensuales: string; // JSON string
}

/**
 * Obtiene las asistencias de estudiantes de un grado específico en un rango de meses
 */
export async function obtenerAsistenciasEstudiantesPorRango(
  nivel: NivelEducativo,
  grado: number,
  mesesArray: number[]
): Promise<AsistenciaEstudianteMensual[]> {
  try {
    // Determinar el nombre de la tabla según nivel y grado
    const nombreTabla: RDP03_Nombres_Tablas =
      nivel === NivelEducativo.PRIMARIA
        ? (`T_A_E_P_${grado}` as RDP03_Nombres_Tablas)
        : (`T_A_E_S_${grado}` as RDP03_Nombres_Tablas);

    console.log(
      `🔍 Consultando asistencias de ${nombreTabla} para meses: ${mesesArray.join(
        ", "
      )}`
    );

    const operation: MongoOperation = {
      operation: "find",
      collection: nombreTabla,
      filter: {
        Mes: { $in: mesesArray },
      },
      options: {
        projection: {
          Id_Estudiante: 1,
          Mes: 1,
          Asistencias_Mensuales: 1,
          _id: 0,
        },
      },
    };

    const result = await RDP03_DB_INSTANCES.executeOperation(operation);

    console.log(
      `✅ ${result.length} registros de asistencia obtenidos de ${nombreTabla}`
    );

    return result || [];
  } catch (error) {
    console.error("❌ Error al obtener asistencias de estudiantes:", error);
    throw error;
  }
}
