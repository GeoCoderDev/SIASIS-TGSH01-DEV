import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

/**
 * Obtiene el ID de un responsable por su nombre de usuario
 */
export async function obtenerIdResponsablePorNombreUsuario(
  nombreUsuario: string
): Promise<string | null> {
  try {
    const operation: MongoOperation = {
      operation: "findOne",
      collection: "T_Responsables",
      filter: {
        Nombre_Usuario: nombreUsuario,
      },
      options: {
        projection: {
          Id_Responsable: "$_id",
          _id: 0,
        },
      },
    };

    const result = await RDP03_DB_INSTANCES.executeMongoDBOperation(operation, {
      useCache: false,
    });

    if (!result) {
      console.log(`⚠️  Responsable no encontrado: ${nombreUsuario}`);
      return null;
    }

    console.log(`✅ ID Responsable obtenido: ${result.Id_Responsable}`);
    return result.Id_Responsable;
  } catch (error) {
    console.error(`❌ Error al obtener ID de Responsable ${nombreUsuario}:`, error);
    throw error;
  }
}