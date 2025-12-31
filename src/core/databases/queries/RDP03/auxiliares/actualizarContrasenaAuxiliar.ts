// src/core/databases/queries/RDP03/rollback/actualizarContrasenaAuxiliar.ts

import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

/**
 * Actualiza la contraseña de un auxiliar en RDP03
 * AUXILIARES ESTÁN EN RDP02 Y RDP03
 */
export async function actualizarContrasenaAuxiliarRDP03(
  nombreUsuario: string,
  contrasenaEncriptada: string
): Promise<boolean> {
  try {
    const operation: MongoOperation = {
      operation: "updateOne",
      collection: "T_Auxiliares",
      filter: {
        Nombre_Usuario: nombreUsuario,
      },
      data: {
        $set: {
          Contraseña: contrasenaEncriptada,
        },
      },
    };

    const result = await RDP03_DB_INSTANCES.executeMongoDBOperation(operation, {
      useCache: false,
    });

    if (!result || result.matchedCount === 0) {
      console.log(`⚠️  No se pudo actualizar Auxiliar en RDP03: ${nombreUsuario}`);
      return false;
    }

    console.log(`✅ Contraseña actualizada en RDP03 para Auxiliar: ${nombreUsuario}`);
    return true;
  } catch (error) {
    console.error(`❌ Error al actualizar contraseña de Auxiliar ${nombreUsuario} en RDP03:`, error);
    throw error;
  }
}