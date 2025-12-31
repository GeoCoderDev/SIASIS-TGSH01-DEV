import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

/**
 * Actualiza la contraseña de un responsable en RDP03
 * RESPONSABLES SOLO ESTÁN EN RDP03
 */
export async function actualizarContrasenaResponsable(
  nombreUsuario: string,
  contrasenaEncriptada: string
): Promise<boolean> {
  try {
    const operation: MongoOperation = {
      operation: "updateOne",
      collection: "T_Responsables",
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
      console.log(`⚠️  No se pudo actualizar Responsable en RDP03: ${nombreUsuario}`);
      return false;
    }

    console.log(`✅ Contraseña actualizada en RDP03 para Responsable: ${nombreUsuario}`);
    return true;
  } catch (error) {
    console.error(`❌ Error al actualizar contraseña de Responsable ${nombreUsuario} en RDP03:`, error);
    throw error;
  }
}