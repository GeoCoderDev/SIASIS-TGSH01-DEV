import { MongoOperation } from "../../../../../interfaces/shared/RDP03/MongoOperation";
import RDP03_DB_INSTANCES from "../../../connectors/mongodb";

/**
 * Actualiza la contraseña de un profesor de primaria en RDP03
 * PROFESORES PRIMARIA ESTÁN EN RDP02 Y RDP03
 */
export async function actualizarContrasenaProfesorPrimariaRDP03(
  nombreUsuario: string,
  contrasenaEncriptada: string
): Promise<boolean> {
  try {
    const operation: MongoOperation = {
      operation: "updateOne",
      collection: "T_Profesores_Primaria",
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
      console.log(`⚠️  No se pudo actualizar Profesor Primaria en RDP03: ${nombreUsuario}`);
      return false;
    }

    console.log(`✅ Contraseña actualizada en RDP03 para Profesor Primaria: ${nombreUsuario}`);
    return true;
  } catch (error) {
    console.error(`❌ Error al actualizar contraseña de Profesor Primaria ${nombreUsuario} en RDP03:`, error);
    throw error;
  }
}