import fs from "fs";
import path from "path";
import { getDriveClient } from "../../../../config/GoogleDrive/getDriveClient";

interface UpdateJsonResult {
  exito: boolean;
  error?: string;
}

/**
 * Actualiza el contenido de un archivo JSON existente en Google Drive
 * @param fileId ID del archivo en Google Drive a actualizar
 * @param jsonData El objeto que se convertirá a JSON y reemplazará el contenido
 * @returns Resultado de la operación
 */
export async function updateJsonFileInDrive(
  fileId: string,
  jsonData: any
): Promise<UpdateJsonResult> {
  try {
    console.log(`🔄 Intentando actualizar archivo con ID: ${fileId}`);

    const drive = await getDriveClient();

    // Verificar si el archivo existe
    try {
      await drive.files.get({
        fileId: fileId,
        fields: "id,name",
      });
    } catch (error) {
      console.warn(`⚠️ El archivo con ID ${fileId} no existe en Google Drive`);
      return {
        exito: false,
        error: `Archivo con ID ${fileId} no encontrado`,
      };
    }

    // Convertir el objeto a string JSON
    const jsonContent = JSON.stringify(jsonData, null, 2);

    // Crear un archivo temporal
    const tempFileName = `temp_update_${Date.now()}.json`;
    const tempFilePath = path.join(__dirname, tempFileName);

    try {
      fs.writeFileSync(tempFilePath, jsonContent);

      // Configurar la solicitud de actualización
      const media = {
        mimeType: "application/json",
        body: fs.createReadStream(tempFilePath),
      };

      // Actualizar el archivo
      await drive.files.update({
        fileId: fileId,
        media: media,
        fields: "id,modifiedTime",
      });

      console.log(`✅ Archivo con ID ${fileId} actualizado correctamente`);

      return {
        exito: true,
      };
    } finally {
      // Eliminar el archivo temporal (siempre se ejecuta)
      try {
        if (fs.existsSync(tempFilePath)) {
          fs.unlinkSync(tempFilePath);
        }
      } catch (cleanupError) {
        console.warn(
          `⚠️ No se pudo eliminar archivo temporal: ${tempFileName}`
        );
      }
    }
  } catch (error) {
    console.error(
      "❌ Error al actualizar archivo JSON en Google Drive:",
      error
    );
    return {
      exito: false,
      error: error instanceof Error ? error.message : String(error),
    };
  }
}
