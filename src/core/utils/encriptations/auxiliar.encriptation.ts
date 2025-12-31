import * as crypto from "crypto";

export function encryptAuxiliarPassword(password: string): string {
  const encryptionKey = process.env.ENCRYPTION_KEY_AUXILIAR;

  if (!encryptionKey) {
    throw new Error(
      "La llave de encriptación para Auxiliares no está definida en las variables de entorno"
    );
  }

  // Crear un IV aleatorio
  const iv = crypto.randomBytes(16);

  // Crear el cifrador
  const cipher = crypto.createCipheriv(
    "aes-256-cbc",
    crypto.createHash("sha256").update(encryptionKey).digest(),
    iv
  );

  // Encriptar la contraseña
  let encrypted = cipher.update(password, "utf8", "hex");
  encrypted += cipher.final("hex");

  // Devolver IV + contraseña encriptada (ambos en hex)
  return iv.toString("hex") + ":" + encrypted;
}
