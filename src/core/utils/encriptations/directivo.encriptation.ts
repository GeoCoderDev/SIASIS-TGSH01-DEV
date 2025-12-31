import * as crypto from "crypto";

/**
 * Encripta una contraseña usando AES-256-CBC con una llave específica
 * @param {string} password - La contraseña en texto plano a encriptar
 * @returns {string} - La contraseña encriptada en formato hexadecimal
 */
export function encryptDirectivoPassword(password: string): string {
  const encryptionKey = process.env.ENCRYPTION_KEY_DIRECTIVO;

  if (!encryptionKey) {
    throw new Error(
      "La llave de encriptación no está definida en las variables de entorno"
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

