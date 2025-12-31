import * as crypto from "crypto";

export function encryptProfesorPrimariaPassword(password: string): string {
  const encryptionKey = process.env.ENCRYPTION_KEY_PROFESOR_PRIMARIA;

  if (!encryptionKey) {
    throw new Error(
      "La llave de encriptación para Profesores de Primaria no está definida en las variables de entorno"
    );
  }

  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(
    "aes-256-cbc",
    crypto.createHash("sha256").update(encryptionKey).digest(),
    iv
  );

  let encrypted = cipher.update(password, "utf8", "hex");
  encrypted += cipher.final("hex");

  return iv.toString("hex") + ":" + encrypted;
}
