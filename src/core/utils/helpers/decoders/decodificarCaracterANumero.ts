/**
 * Decodifica un carácter a su número correspondiente 1..31 según la regla inversa:
 * - '1'..'9' -> 1..9
 * - 'A'..'V' (o 'a'..'v') -> 10..31
 *
 * Devuelve `number` en 1..31 para entradas válidas o `null` para entradas inválidas.
 */
export const decodificarCaracterANumero = (caracter: string): number | null => {
  if (!caracter || typeof caracter !== "string") return null;

  const char = caracter.trim();
  if (char.length === 0) return null;

  const c = char[0];

  // Dígitos 1..9
  if (c >= "1" && c <= "9") return parseInt(c, 10);

  // Letras A..V (aceptamos mayúsculas y minúsculas)
  const upper = c.toUpperCase();
  if (upper >= "A" && upper <= "V") {
    const code = upper.charCodeAt(0); // 'A' = 65
    const number = 10 + (code - 65);
    // safety check
    if (number >= 10 && number <= 31) return number;
  }

  return null;
};
