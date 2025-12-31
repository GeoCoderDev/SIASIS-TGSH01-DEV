// src/jobs/rollback/RollbackContrasenasUsuarios.ts

import { closeClient } from "../../core/databases/connectors/mongodb";
import { closePool } from "../../core/databases/connectors/postgres";

import { encryptDirectivoPassword } from "../../core/utils/encriptations/directivo.encriptation";
import { encryptProfesorPrimariaPassword } from "../../core/utils/encriptations/profesorPrimaria.encriptation";
import { encryptAuxiliarPassword } from "../../core/utils/encriptations/auxiliar.encriptation";
import { encryptProfesorTutorSecundariaPassword } from "../../core/utils/encriptations/profesorTutorSecundaria.encriptation";
import { encryptResponsablePassword } from "../../core/utils/encriptations/responsable.encriptation";
import { encryptPersonalAdministrativoPassword } from "../../core/utils/encriptations/personalAdministrativo.encriptation";
import { RolesSistema } from "../../interfaces/shared/RolesSistema";
import { obtenerIdResponsablePorNombreUsuario } from "../../core/databases/queries/RDP03/responsables/obtenerIdResponsablePorNombreUsuario";
import { obtenerIdDirectivoPorNombreUsuario } from "../../core/databases/queries/RDP02/directivos/obtenerIdDirectivoPorNombreUsuario";
import { obtenerIdPersonalAdministrativoPorNombreUsuario } from "../../core/databases/queries/RDP02/personal-administrativo/obtenerIdPersonalAdministrativoPorNombreUsuario";
import { obtenerIdAuxiliarPorNombreUsuario } from "../../core/databases/queries/RDP02/auxiliares/obtenerIdAuxiliarPorNombreUsuario";
import { obtenerIdProfesorSecundariaPorNombreUsuario } from "../../core/databases/queries/RDP02/profesores-tutores-secundaria/obtenerIdProfesorSecundariaPorNombreUsuario";
import { obtenerIdProfesorPrimariaPorNombreUsuario } from "../../core/databases/queries/RDP02/profesores-primaria/obtenerIdProfesorPrimariaPorNombreUsuario";
import { actualizarContrasenaDirectivo } from "../../core/databases/queries/RDP02/directivos/actualizarContrasenaDirectivo";
import { actualizarContrasenaPersonalAdministrativo } from "../../core/databases/queries/RDP02/personal-administrativo/actualizarContrasenaPersonalAdministrativo";
import { actualizarContrasenaResponsable } from "../../core/databases/queries/RDP03/responsables/actualizarContrasenaResponsable";
import { actualizarContrasenaProfesorPrimariaRDP03 } from "../../core/databases/queries/RDP03/profesores-primaria/actualizarContrasenaProfesorPrimariaRDP03";
import { actualizarContrasenaProfesorSecundariaRDP03 } from "../../core/databases/queries/RDP03/profesores-tutores-secundaria/actualizarContrasenaProfesorSecundaria";
import { actualizarContrasenaAuxiliarRDP03 } from "../../core/databases/queries/RDP03/auxiliares/actualizarContrasenaAuxiliar";
import { actualizarContrasenaProfesorSecundariaRDP02 } from "../../core/databases/queries/RDP02/profesores-tutores-secundaria/actualizarContrasenaProfesorSecundaria";
import { actualizarContrasenaProfesorPrimariaRDP02 } from "../../core/databases/queries/RDP02/profesores-primaria/actualizarContrasenaProfesorPrimaria";
import { actualizarContrasenaAuxiliarRDP02 } from "../../core/databases/queries/RDP02/auxiliares/actualizarContrasenaAuxiliar";

export interface UsuarioParaRollback {
  Rol: RolesSistema;
  Nombre_Usuario: string;
}

export interface PayloadRollbackContrasenas {
  Testing_Key_For_Rollbacks: string;
  usuarios: UsuarioParaRollback[];
}

export interface ResultadoRollbackUsuario {
  Rol: RolesSistema;
  Nombre_Usuario: string;
  Id_Usuario: string;
  Exito: boolean;
  Mensaje: string;
}

/**
 * Valida el secreto de autorización para rollback
 */
function validarSecreto(secretoRecibido: string): boolean {
  const secretoReal = process.env.TESTING_SECRET_KEY_FOR_ROLLBACKS;

  if (!secretoReal) {
    throw new Error(
      "El secreto TESTING_SECRET_KEY_FOR_ROLLBACKS no está configurado en las variables de entorno"
    );
  }

  return secretoRecibido === secretoReal;
}

/**
 * Obtiene la contraseña por defecto según el ID del usuario (primeros 8 caracteres)
 */
function obtenerContrasenaDefault(idUsuario: string): string {
  return idUsuario.substring(0, 8);
}

/**
 * Encripta la contraseña según el rol del usuario
 */
function encriptarContrasenaPorRol(
  contrasena: string,
  rol: RolesSistema
): string {
  switch (rol) {
    case RolesSistema.Directivo:
      return encryptDirectivoPassword(contrasena);

    case RolesSistema.ProfesorPrimaria:
      return encryptProfesorPrimariaPassword(contrasena);

    case RolesSistema.Auxiliar:
      return encryptAuxiliarPassword(contrasena);

    case RolesSistema.ProfesorSecundaria:
    case RolesSistema.Tutor:
      return encryptProfesorTutorSecundariaPassword(contrasena);

    case RolesSistema.Responsable:
      return encryptResponsablePassword(contrasena);

    case RolesSistema.PersonalAdministrativo:
      return encryptPersonalAdministrativoPassword(contrasena);

    default:
      throw new Error(`Rol no soportado para encriptación: ${rol}`);
  }
}

/**
 * Obtiene el ID del usuario según su rol
 * TODAS LAS CONSULTAS SE HACEN EN RDP02, EXCEPTO RESPONSABLES QUE ESTÁN SOLO EN RDP03
 */
async function obtenerIdUsuarioPorRol(
  nombreUsuario: string,
  rol: RolesSistema
): Promise<string | null> {
  switch (rol) {
    case RolesSistema.Directivo:
      return await obtenerIdDirectivoPorNombreUsuario(nombreUsuario);

    case RolesSistema.PersonalAdministrativo:
      return await obtenerIdPersonalAdministrativoPorNombreUsuario(
        nombreUsuario
      );

    case RolesSistema.ProfesorPrimaria:
      return await obtenerIdProfesorPrimariaPorNombreUsuario(nombreUsuario);

    case RolesSistema.ProfesorSecundaria:
    case RolesSistema.Tutor:
      return await obtenerIdProfesorSecundariaPorNombreUsuario(nombreUsuario);

    case RolesSistema.Auxiliar:
      return await obtenerIdAuxiliarPorNombreUsuario(nombreUsuario);

    case RolesSistema.Responsable:
      return await obtenerIdResponsablePorNombreUsuario(nombreUsuario);

    default:
      throw new Error(`Rol no soportado: ${rol}`);
  }
}

/**
 * Actualiza la contraseña en todas las bases de datos necesarias según el rol
 */
async function actualizarContrasenaEnBaseDeDatos(
  nombreUsuario: string,
  rol: RolesSistema,
  contrasenaEncriptada: string
): Promise<void> {
  switch (rol) {
    case RolesSistema.Directivo:
      // SOLO RDP02
      await actualizarContrasenaDirectivo(nombreUsuario, contrasenaEncriptada);
      break;

    case RolesSistema.PersonalAdministrativo:
      // SOLO RDP02
      await actualizarContrasenaPersonalAdministrativo(
        nombreUsuario,
        contrasenaEncriptada
      );
      break;

    case RolesSistema.Responsable:
      // SOLO RDP03
      await actualizarContrasenaResponsable(
        nombreUsuario,
        contrasenaEncriptada
      );
      break;

    case RolesSistema.ProfesorPrimaria:
      // RDP02 + RDP03
      console.log(`🔄 Actualizando Profesor Primaria en RDP02 y RDP03...`);
      await Promise.all([
        actualizarContrasenaProfesorPrimariaRDP02(
          nombreUsuario,
          contrasenaEncriptada
        ),
        actualizarContrasenaProfesorPrimariaRDP03(
          nombreUsuario,
          contrasenaEncriptada
        ),
      ]);
      break;

    case RolesSistema.ProfesorSecundaria:
    case RolesSistema.Tutor:
      // RDP02 + RDP03
      console.log(
        `🔄 Actualizando Profesor Secundaria/Tutor en RDP02 y RDP03...`
      );
      await Promise.all([
        actualizarContrasenaProfesorSecundariaRDP02(
          nombreUsuario,
          contrasenaEncriptada
        ),
        actualizarContrasenaProfesorSecundariaRDP03(
          nombreUsuario,
          contrasenaEncriptada
        ),
      ]);
      break;

    case RolesSistema.Auxiliar:
      // RDP02 + RDP03
      console.log(`🔄 Actualizando Auxiliar en RDP02 y RDP03...`);
      await Promise.all([
        actualizarContrasenaAuxiliarRDP02(nombreUsuario, contrasenaEncriptada),
        actualizarContrasenaAuxiliarRDP03(nombreUsuario, contrasenaEncriptada),
      ]);
      break;

    default:
      throw new Error(`Rol no soportado: ${rol}`);
  }
}

/**
 * Procesa el rollback de contraseña para un usuario individual
 */
async function procesarRollbackUsuario(
  nombreUsuario: string,
  rol: RolesSistema
): Promise<ResultadoRollbackUsuario> {
  const resultado: ResultadoRollbackUsuario = {
    Rol: rol,
    Nombre_Usuario: nombreUsuario,
    Id_Usuario: "",
    Exito: false,
    Mensaje: "",
  };

  try {
    console.log(`\n📋 Procesando usuario: ${nombreUsuario} (Rol: ${rol})`);

    // ============================================================
    // PASO 1: Obtener ID del usuario
    // ============================================================
    const idUsuario = await obtenerIdUsuarioPorRol(nombreUsuario, rol);

    if (!idUsuario) {
      resultado.Mensaje = `Usuario no encontrado en la base de datos`;
      console.log(`❌ ${resultado.Mensaje}`);
      return resultado;
    }

    resultado.Id_Usuario = idUsuario;
    console.log(`✅ ID encontrado: ${idUsuario}`);

    // ============================================================
    // PASO 2: Obtener contraseña por defecto (primeros 8 caracteres del ID)
    // ============================================================
    const contrasenaDefault = obtenerContrasenaDefault(idUsuario);
    console.log(
      `🔑 Contraseña por defecto: ${contrasenaDefault} (primeros 8 chars del ID)`
    );

    // ============================================================
    // PASO 3: Encriptar contraseña según el rol
    // ============================================================
    const contrasenaEncriptada = encriptarContrasenaPorRol(
      contrasenaDefault,
      rol
    );
    console.log(`🔒 Contraseña encriptada correctamente`);

    // ============================================================
    // PASO 4: Actualizar en todas las bases de datos necesarias
    // ============================================================
    await actualizarContrasenaEnBaseDeDatos(
      nombreUsuario,
      rol,
      contrasenaEncriptada
    );

    resultado.Exito = true;
    resultado.Mensaje = `Contraseña restaurada exitosamente`;
    console.log(`✅ ${resultado.Mensaje}`);

    return resultado;
  } catch (error) {
    resultado.Mensaje = `Error al procesar rollback: ${
      error instanceof Error ? error.message : "Error desconocido"
    }`;
    console.error(`❌ ${resultado.Mensaje}`);
    return resultado;
  }
}

/**
 * Función principal del script
 */
async function main() {
  try {
    console.log("🚀 Iniciando rollback de contraseñas de usuarios...\n");

    // ============================================================
    // PASO 1: Validar argumentos de entrada
    // ============================================================
    const args = process.argv.slice(2);
    if (args.length < 1) {
      console.error("❌ Error: Se requiere el payload como JSON");
      console.error(
        'Uso: npm run script -- \'{"Testing_Key_For_Rollbacks":"secreto","usuarios":[...]}\''
      );
      process.exit(1);
    }

    let payload: PayloadRollbackContrasenas;
    try {
      payload = JSON.parse(args[0]);
    } catch (error) {
      console.error("❌ Error: El payload no es un JSON válido");
      process.exit(1);
    }

    console.log(
      `📋 Cantidad de usuarios a procesar: ${payload.usuarios.length}`
    );

    // ============================================================
    // PASO 2: Validar secreto de autorización
    // ============================================================
    console.log("\n🔐 === PASO 2: Validando secreto de autorización ===");

    if (!validarSecreto(payload.Testing_Key_For_Rollbacks)) {
      console.error("❌ ERROR: Secreto de autorización inválido");
      console.error("🚫 ACCESO DENEGADO - Rollback abortado");
      process.exit(1);
    }

    console.log("✅ Secreto de autorización válido - Acceso autorizado");

    // ============================================================
    // PASO 3: Procesar cada usuario
    // ============================================================
    console.log("\n🔄 === PASO 3: Procesando rollback de usuarios ===");

    const resultados: ResultadoRollbackUsuario[] = [];

    for (const usuario of payload.usuarios) {
      const resultado = await procesarRollbackUsuario(
        usuario.Nombre_Usuario,
        usuario.Rol
      );
      resultados.push(resultado);
    }

    // ============================================================
    // PASO 4: Mostrar resumen de resultados
    // ============================================================
    console.log("\n📊 === RESUMEN DE RESULTADOS ===");

    const exitosos = resultados.filter((r) => r.Exito).length;
    const fallidos = resultados.filter((r) => !r.Exito).length;

    console.log(`✅ Exitosos: ${exitosos}`);
    console.log(`❌ Fallidos: ${fallidos}`);
    console.log(`📋 Total procesados: ${resultados.length}`);

    if (fallidos > 0) {
      console.log("\n⚠️  Usuarios con errores:");
      resultados
        .filter((r) => !r.Exito)
        .forEach((r) => {
          console.log(`   - ${r.Nombre_Usuario} (${r.Rol}): ${r.Mensaje}`);
        });
    }

    console.log("\n🎉 Proceso completado");

    // Salir con código de error si hubo fallos
    if (fallidos > 0) {
      process.exit(1);
    }
  } catch (error) {
    console.error("❌ Error fatal en el procesamiento:", error);
    process.exit(1);
  } finally {
    try {
      await Promise.all([closePool(), closeClient()]);
      console.log("🔌 Conexiones cerradas. Finalizando proceso...");
    } catch (closeError) {
      console.error("❌ Error al cerrar conexiones:", closeError);
    }
    process.exit(0);
  }
}

// Ejecutar el script
main();
