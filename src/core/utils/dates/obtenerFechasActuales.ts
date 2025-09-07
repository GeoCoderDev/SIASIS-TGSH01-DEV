import { ENTORNO } from "../../../constants/ENTORNO";
import { FECHA_HORA_MOCKEADAS } from "../../../constants/FECHA_HORA_MOCKEADAS";
import { ZONA_HORARIA_LOCAL } from "../../../constants/ZONA_HORARIA_LOCAL";
import { Entorno } from "../../../interfaces/shared/Entornos";
import getRandomAPI03IntanceURL from "../helpers/functions/getRandomAPI03InstanceURL";
import { generarFechaHoraMockeada } from "./mock";

const API03_ACTIVADO_SEGUN_ENTORNO: Record<Entorno, boolean> = {
  [Entorno.LOCAL]: true,
  [Entorno.DESARROLLO]: true,
  [Entorno.CERTIFICACION]: true,
  [Entorno.PRODUCCION]: false,
  [Entorno.TEST]: false,
};

const USAR_API03 = API03_ACTIVADO_SEGUN_ENTORNO[ENTORNO];

const obtenerHoraAPI03 = async (): Promise<Date> => {
  const response = await fetch(
    `${getRandomAPI03IntanceURL()}/api/time?timezone=${ZONA_HORARIA_LOCAL}`
  );

  if (!response.ok) {
    console.error(`Error al obtener hora de API03: ${response.status}`);
    return new Date(); // Retornar la fecha actual en caso de error
  }

  const data = await response.json();
  return new Date(data.serverTime);
};

export async function obtenerFechasActuales() {
  let fechaUTC: Date;

  if (USAR_API03) {
    // Usar la hora de API03 cuando esté habilitado
    fechaUTC = await obtenerHoraAPI03();
  } else {
    // Lógica original: mock en LOCAL o fecha actual del sistema
    fechaUTC =
      ENTORNO === Entorno.LOCAL && FECHA_HORA_MOCKEADAS
        ? generarFechaHoraMockeada(2025, 8, 5, 9, 30, 0) // 12:30 UTC
        : new Date();
  }

  // Para la fecha local de Perú (UTC-5)
  const fechaLocalPeru = new Date(fechaUTC);

  // CORRECCIÓN: Para UTC-5, RESTAMOS 5 horas al timestamp
  // No manipulamos componentes individuales
  fechaLocalPeru.setTime(fechaUTC.getTime() - 5 * 60 * 60 * 1000);

  return { fechaUTC, fechaLocalPeru };
}
