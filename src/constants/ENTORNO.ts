import { Entorno } from "../interfaces/shared/Entornos";
import "dotenv/config";

export const ENTORNO: Entorno =
  (process.env.ENTORNO! as Entorno) || Entorno.CERTIFICACION;
