import RDP02_DB_INSTANCES from "../../../connectors/postgres";

export interface ConfiguracionesTolerancias {
  toleranciaTardanzaMinutosPrimaria: number;
  toleranciaTardanzaMinutosSecundaria: number;
}

export async function obtenerConfiguracionesToleranciasTardanza(): Promise<ConfiguracionesTolerancias> {
  try {
    const sql = `
      SELECT "Nombre", "Valor"
      FROM "T_Ajustes_Generales_Sistema"
      WHERE "Nombre" IN (
        'TOLERANCIA_TARDANZA_MINUTOS_PRIMARIA',
        'TOLERANCIA_TARDANZA_MINUTOS_SECUNDARIA'
      )
    `;

    const result = await RDP02_DB_INSTANCES.query(sql);

    const config: ConfiguracionesTolerancias = {
      toleranciaTardanzaMinutosPrimaria: 5, // Valor por defecto
      toleranciaTardanzaMinutosSecundaria: 5, // Valor por defecto
    };

    for (const row of result.rows) {
      if (row.Nombre === "TOLERANCIA_TARDANZA_MINUTOS_PRIMARIA") {
        config.toleranciaTardanzaMinutosPrimaria = parseInt(row.Valor, 10);
      } else if (row.Nombre === "TOLERANCIA_TARDANZA_MINUTOS_SECUNDARIA") {
        config.toleranciaTardanzaMinutosSecundaria = parseInt(row.Valor, 10);
      }
    }

    console.log("✅ Configuraciones de tolerancia obtenidas:", config);
    return config;
  } catch (error) {
    console.error("❌ Error al obtener configuraciones de tolerancia:", error);
    throw error;
  }
}
