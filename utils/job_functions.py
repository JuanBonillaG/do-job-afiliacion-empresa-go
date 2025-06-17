import pandas as pd

def compare_users(df_users_afiliacion,df_users_go):
    """
    Compara cambios en todas las columnas de los usuarios de afiliación frente a GO Integro y retorna los usuarios de afiliación que van a ser actualizados.

    Args:
        df_users_afiliacion (DataFrame): DataFrame con los usuarios a actualizar.
        df_users_go (DataFrame): DataFrame con los usuarios de GO Integro.
    Returns:
        DataFrame: DataFrame con los usuarios a actualizar.
    """
    # 1. Define las columnas que se van a usar para la comparación y unión.
    COLS_TO_CHECK = ["document", "email", "first_name", "last_name", "document_type"]
    JOIN_KEY = "document"

    # 2. Une los DataFrames usando solo las columnas necesarias.
    merged_df = pd.merge(
        df_users_afiliacion[COLS_TO_CHECK],
        df_users_go[COLS_TO_CHECK],
        on=JOIN_KEY,
        how="inner",
        suffixes=('_afil', '_go')
    )

    if merged_df.empty:
        return pd.DataFrame(columns=df_users_afiliacion.columns)

    # 3. Realiza la comparación
    diff_mask = pd.Series(False, index=merged_df.index)
    for col in COLS_TO_CHECK:
        if col == JOIN_KEY:
            # No comparar la clave de unión consigo misma
            continue 
        col_afil = col + '_afil'
        col_go = col + '_go'
        diff_mask |= (merged_df[col_afil] != merged_df[col_go]) & \
                     ~(merged_df[col_afil].isnull() & merged_df[col_go].isnull())

    # 4. Obtiene los documentos de los usuarios con cambios.
    documents_to_update = merged_df[diff_mask][JOIN_KEY]

    # Retorna las filas completas del DataFrame de afiliación original.
    return df_users_afiliacion[df_users_afiliacion[JOIN_KEY].isin(documents_to_update)]

def update_users_with_group_items(df_users, df_group_items):
    """
    Agrega a df_users el group_item_id y group_item_type tomado desde GO Integro.

    Args:
        df_users (pd.DataFrame): DataFrame de usuarios proveniente de API Afiliación de empresas.
        df_group_items (pd.DataFrame): DataFrame con id de grupo registrado en GO Integro.

    Returns:
        pd.DataFrame: df_users enriquecido con columnas 'group_item_id' y 'group_item_type'.
    """
    # 1. Extraer la empresa desde la columna 'groups'
    df_users = df_users.copy()
    df_users["empresa"] = df_users["groups"].str.split(":").str[1]

    # 2. Merge con df_group_items en base a nombre de empresa para obtener código dado en GO
    df_merged = df_users.merge(
        df_group_items,
        how="left",
        left_on="empresa",
        right_on="name"
    )

    # 3. Renombra las columnas y limpieza
    df_merged.rename(columns={
        "id": "group_item_id",
        "type": "group_item_type"
    }, inplace=True)

    df_merged.drop(columns=["empresa", "name"], inplace=True)

    return df_merged