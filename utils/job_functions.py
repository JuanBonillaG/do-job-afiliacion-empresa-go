import pandas as pd

def compare_users(df_users_afiliacion,df_users_go):
    """
    Compara cambios en los usuarios de afiliación frente a GO Integro

    Args:
        df_users_afiliacion (DataFrame): DataFrame con los usuarios a actualizar.
        df_users_go (DataFrame): DataFrame con los usuarios de GO Integro.
    Returns:
        DataFrame: DataFrame con los usuarios a actualizar.
    """
    # Filtra usuarios que ya existen en GO INTEGRO
    df_existing_users = df_users_afiliacion[df_users_afiliacion["document"].isin(df_users_go["document"])]

    # Une los dataframes para comparar características
    df_merged = pd.merge(
        df_existing_users,
        df_users_go,
        on="document",
        suffixes=('_new', '_go'),
        how='inner'
    )

    # Lista de columnas a comparar
    columns_to_compare = [
        col for col in df_existing_users.columns
        if col in df_users_afiliacion.columns and col not in ["document", "id"]
    ]

    # Encuentra filas con al menos una diferencia
    mask_diff = df_merged.apply(
        lambda row: any(
            row[f"{col}_new"] != row[f"{col}_go"] for col in columns_to_compare
        ),
        axis=1
    )

    # Usuarios por actualizar
    df_users_to_update = df_merged[mask_diff]

    # Se cambia a las columnas originales
    df_users_to_update = df_users_to_update[[f"{col}_new" for col in df_users_afiliacion.columns]]
    df_users_to_update.columns = df_users_afiliacion.columns

    return df_users_to_update


def update_users_with_group_items(df_users, df_group_items):
    """
    Agrega a df_users el group_item_id y group_item_type tomado desde GO Integro.

    Args:
        df_users (pd.DataFrame): DataFrame de usuarios proveniente de API Afiliación de empresas.
        df_group_items (pd.DataFrame): DataFrame con id de grupo registrado en GO Integro.

    Returns:
        pd.DataFrame: df_users enriquecido con columnas 'group_item_id' y 'group_item_type'.
    """
    # Extraer la empresa desde la columna 'groups'
    df_users = df_users.copy()
    df_users["empresa"] = df_users["groups"].str.split(":").str[1]

    # Merge con df_group_items en base a empresa == name de group_items
    df_merged = df_users.merge(
        df_group_items,
        how="left",
        left_on="empresa",
        right_on="name"
    )

    # Renombra las columnas y limpieza
    df_merged.rename(columns={
        "id": "group_item_id",
        "type": "group_item_type"
    }, inplace=True)

    df_merged.drop(columns=["empresa", "name"], inplace=True)

    return df_merged