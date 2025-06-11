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
    columns_to_compare = [col for col in df_users_afiliacion.columns if col not in ["document", "id"]]

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