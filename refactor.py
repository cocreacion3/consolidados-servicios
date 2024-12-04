import pandas as pd

surgery = "Cirugia"
outpatient_clinic = "Consulta externa"
internment = "Internacion"
urgencies = "Urgencias"
raw_cid10 = "raw_CID10"
cid10 = "CID10"

def load_data(file_name):
    """Load data from an Excel file based on the given file name."""
    file_path = None

    if file_name == surgery:
        file_path = "Data-cruda/BD Cirugía.xlsx"
    elif file_name == outpatient_clinic:
        file_path = "Data-cruda/BD Consulta externa.xlsx"
    elif file_name == internment:
        file_path = "Data-cruda/BD Internación.xlsx"
    elif file_name == urgencies:
        file_path = "Data-cruda/BD Urgencias.xlsx"
    elif file_name == raw_cid10:
        file_path = "CIE10\CIE10 AGRUPADO.xlsx"
    elif file_name == cid10:
        file_path = "CIE10\CID10_clean.xlsx"

    df = pd.read_excel(file_path)
    return df

def create_CID10():
    df = load_data(raw_cid10)
    df = delete_columns(raw_cid10, df)
    df = df.rename(columns={'COD_4.1': 'Codigo_Diagnostico'})
    df.to_excel('CIE10/' + str(cid10) + '_clean.xlsx', index=False)
    print(df.head()) 
    return df

def delete_columns(file_name, df):
    """
    Delete columns from the given DataFrame based on the given file name.
    """
    # Define the columns to keep
    keep_columns = {
        surgery: ['VALIDAR REPETIDOS', 'Codigo DX Principal', 'Diagnostico Principal'],
        outpatient_clinic: ['VALIDACIÓN JUNTA DIRECTIVA Y PRODUCCIÓN AMBULATORIO', 'Cod Diag Princl', 'Diag.Princ'],
        internment: ['VALIDACION INTERNACION', 'Código diagnóstico alta', 'Texto diagnóstico alta'],
        urgencies: ['Clase de consulta', 'Cod Diag Princl', 'Diag.Princ'],
        raw_cid10: ['COD_4.1', 'CATEGORÍA', 'SUBCATEGORÍA']
    }[file_name]

    # Filter the columns
    columns = [col for col in keep_columns if col in df.columns]
    if columns:
        df = df[columns]
    else:
        print("No se encontraron las columnas especificadas")

    return df

def remove_rows(file_name, df):
    if(file_name == surgery):
        df = df[df['VALIDAR REPETIDOS'] == 'NO REPETIDO']
    elif(file_name == outpatient_clinic):
        df = df[df['VALIDACIÓN JUNTA DIRECTIVA Y PRODUCCIÓN AMBULATORIO'] == 'TENER EN CUENTA J. DIRECTIVA']
    elif(file_name == internment):
        df = df[df['VALIDACION INTERNACION'] == 'INTERNACION']
    elif(file_name == urgencies):
        df = df[df['Clase de consulta'] == 'Urgencias']
    print(df.head()) 
    return df

def count_codes(file_name, df):
    column_mappings = {
        surgery: ('Codigo DX Principal', 'Diagnostico Principal'),
        outpatient_clinic: ('Cod Diag Princl', 'Diag.Princ'),
        internment: ('Código diagnóstico alta', 'Texto diagnóstico alta'),
        urgencies: ('Cod Diag Princl', 'Diag.Princ')
    }

    if file_name in column_mappings:
        code_col, diag_col = column_mappings[file_name]

        # Group by code column and count the occurrences
        df_counted = df.groupby(code_col)[diag_col].count().reset_index()
        
        # Rename the count column
        df_counted = df_counted.rename(columns={diag_col: 'Count'})

        # Add the original diagnosis column if it exists
        if diag_col in df.columns:
            df_counted = df_counted.merge(df[[code_col, diag_col]].drop_duplicates(), on=code_col)

        # Sort results by count in descending order
        df_sorted = df_counted.sort_values(by='Count', ascending=False)

        # Rename the code column al final
        df_sorted = df_sorted.rename(columns={code_col: 'Codigo_Diagnostico'})
        df_sorted = df_sorted.rename(columns={diag_col: 'Diagnostico'})

        # Save to Excel file
        # df_sorted.to_excel(str(file_name)+'.xlsx', index=False)
        
        return df_sorted

def add_categories(df1, df2):
    # Elimina las filas duplicadas en la columna Codigo_Diagnostico
    df2 = df2.drop_duplicates(subset='Codigo_Diagnostico')

    # Crea un diccionario que mapea los códigos de diagnóstico con las categorías y subcategorías
    mapping = df2.set_index('Codigo_Diagnostico')[['CATEGORÍA', 'SUBCATEGORÍA']].to_dict(orient='index')

    # Utiliza el método map para buscar los valores de las columnas CATEGORÍA y SUBCATEGORÍA en df2 y copiarlos en df1
    df1['CATEGORÍA'] = df1['Codigo_Diagnostico'].map(lambda x: mapping.get(x, {}).get('CATEGORÍA'))
    df1['SUBCATEGORÍA'] = df1['Codigo_Diagnostico'].map(lambda x: mapping.get(x, {}).get('SUBCATEGORÍA'))

    print(df1.head()) 
    df1.to_excel('output.xlsx', index=False)


file_name = urgencies
cid10_file = cid10

df = load_data(file_name)
df = delete_columns(file_name, df)
df = remove_rows(file_name, df)
df = count_codes(file_name, df)

df2 = load_data(cid10_file)
add_categories(df , df2)

# df = create_CID10()