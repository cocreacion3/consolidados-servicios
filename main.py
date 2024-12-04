import pandas as pd

surgery = "Cirugia"
outpatient_clinic = "Consulta externa"
internment = "Internacion"
urgencies = "Urgencias"

def loadData(file_name):
    df = None
    file_path = None
    if file_name == surgery:
        file_path = "Data-cruda\BD Cirugía.xlsx"
    elif file_name == outpatient_clinic:
        file_path = "Data-cruda\BD Consulta externa.xlsx"
    elif file_name == internment:
        file_path = "Data-cruda\BD Internación.xlsx"
    elif file_name == urgencies:
        file_path = "Data-cruda\BD Urgencias.xlsx"

    df= pd.read_excel(file_path)
    print(df.head()) 
    return df

def delete_columns(file_name, df):
    if(file_name == surgery):
        keep_columns = ['VALIDAR REPETIDOS', 'Codigo DX Principal', 'Diagnostico Principal']
    elif(file_name == outpatient_clinic):
        keep_columns = ['VALIDACIÓN JUNTA DIRECTIVA Y PRODUCCIÓN AMBULATORIO', 'Cod Diag Princl', 'Diag.Princ']
    elif(file_name == internment):
        keep_columns = ['VALIDACION INTERNACION', 'Código diagnóstico alta', 'Texto diagnóstico alta']
    elif(file_name == urgencies):
        keep_columns = ['Clase de consulta', 'Cod Diag Princl', 'Diag.Princ']
    

    columns = [col for col in keep_columns if col in df.columns]
    if columns:
        df = df[columns]
    else:
        print("No se encontraron las columnas especificadas")

    print(df.head()) 
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
    if(file_name == surgery):
        # Group by "Codigo DX Principal" and count the number of rows
        df_counted = df.groupby('Codigo DX Principal')['Diagnostico Principal'].count().reset_index()
        # Rename the count column
        df_counted = df_counted.rename(columns={'Diagnostico Principal': 'Count'})
        # Check if the column "Diagnostico Principal" exists in the DataFrame
        if 'Diagnostico Principal' in df.columns:
            # Add the original "Diagnostico Principal" column
            df_counted = df_counted.merge(df[['Codigo DX Principal', 'Diagnostico Principal']].drop_duplicates(), on='Codigo DX Principal')
    elif(file_name == outpatient_clinic):
        # Group by "Cod Diag Princl" and count the number of rows
        df_counted = df.groupby('Cod Diag Princl')['Diag.Princ'].count().reset_index()
        # Rename the count column
        df_counted = df_counted.rename(columns={'Diag.Princ': 'Count'})                
        # Check if the column "Diag.Princ" exists in the DataFrame        
        if 'Diag.Princ' in df.columns:
            # Add the original "Diag.Princ" column
            df_counted = df_counted.merge(df[['Cod Diag Princl', 'Diag.Princ']].drop_duplicates(), on='Cod Diag Princl')
    elif(file_name == internment):
        # Group by "Código diagnóstico alta" and count the number of rows
        df_counted = df.groupby('Código diagnóstico alta')['Texto diagnóstico alta'].count().reset_index()
        # Rename the count column
        df_counted = df_counted.rename(columns={'Texto diagnóstico alta': 'Count'})
        # Check if the column "Texto diagnóstico alta" exists in the DataFrame
        if 'Texto diagnóstico alta' in df.columns:
            # Add the original "Texto diagnóstico alta" column
            df_counted = df_counted.merge(df[['Código diagnóstico alta', 'Texto diagnóstico alta']].drop_duplicates(), on='Código diagnóstico alta')
    elif(file_name == urgencies):
        # Group by "Cod Diag Princl" and count the number of rows
        df_counted = df.groupby('Cod Diag Princl')['Diag.Princ'].count().reset_index()
        # Rename the count column
        df_counted = df_counted.rename(columns={'Diag.Princ': 'Count'})                
        # Check if the column "Diag.Princ" exists in the DataFrame        
        if 'Diag.Princ' in df.columns:
            # Add the original "Diag.Princ" column
            df_counted = df_counted.merge(df[['Cod Diag Princl', 'Diag.Princ']].drop_duplicates(), on='Cod Diag Princl')


    # Sort the results from highest to lowest
    df = df_counted.sort_values(by='Count', ascending=False)
    print(df.head()) 
    output_file = 'resultados.xlsx'
    df.to_excel(output_file, index=False)
    return df

file_name = urgencies

df = loadData(file_name)
df = delete_columns(file_name, df)
df = remove_rows(file_name, df)
df = count_codes(file_name, df)