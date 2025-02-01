import pandas as pd
import cx_Oracle

"""
This code is intended to automate the creation of scripts by generating ddl from the received values(excel, oracle). 
This code was used in my work, you can modify it at your discretion
"""

def get_from_oracle(tns, user, password, schema, table):
    """
    func gets column names for table from sys table
    :param tns: name of tns from file tnsnames_ora
    :param user: user (login)
    :param password:
    :param schema: schema where table is
    :param table: table name
    :return: columns
    """
    try:
        connection = cx_Oracle.connect(user=user, password=password, dsn=tns)
        print('Success conection to Oracle database')
    except cx_Oracle.DatabaseError as e:
        print(f'error conn: {e}')
        return []

    cursor = connection.cursor()

    try:
        cursor.execute(f"""
            SELECT COLUMN_NAME
            FROM DBA_TAB_COLUMNS
            WHERE OWNER = '{schema}' AND TABLE_NAME = '{table}'
        """)
        db_columns = [col[0] for col in cursor.fetchall()]
        print(f'Column from Oracle for {schema}.{table}: {db_columns}')
        return db_columns
    except cx_Oracle.DatabaseError as e:
        print(f'Error when try to get columns for {schema}.{table}: {e}')
        return []
    finally:
        cursor.close()
        connection.close()


def group_attributes_by_schema_and_table(excel_file):
    """
    function for reading Excel and grouping by column values,
    composing ddl scripts based on the resulting columns
    :param excel_file:
    :return: ddl script
    """
    # Чтение данных из файла
    data = pd.read_excel(excel_file)

    # Группировка данных по схеме и таблице
    grouped_data = data.groupby(['Schema', 'Table']).agg(
        All_Attributes=('Attribute ', list),
        Key_Attributes=('Attribute',
                           lambda x: [attr for attr, key in zip(x, data.loc[x.index, 'Key status (Primary)']) if
                                      key == 'Y']),
        Non_Primary_attributes=('Attribute', lambda x: [
            attr for i, attr in enumerate(x)
            if data.loc[x.index[i], 'Key status (Primary)'] == 'N'
        ])
    ).reset_index()

    # Печать сгруппированных данных и формирование SQL-запросов
    for index, row in grouped_data.iterrows():
        schema = row['Schema']
        table = row['Table']
        all_attributes = row['All_Attributes']
        key_attributes = row['Key_Attributes']
        non_primary_attributes = row['Non_Primary_attributes']

        print(
            f"Schema: {schema}, Table: {table}, All Attributes: {all_attributes}, Key Attributes: {key_attributes}n")


        # Получаем столбцы из Oracle
        ora_columns = get_from_oracle(tns='NAME_TNS_', user='USER', password='PASS#TQ',
                                      schema=schema.upper(), table=table.upper())

        # Формируем списки столбцов
        green_columns = ['g.' + col for col in ora_columns if col not in all_attributes]
        green_select = ",nt".join(green_columns)

        # Формируем условия для JOIN
        join_conditions = " AND ".join([f"r.{key} = g.{key}" for key in key_attributes])

        # Формирование SQL-запроса
        sql_query = f"""
CREATE VIEW {schema}_{table} AS
SELECT {', '.join(['r.' + attr for attr in non_primary_attributes])},
       {green_select}
FROM {schema}.{table} r
LEFT JOIN {schema}.{table} g ON {join_conditions};
"""
        return sql_query


group_attributes_by_schema_and_table('name_excel_file')