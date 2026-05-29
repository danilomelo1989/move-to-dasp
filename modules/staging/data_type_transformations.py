from pyspark.sql.functions import col, to_date, lit, to_timestamp, regexp_replace, trim, when, expr, desc
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql import Window
from pyspark.sql import functions as F
from modules.utils.connectors import SnowflakeConnector


class Table():
    def __init__(self, table_name, table=None, schema='REFATORACAO') -> None:
        self.table_name = table_name
        if table is not None:
            self.table = table
        else:
            self.table = self._get_table(schema)  

    def column_cast(self, config):
        column_select = []
        for column in self.table.columns:
            column_info = config.get(column, {})

            if column_info.get('type') == 'timestamp':
                column_select.append(
                    self._cast_timestamp(column, column_info.get('rename', column))
                )
            elif column_info.get('type') == 'date':
                format_value = column_info.get('format')
                column_name = column_info.get('rename', column)

                if isinstance(format_value, list):
                    date_expression = None
                    for fmt in format_value:
                        if date_expression is None:
                            date_expression = when(col(column_name).isNotNull(), self._cast_date(column_name, column_name, fmt))
                        else:
                            date_expression = date_expression.when(col(column_name).isNotNull(), self._cast_date(column_name, column_name, fmt))

                    column_select.append(date_expression.otherwise(None).alias(column_name))

                elif isinstance(format_value, str):
                    column_select.append(self._cast_date(column_name, column_name, format_value))

            elif column_info.get('type') == 'integer':
                column_select.append(self._cast_integer(column, column_info.get('rename', column)))
            elif column_info.get('type') == 'money':
                column_select.append(self._cast_money(column, column_info.get('rename', column)))
            elif column_info.get('type') == 'number_comma':
                column_select.append(self._cast_comma(column, column_info.get('rename', column)))                
            elif column_info.get('type') == 'money_signaled':
                column_select.append(self._cast_money_signaled(column, column_info.get('rename', column)))                
            elif column_info.get('type') == 'string':
                column_select.append(self._cast_string(column, column_info.get('rename', column)))
            elif column_info.get('type') == 'regex':
                column_select.append(
                    self._regex_string(
                        column, 
                        column_info.get('rename', column),
                        column_info.get('pattern'),
                        column_info.get('replace')
                    )
                )
            else:
                pass

        self.table = self.table.select(
            *column_select
        )

    def upload_table_to_snowflake(self, table_name, schema='DEV', mode ='Overwrite'):
        conn = SnowflakeConnector()

        conn.save_snowflake_table(self.table, table_name, schema, mode)

    def filter_table(self, condition):
        filtered_table = self.table.filter(condition)

        self.table = filtered_table
        
    def add_column(self, column_name:str, value:str):
        cols = self.table.columns

        self.table = self.table.select(*cols, lit(value).alias(column_name))


    def add_dense_rank(self, partitionby , orderby):
        cols = self.table.columns
        window = Window.partitionBy(partitionby).orderBy(desc(orderby)) 
        self.table = self.table.select(*cols, lit(F.dense_rank().over(window)).alias('DENSE_RANK'))


    def _regex_string(self, column_name: str, column_rename: str, pattern: str, replacement: str=''):
        column = trim(regexp_replace(
            col(column_name),
            pattern,
            replacement
	    ))

        return column.alias(column_rename)

    def _cast_string(self, column_name, column_rename):
        column = trim(col(column_name))

        return column.alias(column_rename)

    def _cast_timestamp(self, column_name, column_rename):
        column = to_timestamp(col(column_name))

        return column.alias(column_rename)

    def _cast_date(self, column_name, column_rename, date_format):    
        column = to_date(col(column_name), date_format)

        return column.alias(column_rename)

    def _cast_integer(self, column_name, column_rename):
        column = col(column_name).cast(IntegerType())

        return column.alias(column_rename)

    def _cast_money(self, column_name, column_rename):
        column = regexp_replace(
            col(column_name),
            '[.,]',
            ''
        ).cast(FloatType()) / 100

        return column.alias(column_rename)

    def _cast_comma(self, column_name, column_rename):
        column = regexp_replace(
            col(column_name),
            '[,]',
            '.'
        ).cast(FloatType())

        return column.alias(column_rename)
    



    def _cast_money_signaled(self, column_name, column_rename):
        column = regexp_replace(
            col(column_name),
            '[.,C-]',
            ''
        ).cast(FloatType()) / 100

        return column.alias(column_rename)
    
    
    def _get_table(self, schema):
        conn = SnowflakeConnector()

        table = conn.get_snowflake_table(self.table_name, schema)

        return table
