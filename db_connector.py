import pymysql
import psycopg2
from config import Config

# 确保从 pymysql.cursors 中正确导入 DictCursor
import pymysql.cursors


class DatabaseConnector:
    def __init__(self, db_type):
        self.config = Config()
        self.db_type = db_type.lower()

    def get_connection(self):
        """获取数据库连接"""
        try:
            if self.db_type == 'mysql':
                return self._get_mysql_connection()
            elif self.db_type == 'postgres':
                return self._get_postgres_connection()
            else:
                raise ValueError(f"不支持的数据库类型: {self.db_type}")
        except Exception as e:
            raise Exception(f"数据库连接失败: {str(e)}")

    def _get_mysql_connection(self):
        """获取MySQL连接"""
        connection_params = {
            'host': self.config.MYSQL_HOST,
            'port': int(self.config.MYSQL_PORT),
            'user': self.config.MYSQL_USER,
            'password': self.config.MYSQL_PASSWORD,
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }
        # 如果没有指定数据库，则不传递数据库名
        if self.config.MYSQL_DB:
            connection_params['database'] = self.config.MYSQL_DB

        return pymysql.connect(**connection_params)

    def _get_postgres_connection(self):
        """获取PostgreSQL连接"""
        connection_params = {
            'host': self.config.POSTGRES_HOST,
            'port': int(self.config.POSTGRES_PORT),
            'user': self.config.POSTGRES_USER,
            'password': self.config.POSTGRES_PASSWORD,
            'database': self.config.POSTGRES_DB
        }
        return psycopg2.connect(**connection_params)

    def search_tables_by_description(self, search_text):
        """根据描述信息搜索表"""
        try:
            if self.db_type == 'mysql':
                return self._search_mysql_tables(search_text)
            elif self.db_type == 'postgres':
                return self._search_postgres_tables(search_text)
            else:
                return []
        except Exception as e:
            raise Exception(f"搜索表信息失败: {str(e)}")

    def _search_mysql_tables(self, search_text):
        """在MySQL中搜索表信息"""
        # 如果数据库名为空，查询所有数据库（排除系统数据库）
        if not self.config.MYSQL_DB:
            query = """
                    SELECT TABLE_SCHEMA  AS database_name,
                           TABLE_NAME    AS table_name,
                           TABLE_COMMENT AS table_comment
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                      AND TABLE_COMMENT LIKE %s \
                    """
            params = (f'%{search_text}%',)
        else:
            query = """
                    SELECT TABLE_NAME    AS table_name,
                           TABLE_COMMENT AS table_comment
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s
                      AND TABLE_COMMENT LIKE %s \
                    """
            params = (self.config.MYSQL_DB, f'%{search_text}%')

        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()
        connection.close()
        return results

    def _search_postgres_tables(self, search_text):
        """在PostgreSQL中搜索表信息"""
        query = """
                SELECT schemaname             AS database_name,
                       tablename              AS table_name,
                       obj_description(c.oid) AS table_comment
                FROM pg_tables t
                         LEFT JOIN pg_class c ON c.relname = t.tablename
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                  AND (tablename ILIKE %s OR obj_description(c.oid) ILIKE %s) \
                """
        connection = self.get_connection()
        cursor = connection.cursor()
        cursor.execute(query, (f'%{search_text}%', f'%{search_text}%'))
        results = cursor.fetchall()
        cursor.close()
        connection.close()
        return results
