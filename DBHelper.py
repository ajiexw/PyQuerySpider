#coding=utf-8
import MySQLdb, MySQLdb.cursors
import web

# 数据库配置,若想把DBHelper.py单独用于你的其它项目中,仅需修改此配置即可
DB_PASSWORD = 'xxxxxxxx'
DB_DATABASE = 'xxxxxxxx'
DB_HOST     = 'xxxxxxxx'
DB_USER     = 'xxxxxxxx'
DB_CHARSET  = 'utf8'
DB_TIMEOUT  = 3600

def _exceptMySQLdbException(e):
    if len(e.args) > 0:
        if e.args[0] in [1044, 1045]:
            print 'ERROR: 没有访问数据库%s的权限,请检查site_helper.config中的数据库配置是否正确' % DB_DATABASE
            #print '或者使用 mysql -uroot -p < %sdoc/sql/init_database.sql 来初始化数据库' % site_helper.config.APP_ROOT_PATH
            exit(0)
        elif e.args[0] == 1049:
            print 'ERROR: 数据库%s不存在,请检查site_helper.config中的数据库配置是否正确' % DB_DATABASE
            exit(0)
        elif e.args[0] == 2003:
            print 'ERROR: 找不到数据库服务器%s,请检查site_helper.config中的数据库配置是否正确' % DB_HOST
            exit(0)

def createDictDB():
    try:
        db = MySQLdb.connect(host=DB_HOST,user=DB_USER,passwd=DB_PASSWORD,charset=DB_CHARSET,db=DB_DATABASE,cursorclass=MySQLdb.cursors.DictCursor, connect_timeout=DB_TIMEOUT)
        db.autocommit(True) # 自动commit， 否者connection可能读取不到最新的数据
        db.cursor().execute('set wait_timeout=%s', [DB_TIMEOUT])
    except Exception as e:
        _exceptMySQLdbException(e)
        raise
    return db
    
def createTupleDB():
    try:
        db = MySQLdb.connect(host=DB_HOST,user=DB_USER,passwd=DB_PASSWORD,charset=DB_CHARSET,db=DB_DATABASE, connect_timeout=DB_TIMEOUT)
        db.autocommit(True)
        db.cursor().execute('set wait_timeout=%s', [DB_TIMEOUT])
    except Exception as e:
        _exceptMySQLdbException(e)
        raise
    return db

class DBHelper:
    # 创建访问数据库的connection, 所有DBHelper的实例共用这两个connection
    DB_DICT = createDictDB()
    DB_TUPLE = createTupleDB()

    def __init__(self):
        self.db_dict  = DBHelper.DB_DICT
        self.db_tuple = DBHelper.DB_TUPLE

    def fetchOne(self, query_string, argv=(), ignore_assert=False):
        '''return a dict'''
        if not ignore_assert:
            assert('select' in query_string.lower())
            assert(' limit ' not in query_string.lower())
        query_string += ' limit 1'
        cursor = self.db_dict.cursor()
        try:
            cursor.execute(query_string, argv)
        except:
            print 'query string is:', query_string
            print 'argv are:', argv
            raise
        one = cursor.fetchone()
        if one is not None:
            one = web.Storage(self._toUtf8(one))
        return one

    def fetchSome(self, query_string, argv=(), ignore_assert=False):
        '''return a list of dict'''
        if not ignore_assert:
            assert('select' in query_string.lower())
        cursor = self.db_dict.cursor()
        try:
            cursor.execute(query_string, argv)
        except:
            print 'query string is:', query_string
            print 'argv are:', argv
            raise
        return [web.Storage(self._toUtf8(one)) for one in cursor.fetchall()]

    def fetchFirst(self, query_string, argv=(), ignore_assert=False):
        '''return a int or string(etc.) of the colume's first value in query.'''
        if not ignore_assert:
            assert('select' in query_string.lower())
        cursor = self.db_tuple.cursor()
        try:
            cursor.execute(query_string, argv)
        except:
            print 'query string is:', query_string
            print 'argv are:', argv
            raise
        one = cursor.fetchone()
        if one is not None:
            one = one[0]
            if type(one) is unicode:
                one = one.encode('utf-8')
        return one

    def fetchSomeFirst(self, query_string, argv=(), ignore_assert=False):
        '''like fetchFirst, but return a list. '''
        if not ignore_assert:
            assert('select' in query_string.lower())
        cursor = self.db_tuple.cursor()
        try:
            cursor.execute(query_string, argv)
        except:
            print 'query string is:', query_string
            print 'argv are:', argv
            raise
        retList = []
        for one in cursor.fetchall():
            first = one[0]
            if type(first) is unicode:
                first = first.encode('utf-8')
            retList.append(first)
        return retList

    def insert(self, query_string, argv=(), ignore_assert=False):
        if not ignore_assert:
            assert('insert' in query_string.lower() or 'replace' in query_string.lower())
        cursor = self.db_tuple.cursor()
        try:
            cursor.execute(query_string, argv)
        except:
            print 'query string is:', query_string
            print 'argv are:', argv
            raise
        self.db_tuple.commit()
        return cursor.lastrowid

    def delete(self, query_string, argv=(), ignore_assert=False):
        if not ignore_assert:
            assert('delete' in query_string.lower())
            assert('where' in query_string.lower())
        cursor = self.db_tuple.cursor()
        try:
            affected = cursor.execute(query_string, argv)
        except:
            print 'query string is:', query_string
            print 'argv are:', argv
            raise
        self.db_tuple.commit()
        return affected

    def update(self, query_string, argv=(), ignore_assert=False):
        if not ignore_assert:
            assert('update' in query_string.lower())
            assert('where' in query_string.lower())
        cursor = self.db_tuple.cursor()
        try:
            affected = cursor.execute(query_string, argv)
        except:
            print 'query string is:', query_string
            print 'argv are:', argv
            raise
        self.db_tuple.commit()
        return affected

    # 执行一条复杂的查询语句，当其它函数不能满足需求时使用
    def executeQuery(self, query, argv=(), ignore_assert=False):
        affected = self.db_tuple.cursor().execute(query, argv)
        self.db_tuple.commit()
        return affected

    def isTableExists(self, table_name):
        cursor = self.db_tuple.cursor()
        cursor.execute("SHOW TABLES LIKE '%s';" % table_name)
        one = cursor.fetchone()
        return one is not None

    def isColumnExists(self, table_name, column_name):
        return self.isTableExists(table_name) and column_name in self.getTableColumns(table_name)

    # 获得某张表中的字段名
    def getTableColumns(self, table_name):
        '''return a list of names'''
        cursor = self.db_tuple.cursor()
        query_string = 'desc %s' % table_name
        cursor.execute(query_string)
        retList = []
        for one in cursor.fetchall():
            first = one[0]
            if type(first) is unicode:
                first = first.encode('utf-8')
            retList.append(first)
        return retList

    def _toUtf8(self, row):
        newRow = {}
        for k,v in row.items():
            if v is not None:
                if type(v) is unicode:
                    newRow[k] = v.encode('utf-8')
                else:
                    newRow[k] = v
            else:
                newRow[k] = None
        return newRow

    def _getColumnNames(self):
        cursor = self.db_tuple.cursor()
        query_string = 'desc %s' % self.model.table_name
        cursor.execute(query_string)
        retList = []
        for one in cursor.fetchall():
            first = one[0]
            if type(first) is unicode:
                first = first.encode('utf-8')
            retList.append(first)
        return retList
