import MySQLdb

from settings import Settings

class PointsDB(object):
    NAME = 0
    POINTS = 1
    EMAIL = 2
    PERSON_ID = 3
    DISPLAY_NAME = 4
    ROOM_ID = 5
    ORG_ID = 6
    TEAM_ID = 7

    def __init__(self):
        self.num_results = 5
        self.table = "all_points"

    def connect(self):
        return MySQLdb.connect(host=Settings.db_hostname,
                               user=Settings.user,
                               passwd=Settings.password,
                               db=Settings.db_name)

    def insert(self, roomId, name, points=1, email=None, personId=None, displayName=None, orgId=None, teamId=None, tried=False):
        if displayName == None:
            displayName = name
        conn = self.connect()
        c = conn.cursor()
        try:
            c.execute("INSERT INTO {0} VALUES (%s,%s,%s,%s,%s,%s,%s,%s)".format(self.table), (name, int(points), email, personId, displayName, roomId, orgId, teamId))
            conn.commit()
        except Exception as e:
            if not tried:
                self.create_table()
                self.insert(roomId, name, points, email, personId, displayName, orgId, teamId, tried=True)
            else:
                print("Insert exception: {0}".format(e))
        conn.close()

    def create_table(self):
        conn = self.connect()
        c = conn.cursor()
        c.execute("SELECT * FROM information_schema.tables WHERE table_name = '{0}'".format(self.table))
        if c.fetchone() == None:#creates the table if doesn't exist
            c.execute("CREATE TABLE {0} (name text, points integer, email text, personId text, displayName text, roomId text, orgId text, teamId text)".format(self.table))
            conn.commit()
        conn.close()

    def update(self, roomId, key, value, points=1):
        conn = self.connect()
        c = conn.cursor()
        update_sttmnt = "UPDATE {0} SET points=points+%s WHERE {1}=%s AND roomId=%s".format(self.table, key)
        update_tuple = (points, value, roomId,)
        c.execute(update_sttmnt, update_tuple)
        conn.commit()
        conn.close()

    def get(self, id_key, id_value, key, value):
        result = None
        try:
            conn = self.connect()
            c = conn.cursor()
            c.execute("SELECT name, SUM(points) as sum_points, email, personId, displayName FROM {0} WHERE {1}=%s AND {2}=%s;".format(self.table, key, id_key), (value, id_value,))
            result = c.fetchone()
            conn.commit()
            conn.close()
        except MySQLdb.ProgrammingError as e:#No table for that room yet
            print("get error: {0}".format(e))
        return result

    def get_points(self, id_key, id_value, key, value):
        result = self.get(id_key, id_value, key, value)
        if result != None:
            return result[self.POINTS]
        else:
            return None

    def leaderboard(self, id_key, id_value):
        results = []
        try:
            conn = self.connect()
            c = conn.cursor()
            c.execute("SELECT displayName, SUM(points) as sum_points FROM {0} WHERE {1}=%s GROUP BY displayName, personId ORDER BY sum_points DESC LIMIT %s".format(self.table, id_key), (id_value, self.num_results))
            results = c.fetchall()
            conn.close()
            print results
        except MySQLdb.OperationalError as e:#No table for that room yet
            print e
        return results

    def list_all_tables(self):
        """
        Useful for listing all tables if everything needs to be deleted for testing.
        """
        results = []
        try:
            conn = self.connect()
            c = conn.cursor()
            c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = %s;", (Settings.db_name,))
            results = c.fetchall()
            conn.close()
        except MySQLdb.OperationalError as e:#No table for that room yet
            print e
        return results

    def drop_table(self, table_name):
        try:
            conn = self.connect()
            c = conn.cursor()
            c.execute("DROP TABLE IF EXISTS {0};".format(table_name))
            conn.close()
        except MySQLdb.OperationalError as e:#No table for that room yet
            print e

if __name__ == "__main__":
    #Testing
    p = PointsDB()
    print p.list_all_tables()
