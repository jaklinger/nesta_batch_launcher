from webscraping.some_import import Something
import pymysql


def execute(**params):
    a_field = ""
    exception = ""
    try:
        # Get the data
        a_field = Something.get_data()
    except Exception as err:
        # Save any error messages for debugging
        exception = str(err)

    # Write to database
    connection = pymysql.connect(read_default_file=params['dbconf'])
    with connection.cursor() as cur:
        sql = ("INSERT INTO `<whatever_database>` "
               "(`id`, `whatever_field`, `exception_text`) "
               "VALUES (%s, %s, %s)")
        cur.execute(sql, (params['id'], a_field, exception))
    connection.commit()

if __name__ == "__main__":
    params = dict(url="http://www.some_url.com", id=23,
                  dbconf="innovation-mapping-tier0.config")
    execute(**params)
