import click
import mysql.connector

from loguru import logger


@click.command()
@click.option('-u', '--username', required=True)
@click.option('-d', '--database', required=True)
@click.option('-h', '--host', required=True)
@click.option('--dry-run', is_flag=True)
@click.password_option('-p', '--password', confirmation_prompt=False)
@click.option('-P', '--pattern')
@click.option('-S', '--state')
def run(username, password, database, host, pattern, state, dry_run):
    
    cnx = mysql.connector.connect(
        user=username, 
        password=password,
        host=host,
    )
    try:
        cursor = cnx.cursor(buffered=True)

        cursor.execute(
            "SELECT ID, STATE, INFO FROM INFORMATION_SCHEMA.PROCESSLIST WHERE DB=%s;",
            (database,)
        )
        
        for _id, row_state, info in cursor:
            if pattern and info is not None:
                if info.lower().startswith(pattern.lower()):
                    print(_id, state, info)
                    if not dry_run:
                        try:
                            kill_cs = cnx.cursor()
                            kill_cs.execute("KILL {};".format(_id))
                            kill_cs.close()
                        except:
                            pass
            if state:
                if row_state.lower().strip() == state.lower().strip():
                    print(_id, state, info)
                    if not dry_run:
                        try:
                            kill_cs = cnx.cursor()
                            kill_cs.execute("KILL {};".format(_id))
                            kill_cs.close()
                        except:
                            pass
            
        cursor.close()
    except:
        logger.exception('what?')
        cnx.close()

if __name__ == "__main__":
    run()