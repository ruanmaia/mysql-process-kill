import click
import mysql.connector

from loguru import logger


@click.command()
@click.option('-u', '--username', required=True)
@click.option('-d', '--database', required=True)
@click.option('-h', '--host', required=True)
@click.option('--dry-run', is_flag=True)
@click.password_option('-p', '--password', confirmation_prompt=False)
@click.option('-S', '--state', is_flag=True)
@click.option('-P', '--pattern', is_flag=True)
@click.option('-C', '--command', is_flag=True)
@click.argument('statement', nargs=-1)
def run(username, password, database, host, pattern, state, command, statement, dry_run):
    
    cnx = mysql.connector.connect(
        user=username, 
        password=password,
        host=host,
    )
    try:
        cursor = cnx.cursor(buffered=True)

        cursor.execute(
            "SELECT ID, STATE, INFO, COMMAND FROM INFORMATION_SCHEMA.PROCESSLIST WHERE DB=%s;",
            (database,)
        )

        for _id, row_state, command, info in cursor:
            if pattern and info is not None:
                if info.lower().startswith(statement.lower()):
                    print(_id, command, row_state, info)
                    if not dry_run:
                        try:
                            kill_cs = cnx.cursor()
                            kill_cs.execute("KILL {};".format(_id))
                            kill_cs.close()
                        except:
                            pass
            elif state and row_state is not None:
                if row_state.lower().strip() == statement.lower().strip():
                    print(_id, command, row_state, info)
                    if not dry_run:
                        try:
                            kill_cs = cnx.cursor()
                            kill_cs.execute("KILL {};".format(_id))
                            kill_cs.close()
                        except:
                            pass
            elif command and command is not None:
                if command.lower().strip() == statement.lower().strip():
                    print(_id, command, row_state, info)
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