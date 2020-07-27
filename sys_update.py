"""
Update Module
"""
import traceback

from utils import grc
from conf import static
from utils.cls.core import Customizer
from utils.update_manager import UpdateManager
from utils.cls.pltfm.gmail import send_error_email
SCRIPT_NAME = 'GRC Platform Update Script'
DEBUG = False
if DEBUG:
    print('WARN: Error reporting disabled. DEBUG = TRUE')


def main() -> None:
    um = UpdateManager(
        auth_token=static.UPDATE_KEY,
        username=static.UPDATE_USERNAME,
        repository=static.UPDATE_REPOSITORY
    )

    um.download_latest()
    um.perform_update()
    return


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        if not DEBUG:
            send_error_email(
                client_name=Customizer().client,
                script_name=SCRIPT_NAME,
                to=Customizer().recipients,
                error=error,
                stack_trace=traceback.format_exc(),
                engine=grc.create_application_sql_engine()
            )
        raise
