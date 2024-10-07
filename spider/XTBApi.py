import enum
import json
from websockets.sync.client import ClientConnection, connect
from websockets.exceptions import WebSocketException
import logging
LOGGER = logging.getLogger('XTBApi')
LOGGER.setLevel(logging.INFO)

SERVER_URL = "ws.xtb.com"


# #
# Exceptions classes
# #
class Exceptions(Exception):
    LOGGER = logging.getLogger('XTBApi.exceptions')


class CommandFailed(Exceptions):
    """when a command fail"""
    def __init__(self, response):
        self.msg = f"Command failed with error code {response['errorCode']} - {response['errorDescr']}"
        self.LOGGER.error(self.msg)
        super().__init__(self.msg)


class NotLogged(Exceptions):
    """when not logged"""
    def __init__(self):
        self.msg = "Not logged, please log in"
        self.LOGGER.exception(self.msg)
        super().__init__(self.msg)


class SocketError(Exceptions):
    """when socket is already closed
    may be the case of server internal error"""
    def __init__(self):
        self.msg = "SocketError, may be an internal error"
        self.LOGGER.error(self.msg)
        super().__init__(self.msg)


class TransactionRejected(Exceptions):
    """transaction rejected error"""
    def __init__(self, status_code):
        self.status_code = status_code
        self.msg = f"Transaction rejected with error code {status_code}"
        self.LOGGER.error(self.msg)
        super().__init__(self.msg)


# #
# XTB API classes
# #
class STATUS(enum.IntEnum):
    LOGGED = 1  # enum.auto()
    NOT_LOGGED = 0  # enum.auto()


def _construct_cmd(command: str, **kwargs):
    """helper function to construct command format"""
    cmd = {"command": command}
    if kwargs:
        cmd['arguments'] = {k: v for k, v in kwargs.items()}
    return cmd


class BaseClient(object):
    """Base client class"""

    def __init__(self, user: str, token: str, mode: str) -> None:
        self.ws: ClientConnection | None = None
        self.ws_uri = f'wss://{SERVER_URL}/{mode}'
        self._user = user
        self._token = token
        self.status = STATUS.NOT_LOGGED
        LOGGER.debug("BaseClient inited")
        self.LOGGER = logging.getLogger('XTBApi.BaseClient')

    def _send_command(self, data: dict):
        """send command to api"""
        try:
            self.ws.send(json.dumps(data))
            response = self.ws.recv()
        except WebSocketException:
            raise SocketError()

        res = json.loads(response)
        if not res['status']:
            self.LOGGER.debug(res)
            raise CommandFailed(res)

        self.LOGGER.info("CMD: done")
        self.LOGGER.debug(res)
        return res

    def login(self):
        """login command"""
        cmd = _construct_cmd('login', userId=self._user, password=self._token)
        self.ws = connect(self.ws_uri)
        self.status = STATUS.LOGGED
        self.LOGGER.info("CMD: login...")
        return self._send_command(cmd)

    def logout(self):
        """logout command"""
        cmd = _construct_cmd('logout')
        self.status = STATUS.NOT_LOGGED
        self.LOGGER.info("CMD: logout...")
        return self._send_command(cmd)


class Client(BaseClient):
    """advanced class of client"""
    def __init__(self, user: str, token: str, mode: str):
        super().__init__(user, token, mode)
        self.LOGGER = logging.getLogger('XTBApi.Client')
        self.LOGGER.info("Client inited")

    def get_server_time(self):
        """logout command"""
        cmd = _construct_cmd('getServerTime')
        self.status = STATUS.NOT_LOGGED
        self.LOGGER.info("CMD: get server time...")
        return self._send_command(cmd)

    def get_chart_range_request(self, symbol: str, period: int, start: int, end: int, ticks: int):
        """getChartRangeRequest command"""
        cmd = _construct_cmd(
            "getChartRangeRequest",
            info={
                "symbol": symbol,
                "period": period,
                "start": start * 1000,
                "end": end * 1000,
                "ticks": ticks
            }
        )
        self.LOGGER.info(f"CMD: get chart range request for {symbol} of "
                         f"{period} from {start} to {end} with ticks of "
                         f"{ticks}...")
        return self._send_command(cmd)
