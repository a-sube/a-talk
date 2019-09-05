import ast
import time

from aiohttp import WSMsgType

from db import db_select_and_validate, db_create_user,\
    db_get_channles, db_create_channel, db_get_users


from utils import decode_token, encode_user, generate_msg


# TODO
async def validateData(data, op) -> bool:
    # pass
    return True


class Msg:
    def __init__(self, data):
        self.op = data["operation"]
        self.data = data["data"]
        self.error = ""


    def token(self):
        return self.data.get("token", "")

    def set_op(self, op_num: int):
        self.op = op_num

    async def set_data(self, **kwargs):
        for k in kwargs:
            self.data[k] = kwargs[k]


class Conn:

    def __init__(self, ws, app):
        self.ws = ws
        self.app = app
        self.lang = "en"

    async def close(self):
        await self.ws.close()

    async def remove(self):
        # for k in self.app['connections']:
        #     print(k == self)
        self.app["connections"].remove(self)

    async def read_msg(self):

        async for msg in self.ws:
            if msg.type == WSMsgType.TEXT:

                if msg.data == "close":
                    await self.ws.close()
                    await self.remove()
                else:
                    await self.process(msg.data)

            elif msg.type == WSMsgType.ERROR:
                await self.remove()
                print("ws connection closed with exception %s" %
                      self.ws.exception())

        await self.remove()
        print("websocket connection closed")

    async def process(self, d):
        try:
            data = ast.literal_eval(d)  # safely evaluate string to dict
            valid = await validateData(data["data"], data["operation"])

            if valid:
                msg = Msg(data)
                to_send = await self.do_operation(msg)

                await self.ws.send_json(to_send)

            else:
                await self.ws.send_json({
                    "status": 0,  # fail
                    "error": "unknown operation or invalid data",
                })
        except:
            # handle invalid msg
            await self.ws.send_json({
                "status": 0,  # fail
                "error": "invalid msg",
            })

    async def do_operation(self, msg):
        if msg.op == -1:  # ignore
            return await generate_msg(-1)

        if msg.op == 1:  # auth request
            return await self.do_login(msg)

        if msg.op == 2:  # create user
            return await self.create_user(msg)

        if msg.op == 3:  # get channels
            return await self.get_channels(msg)

        if msg.op == 4:  # create channel
            await self.create_channel(msg)
            msg.set_op(-1)
            return await generate_msg(-1)

        if msg.op == 5:  # get users
            return await self.get_users(msg)

        if msg.op == 6:  # new msg sent to channel
            await self.handle_message(msg)
            msg.set_op(-1)
            return await generate_msg(-1)

        if msg.op == 7: # set language
            return await self.set_language(msg)

    async def do_login(self, msg: Msg):

        if msg.token():  # autologin

            valid, user = await decode_token(
                self.app["config"]["secret"], msg.token())

            if valid:
                user["token"] = msg.token()
                return await generate_msg(msg.op, user=user)

        valid, user = await db_select_and_validate(self.app["db"], msg.data)

        if valid:
            user["token"] = await encode_user(self.app["config"]["secret"], user)

            return await generate_msg(msg.op, user=user)

        return await generate_msg(0, error="invalid user credentials", initiator="login")

    async def create_user(self, msg):

        await msg.set_data(server_id=1, staff=False, admin=False)
        _id = await db_create_user(self.app["db"], msg.data)
        if _id > 0:

            valid, user = await db_select_and_validate(self.app["db"], msg.data)

            if valid:
                user["token"] = await encode_user(self.app["config"]["secret"], user)

                await self.broadcast_new_user(user)
                return await generate_msg(msg.op, user=user)

        return await generate_msg(0, error="invalid user credentials", initiator="create_user")

    async def get_users(self, msg):
        users = await db_get_users(self.app["db"], msg.data["server_id"])
        return await generate_msg(msg.op, data={"users": users})

    async def get_channels(self, msg):
        channels = await db_get_channles(self.app["db"], msg.data["server_id"])
        return await generate_msg(msg.op, data={"channels": channels})

    async def create_channel(self, msg):
        msg.set_op(3)
        await db_create_channel(self.app["db"], msg.data)
        await self.broadcast_channels(msg)

    async def handle_message(self, msg):
        # TODO add to db
        await self.broadcast_new_message(msg)

    async def broadcast_channels(self, msg):
        to_send = await self.get_channels(msg)

        await self.app['msg_queue'].add(to_send)

    async def broadcast_new_user(self, user):
        users = [{
            "user_id": user["user_id"],
            "user_name": user["user_name"],
            "admin": user["admin"],
            "staff": user["staff"]
        }]

        to_send = await generate_msg(5, data={"users": users})

        await self.app['msg_queue'].add(to_send)

    async def broadcast_new_message(self, msg):
        if msg.data != {}:
            to_send = await generate_msg(6, data=msg.data)
            to_send["data"]["created_at"] = int(time.time())

            await self.app['msg_queue'].add(to_send)

    async def send_j(self, data):
        # print(data)
        await self.ws.send_json(data)

    async def set_language(self, msg):
        self.lang = msg.data['lang']
        return await generate_msg(-1)

