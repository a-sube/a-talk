import time
import datetime
import secrets
from aiopg.sa import create_engine


from sqlalchemy import (
    MetaData, Table, Column, ForeignKey,
    Integer, String, Boolean, LargeBinary, Text, DateTime,
    PrimaryKeyConstraint, select, and_, or_, not_,
    text
)

from utils import gen_hash_and_salt, check_pswd


meta = MetaData()

servers = Table(
    "servers", meta,

    Column("server_id", Integer, primary_key=True, autoincrement=True),
    Column("server_name", String(255), nullable=False, unique=True),
)

channels = Table(
    "channels", meta,

    Column("channel_id", Integer, nullable=False, autoincrement=True),
    Column("server_id", Integer, ForeignKey(
        "server.server_id", ondelete="CASCADE", onupdate="CASCADE")),
    Column("channel_name", String(255), nullable=False),
    PrimaryKeyConstraint("server_id", "channel_id"),
)

users = Table(
    "users", meta,

    Column("server_id", Integer, ForeignKey(
        "server.server_id", ondelete="CASCADE", onupdate="CASCADE")),
    Column("user_id", Integer, autoincrement=True, primary_key=True),
    Column("user_name", String(255), nullable=False, unique=True),
    Column("password", LargeBinary(64), nullable=False),
    Column("salt", LargeBinary(64), nullable=False),
    Column("staff", Boolean),
    Column("admin", Boolean),
)


messages = Table(
    "messages", meta,

    Column("channel_id", Integer, ForeignKey(
        "channels.channel_id", ondelete="CASCADE", onupdate="CASCADE")),
    Column("server_id", Integer, ForeignKey(
        "servers.server_id", ondelete="CASCADE", onupdate="CASCADE")),
    Column("user_id", Integer, ForeignKey(
        "users.user_id", ondelete="CASCADE", onupdate="CASCADE")),  # author

    Column("message_id", Integer, autoincrement=True),
    Column("content", Text, nullable=False),
    Column("created_at", DateTime, default=datetime.datetime.utcnow),
    PrimaryKeyConstraint("channel_id", "message_id"),
)


async def db_create_tables(engine):
    async with engine.acquire() as conn:
        await conn.execute("DROP TABLE IF EXISTS servers CASCADE")
        await conn.execute("DROP TABLE IF EXISTS channels CASCADE")
        await conn.execute("DROP TABLE IF EXISTS users CASCADE")
        await conn.execute("DROP TABLE IF EXISTS messages CASCADE")

        await conn.execute('''
            CREATE TABLE servers (
                server_id serial PRIMARY KEY,
                server_name varchar(255) UNIQUE NOT NULL
            )
        ''')

        await conn.execute('''
            CREATE TABLE channels (
                channel_id serial,
                server_id int REFERENCES servers(server_id) ON DELETE CASCADE ON UPDATE CASCADE,
                channel_name varchar(255) NOT NULL,
                PRIMARY KEY (server_id, channel_id)
            )
        ''')

        await conn.execute('''
            CREATE TABLE users (
                user_id serial PRIMARY KEY,
                server_id int REFERENCES servers(server_id) ON DELETE CASCADE ON UPDATE CASCADE,
                user_name varchar(255) UNIQUE NOT NULL,
                password bytea NOT NULL,
                salt bytea NOT NULL,
                staff boolean NOT NULL,
                admin boolean NOT NULL
            )
        ''')

        await conn.execute('''
            CREATE TABLE messages (
                server_id int NOT NULL,
                channel_id int NOT NULL,

                user_id int REFERENCES users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,

                message_id serial PRIMARY KEY,
                content TEXT,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (server_id, channel_id) REFERENCES channels(server_id, channel_id) ON DELETE CASCADE ON UPDATE CASCADE
            )
        ''')


async def db_create_server(engine, data):
    server_name = data["server_name"]
    async with engine.acquire() as conn:
        server_id = await conn.scalar(
            servers.insert().values(server_name=server_name)
        )

        return server_id


async def db_create_channel(engine, data):
    channel_name, server_id = data["channel_name"], data["server_id"]

    async with engine.acquire() as conn:
        channel_id = await conn.scalar(
            channels.insert().values(
                channel_name=channel_name,
                server_id=server_id,
            )
        )

        return channel_id


async def db_create_user(engine, data):
    try:
        user_name, password, server_id, admin, staff =\
            data["user_name"], data["password"], data["server_id"],\
            data["admin"], data["staff"]

        hashed, salt = await gen_hash_and_salt(password)

        async with engine.acquire() as conn:
            user_id = await conn.scalar(
                users.insert().values(
                    server_id=server_id,
                    user_name=user_name,
                    password=hashed,
                    salt=salt,
                    staff=staff,
                    admin=admin
                )
            )

            return user_id
    except:
        return 0

async def db_create_message(engine, data):
    server_id, channel_id, user_id, content =\
        data["server_id"], data["channel_id"], data["user_id"], data["content"]

    async with engine.acquire() as conn:
        message_id = await conn.scalar(
            messages.insert().values(
                server_id=server_id,
                channel_id=channel_id,
                user_id=user_id,
                content=content
            )
        )

        return message_id


async def db_select_and_validate(engine, data) -> (bool, dict):
    user_name, password =\
        data["user_name"], data["password"]
    async with engine.acquire() as conn:

        # query = select([users]).where(and_(users.c.user_name == user_name,users.c.server_id == server_id))
        # result = await conn.execute(query)

        text_query = text(
            "SELECT * FROM users "
            "WHERE user_name = :user_name "
        )

        result = await conn.execute(
            text_query, user_name=user_name)

        row = await result.first()
        result.close()

        if row:
            valid = await check_pswd(
                password, row["password"].tobytes(), row["salt"].tobytes())

            if valid:
                user = {
                    "user_id": row["user_id"],
                    "user_name": row["user_name"],
                    "server_id": row["server_id"],
                    "staff": row["staff"],
                    "admin": row["admin"]
                }

                return valid, user

        return False, {}


async def db_get_users(engine, server_id):
    print(server_id)
    async with engine.acquire() as conn:
        # query = select([users.c.server_id, users.c.user_name, users.c.user]).where(users.c.server_id == server_id)
        t_query = text(
            "SELECT user_id, user_name, admin, staff FROM users "
            "WHERE server_id = :server_id "
        )
        result = await conn.execute(t_query, server_id=server_id)

        data = []
        for row in result:
            data.append({
                "user_id": row["user_id"],
                "user_name": row["user_name"],
                "admin": row["admin"],
                "staff": row["staff"]
            })

        result.close()
        return data


async def db_get_channles(engine, server_id):

    async with engine.acquire() as conn:
        query = select([channels]).where(channels.c.server_id == server_id)
        result = await conn.execute(query)

        row = await result.fetchall()
        result.close()
        data = []
        for col in row:
            data.append({
                "channel_id": col["channel_id"],
                "channel_name": col["channel_name"],
                "server_id": col["server_id"],
            })

        return data


async def db_init(app):

    conf = app["config"]["postgres"]
    engine = await create_engine(
        database=conf["database"],
        user=conf["user"],
        password=conf["password"],
        host=conf["host"],
        port=conf["port"],
        minsize=conf["minsize"],
        maxsize=conf["maxsize"],
    )

    app["db"] = engine

    await db_create_tables(app["db"])  # or engine
    server_id = await db_create_server(engine, {"server_name": "initial"})

    channel_id = await db_create_channel(app["db"], {
        "server_id": server_id,
        "channel_name": "general",
    })

    user_id = await db_create_user(app["db"], {
        "server_id": server_id,
        "user_name": "admin",
        "password": "adminpswd",
        "admin": True,
        "staff": True,
    })

    await db_create_message(app["db"], {
        "server_id": server_id,
        "channel_id": channel_id,
        "user_id": user_id,
        "content": "Hello from admin!",
    })


async def db_close(app):
    app["db"].close()
    await app["db"].wait_closed()
