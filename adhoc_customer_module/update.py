import asyncio
import csv
from sqlalchemy import Table, Column, Integer, String, MetaData, select, desc, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine


async def delete_records(engine: AsyncEngine, old_ids):
    async with engine.begin() as conn:
        query = 'DELETE from companies where id in (' + old_ids + ')'
        await conn.execute(text(query))
        query = 'DELETE from domains where id in (' + old_ids + ')'
        await conn.execute(text(query))
        

async def update_records(engine: AsyncEngine, id: int, old_ids):
    async with engine.begin() as conn:
        query = text(f"""
                UPDATE contacts set company_id = {id} where company_id in ({old_ids});
            """)
        await conn.execute(query)
        query = text(f"""
                UPDATE conversations set company_id = {id} where company_id in ({old_ids});
            """)
        await conn.execute(query)


async def main():
    connection_string = (
        "mysql+aiomysql://root:tVwRt5f9lsGUs@test-customer-module.csfrvl4jinij.us-west-2.rds.amazonaws.com:3306/customer-module"
    )

    engine = create_async_engine(
        connection_string,
        echo=False,
        pool_size=1,
        max_overflow=3,
    )

    with open('to_discard.csv', mode ='r')as file:
        csv_file = csv.reader(file)
        next(csv_file)
        for line in csv_file:
            old_ids = ','.join((str(i) for i in line[1:]))
            await delete_records(engine, old_ids)
            await update_records(engine, line[0], old_ids)
            print(line) 


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    print("done")
 