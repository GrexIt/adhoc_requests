import asyncio
import csv 
from sqlalchemy import Table, Column, Integer, String, MetaData, select, desc, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine


async def fetch_dup_company(engine: AsyncEngine):
    async with engine.connect() as conn:
        
        query = text(" SET sql_mode = ''; ")
        await conn.execute(query)

        query = text("""
                SELECT id, name, ug_id from companies c group by name, ug_id having count(*)>1 limit 100;
            """)
        result = list(await conn.execute(query))
        return result


async def fetch_old_company_ids(engine: AsyncEngine, id: int, name: str, ug_id: int):
    async with engine.connect() as conn:
        query = text(f"""
                SELECT id from companies c where name="{name}" and ug_id={ug_id} and id!={id};
            """)
        result = list(await conn.execute(query))
        return result


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

    rows = await fetch_dup_company(engine)

    fields = ['ID', 'Name', 'UG_ID']  
    with open( "to_keep.csv" , 'w') as csvfile: 
        csvwriter = csv.writer(csvfile)          
        csvwriter.writerow(fields) 
        csvwriter.writerows(rows)

    fields = ['Original_ID','Old_IDs']  
    with open("to_discard.csv", 'w') as csvfile: 
        csvwriter = csv.writer(csvfile)          
        csvwriter.writerow(fields)  
        for i in rows:
            temp = [i[0]]
            old_ids = await fetch_old_company_ids(engine, i[0], i[1], i[2])
            for j in old_ids:
                temp.append(j[0])
            csvwriter.writerow(temp)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    print("done")
