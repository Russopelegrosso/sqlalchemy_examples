import asyncio
import datetime

start_time = datetime.datetime.now()


async def assistant_gets_calls(counts):
    print('answer call')
    counter = 0
    while counter < counts:
        await asyncio.sleep(1/10)
        counter += 1


async def assistant_prints_docs(counts):
    counter = 0
    while counter < counts:
        print('print docs')
        await asyncio.sleep(2)
        counter += 1


async def assistant_sends_emails(counts):
    counter = 0
    while counter < counts:
        print('send emails')
        await asyncio.sleep(3)
        counter += 1


async def assistant_prepares_a_report(counts):
    counter = 0
    while counter < counts:
        print('prepare a report')
        await asyncio.sleep(5)
        counter += 1


async def main():
    tasks = [
        assistant_gets_calls(20),
        assistant_prints_docs(10),
        assistant_sends_emails(5),
        assistant_prepares_a_report(2)

    ]
    await asyncio.gather(*tasks)
    print(datetime.datetime.now() - start_time)

if __name__ == '__main__':
    asyncio.run(main())
