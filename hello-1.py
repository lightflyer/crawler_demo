import asyncio

async def task(name, delay):
    await asyncio.sleep(delay)
    return f"Task {name} completed after {delay} seconds"

async def main():
    # 方式一: 单个创建任务
    task_x = asyncio.create_task(task("X", 5))
    task_a = asyncio.create_task(task("A", 1))
    task_b = asyncio.create_task(task("B", 2))
    task_c = asyncio.create_task(task("C", 3))

    # 方式二: 批量创建任务
    tasks = [asyncio.create_task(task(f"Task {i}", 5-i)) for i in range(1, 4)]
    

    # 等待特定任务完成
    # result_a = await task_a
    # print(result_a)

    # 或者等待所有任务完成
    # results = await asyncio.gather(task_x, task_a, task_b, task_c)
    results = await asyncio.gather(*tasks)
    print("All tasks completed")
    for result in results:
        print(result)


# 运行主函数
asyncio.run(main())