from typing import Optional, Any, Callable, Dict
from datetime import datetime
from sqlmodel import Field, Session, SQLModel, create_engine, select, JSON, Column
from enum import Enum
import asyncio
import httpx
from pathlib import Path
import logging
import json
import importlib
import inspect

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 定义任务状态
class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"

# 定义任务模型
class CrawlTask(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    url: str
    status: TaskStatus = Field(default=TaskStatus.PENDING)
    retry_count: int = Field(default=0)
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    error_message: Optional[str] = None
    callback: Optional[str] = Field(default=None)  # 格式: "module_name.function_name"
    params: Dict[str, Any] = Field(default={}, sa_column=Column(JSON))

# 数据库连接
DATABASE_URL = "sqlite:///crawler.db"
engine = create_engine(DATABASE_URL)

# 创建数据库表
def init_db():
    SQLModel.metadata.create_all(engine)

class Spider:
    def __init__(self, max_retries: int = 3, max_concurrent_tasks: int = 5):
        self.max_retries = max_retries
        self.max_concurrent_tasks = max_concurrent_tasks
        self.client = httpx.AsyncClient(
            timeout=30.0,
            limits=httpx.Limits(max_connections=max_concurrent_tasks),
            follow_redirects=True
        )
        
    def load_callback(self, callback_path: str) -> Callable:
        """
        从字符串加载回调函数
        callback_path 格式: "module_name.function_name" 或 "module_name.class_name.method_name"
        """
        if not callback_path:
            return None
            
        try:
            parts = callback_path.split('.')
            module_name = '.'.join(parts[:-1])
            function_name = parts[-1]
            
            module = importlib.import_module(module_name)
            return getattr(module, function_name)
        except Exception as e:
            logger.error(f"Error loading callback {callback_path}: {str(e)}")
            return None

    async def process_url(self, task: CrawlTask) -> bool:
        """处理单个URL的下载任务"""
        try:
            response = await self.client.get(task.url)
            if response.status_code == 200:
                # 如果有回调函数，执行它
                if task.callback:
                    callback_func = self.load_callback(task.callback)
                    if callback_func:
                        if inspect.iscoroutinefunction(callback_func):
                            # 异步回调
                            resp = await callback_func(response, **task.params)
                        else:
                            # 同步回调
                            resp = callback_func(response, **task.params)
                        return resp
                    else:
                        logger.error(f"Failed to load callback for task: {task.id}")
                        return False
                
                logger.info(f"Successfully processed URL: {task.url}")
                return True
            else:
                logger.error(f"Failed to fetch URL: {task.url}, status: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Error processing URL: {task.url}, error: {str(e)}")
            return False

    async def process_batch(self, tasks: list[CrawlTask]):
        """并发处理一批任务"""
        return await asyncio.gather(*[self.process_url(task) for task in tasks])

    def update_task_status(self, task: CrawlTask, status: TaskStatus, error_message: str = None):
        """更新任务状态"""
        with Session(engine) as session:
            task.status = status
            task.updated_at = datetime.now()
            if error_message:
                task.error_message = error_message
            session.add(task)
            session.commit()

    async def run(self):
        """运行爬虫"""
        try:
            while True:
                with Session(engine) as session:
                    # 获取待处理的任务
                    query = select(CrawlTask).where(
                        CrawlTask.status.in_([TaskStatus.PENDING, TaskStatus.FAILED]),
                        CrawlTask.retry_count < self.max_retries
                    ).limit(self.max_concurrent_tasks)
                    
                    tasks = session.exec(query).all()
                    if not tasks:
                        logger.info("No tasks to process")
                        break

                    # 更新任务状态为运行中
                    for task in tasks:
                        self.update_task_status(task, TaskStatus.RUNNING)

                    # 处理任务
                    results = await self.process_batch(tasks)

                    # 更新任务状态
                    for task, success in zip(tasks, results):
                        if success:
                            self.update_task_status(task, TaskStatus.COMPLETED)
                        else:
                            task.retry_count += 1
                            if task.retry_count >= self.max_retries:
                                self.update_task_status(task, TaskStatus.FAILED, "Max retries reached")
                            else:
                                self.update_task_status(task, TaskStatus.PENDING)
        finally:
            await self.client.aclose()

def add_task(url: str, callback: str = None, params: Dict[str, Any] = None):
    """
    添加新任务到队列
    
    Args:
        url (str): 要爬取的URL
        callback (str): 回调函数的路径，格式如 "module_name.function_name"
        params (dict): 传递给回调函数的参数
    """
    with Session(engine) as session:
        task = CrawlTask(
            url=url,
            callback=callback,
            params=params or {}
        )
        session.add(task)
        session.commit()

# 示例使用
async def example_callback(response: httpx.Response, save_path: str = None, **kwargs):
    """示例回调函数"""
    if save_path:
        content = response.content
        Path(save_path).write_bytes(content)
        logger.info(f"Saved content to {save_path}")

async def main():
    # 初始化数据库
    init_db()
    
    # 添加测试任务
    add_task(
        url="https://example.com/image.jpg",
        callback="__main__.example_callback",
        params={"save_path": "image.jpg"}
    )
    
    # 创建并运行爬虫
    spider = Spider(max_retries=3, max_concurrent_tasks=5)
    await spider.run()

if __name__ == "__main__":
    asyncio.run(main())

