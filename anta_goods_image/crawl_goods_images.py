import asyncio
from sqlalchemy import Engine
from sqlmodel import SQLModel, Field, JSON, Session, create_engine, sessionmaker
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from httpx import Response, AsyncClient
import json
import logging
import inspect
from sqlalchemy.exc import IntegrityError
from typing import Optional, Tuple

logger = logging.getLogger(__name__)



# 定义请求类型
class RequestType(str, Enum):
    GET = "get"
    POST = "post"
    PUT = "put"
    DELETE = "delete"
    HEAD = "head"
    OPTIONS = "options"
    PATCH = "patch"

# 定义任务状态
class TaskStatus(str, Enum):
    PENDING = "pending" # 待执行
    RUNNING = "running" # 执行中
    COMPLETED = "completed" # 完成
    FAILED = "failed" # 失败

@dataclass
class Request:
    url: str
    method: RequestType
    request_data: dict
    callback: callable
    parse_params: dict

class CrawlTask(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    url: str
    status: TaskStatus = Field(default=TaskStatus.PENDING)
    created_at: datetime = Field(default=datetime.now())
    updated_at: datetime = Field(default=datetime.now())
    callback: str
    request_data: str = Field(default='{}', sa_type=JSON)
    parse_params: str = Field(default='{}', sa_type=JSON)
    method: RequestType
    exception: str = Field(default=None)
    parent_id: int = Field(default=None)
    children_id: list[int] = Field(default=[])

class TaskManager:
    def __init__(self, db_url: str):
        self.engine = create_engine(db_url)
        self.session = sessionmaker(bind=self.engine)

    def add_task(self, task: CrawlTask) -> int:
        with self.session() as session:
            session.add(task)
            task_id = session.commit()
            return task_id
        
    def update_task(self, task: CrawlTask) -> int:
        with self.session() as session:
            task.updated_at = datetime.now()
            session.query(CrawlTask).filter(CrawlTask.id == task.id).update(task.model_dump())
            task_id = session.commit()
            return task_id
    
    def get_pending_task(self) -> list[CrawlTask]:
        with self.session() as session:
            tasks = session.query(CrawlTask).filter(
                CrawlTask.status == TaskStatus.PENDING
            ).limit(5).all()
            
            # Update status to RUNNING for fetched tasks
            for task in tasks:
                task.status = TaskStatus.RUNNING
                task.updated_at = datetime.now()
            
            session.commit()
            return tasks
    


class SpiderMixin:
    
    def request2task(self, request: Request) -> CrawlTask:
        return CrawlTask(
            url=request.url,
            method=request.method,
            request_data=json.dumps(request.request_data),
            parse_params=json.dumps(request.parse_params),
            callback=request.callback.__name__,
        )

    def task2request(self, task: CrawlTask) -> Request:
        return Request(
            url=task.url,
            method=task.method,
            request_data=json.loads(task.request_data),
            parse_params=json.loads(task.parse_params),
            callback=getattr(self, task.callback),
        )
    
    async def download_flow(self, request: Request) -> Response:
        try:
            result = await self.request_middleware(request)
            if isinstance(result, Request):
                return await self.request(result)
            response = await self.response_middleware(result)
            return response
        except Exception as e:
            return Response(status_code=500, text=str(e))

    async def request_middleware(self, request: Request) -> Response | Request:
        return request
    
    async def response_middleware(self, response: Response) -> Response:
        return response
    
    async def request(self, request: Request) -> Response:
        with AsyncClient() as client:
            response = await client.request(request.method, request.url, **request.request_data)
            return response
        
    async def crawl(self, task: CrawlTask) -> None:
        raise NotImplementedError("Subclass must implement crawl method")
    async def pipeline(self, result: SQLModel) -> None:
        raise NotImplementedError("Subclass must implement pipeline method")
    
    def start_request(self) -> list[Request]:
        raise NotImplementedError("Subclass must implement start method")
    
    async def run(self) -> None:
        raise NotImplementedError("Subclass must implement run method")
            

class Spider(SpiderMixin):
    def __init__(self, task_manager: TaskManager) -> None:
        self.task_manager = task_manager

    async def crawl(self, task: CrawlTask) -> None:
        try:
            task.status = TaskStatus.RUNNING
            self.task_manager.update_task(task)
            
            request = self.task2request(task)
            response = await self.download_flow(request)
            children_task_ids = []
            result = request.callback(response, **request.parse_params)
            if inspect.isasyncgen(result):
                async for item in result:
                    if isinstance(item, Request):
                        new_task = self.request2task(item)
                        new_task.parent_id = task.id
                        task_id = self.task_manager.add_task(new_task)
                        children_task_ids.append(task_id)
                    elif isinstance(result, SQLModel):
                        await self.pipeline(result)
            else:
                logger.error(f"Invalid callback result: {result}")
            task.status = TaskStatus.FINISHED
            task.children_task_ids = children_task_ids
            self.task_manager.update_task(task)
        except Exception as e:
            logger.error(f"Error in crawl: {e}")
            task.status = TaskStatus.FAILED
            task.exception = str(e)
            task_id = self.task_manager.update_task(task.id, status=TaskStatus.FAILED)

    async def run(self) -> None:
        requests = self.start_request()
        for request in requests:
            task = self.request2task(request)
            self.task_manager.add_task(task)
        terminate_time = 60
        while True:

            tasks = self.task_manager.get_pending_task()
            if not tasks:
                logger.info("No pending task found")
                terminate_time -= 1
            else:
                terminate_time = 60
                await asyncio.gather(*[self.crawl(task) for task in tasks])
            await asyncio.sleep(1)
            if terminate_time <= 0:
                break

class Size(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    size: str = Field(unique=True, index=True)
    created_at: datetime = Field(default=datetime.now())

class Brand(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    brand: str = Field(unique=True, index=True)
    created_at: datetime = Field(default=datetime.now())

class Color(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    color: str = Field(unique=True, index=True)
    created_at: datetime = Field(default=datetime.now())

class Category(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    category: str
    parent_id: int = Field(default=None)
    children_id: list[int] = Field(default=[])
    created_at: datetime = Field(default=datetime.now())

class ImageType(Enum):
    MAIN = "main"
    SIDE = "side"
    DETAIL = "detail"

class Image(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    url: str = Field(unique=True, index=True)
    image_type: ImageType
    save_path: str = Field(unique=True, index=True)
    created_at: datetime = Field(default=datetime.now())

class SKU(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    color_id: int = Field(foreign_key="color.id")
    size_id: int = Field(foreign_key="size.id")
    goods_id: int = Field(foreign_key="goods.id")
    price: float = Field(default=0.0)
    stock: int = Field(default=0)
    created_at: datetime = Field(default=datetime.now())
    
    class Config:
        table = True
        schema_extra = {
            "sa_column_args": {
                "unique_together": ["color_id", "size_id", "goods_id"]
            }
        }

class Goods(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    title: str
    created_at: datetime = Field(default=datetime.now())
    brand_id: int = Field(foreign_key="brand.id")
    category_id: int = Field(foreign_key="category.id")
    sku_ids: list[int] = Field(default=[])
    image_ids: list[int] = Field(default=[])

class DataManager:
    def __init__(self, engine: Engine):
        self.engine = engine

    def get_or_create(self, model: SQLModel, **kwargs) -> Tuple[SQLModel, bool]:
        """
        获取或创建记录，返回 (实例, 是否新创建)
        """
        with Session(self.engine) as session:
            instance = self.session.exec(model).filter_by(**kwargs).first()
            if instance:
                return instance, False
        
            instance = model(**kwargs)
            try:
                session.add(instance)
                session.commit()
                return instance, True
            except IntegrityError:
                self.session.rollback()
                return session.exec(model).filter_by(**kwargs).first(), False

    def get_or_create_size(self, size: str) -> Size:
        instance, _ = self.get_or_create(Size, size=size)
        return instance

    def get_or_create_brand(self, brand: str) -> Brand:
        instance, _ = self.get_or_create(Brand, brand=brand)
        return instance

    def get_or_create_color(self, color: str) -> Color:
        instance, _ = self.get_or_create(Color, color=color)
        return instance

    def get_or_create_image(self, url: str, image_type: ImageType, save_path: str) -> Image:
        instance, _ = self.get_or_create(
            Image,
            url=url,
            image_type=image_type,
            save_path=save_path
        )
        return instance

    def get_or_create_sku(
            self, 
            color_id: int, 
            size_id: int, 
            price: float=None, 
            stock: int=None
        ) -> SKU:
        with Session(self.engine) as session:
            instance = session.exec(SKU).filter_by(
                color_id=color_id,
                size_id=size_id
            ).first()
        
        if instance:
            # 更新价格和库存
            instance.price = price
            instance.stock = stock
            self.session.commit()
            return instance
        
        instance = SKU(
            color_id=color_id,
            size_id=size_id,
            price=price,
            stock=stock
        )
        self.session.add(instance)
        self.session.commit()
        return instance

    def get_or_create_goods(self, title: str, brand_id: int, category_id: int) -> Goods:
        instance = self.session.query(Goods).filter_by(title=title).first()
        if instance:
            return instance
        
        instance = Goods(
            title=title,
            brand_id=brand_id,
            category_id=category_id
        )
        self.session.add(instance)
        self.session.commit()
        return instance

    def add_sku_to_goods(self, goods: Goods, sku: SKU) -> None:
        if sku.id not in goods.sku_ids:
            goods.sku_ids.append(sku.id)
            self.session.commit()

    def add_image_to_goods(self, goods: Goods, image: Image) -> None:
        if image.id not in goods.image_ids:
            goods.image_ids.append(image.id)
            self.session.commit()

class GoodsImagePipeline:
    def __init__(self, session: Session) -> None:
        self.session = session

    def save_image(self, image: Image) -> None:
        self.session.add(image)
        self.session.commit()

class GoodsImageSpider(Spider):
    def __init__(self, db_url: str) -> None:
        super().__init__(TaskManager(db_url))   

    async def pipeline(self, result: SQLModel) -> None:
        print(result)

    async def download_image(self, image: Image) -> bool:
        try:
            new_task = CrawlTask(
                url=image.url,
                method=RequestType.GET,
                request_data={},
                callback=None,
                parse_params={}
            )
            new_task.status = TaskStatus.RUNNING
            response = await self.download_flow(image.url)
            if response.status_code == 200:
                with open(image.save_path, "wb") as f:
                    f.write(response.content)
                return True
            else:
                return False
            
            
        except Exception as e:
            logger.error(f"Error downloading image: {e}")
            return False
            

if __name__ == "__main__":
    pass