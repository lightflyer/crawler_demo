import asyncio
import copy
from itertools import product
from pathlib import Path
import re
from sqlalchemy import Engine
from sqlmodel import SQLModel, Field, JSON, Session, create_engine, select
from enum import Enum
from dataclasses import dataclass
from datetime import datetime
from httpx import URL, Response, AsyncClient
import json
import logging
import inspect
from sqlalchemy.exc import IntegrityError
from typing import Any, AsyncGenerator, Dict, Tuple, Type, List
logger = logging.getLogger(__name__)

NAMESPACE = {
    "男性": "men",
    "女性": "women",
    "中性": "neutral",
    "鞋类": "shoes",
    "服饰": "clothes",
    "配件": "accessories",
}

BASE_DIR = Path(__file__).parent
IMAGE_DIR = BASE_DIR.joinpath("images")


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


class ImageType(Enum):
    MAIN = "主图"
    SIDE = "sku图"
    DETAIL = "详情图"
    OTHER = "其他"

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
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    callback: str
    request_data: str = Field(default='{}', sa_type=JSON)
    parse_params: str = Field(default='{}', sa_type=JSON)
    method: RequestType
    exception: str | None = Field(default=None)
    parent_id: int | None = Field(default=None)
    children_id: List[int] = Field(default=[], sa_type=JSON)

class Image(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    url: str = Field(unique=True, index=True)
    image_type: ImageType
    save_path: str = Field(unique=True, index=True)
    image_name: str
    goods_id: str
    attr_alias: str
    created_at: datetime = Field(default=datetime.now())

class AntaGoods(SQLModel, table=True):
    id: int = Field(default=None, primary_key=True)
    alias_id: str
    market_price: float
    price: float
    name: str
    info: str
    description: str
    content: str
    title: str
    url: str
    mobile_url: str
    cate_id: str
    brand_id: str
    children_info: List[Dict[str, Any]] = Field(default=[], sa_type=JSON)
    created_at: datetime = Field(default=datetime.now())
    

def check_dirs() -> None:
    dir_names = list(NAMESPACE.values())
    dir_list = product(dir_names[:3], dir_names[3:], ["main", "detail"])
    for dir in dir_list:
        dir = IMAGE_DIR.joinpath(*dir)
        dir.mkdir(parents=True, exist_ok=True)


class TaskManager:
    def __init__(self, engine: Engine):
        self.engine = engine

    def add_task(self, task: CrawlTask) -> int:
        with Session(self.engine) as session:
            session.add(task)
            session.commit()
            return task.id
        
    def update_task(self, task: CrawlTask) -> int:
        with Session(self.engine) as session:
            # 先获取数据库中的任务
            db_task = session.get(CrawlTask, task.id)
            if not db_task:
                return None
            
            # 更新任务状态
            for key, value in task.model_dump(exclude={'id'}).items():
                setattr(db_task, key, value)
            db_task.updated_at = datetime.now()
            
            session.add(db_task)
            session.commit()
            return db_task.id
    
    def update_task_status(self, task_id: int, status: TaskStatus, exception: str = None) -> int:
        """更新任务状态的便捷方法"""
        with Session(self.engine) as session:
            db_task = session.get(CrawlTask, task_id)
            if not db_task:
                return None
            
            db_task.status = status
            db_task.updated_at = datetime.now()
            if exception:
                db_task.exception = exception
            
            session.add(db_task)
            session.commit()
            return db_task.id
    
    def get_pending_task(self) -> list[CrawlTask]:
        with Session(self.engine) as session:
            statement = select(CrawlTask).where(
                CrawlTask.status == TaskStatus.PENDING
            ).limit(5)
            
            tasks = session.exec(statement).all()
            
            # 在返回之前，将任务对象转换为字典
            task_dicts = [task.model_dump() for task in tasks]
            
            # 更新状态
            for task in tasks:
                task.status = TaskStatus.RUNNING
                task.updated_at = datetime.now()
                session.add(task)
            
            session.commit()
            
            # 返回任务字典列表
            return task_dicts

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

        if not hasattr(self, task.callback):
            callback = None
        else:
            callback = getattr(self, task.callback)

        return Request(
            url=task.url,
            method=task.method,
            request_data=json.loads(task.request_data),
            parse_params=json.loads(task.parse_params),
            callback=callback,
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
    async def pipeline(self, result: Type[SQLModel]) -> None:
        raise NotImplementedError("Subclass must implement pipeline method")
    
    def start_request(self) -> list[Request]:
        raise NotImplementedError("Subclass must implement start method")
    
    async def run(self) -> None:
        raise NotImplementedError("Subclass must implement run method")
            
class Spider(SpiderMixin):
    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self.task_manager = TaskManager(engine)

    async def crawl(self, task_dict: dict) -> None:
        try:
            # TODO: 取消session
            with Session(self.engine) as session:
                # 从字典创建新的任务对象
                task = CrawlTask(**task_dict)
                
                # 更新任务状态为运行中
                self.task_manager.update_task_status(task.id, TaskStatus.RUNNING)
                
                request = self.task2request(task)
                response = await self.download_flow(request)
                children_task_ids = []
                
                if request.callback:
                    result = request.callback(response, **request.parse_params)
                    if inspect.isasyncgen(result):
                        async for item in result:
                            if isinstance(item, Request):
                                new_task = self.request2task(item)
                                new_task.parent_id = task.id
                                task_id = self.task_manager.add_task(new_task)
                                children_task_ids.append(task_id)
                            elif isinstance(item, SQLModel):
                                await self.pipeline(item)
                    else:
                        logger.error(f"Invalid callback result: {result}")
                
                task.children_id = children_task_ids
                task.status = TaskStatus.COMPLETED
                # 更新任务状态为完成
                self.task_manager.update_task(task)
                
        except Exception as e:
            logger.error(f"Error in crawl: {e}")
            # 更新任务状态为失败
            self.task_manager.update_task_status(
                task_dict['id'],  # 使用字典中的ID
                TaskStatus.FAILED,
                exception=str(e)
            )

    async def run(self) -> None:
        # 获取初始请求
        async for request in self.start_request():
            task = self.request2task(request)
            self.task_manager.add_task(task)

        terminate_time = 60
        while True:
            # 获取待处理任务
            task_dicts = self.task_manager.get_pending_task()
            if not task_dicts:
                logger.info("No pending task found")
                terminate_time -= 1
            else:
                terminate_time = 60
                await asyncio.gather(*[self.crawl(task_dict) for task_dict in task_dicts])
            
            await asyncio.sleep(1)
            if terminate_time <= 0:
                break


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

    def get_or_create_image(self, url: str, image_type: ImageType, save_path: str) -> Image:
        instance, _ = self.get_or_create(
            Image,
            url=url,
            image_type=image_type,
            save_path=save_path
        )
        return instance
    
    def add_goods(self, goods: AntaGoods) -> int:
        try:
            with Session(self.engine) as session:
                session.add(goods)
                session.commit()
                return goods.id

        except IntegrityError as e:
            self.session.rollback()
            logger.error(f"IntegrityError while adding goods: {e}")
            raise
        except Exception as e:
            self.session.rollback() 
            logger.error(f"Error while adding goods: {e}")
            raise
    
    def add_images(self, images: List[Image]) -> Tuple[List[int], List[Image]]:
        """
        批量添加图片数据到数据库，如果批量添加失败则逐个添加
        
        Args:
            images: 图片对象列表
            
        Returns:
            Tuple[List[int], List[Image]]: (成功添加的图片ID列表, 添加失败的图片对象列表)
        """
        success_ids = []
        failed_images = []
        
        try:
            # 首先尝试批量添加
            with Session(self.engine) as session:
                session.add_all(images)
                session.commit()
                success_ids = [image.id for image in images]
                return success_ids, failed_images
                
        except Exception as e:
            logger.warning(f"Batch insert failed, trying individual inserts: {e}")
            session.rollback()
            
            # 批量添加失败，改为逐个添加
            for image in images:
                try:
                    with Session(self.engine) as session:
                        session.add(image)
                        session.commit()
                        success_ids.append(image.id)
                except Exception as e:
                    logger.error(f"Failed to add image {image.url}: {e}")
                    failed_images.append(image)
                    session.rollback()
            
            return success_ids, failed_images
    def add_image_to_goods(self, goods: AntaGoods, image: Image) -> None:
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
    def __init__(self, engine: Engine) -> None:
        super().__init__(engine)
        self.data_manager = DataManager(engine)

    async def pipeline(self, result: Type[SQLModel]) -> None:
        try:
            self.data_manager.add_goods(result)
        except Exception as e:
            logger.error(f"Error while adding goods: {e}")
            raise e
        
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
            request = self.task2request(new_task)
            response = await self.download_flow(request)
            if response.status_code == 200:
                with open(image.save_path, "wb") as f:
                    f.write(response.content)
                new_task.status = TaskStatus.COMPLETED
                return True
            else:
                return False
        except Exception as e:
            logger.error(f"Error downloading image: {e}")
            new_task.status = TaskStatus.FAILED
            new_task.parse_params = image.model_dump()
            new_task.exception = str(e)
            return False
        finally:
            self.task_manager.add_task(new_task)

    async def parse_goods_list(self, response: Response, goods_cate: List[str]) -> AsyncGenerator[Request | Type[SQLModel], Any]:
        data = response.json()
        for item in data.get("data", []):
            info = item.get("info", {})
            params = copy.deepcopy(item)
            params["goods_cate"] = goods_cate
            url = URL(response.url).join(info.get("url"))
            yield Request(
                url=str(url),
                method=RequestType.GET,
                request_data={},
                callback=self.parse_goods_detail,
                parse_params=params
            )

    async def parse_goods_detail(self, response: Response, **kwargs) -> AsyncGenerator[Type[SQLModel], Any]:
        goods_cate = kwargs.get("goods_cate", [])
        script_match = re.search(r'var\s+proData\s*=\s*({.*?});(?=\s*var|</script>)', response.text, re.DOTALL)
        if script_match:
            script_data = script_match.group(1)
            try:
                cleaned_data = clean_json_string(script_data)
                product_data = json.loads(cleaned_data)
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析错误: {e}, response url: {response.url}")
                # 保存清理后的文本以便调试
                with open(f"cleaned_data_{response.url}.txt", "w", encoding="utf-8") as debug_file:
                    debug_file.write(cleaned_data)
                raise e
            
            product_keyword = product_data.get("keyword", "") or product_data.get("pro_title", "") or kwargs.get("pro_name", "")

            goods = AntaGoods(
                id=product_data.get("id_goods", "") or kwargs.get("id_goods", ""),
                alias_id=product_data.get("id_alias", "") or kwargs.get("id_alias", ""),
                market_price=kwargs.get("market_price", ""),
                price=product_data.get("price", "") or kwargs.get("price", ""),
                name=product_data.get("pro_name", "") or kwargs.get("pro_name", ""),
                info=product_data.get("pro_info", "") or kwargs.get("pro_info", ""),
                description=product_data.get("pro_intro", "") or kwargs.get("pro_intro", ""),
                content=product_data.get("pro_content", "") or kwargs.get("pro_content", ""),
                title=product_data.get("pro_title", "") or kwargs.get("pro_title", ""),
                url=kwargs.get("url", ""),
                mobile_url=kwargs.get("murl", ""),
                cate_id=product_data.get("cate_id", "") or kwargs.get("cate_id", ""),
                brand_id=product_data.get("id_brand", ""),
                children_info=product_data.get("sku_info", []),
            )
            colors = {}
            for color_id, color_info in product_data.get("color", {}).items():
                color_id = color_info.get("id_pa", "")
                if color_id:
                    colors[color_id] = {
                        "attr_name": color_info.get("attr_name", ""),
                        "attr_alias": color_info.get("attr_alias", ""),
                        "order_id": color_info.get("order_id", ""),
                    }
            image_data = product_data.get("image", {})
            
            images = []

            for item in image_data.get("bd", []):
                image = Image()
                image.url = item.get("path", "")
                image.image_type = ImageType.DETAIL
                image.goods_id = item.get("id_goods", "")
                image_suffix = image.url.split(".")[-1]
                image.attr_alias = "-".join([*goods_cate, product_keyword])
                image_name = f"{image.attr_alias}-{ImageType.DETAIL}_{item.get('order_id', '')}.{image_suffix}"
                image.image_name = image_name
                if len(goods_cate) >= 2:
                    image.save_path = IMAGE_DIR.joinpath(NAMESPACE.get(goods_cate[0], ""), NAMESPACE.get(goods_cate[1], ""), "detail", image.image_name)
                else:
                    image.save_path = IMAGE_DIR.joinpath("detail", image.image_name)
                images.append(image)

            main_images = image_data.get("master", {})
            for color_id, color_info in colors.items():
                for _, main_image_item in main_images.get(color_id, {}).items():
                    image = Image()
                    image.url = main_image_item.get("path", "")
                    image.image_type = ImageType.MAIN
                    image.goods_id = main_image_item.get("id_goods", "")
                    image.attr_alias = "-".join([*goods_cate, product_keyword, color_info.get("attr_name", "")])
                    image_suffix = image.url.split(".")[-1]
                    image_name = f"{image.attr_alias}-{ImageType.MAIN}_{item.get('order_id', '')}.{image_suffix}"
                    image.image_name = image_name
                    if len(goods_cate) >= 2:
                        image.save_path = IMAGE_DIR.joinpath(NAMESPACE.get(goods_cate[0], ""), NAMESPACE.get(goods_cate[1], ""), "main", image.image_name)
                    else:
                        image.save_path = IMAGE_DIR.joinpath("main", image.image_name)
                    images.append(image)
            
            results = await asyncio.gather(*[self.download_image(image) for image in images])
            successful_images = []
            failed_images = []
            
            for image, result in zip(images, results):
                if result:
                    successful_images.append(image)
                else:
                    failed_images.append(image)
                    logger.error(f"Failed to download image: {image.url}, goods_id: {image.goods_id}")
            
            # Save successful image IDs to goods object
            goods.image_ids = [image.goods_id for image in successful_images]
            
            # Print failed downloads
            if failed_images:
                print(f"Failed to download {len(failed_images)} images:")
                for image in failed_images:
                    print(f"- Image URL: {image.url}")
                    print(f"  Goods ID: {image.goods_id}")
                    print(f"  Image type: {image.image_type}")
            image_ids = self.data_manager.add_images(successful_images)
            goods.image_ids = image_ids
            yield goods

    async def parse_primary_categories(self, response: Response) -> AsyncGenerator[Type[SQLModel], Any]:
        data = response.json()
        primary_categories = []
        sex_categories = []
        for item in data.get("para", {}).get("newbar", []):
            if item.get("code") == "j":
                sex_categories.extend(item.get("_child", {}).values())
            if item.get("code") == "e":
                primary_categories.extend(item.get("_child", {}).values())
                
                yield Request()
        categories = product(sex_categories, primary_categories)
        for sex_category, primary_category in categories:
            cate_code = f"j{sex_category.get("code")}-e{primary_category.get("code")}"
            goods_cate = [sex_category.get("name"), primary_category.get("name")]
            url = URL(response.url).join(f"/{cate_code}")
            yield Request(
                url=str(url),
                method=RequestType.POST,
                request_data={},
                callback=self.parse_secondary_categories,
                parse_params={"goods_cate": goods_cate}
            )
            break
        
    async def parse_secondary_categories(self, response: Response, goods_cate: List[str]) -> AsyncGenerator[Type[SQLModel], Any]:
        data = response.json()
        secondary_categories = []
        for item in data.get("data", {}).get("newbar", []):
            if item.get("code") == "f":
                secondary_categories.extend(item.get("_child", {}).values())
        for secondary_category in secondary_categories:
            second_cate_code = f"f{secondary_category.get("code")}"
            goods_cate.append(secondary_category.get("name"))
            url = f"{str(response.url)}-{second_cate_code}"
            yield Request(
                url=url,
                method=RequestType.POST,
                request_data={"content": "p=1"},
                callback=self.parse_goods_list,
                parse_params={"goods_cate": goods_cate}
            )
            break

    async def start_request(self) -> AsyncGenerator[Request, Any]:
        # url = "https://www.anta.cn/list"
        # yield Request(
        #     url=url,
        #     method=RequestType.POST,
        #     request_data={},
        #     callback=self.parse_primary_categories,
        #     parse_params={}
        # )
        url = "https://www.anta.cn/goods-314868.html"
        yield Request(
            url=url,
            method=RequestType.GET,
            request_data={},
            callback=self.parse_goods_detail,
            parse_params={"goods_cate": ["男性", "鞋类", "篮球鞋"]}
        )

def clean_json_string(json_str: str) -> str:
    """清理JSON字符串中的无效字符和转义序列"""
    # 移除注释
    json_str = re.sub(r'//.*?\n|/\*.*?\*/', '', json_str, flags=re.S)
    
    # 处理正则表达式
    json_str = re.sub(r':\s*/.*?/[gim]*(?=,|\s*})', ': ""', json_str)
    
    # 处理特殊字符
    json_str = json_str.replace('\n', '\\n').replace('\r', '\\r')
    json_str = json_str.replace('\t', '\\t')
    
    # 处理未闭合的引号
    json_str = re.sub(r'([^\\])"([^"]*$)', r'\1"\2"', json_str)
    
    # 处理JavaScript undefined
    json_str = re.sub(r':\s*undefined\s*([,}])', r': null\1', json_str)
    
    # 处理单引号
    json_str = json_str.replace("'", "\\'")
    
    # 处理多余的逗号
    json_str = re.sub(r',\s*([\]}])', r'\1', json_str)
    
    # 尝试找到第一个完整的JSON对象
    try:
        start_brace = json_str.find('{')
        if start_brace == -1:
            return json_str
            
        stack = []
        for i, char in enumerate(json_str[start_brace:], start=start_brace):
            if char == '{':
                stack.append(char)
            elif char == '}':
                stack.pop()
                if not stack:  # 找到匹配的结束括号
                    return json_str[start_brace:i+1]
    except:
        pass
        
    return json_str

def init_db(db_url: str) -> Engine:
    engine = create_engine(db_url)
    SQLModel.metadata.create_all(engine)
    return engine

def main():
    check_dirs()
    engine = init_db("sqlite:///anta_goods.db")
    spider = GoodsImageSpider(engine)
    asyncio.run(spider.run())

if __name__ == "__main__":
    # check_dirs()
    main()