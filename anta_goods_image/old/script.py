async def get_goods_data(url: str) -> dict:
    # async with httpx.AsyncClient() as client:
    #     response = await client.get(url)
    #     text = response.text
    #     with open("goods-314868.html", "w", encoding="utf-8") as f:
    #         f.write(text)
    with open("goods-314868.html", "r", encoding="utf-8") as f:
        text = f.read()
    # 提取 proData
    script_match = re.search(r'var\s+proData\s*=\s*({.*?});(?=\s*var|</script>)', text, re.DOTALL)
    if script_match:
        script_data = script_match.group(1)
        # 清理并解析JSON
        try:
            cleaned_data = clean_json_string(script_data)
            product_data = json.loads(cleaned_data)
            # 保存成功解析的JSON以便检查
            with open("parsed_data.json", "w", encoding="utf-8") as f:
                json.dump(product_data, f, ensure_ascii=False, indent=2)
            return product_data
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {e}")
            # 保存清理后的文本以便调试
            with open("cleaned_data.txt", "w", encoding="utf-8") as debug_file:
                debug_file.write(cleaned_data)
            return None
        
def parse_goods_data(product_data: dict, **kwargs) -> AntaGoods:

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
    image_dir = Path(__file__).parent.joinpath("images")
    shoes_dir = image_dir.joinpath("shoes")
    clothes_dir = image_dir.joinpath("clothes")
    shoes_main_dir = shoes_dir.joinpath("main")
    shoes_detail_dir = shoes_dir.joinpath("detail")
    clothes_main_dir = clothes_dir.joinpath("main")
    clothes_detail_dir = clothes_dir.joinpath("detail")

    for item in image_data.get("bd", []):
        image = Image()
        image.url = item.get("path", "")
        image.image_type = ImageType.DETAIL
        image.goods_id = item.get("id_goods", "")
        image_suffix = image.url.split(".")[-1]
        image_name = f"{product_keyword}_{item.get('order_id', '')}.{image_suffix}"
        image.image_name = image_name
        # TODO 要在这里边加入成人、孩子、男女、品类的判断, 方便导航到正确的目录
        image.save_path = image_dir.joinpath(image.image_name)
        images.append(image)

    main_colors = image_data.get("master", {})
    for color_id, color_info in main_colors.items():
        for _, main_image_item in main_colors.get(color_id, {}).items():
            image = Image()
            image.url = main_image_item.get("path", "")
            image.image_type = ImageType.MAIN
            image.goods_id = main_image_item.get("id_goods", "")
            image.attr_alias = main_image_item.get("attr_alias", "")
            image_suffix = image.url.split(".")[-1]
            image_name = f"{product_keyword}_{image.attr_alias}_{item.get('order_id', '')}.{image_suffix}"
            image.image_name = image_name
            image.save_path = image_dir.joinpath(image.image_name)
            images.append(image)

    # await asyncio.gather(*[self.download_image(image) for image in images])

    result = await asyncio.gather([self.download_image(image) for image in images])
    print(result)
    
    return goods

if __name__ == "__main__":
    asyncio.run(get_goods_data("https://www.anta.cn/goods-314868.html"))