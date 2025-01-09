import httpx 
import json
def compare_list_element(data_list: list) -> list:
    """
    比较列表中的元素值是不是都是一样的，列表元素包括list和dict
    找出不一样的值并记录
    """
    for element in data_list:
        if isinstance(element, list):
            if not all(element == data_list[0] for element in data_list):
                return False
        elif isinstance(element, dict):
            if not all(element == data_list[0] for element in data_list):
                return False
    return True



def main():
    

    url_child_shoes = "https://www.anta.cn/list/e1-k4"
    url_all = "https://www.anta.cn/list/"
    url_man = "https://www.anta.cn/list/j1"
    url_man_shoes = "https://www.anta.cn/list/j1-e1"
    url_man_cotton_shoes = "https://www.anta.cn/list/j1-e1-f26"

    payload = "p=1"
    headers = {
        'Content-Type': 'text/plain',
        'Cookie': 'acw_tc=ac11000117363872754984679e003b32251bf9a83725dfb4d7af787304e3c3'
    }
    with httpx.Client() as client:
        child_shoes_response = client.post(url_child_shoes, headers=headers, data=payload)
        child_shoes_new_bar = child_shoes_response.json().get("para", {}).get("newbar", [])
        all_response = client.post(url_all, headers=headers, data=payload)
        all_new_bar = all_response.json().get("para", {}).get("newbar", [])
        man_response = client.post(url_man, headers=headers, data=payload)
        man_new_bar = man_response.json().get("para", {}).get("newbar", [])
        man_shoes_response = client.post(url_man_shoes, headers=headers, data=payload)
        man_shoes_new_bar = man_shoes_response.json().get("para", {}).get("newbar", [])
        man_cotton_shoes_response = client.post(url_man_cotton_shoes, headers=headers, data=payload)
        man_cotton_shoes_new_bar = man_cotton_shoes_response.json().get("para", {}).get("newbar", [])
        def analyze_new_bars(bars_dict):
            """分析多个new_bar的异同"""
            # 提取所有bar的keys用于比较结构
            all_keys = set()
            for bar_name, bar_data in bars_dict.items():
                for item in bar_data:
                    all_keys.add(json.dumps(item, sort_keys=True))
            
            # 找出共同元素
            common_elements = []
            different_elements = {}
            
            # 转换回Python对象
            all_keys = [json.loads(k) for k in all_keys]
            
            # 分析每个元素是否在所有new_bar中都存在
            for element in all_keys:
                element_str = json.dumps(element, sort_keys=True)
                exists_in_all = True
                missing_in = []
                
                for bar_name, bar_data in bars_dict.items():
                    if not any(json.dumps(item, sort_keys=True) == element_str for item in bar_data):
                        exists_in_all = False
                        missing_in.append(bar_name)
                
                if exists_in_all:
                    common_elements.append(element)
                else:
                    different_elements[json.dumps(element, ensure_ascii=False)] = {
                        "element": element,
                        "missing_in": missing_in
                    }
            
            print("\n=== 分析结果 ===")
            print("\n共同元素:")
            for elem in common_elements:
                print(json.dumps(elem, ensure_ascii=False, indent=2))
            
            print("\n不同元素:")
            for elem_info in different_elements.values():
                print("\n元素:")
                print(json.dumps(elem_info["element"], ensure_ascii=False, indent=2))
                print("缺失在:", ", ".join(elem_info["missing_in"]))
            
            return common_elements, different_elements

        # 创建包含所有new_bar的字典
        all_bars = {
            "child_shoes_new_bar": child_shoes_new_bar,
            "all_new_bar": all_new_bar,
            "man_new_bar": man_new_bar,
            "man_shoes_new_bar": man_shoes_new_bar,
            "man_cotton_shoes_new_bar": man_cotton_shoes_new_bar
        }
        
        # 分析异同
        common_elements, different_elements = analyze_new_bars(all_bars)
        
        with open("common_elements.json", "w") as f:
            json.dump(common_elements, f, ensure_ascii=False, indent=2)
        with open("different_elements.json", "w") as f:
            json.dump(different_elements, f, ensure_ascii=False, indent=2)
        
        print("common_elements.json 文件已创建，内容为：")
        print(common_elements)
        print("different_elements.json 文件已创建，内容为：")
        print(different_elements)

        # result = compare_list_element([child_shoes_new_bar, all_new_bar, man_new_bar, man_shoes_new_bar, man_cotton_shoes_new_bar])
        # if result:
        #     print("列表中的元素值都是一样的")
        #     with open("new_bar.json", "w") as f:
        #         json.dump(child_shoes_new_bar, f)
        #     print(f"new_bar.json 文件已创建，内容为：{child_shoes_new_bar}")
        # else:
        #     print("列表中的元素值不都是一样的")
        #     # 确保转码为中文
        #     with open("new_bar.json", "w", encoding="utf-8") as f:
        #         data = {
        #             "child_shoes_new_bar": child_shoes_new_bar,
        #             "all_new_bar": all_new_bar,
        #             "man_new_bar": man_new_bar,
        #             "man_shoes_new_bar": man_shoes_new_bar,
        #             "man_cotton_shoes_new_bar": man_cotton_shoes_new_bar
        #         }
        #         json.dump(data, f, ensure_ascii=False, indent=2)

        #     print(f"new_bar.json 文件已创建，内容为：{data}")

        


if __name__ == "__main__":
    main()
