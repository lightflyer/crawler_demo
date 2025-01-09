import requests

def search_anta_ai(search_text, skip=0, top=10, min_score=0.7):
    """
    向安踏AI搜索API发起请求
    
    Args:
        search_text (str): 搜索文本
        skip (int): 跳过的结果数量
        top (int): 返回的最大结果数量
        min_score (float): 最小相关度分数
    
    Returns:
        dict: API响应的JSON数据
    """
    url = "https://ai.anta.com/finance/api/search"
    
    # 设置请求头
    headers = {
        "api-key": "9bf5bd66b0174b158c0af60034cab547",
        "aip-application-id": "6bf88901-4611-c612-212e-3a13f6202953",
        "User-Agent": "Apifox/1.0.0 (https://apifox.com)",
        "Content-Type": "application/json"
    }
    
    # 构建请求体
    payload = {
        "filter": "",
        "minScore": min_score,
        "skip": skip,
        "search": [
            {
                "text": search_text
            }
        ],
        "top": top
    }
    
    try:
        # 发送POST请求
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            params={"api-version": "2023-11-01"}
        )
        
        # 确保请求成功
        response.raise_for_status()
        
        # 返回JSON响应
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"请求发生错误: {e}")
        return None


if __name__ == "__main__":
    print(search_anta_ai("请假"))
