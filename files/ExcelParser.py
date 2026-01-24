import pandas as pd
from io import BytesIO
from typing import List, Dict, Any

class ExcelParser:
    """
    Excel 解析器：将每一行视为一个独立的语义单元
    """
    def parse(self, stream: BytesIO) -> List[Dict[str, Any]]:
        # 1. 读取 Excel
        # 注意：如果 Excel 很大，可以使用 usecols 参数只读取需要的列
        df = pd.read_excel(stream)
        
        # 2. 核心修改：处理不可序列化的类型
        # 处理日期时间类型：转换为 ISO 格式的字符串
        for col in df.select_dtypes(include=['datetime', 'datetimetz']).columns:
            # 使用 isoformat() 或者 strftime，推荐 isoformat 保持标准
            df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
        # 3. 处理空值
        # 注意：fillna("") 要在日期转换后执行，否则 NaN 会干扰日期处理
        df = df.fillna("")
        
        # 4. 将 DataFrame 转换为原生 Python 列表字典
        # orient="records" 生成 List[Dict]
        rows = df.to_dict(orient="records")
        
        return rows