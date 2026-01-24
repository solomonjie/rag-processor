from rawclean.interface import BaseCleaner


class ExcelCleaner(BaseCleaner):
    def clean(self, raw_data: list) -> str:
        """假设 Excel Parser 返回的是 List[List] 结构"""
        rows = []
        for row in raw_data:
            # 过滤掉全空的行，并将单元格转为字符串
            clean_row = [str(cell).strip() for cell in row if cell is not None]
            if clean_row:
                rows.append(" | ".join(clean_row)) # 使用竖线分隔单元格
        return "\n".join(rows)